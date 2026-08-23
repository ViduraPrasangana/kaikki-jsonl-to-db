"""Load a kaikki.org JSONL dictionary dump into SQLite or PostgreSQL.

    # SQLite (default)
    python converter.py

    # PostgreSQL, loading without indexes so they can be applied later
    python converter.py --backend postgres \
        --target "postgresql://user:pass@localhost:5432/kaikki" --no-indexes

Interrupted runs resume from the Progress bookmark, so re-running the same
command continues where it stopped.
"""

import argparse
import json
import time

from backends import get_backend, read_sql, SCHEMA_FILES, INDEX_FILES
from db import read_last_processed_line, write_progress, insert_word_def

DEFAULT_TARGETS = {
    'sqlite': 'wikitionary-kaikki-english.db',
    'postgres': 'postgresql://localhost:5432/kaikki',
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--backend', choices=('sqlite', 'postgres'), default='sqlite',
                        help='database engine to load into (default: sqlite)')
    parser.add_argument('--target', default=None,
                        help='SQLite file path, or PostgreSQL connection string')
    parser.add_argument('--input', default='kaikki.org-dictionary-English.jsonl',
                        help='kaikki JSONL dump to read')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='entries per transaction (default: 1000)')
    parser.add_argument('--log-every', type=int, default=10000,
                        help='lines between progress messages (default: 10000)')
    parser.add_argument('--failed-lines', default='failed_lines.txt',
                        help='file to append failed line numbers to')
    parser.add_argument('--no-copy', dest='use_copy', action='store_false',
                        help='postgres only: use INSERT per row instead of buffered COPY')
    parser.set_defaults(use_copy=True)
    indexes = parser.add_mutually_exclusive_group()
    indexes.add_argument('--indexes', dest='indexes', action='store_true',
                         help='apply the index file after loading')
    indexes.add_argument('--no-indexes', dest='indexes', action='store_false',
                         help='skip indexing, to apply the index file yourself later')
    parser.set_defaults(indexes=True)
    args = parser.parse_args()
    if args.target is None:
        args.target = DEFAULT_TARGETS[args.backend]
    return args


def report(line_number, processed, failed, window_lines, window_start, started, stats):
    """Print one progress line and return the start time of the next window."""
    now = time.time()
    window = max(now - window_start, 1e-9)
    overall = max(now - started, 1e-9)
    elapsed = time.strftime('%H:%M:%S', time.gmtime(overall))
    # COPY count is only meaningful on the postgres copy path.
    written = f"{stats['rows']:,} rows"
    if stats['copies']:
        written += f", {stats['copies']:,} COPY"
    print(f"[batch {stats['batches']:,}] {line_number:,} lines | "
          f"{processed:,} ok, {failed:,} failed | "
          f"{written} | "
          f"{window_lines / window:,.0f}/s (avg {processed / overall:,.0f}/s) | {elapsed}",
          flush=True)
    return now


def write_failed_line(file_path, line_number, error):
    with open(file_path, 'a', encoding='utf-8') as handle:
        handle.write(f"{line_number}\t{error}\n")


def main():
    args = parse_args()
    backend = get_backend(args.backend, args.target, use_copy=args.use_copy)
    conn = backend.connect()

    backend.run_script(conn, read_sql(SCHEMA_FILES[backend.name]))
    print(f"Schema applied from {SCHEMA_FILES[backend.name]}")

    cur = backend.cursor(conn)
    last_processed_line = read_last_processed_line(cur)
    cur.close()
    if last_processed_line:
        print(f"Resuming after line {last_processed_line}")

    processed = 0
    failed = 0
    line_number = last_processed_line
    started = time.time()
    window_start = started
    last_logged_line = last_processed_line
    # batches == commits; rows/copies are what actually reached the server.
    stats = {'batches': 0, 'rows': 0, 'copies': 0}

    with open(args.input, 'r', encoding='utf-8') as handle:
        batch = []
        for line_number, line in enumerate(handle, 1):
            if line_number <= last_processed_line:
                continue
            try:
                batch.append((json.loads(line), line_number))
            except Exception as error:
                failed += 1
                print(f"Failed to parse line {line_number}: {error}")
                write_failed_line(args.failed_lines, line_number, error)
                continue

            if len(batch) >= args.batch_size:
                processed, failed = insert_batch(backend, conn, batch, processed,
                                                 failed, args, stats)
                batch = []
                # Progress is reported per --log-every lines, independently of
                # how often we commit, so a small batch size does not flood the
                # log on a multi-million line dump.
                if line_number - last_logged_line >= args.log_every:
                    window_start = report(line_number, processed, failed,
                                          line_number - last_logged_line,
                                          window_start, started, stats)
                    last_logged_line = line_number

        if batch:
            processed, failed = insert_batch(backend, conn, batch, processed,
                                             failed, args, stats)
        if line_number > last_logged_line:
            report(line_number, processed, failed, line_number - last_logged_line,
                   window_start, started, stats)

    # Client-assigned ids bypass the identity sequences; move them past what
    # was used so later inserts do not collide.
    if backend.name == 'postgres' and args.use_copy:
        cur = backend.cursor(conn)
        cur.sync_sequences()
        conn.commit()

    if args.indexes:
        index_file = INDEX_FILES[backend.name]
        print(f"Applying {index_file} ...")
        backend.run_script(conn, read_sql(index_file))
        print("Indexes applied.")
    else:
        print(f"Skipping indexes. Apply {INDEX_FILES[backend.name]} when ready.")

    conn.close()
    print(f"Done. {processed:,} entries stored, {failed:,} failed, "
          f"{stats['rows']:,} rows in {stats['batches']:,} commits.")


def insert_batch(backend, conn, batch, processed, failed, args, stats):
    """Insert a batch in one transaction.

    Each entry runs inside its own SAVEPOINT so a bad row rolls back alone and
    the rest of the batch still commits. The batch is always consumed, never
    retried.
    """
    cur = backend.cursor(conn)
    for data, line_number in batch:
        try:
            cur.begin_entry()
            insert_word_def(cur, data, line_number)
            cur.end_entry()
            processed += 1
        except Exception as error:
            cur.abort_entry()
            failed += 1
            print(f"Failed to process line {line_number}: {error}")
            write_failed_line(args.failed_lines, line_number, error)
    cur.flush()
    write_progress(cur, batch[-1][1])
    conn.commit()
    stats['batches'] += 1
    stats['rows'] += cur.rows_written
    stats['copies'] += cur.copies
    cur.close()
    return processed, failed


if __name__ == '__main__':
    main()
