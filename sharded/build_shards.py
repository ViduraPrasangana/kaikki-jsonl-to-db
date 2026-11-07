"""Load a kaikki / wiktextract JSONL dump into N PostgreSQL shards, one
zstd-compressed row per line.

Every shard gets one row per entry whose shardkey.shard_for(word) is its index.
The line is stored byte for byte (lossless), compressed with a zstd dictionary
trained on a sample of the dump. The dictionary is stored in every shard.

    # fresh build: trains the dictionary, (re)creates the schema on every shard
    python build_shards.py --recreate \
        --input raw-wiktextract-data.jsonl \
        --shard-url postgresql://postgres:postgres@localhost:5432/wiktionary_s0 \
        --shard-url postgresql://postgres:postgres@localhost:5432/wiktionary_s1 \
        --shard-url postgresql://postgres:postgres@localhost:5432/wiktionary_s2

    # interrupted? continue from the last committed batch
    python build_shards.py --resume --input ... --shard-url ... (same URLs, same order)

The order of --shard-url is the shard index: the first URL is shard 0. The
shard count is the number of URLs. Without --shard-url, the URLs come from
SHARD_URLS (comma separated) or, failing that, shard_urls.txt.
"""

import argparse
import collections
import json
import os
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

import psycopg
import zstandard as zstd

import shardkey
from add_languages import pick_names, rebuild as rebuild_languages, write_languages
from shardclient import default_urls, masked

HERE = os.path.dirname(os.path.abspath(__file__))
HASH_SCHEME = 'fnv1a32(fold(word))'
COPY_SQL = ('COPY entry (id, word, word_key, lang_code, pos, data) '
            'FROM STDIN (FORMAT BINARY)')
COPY_TYPES = ['int4', 'text', 'text', 'text', 'text', 'bytea']


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--input', required=True, help='JSONL dump to load')
    ap.add_argument('--shard-url', action='append', default=[],
                    help='PostgreSQL URL of a shard; repeat once per shard, in index order')
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument('--recreate', action='store_true',
                      help='drop and recreate the tables on every shard, then load from line 1')
    mode.add_argument('--resume', action='store_true',
                      help='continue an interrupted build from its last committed line')
    ap.add_argument('--level', type=int, default=19, help='zstd level (default 19)')
    ap.add_argument('--dict-size', type=int, default=112_640, help='dictionary size in bytes')
    ap.add_argument('--dict-samples', type=int, default=20_000,
                    help='lines sampled from the dump to train the dictionary')
    ap.add_argument('--dict-file', help='use this existing dictionary instead of training one')
    ap.add_argument('--dict-out', help='also save the dictionary to this file')
    ap.add_argument('--workers', type=int, default=max(1, (os.cpu_count() or 2) - 1),
                    help='compression processes (default: CPUs - 1)')
    ap.add_argument('--batch-lines', type=int, default=50_000,
                    help='lines per commit; also the resume granularity')
    ap.add_argument('--limit', type=int, help='stop after this many lines (for test runs)')
    ap.add_argument('--no-indexes', action='store_true', help='skip indexes.sql at the end')
    ap.add_argument('--failed-lines', default='failed_lines.txt',
                    help='lines that are not valid JSON are logged here, not loaded')
    args = ap.parse_args()

    args.shard_url = args.shard_url or default_urls()
    if not args.shard_url:
        ap.error('no shard URLs: pass --shard-url, set SHARD_URLS, or fill in shard_urls.txt')
    return args


# ---------------------------------------------------------------------------
# Dictionary
# ---------------------------------------------------------------------------

def sample_lines(path, count, seed=42):
    """Random complete lines from anywhere in the file, without reading it all."""
    size = os.path.getsize(path)
    rng = random.Random(seed)
    lines = []
    with open(path, 'rb') as f:
        for offset in sorted(rng.randrange(size) for _ in range(count)):
            f.seek(offset)
            f.readline()                      # finish the partial line we landed in
            line = f.readline().rstrip(b'\r\n')
            if line:
                lines.append(line)
    return lines


def train_dictionary(args):
    print(f'Sampling {args.dict_samples:,} lines to train a {args.dict_size:,} byte dictionary...',
          flush=True)
    started = time.time()
    samples = sample_lines(args.input, args.dict_samples)
    d = zstd.train_dictionary(args.dict_size, samples, threads=-1)
    print(f'  dictionary {d.dict_id()} trained in {time.time() - started:.0f}s', flush=True)
    return d.as_bytes()


# ---------------------------------------------------------------------------
# Compression workers
# ---------------------------------------------------------------------------

_worker = {}


def init_worker(dict_bytes, level, shard_count):
    d = zstd.ZstdCompressionDict(dict_bytes)
    d.precompute_compress(level=level)
    _worker['c'] = zstd.ZstdCompressor(level=level, dict_data=d, write_checksum=False,
                                       write_content_size=True, write_dict_id=True)
    _worker['d'] = zstd.ZstdDecompressor(dict_data=d)
    _worker['n'] = shard_count


def clean(text):
    # PostgreSQL text cannot hold NUL. Only the indexed columns are cleaned;
    # `data` keeps the original bytes.
    return text.replace('\x00', '') if isinstance(text, str) else text


def process_chunk(chunk):
    """[(line_no, raw bytes)] -> ([(shard, row)], [(line_no, error)], raw_bytes, stored_bytes,
    Counter[(lang_code, lang name)])"""
    c, d, n = _worker['c'], _worker['d'], _worker['n']
    rows, failed, raw_bytes, stored_bytes = [], [], 0, 0
    languages = collections.Counter()
    for line_no, raw in chunk:
        try:
            obj = json.loads(raw)
        except ValueError as e:
            failed.append((line_no, str(e)))
            continue
        word = obj.get('word')
        if not isinstance(word, str):
            word = obj.get('title') if isinstance(obj.get('title'), str) else ''
        word = clean(word)
        key = shardkey.fold(word)
        pos = obj.get('pos')
        data = c.compress(raw)
        if d.decompress(data) != raw:          # cheap, and proves the row is lossless
            raise RuntimeError(f'line {line_no}: zstd round trip mismatch')
        shard = shardkey.fnv1a_32(key.encode('utf-8')) % n
        lang_code = clean(obj.get('lang_code') or '')
        rows.append((shard, (line_no, word, key, lang_code,
                             clean(pos) if isinstance(pos, str) else None, data)))
        if isinstance(obj.get('lang'), str) and obj['lang']:
            languages[(lang_code, obj['lang'])] += 1
        raw_bytes += len(raw)
        stored_bytes += len(data)
    return rows, failed, raw_bytes, stored_bytes, languages


# ---------------------------------------------------------------------------
# Shard setup
# ---------------------------------------------------------------------------

def run_sql_file(conn, filename):
    """Execute a .sql file one statement at a time (VACUUM refuses to run
    inside the implicit transaction of a multi-statement string)."""
    with open(os.path.join(HERE, filename), encoding='utf-8') as f:
        body = '\n'.join(l.split('--')[0] for l in f.read().splitlines())
    for statement in body.split(';'):
        if statement.strip():
            conn.execute(statement)


def recreate(conns, args):
    if args.dict_file:
        with open(args.dict_file, 'rb') as f:
            dict_bytes = f.read()
    else:
        dict_bytes = train_dictionary(args)
    dict_id = zstd.ZstdCompressionDict(dict_bytes).dict_id()
    build_id = time.strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:8]
    for index, conn in enumerate(conns):
        run_sql_file(conn, 'schema.sql')
        conn.execute('INSERT INTO zstd_dict (id, dict) VALUES (%s, %s)', (dict_id, dict_bytes))
        conn.execute("""INSERT INTO meta (shard_index, shard_count, build_id, hash_scheme,
                                          dict_id, source_file)
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                     (index, len(conns), build_id, HASH_SCHEME, dict_id,
                      os.path.basename(args.input)))
    print(f'Schema created on {len(conns)} shard(s), build {build_id}', flush=True)
    return dict_bytes, 0


def resume(conns):
    metas, dicts = [], []
    for index, conn in enumerate(conns):
        row = conn.execute("""SELECT m.shard_index, m.shard_count, m.build_id, m.last_line,
                                     m.complete, d.dict
                              FROM meta m JOIN zstd_dict d ON d.id = m.dict_id""").fetchone()
        if row is None:
            sys.exit(f'shard {index} has no meta row; start with --recreate')
        metas.append(row)
        dicts.append(bytes(row[5]))
    for index, (shard_index, shard_count, build_id, *_rest) in enumerate(metas):
        if shard_index != index or shard_count != len(conns) or build_id != metas[0][2]:
            sys.exit(f'URL #{index} holds shard {shard_index}/{shard_count} of build {build_id}; '
                     f'pass the same URLs in the same order as the original build')
    if any(d != dicts[0] for d in dicts):
        sys.exit('shards have different dictionaries; start again with --recreate')
    if all(m[4] for m in metas):
        sys.exit('this build is already complete')

    # Each shard commits its own part of a batch, so a crash can leave some
    # shards one batch ahead. Roll everyone back to the common bookmark.
    last_line = min(m[3] for m in metas)
    for conn in conns:
        conn.execute('DELETE FROM entry WHERE id > %s', (last_line,))
        conn.execute('UPDATE meta SET last_line = %s', (last_line,))
    print(f'Resuming build {metas[0][2]} after line {last_line:,}', flush=True)
    return dicts[0], last_line


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def read_chunks(args, start_line, chunk_lines=1000):
    """Yield [(line_no, raw)] chunks, skipping lines already committed."""
    chunk = []
    with open(args.input, 'rb') as f:
        for line_no, raw in enumerate(f, 1):
            if args.limit and line_no > args.limit:
                break
            if line_no <= start_line:
                continue
            raw = raw.rstrip(b'\r\n')
            if not raw:
                continue
            chunk.append((line_no, raw))
            if len(chunk) >= chunk_lines:
                yield chunk, line_no, f.tell()
                chunk = []
    if chunk:
        yield chunk, chunk[-1][0], None


def flush(conns, buffers, last_line):
    for conn, rows in zip(conns, buffers):
        with conn.transaction():
            with conn.cursor() as cur:
                if rows:
                    with cur.copy(COPY_SQL) as copy:
                        copy.set_types(COPY_TYPES)
                        for row in rows:
                            copy.write_row(row)
                cur.execute('UPDATE meta SET last_line = %s', (last_line,))
        rows.clear()


def load(conns, args, dict_bytes, start_line):
    n = len(conns)
    buffers = [[] for _ in range(n)]
    total_raw = total_stored = loaded = failed_count = 0
    file_size = os.path.getsize(args.input)
    started = last_report = time.time()
    pending_line = start_line
    next_flush = start_line + args.batch_lines
    languages = collections.Counter()

    with Pool(args.workers, init_worker, (dict_bytes, args.level, n)) as pool, \
            open(args.failed_lines, 'a', encoding='utf-8') as failed_log:
        # Bounded queue of in-flight chunks: Pool.imap would read the whole
        # 24 GB file ahead of the workers.
        in_flight = collections.deque()

        def drain_one():
            nonlocal total_raw, total_stored, loaded, failed_count, pending_line, next_flush
            result, chunk_last, offset = in_flight.popleft()
            rows, failed, raw_bytes, stored_bytes, chunk_languages = result.get()
            languages.update(chunk_languages)
            for shard, row in rows:
                buffers[shard].append(row)
            for line_no, error in failed:
                failed_log.write(f'{line_no}\t{error}\n')
            failed_count += len(failed)
            loaded += len(rows)
            total_raw += raw_bytes
            total_stored += stored_bytes
            pending_line = chunk_last
            if pending_line >= next_flush:
                flush(conns, buffers, pending_line)
                next_flush = pending_line + args.batch_lines
            return offset

        for chunk, chunk_last, offset in read_chunks(args, start_line):
            in_flight.append((pool.apply_async(process_chunk, (chunk,)), chunk_last, offset))
            if len(in_flight) >= args.workers * 4:
                done_offset = drain_one()
                now = time.time()
                if now - last_report >= 30 and done_offset:
                    rate = loaded / (now - started)
                    pct = done_offset / file_size
                    eta = (now - started) / pct * (1 - pct) if pct else 0
                    ratio = total_raw / total_stored if total_stored else 0
                    print(f'line {pending_line:,} | {pct:6.1%} | {loaded:,} rows '
                          f'({rate:,.0f}/s) | ratio {ratio:.2f}x | {failed_count} failed | '
                          f'ETA {eta / 60:.0f} min', flush=True)
                    last_report = now
        while in_flight:
            drain_one()
        flush(conns, buffers, pending_line)

    # A resumed run only saw part of the file, so count languages from the
    # stored rows instead.
    if start_line:
        rebuild_languages(args.shard_url)
    else:
        write_languages(conns, pick_names(languages))
    for conn in conns:
        conn.execute('UPDATE meta SET complete = true, last_line = %s', (pending_line,))
    elapsed = time.time() - started
    note = f' (see {args.failed_lines})' if failed_count else ''
    print(f'Loaded {loaded:,} rows in {elapsed / 60:.1f} min, {failed_count} failed{note}',
          flush=True)
    if total_stored:
        print(f'Payload: {total_raw / 1e9:.2f} GB raw -> {total_stored / 1e9:.2f} GB compressed '
              f'({total_raw / total_stored:.2f}x) this run', flush=True)


def build_indexes(urls):
    def one(url):
        with psycopg.connect(url, autocommit=True) as conn:
            run_sql_file(conn, 'indexes.sql')
    print('Building indexes on all shards in parallel...', flush=True)
    started = time.time()
    with ThreadPoolExecutor(len(urls)) as ex:
        list(ex.map(one, urls))
    print(f'  done in {(time.time() - started) / 60:.1f} min', flush=True)


def report_sizes(conns):
    print('\nshard | rows        | entry table | database')
    for index, conn in enumerate(conns):
        rows, table, db = conn.execute(
            """SELECT (SELECT count(*) FROM entry),
                      pg_size_pretty(pg_total_relation_size('entry')),
                      pg_size_pretty(pg_database_size(current_database()))""").fetchone()
        print(f'{index:5} | {rows:11,} | {table:>11} | {db:>8}')


def main():
    args = parse_args()
    # --recreate drops tables, so always show which databases are about to be used.
    for index, url in enumerate(args.shard_url):
        print(f'shard {index}: {masked(url)}', flush=True)
    conns =[psycopg.connect(url, autocommit=True) for url in args.shard_url]
    try:
        dict_bytes, start_line = recreate(conns, args) if args.recreate else resume(conns)
        if args.dict_out:
            with open(args.dict_out, 'wb') as f:
                f.write(dict_bytes)
        load(conns, args, dict_bytes, start_line)
        if not args.no_indexes:
            build_indexes(args.shard_url)
        report_sizes(conns)
    finally:
        for conn in conns:
            conn.close()


if __name__ == '__main__':
    main()
