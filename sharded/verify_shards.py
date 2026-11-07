"""Check a finished build before (and after) uploading it.

    python verify_shards.py --shard-url ... --shard-url ... --shard-url ...
    python verify_shards.py --source raw-wiktextract-data.jsonl   # also compare the row total

Checks:
  * every shard has the right index, the same shard count, build and dictionary
  * the build is marked complete
  * sampled rows decompress, parse, carry the right word, and live on the shard
    their word hashes to
  * per-shard size against the storage limit
  * with --source: total rows == non-empty source lines minus failed lines
"""

import argparse
import json
import sys

import shardkey
from shardclient import Shards, default_urls


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--shard-url', action='append', default=[])
    ap.add_argument('--sample', type=int, default=2000, help='rows to decode per shard')
    ap.add_argument('--limit-gb', type=float, default=5.0, help='storage limit per database')
    ap.add_argument('--source', help='JSONL file the build was made from')
    ap.add_argument('--failed-lines', default='failed_lines.txt')
    args = ap.parse_args()

    problems = []
    with Shards(args.shard_url or default_urls()) as shards:
        n = len(shards.conns)
        print(f'build {shards.metas[0][2]}, {n} shard(s), dictionary {shards.metas[0][3]}')
        total = 0
        for i, conn in enumerate(shards.conns):
            complete = shards.metas[i][4]
            rows, db_bytes = conn.execute(
                'SELECT count(*), pg_database_size(current_database()) FROM entry').fetchone()
            total += rows
            sample = conn.execute(
                'SELECT id, word, word_key, lang_code, data FROM entry TABLESAMPLE SYSTEM (1) '
                'LIMIT %s', (args.sample,)).fetchall()
            bad = 0
            for row_id, word, word_key, lang_code, data in sample:
                try:
                    obj = shards.decode(data)
                except Exception as e:                       # noqa: BLE001
                    problems.append(f'shard {i} row {row_id}: cannot decode: {e}')
                    bad += 1
                    continue
                if word_key != shardkey.fold(word):
                    problems.append(f'shard {i} row {row_id}: word_key {word_key!r} != fold({word!r})')
                    bad += 1
                if shardkey.shard_for(word, n) != i:
                    problems.append(f'shard {i} row {row_id}: {word!r} belongs on shard '
                                    f'{shardkey.shard_for(word, n)}')
                    bad += 1
                if isinstance(obj.get('word'), str) and obj['word'] != word:
                    problems.append(f'shard {i} row {row_id}: column word {word!r} != '
                                    f'JSON word {obj.get("word")!r}')
                    bad += 1
                if (obj.get('lang_code') or '') != lang_code:
                    problems.append(f'shard {i} row {row_id}: lang_code mismatch')
                    bad += 1
            gb = db_bytes / 1e9
            flag = '' if gb < args.limit_gb * 0.9 else '  <-- close to the limit'
            if gb >= args.limit_gb:
                problems.append(f'shard {i} is {gb:.2f} GB, over the {args.limit_gb} GB limit')
            if not complete:
                problems.append(f'shard {i}: build not marked complete (interrupted? use --resume)')
            print(f'  shard {i}: {rows:,} rows, {gb:.2f} GB, sampled {len(sample)} rows, '
                  f'{bad} bad{flag}')
        print(f'  total: {total:,} rows')

    if args.source:
        lines = 0
        with open(args.source, 'rb') as f:
            for raw in f:
                if raw.strip():
                    lines += 1
        failed = 0
        try:
            with open(args.failed_lines, encoding='utf-8') as f:
                failed = len({l.split('\t', 1)[0] for l in f if l.strip()})
        except FileNotFoundError:
            pass
        print(f'  source: {lines:,} non-empty lines, {failed} failed')
        if total != lines - failed:
            problems.append(f'row total {total:,} != source lines {lines:,} - failed {failed}')

    if problems:
        print(f'\n{len(problems)} problem(s):')
        for p in problems[:50]:
            print('  ' + p)
        sys.exit(1)
    print('\nOK')


if __name__ == '__main__':
    main()
