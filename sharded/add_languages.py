"""Create or refresh the `language` table (lang_code -> name) on every shard.

build_shards.py fills it during a fresh build. Use this for shards built before
the table existed, or after a --resume build (which runs it automatically).
It decompresses every row once, all shards in parallel, and keeps the most
common name for each code (a few codes carry two names in the dump, e.g. adi:
Adi / Mising). Additive: it only touches the `language` table.

    python add_languages.py --shard-url ... --shard-url ... --shard-url ...
"""

import argparse
import collections
import json
import time
from concurrent.futures import ProcessPoolExecutor

import psycopg
import zstandard as zstd

from shardclient import default_urls

DDL = """CREATE TABLE IF NOT EXISTS language (
             lang_code  text    PRIMARY KEY,
             name       text    NOT NULL,   -- most common `lang` value for this code
             entries    integer NOT NULL    -- entries with this code, across all shards
         )"""


def count_shard(url):
    """Counter[(lang_code, name)] for one shard."""
    counts = collections.Counter()
    with psycopg.connect(url) as conn:
        dict_bytes = conn.execute(
            'SELECT d.dict FROM zstd_dict d JOIN meta m ON m.dict_id = d.id').fetchone()[0]
        dctx = zstd.ZstdDecompressor(dict_data=zstd.ZstdCompressionDict(bytes(dict_bytes)))
        with conn.cursor(name='scan') as cur:                # server-side: streams, not 3.6M rows in RAM
            cur.itersize = 20_000
            cur.execute('SELECT lang_code, data FROM entry')
            for lang_code, data in cur:
                name = json.loads(dctx.decompress(bytes(data))).get('lang')
                if isinstance(name, str) and name:
                    counts[(lang_code, name)] += 1
    return counts


def pick_names(counts):
    """{lang_code: (name, entries)}: most common name per code."""
    by_code = collections.defaultdict(collections.Counter)
    for (code, name), n in counts.items():
        by_code[code][name] += n
    return {code: (names.most_common(1)[0][0], sum(names.values()))
            for code, names in by_code.items()}


def write_languages(conns, languages):
    rows = [(code, name, n) for code, (name, n) in sorted(languages.items())]
    for conn in conns:
        with conn.transaction():
            conn.execute(DDL)
            conn.execute('TRUNCATE language')
            with conn.cursor() as cur:
                with cur.copy('COPY language (lang_code, name, entries) FROM STDIN') as copy:
                    for row in rows:
                        copy.write_row(row)


def rebuild(urls):
    started = time.time()
    with ProcessPoolExecutor(len(urls)) as ex:
        total = sum(ex.map(count_shard, urls), collections.Counter())
    languages = pick_names(total)
    conns = [psycopg.connect(u, autocommit=True) for u in urls]
    try:
        write_languages(conns, languages)
    finally:
        for c in conns:
            c.close()
    print(f'language table: {len(languages):,} codes written to {len(urls)} shard(s) '
          f'in {(time.time() - started) / 60:.1f} min', flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--shard-url', action='append', default=[])
    args = ap.parse_args()
    urls = args.shard_url or default_urls()
    if not urls:
        ap.error('no shard URLs: pass --shard-url, set SHARD_URLS, or fill in shard_urls.txt')
    rebuild(urls)


if __name__ == '__main__':
    main()
