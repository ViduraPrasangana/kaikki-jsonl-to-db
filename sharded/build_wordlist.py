"""Build the app's offline word list (wikitionary-wordlist-v<N>.db.zip) from the shards.

The app ships this SQLite file as an asset and searches it for suggestions while the user
types:

    SELECT word FROM UniqueEnWords WHERE word LIKE ? ORDER BY LENGTH(word), word LIMIT ?

It holds every distinct word (exact spelling) of the chosen languages, all of them by
default. The table keeps the app's name and column; unlike v1 the word column is
COLLATE NOCASE, so a prefix LIKE ('run%') is answered from the primary key instead of a
scan of every row. NOCASE treats "Par" and "par" as equal, so the key is (word, n), with
n numbering the case variants; every spelling is kept.

    python build_wordlist.py                       # all languages -> out/wikitionary-wordlist-v2.db.zip
    python build_wordlist.py --lang en --version 2 # English only
"""

import argparse
import os
import sqlite3
import time
import zipfile

from shardclient import Shards, default_urls, masked

HERE = os.path.dirname(os.path.abspath(__file__))
ASCII_LOWER = str.maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')


def nocase(word):
    """The key SQLite's NOCASE collation compares: only ASCII letters are folded."""
    return word.translate(ASCII_LOWER)


def read_words(shards, langs):
    words = []
    for i, conn in enumerate(shards.conns):
        sql = 'SELECT DISTINCT word FROM entry WHERE word IS NOT NULL AND word <> \'\''
        params = []
        if langs:
            sql += ' AND lang_code = ANY(%s)'
            params.append(langs)
        t = time.time()
        rows = conn.execute(sql, params).fetchall()
        words.extend(r[0] for r in rows)
        print(f'  shard {i}: {len(rows):,} words ({time.time() - t:.0f}s)')
    # Shards are split by the folded word, so one spelling never sits in two of them;
    # the set only guards against that changing.
    return sorted(set(words), key=lambda w: (nocase(w), w))


def write_db(path, words):
    if os.path.exists(path):
        os.remove(path)
    db = sqlite3.connect(path)
    db.executescript('''
        PRAGMA page_size = 4096;
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        CREATE TABLE UniqueEnWords (
            word TEXT NOT NULL COLLATE NOCASE,
            n    INTEGER NOT NULL,  -- 0, 1, … for spellings NOCASE treats as equal (PAR, Par, par)
            PRIMARY KEY (word, n)
        ) WITHOUT ROWID;
    ''')

    def rows():
        prev, n = None, 0
        for w in words:  # sorted by NOCASE key, so variants of one key are adjacent
            key = nocase(w)
            n = n + 1 if key == prev else 0
            prev = key
            yield w, n

    with db:
        db.executemany('INSERT INTO UniqueEnWords (word, n) VALUES (?, ?)', rows())
    db.execute('PRAGMA journal_mode = DELETE')
    db.execute('VACUUM')
    db.close()


def check_db(path, expected):
    db = sqlite3.connect(path)
    count = db.execute('SELECT count(*) FROM UniqueEnWords').fetchone()[0]
    assert count == expected, (count, expected)
    q = 'SELECT word FROM UniqueEnWords WHERE word LIKE ? ORDER BY LENGTH(word) ASC, word ASC LIMIT 50'
    plan = ' / '.join(r[3] for r in db.execute('EXPLAIN QUERY PLAN ' + q, ('run%',)))
    print(f'  rows {count:,}; query plan: {plan}')
    for key in ('r%', 'run%', 'Par%', 'xylo%'):
        t = time.perf_counter()
        found = [r[0] for r in db.execute(q, (key,))]
        print(f'  {key:6} {1000 * (time.perf_counter() - t):6.1f} ms  {found[:6]}')
    db.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--shard-url', action='append', help='shard URL, in shard order (default: shard_urls.txt)')
    ap.add_argument('--lang', action='append', help='only these language codes, e.g. --lang en (default: all)')
    ap.add_argument('--name', default='wikitionary-wordlist', help='file name the app expects (EnglishWordListRepository.DBNAME)')
    ap.add_argument('--version', type=int, default=2, help='version in the file name (EnglishWordListRepository.DB_VERSION)')
    ap.add_argument('--out', default=os.path.join(HERE, 'out'), help='output folder')
    args = ap.parse_args()

    urls = args.shard_url or default_urls()
    print('shards:', ', '.join(masked(u) for u in urls))
    os.makedirs(args.out, exist_ok=True)
    db_name = f'{args.name}-v{args.version}.db'
    db_path = os.path.join(args.out, db_name)
    zip_path = db_path + '.zip'

    with Shards(urls) as shards:
        print('reading words', '(all languages)' if not args.lang else f'({", ".join(args.lang)})')
        words = read_words(shards, args.lang)
    print(f'{len(words):,} distinct words')

    t = time.time()
    write_db(db_path, words)
    print(f'wrote {db_path} ({os.path.getsize(db_path) / 1e6:.1f} MB, {time.time() - t:.0f}s)')
    check_db(db_path, len(words))

    t = time.time()
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as z:
        z.write(db_path, db_name)
    print(f'wrote {zip_path} ({os.path.getsize(zip_path) / 1e6:.1f} MB, {time.time() - t:.0f}s)')


if __name__ == '__main__':
    main()
