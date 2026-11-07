"""Read side of the shards, in Python: connect, check the layout, look words up.

A backend in another language does the same steps; this module is for
scripts, checks and debugging, and is the reference for that port.

    from shardclient import Shards
    with Shards(['postgresql://...s0', 'postgresql://...s1', 'postgresql://...s2']) as s:
        for entry in s.lookup('run', lang_code='en'):
            print(entry['pos'], entry['senses'][0]['glosses'])
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor

import psycopg
import zstandard as zstd

import shardkey


SHARD_URLS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'shard_urls.txt')


def default_urls():
    """Shard URLs when none are given on the command line, in shard order:
    the SHARD_URLS environment variable (comma separated) if set, otherwise
    shard_urls.txt next to these scripts (one URL per line, # comments)."""
    env = os.environ.get('SHARD_URLS', '')
    if env.strip():
        return [u.strip() for u in env.split(',') if u.strip()]
    try:
        with open(SHARD_URLS_FILE, encoding='utf-8') as f:
            lines = (line.split('#', 1)[0].strip() for line in f)
            return [line for line in lines if line]
    except FileNotFoundError:
        return []


def masked(url):
    """postgresql://user:secret@host/db -> postgresql://user:***@host/db, for logs."""
    scheme, sep, rest = url.partition('://')
    creds, at, host = rest.rpartition('@')
    if not at or ':' not in creds:
        return url
    return f'{scheme}{sep}{creds.split(":", 1)[0]}:***@{host}'


class Shards:
    def __init__(self, urls):
        if not urls:
            raise ValueError('no shard URLs: pass --shard-url, set SHARD_URLS, '
                             f'or list them in {SHARD_URLS_FILE}')
        self.conns = [psycopg.connect(u, autocommit=True) for u in urls]
        self.metas = [c.execute("""SELECT shard_index, shard_count, build_id, dict_id, complete
                                   FROM meta""").fetchone() for c in self.conns]
        self.check_layout()
        dict_bytes = bytes(self.conns[0].execute(
            'SELECT dict FROM zstd_dict WHERE id = %s', (self.metas[0][3],)).fetchone()[0])
        self.decompressor = zstd.ZstdDecompressor(dict_data=zstd.ZstdCompressionDict(dict_bytes))

    def check_layout(self):
        """Refuse to run against shards that are swapped, missing, or mixed builds."""
        n = len(self.conns)
        for i, meta in enumerate(self.metas):
            if meta is None:
                raise RuntimeError(f'URL #{i} has no meta row; is it a shard database?')
            shard_index, shard_count, build_id, dict_id, _complete = meta
            if shard_index != i or shard_count != n:
                raise RuntimeError(f'URL #{i} is shard {shard_index} of {shard_count}, '
                                   f'expected shard {i} of {n}; check the URL order')
            if build_id != self.metas[0][2] or dict_id != self.metas[0][3]:
                raise RuntimeError(f'URL #{i} is from build {build_id}, '
                                   f'not {self.metas[0][2]}')

    def decode(self, data):
        return json.loads(self.decompressor.decompress(bytes(data)))

    def lookup(self, word, lang_code=None):
        """All entries for a word (case-insensitive), from the one shard that holds it."""
        key = shardkey.fold(word)
        conn = self.conns[shardkey.fnv1a_32(key.encode('utf-8')) % len(self.conns)]
        sql = 'SELECT data FROM entry WHERE word_key = %s'
        params = [key]
        if lang_code:
            sql += ' AND lang_code = %s'
            params.append(lang_code)
        return [self.decode(row[0]) for row in conn.execute(sql + ' ORDER BY id', params)]

    def prefix(self, text, limit=20, lang_code=None):
        """Autocomplete: distinct headwords starting with text, from every shard."""
        key = shardkey.fold(text)
        pattern = key.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
        sql = 'SELECT DISTINCT word_key, word FROM entry WHERE word_key LIKE %s'
        params = [pattern]
        if lang_code:
            sql += ' AND lang_code = %s'
            params.append(lang_code)
        sql += ' ORDER BY word_key, word LIMIT %s'
        params.append(limit)

        # Hashing scatters a prefix across all shards: ask each, merge.
        def one(conn):
            # psycopg connections are not shared across threads here: each
            # thread uses its own shard's connection.
            return conn.execute(sql, params).fetchall()
        with ThreadPoolExecutor(len(self.conns)) as ex:
            rows = [r for part in ex.map(one, self.conns) for r in part]
        return [word for _key, word in sorted(set(rows))][:limit]

    def close(self):
        for c in self.conns:
            c.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
