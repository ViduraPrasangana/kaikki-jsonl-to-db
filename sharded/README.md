# Sharded, compressed Wiktionary database

The whole kaikki / wiktextract dump (all languages, every field, 10.8M entries)
split across **N small PostgreSQL databases**. It is built to fit Railway's
5 GB-per-database limit. The relational schema in the parent folder is 41 GB
for the same data.

| | Relational (`../`) | Sharded (this folder) |
|---|---|---|
| Size | 41 GB, one database | ~5 GB total, ~1.7 GB per shard with 3 shards (estimate) |
| Data | Normalised into ~29 tables | Original JSON line per entry, byte for byte |
| Queries | Any SQL | Lookup by word, prefix autocomplete |

## How it works

```
one JSONL line ──► zstd (trained dictionary, ~6x) ──► row in shard  fnv1a32(fold(word)) % N
```

- **One row per entry.** `data` holds the original JSON line, zstd-compressed
  with a dictionary trained on the dump. Compressing short rows one at a time
  only gets about 3x. The shared dictionary roughly doubles that.
- **Routing.** `fold(word)` applies NFC, then lowercases each character
  separately. Its result is hashed with FNV-1a 32, and the hash modulo N picks
  the shard. "Run", "run" and "RUN", in every language, all land on the same
  shard, so a lookup is one query on one database.
- **Lossless.** The loader decompresses every row it writes and compares it
  with the original. A 100k-line test build matched the source byte for byte.
- **Each shard describes itself.** The `meta` table records the shard index,
  shard count, build id and dictionary id. Readers refuse to start if the
  URLs are in the wrong order or come from different builds.
- **Changing N means rebuilding from the JSONL.** There's no migration
  between shard counts.

## Files

| File | Role |
|---|---|
| `schema.sql` | Per-shard tables: `entry`, `zstd_dict`, `meta` |
| `indexes.sql` | `word_key` index (exact and prefix lookup), applied after the load |
| `shardkey.py` | `fold()`, `fnv1a_32()`, `shard_for()`: the routing rule |
| `build_shards.py` | JSONL → N shards: trains the dictionary, compresses in parallel, resumable |
| `verify_shards.py` | Post-build check: layout, decoding, routing, sizes, row total |
| `shardclient.py` | Python reader (`Shards.lookup`, `Shards.prefix`) |
| `lookup.py` | CLI around `shardclient.py` |
| `requirements.txt` | `psycopg[binary]`, `zstandard` |

## 1. Build locally

```bash
pip install -r requirements.txt

createdb -U postgres wiktionary_s0
createdb -U postgres wiktionary_s1
createdb -U postgres wiktionary_s2

python build_shards.py --recreate \
    --input "../../../Databases/raw-wiktextract-data.jsonl/raw-wiktextract-data.jsonl/raw-wiktextract-data.jsonl" \
    --shard-url postgresql://postgres:postgres@localhost:5432/wiktionary_s0 \
    --shard-url postgresql://postgres:postgres@localhost:5432/wiktionary_s1 \
    --shard-url postgresql://postgres:postgres@localhost:5432/wiktionary_s2 \
    --dict-out zstd_dict.bin
```

- The **order of `--shard-url` is the shard index**. The shard count is how many
  URLs you pass. You can set `SHARD_URLS` (comma-separated) instead.
- On 12 cores the test build did about 5,500 rows/s. Expect **30–40 minutes**
  for the full 24.5 GB dump. Progress is printed every 30 s.
- **If it's interrupted**, run the same command with `--resume` in place of
  `--recreate`. Shards that committed one batch more than the others are rolled
  back to the common bookmark, so there are no duplicates or gaps (tested).
- Lines that aren't valid JSON go to `failed_lines.txt` and are skipped. The
  relational load of this dump had none.
- `--limit 100000` makes a quick test build.

Then check it:

```bash
set SHARD_URLS=postgresql://postgres:postgres@localhost:5432/wiktionary_s0,postgresql://...s1,postgresql://...s2
python verify_shards.py --source "<same JSONL>"
python lookup.py run --lang en
python lookup.py --prefix encyc
```

`verify_shards.py` fails if any shard is at or over `--limit-gb` (default 5). It
also flags a shard that has passed 90% of the limit.

## 2. Upload to Railway

Create one PostgreSQL service per shard in the same Railway project. Then dump
each local shard and restore it into the matching service:

```bash
pg_dump -U postgres -Fc -Z0 -f s0.dump wiktionary_s0      # -Z0: data is already compressed
pg_restore --no-owner --no-privileges -d "<railway shard 0 URL>" s0.dump
# ...same for s1, s2
```

Use each service's **public** URL for the restore. The API should use the
**private** URL. Keep shard *i*'s dump going to the service you'll list *i*th
in `SHARD_URLS`. If you mix them up, nothing breaks silently: the API refuses
to start and tells you which URL is wrong. Afterwards, run
`verify_shards.py` against the Railway URLs.

Each Railway Postgres service bills for its own memory and CPU while running,
so check the project's usage estimate with three databases before relying on
the $5 plan.

## 3. Reading from a backend

A backend in any language needs the same few steps. `shardclient.py` is the
reference implementation.

1. **Connect** to the shards in order, from `SHARD_URLS` (private Railway URLs).
2. **Check `meta` on startup:** shard *i* must have `shard_index = i` and
   `shard_count = N`, all shards must share `build_id` and `dict_id`, and
   `complete` must be true.
3. **Load the dictionary** once: `SELECT dict FROM zstd_dict WHERE id = <dict_id>`.
4. **Lookup:** compute `key = fold(word)` and `shard = fnv1a32(utf8(key)) % N`,
   then run `SELECT data FROM entry WHERE word_key = $1 [AND lang_code = $2] ORDER BY id`
   on that one shard. Decompress each `data` value with zstd and the
   dictionary. The result is the original wiktextract JSON.
5. **Autocomplete:** run `SELECT DISTINCT word_key, word FROM entry WHERE word_key LIKE $1 || '%'`
   on every shard in parallel, then merge and sort. Escape `\`, `%` and `_` in
   the input.

The frames are standard zstd with the dictionary id embedded, so any zstd
library that accepts a dictionary can decode them.

## Rules that must not drift

1. **The loader and the API must use the same shard count and the same URL
   order.** `meta` enforces this at startup.
2. **`fold` and the hash are a contract.** If you change `shardkey.py`, change
   the backend the same way and rebuild the shards. Never use a language's
   built-in string hash (Python's `hash()`, Go's map hash): those are
   randomised per process.
3. **Lowercase one character at a time** with the Unicode simple mapping, and
   map `İ` (U+0130) to `i`. Python's `str.lower()` on a whole string applies
   the Greek final-sigma rule, which most other languages don't. When you port
   `fold`, compare its output with `shardkey.py` on a sample of real headwords,
   especially non-Latin ones. Python 3.12 uses Unicode 15.0.

## Limits of this layout

- **Lookup only by headword.** Exact match and prefix work. The database can't
  search inside definitions or translations, or filter by tag. For a feature
  like that, add a small side table (for example `translation_word → entry id`)
  rather than going back to the full relational schema.
- **Inflections and etymology hops are a second lookup.** `ran` has its own
  entry pointing to `run` via `senses[].form_of`, and the API follows it with a
  second `Lookup`. The same goes for each step of an etymology tree.
