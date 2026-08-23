# kaikki dataset converter

Converts [kaikki.org](https://kaikki.org) Wiktionary JSONL dumps into a
normalised SQLite or PostgreSQL database.

## Usage

See [COMMANDS.md](COMMANDS.md) for starting, resuming, pointing at a JSONL
path, applying indexes later, and retrying failed lines.

```bash
# SQLite (default)
python3 converter.py

# PostgreSQL, deferring indexes so the load runs unconstrained
python3 converter.py --backend postgres \
    --target "postgresql://user:pass@localhost:5432/kaikki" --no-indexes
```

Long runs:

```bash
nohup python3 converter.py > output.log 2>&1 &
tail -f output.log
```

Interrupted runs resume automatically from the `Progress` bookmark — just
re-run the same command.

### Options

| Flag | Default | Purpose |
|---|---|---|
| `--backend` | `sqlite` | `sqlite` or `postgres` |
| `--target` | `wikitionary-kaikki-english.db` | SQLite path, or PostgreSQL connection string |
| `--input` | `kaikki.org-dictionary-English.jsonl` | dump to read |
| `--batch-size` | `1000` | entries per transaction |
| `--log-every` | `10000` | lines between progress messages |
| `--indexes` / `--no-indexes` | `--indexes` | apply the index file after loading |

The PostgreSQL backend needs psycopg 3: `pip install "psycopg[binary]"`.

## Schema

DDL lives in plain SQL files, one pair per engine:

| | Tables | Indexes, FTS, foreign keys |
|---|---|---|
| SQLite | `schema_sqlite.sql` | `indexes_sqlite.sql` |
| PostgreSQL | `schema_postgres.sql` | `indexes_postgres.sql` |

Indexes and foreign keys are deliberately kept out of the schema files so the
bulk load runs unconstrained. Apply them afterwards with `--indexes`, or by
hand:

```bash
sqlite3 wikitionary-kaikki-english.db < indexes_sqlite.sql
psql -d kaikki -f indexes_postgres.sql
```

One `Word` row per JSONL line, fanning out into ~29 tables. Notable points:

- **`WordRelation` / `SenseRelation`** hold every relation type (derived,
  synonyms, antonyms, hypernyms, hyponyms, meronyms, holonyms,
  coordinate_terms, related, troponyms, …) discriminated by an integer
  `relation_type`. Benchmarked ~2x faster than one table per relation for the
  "fetch everything for this word" query a dictionary page runs. Use the
  `v_WordRelation` / `v_SenseRelation` views to get the name back.
- **`SenseTag`** carries the register and domain labels (`informal`, `slang`,
  `obsolete`, `UK`, `transitive`). `is_raw = 1` marks a wiktextract `raw_tag`.
- **`SenseFormOf` / `SenseAltOf`** resolve inflections and spelling variants
  (`ran` → `run`, `betcher` → `bet`), which is what makes lookup of a
  non-lemma form work.
- **`SenseExample.type`** distinguishes `example` (usage) from `quotation`
  (attested, with a `ref`).
- **`Translation`** hangs off `Word`, not `Sense` — kaikki only ever emits
  translations at word level.
- **`Descendant`** is a tree: `parent_id` plus a derived `depth`.

## Layout

| File | Role |
|---|---|
| `converter.py` | CLI, batching, resume, error isolation |
| `db.py` | backend-agnostic insert layer |
| `backends.py` | SQLite / PostgreSQL cursor adapters |
| `schema_*.sql`, `indexes_*.sql` | DDL |
