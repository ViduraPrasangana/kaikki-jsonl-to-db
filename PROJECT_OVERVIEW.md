# Project overview: kaikki-jsonl-to-db

A memory refresher: what this project does, how the pieces fit, and where work
stopped last time.

## In one sentence

It takes the [kaikki.org](https://kaikki.org) Wiktionary dump (one huge JSONL
file, one JSON object per dictionary entry) and loads it into a **normalised
SQLite or PostgreSQL database**. It also has query files and tools for reading
that database back: senses, inflected forms, and etymology trees.

It is part of the larger **Lexica** project. The query files mention a Go API
(`internal/repository/form.go`, `FindWordsByText`) that reads this database.

## The pipeline

```
kaikki.org-dictionary-English.jsonl
        │
        ▼
converter.py   ── CLI: batching, resume from a Progress bookmark, retry failed lines
        │
        ▼
db.py          ── backend-agnostic insert layer (one Word row → ~29 tables)
        │
        ▼
backends.py    ── SQLite / PostgreSQL (psycopg 3) cursor adapters
        │
        ▼
schema_*.sql   ── tables only (no indexes, so the bulk load runs fast)
indexes_*.sql  ── indexes, FTS5 (SQLite), and foreign keys, applied after the load
```

Typical run against PostgreSQL:

```bash
python3 converter.py --backend postgres \
    --target "postgresql://user:pass@localhost:5432/kaikki" --no-indexes
psql -d kaikki -f indexes_postgres.sql
```

If a run is interrupted, re-running the same command picks up where it
stopped. `COMMANDS.md` covers every option.

## Key design decisions (all committed)

| Decision | Why |
|---|---|
| Indexes and foreign keys live in separate files | The bulk load runs without constraints. Indexes are built once at the end. |
| `WordRelation` / `SenseRelation` hold every relation type, with an integer `relation_type` | Benchmarked about 2x faster than one table per relation for the "everything about this word" query |
| Shared `Tag` and `Category` lookup tables ("unified tag table", the latest commit) | Tag strings repeat a huge amount (`formtag.tag` about 103,000x over 310M rows). Storing an integer id saves a lot of space. |
| Junction tables have no surrogate `id` | Saved 14.3 GB on a full load, and nothing referenced those ids |
| `v_*` views (`v_FormTag`, `v_SenseTag`, `v_WordRelation`, …) | Turn the integer ids back into readable names |
| `normalize_postgres.sql` | One-off migration for a database loaded **before** the unified tag change. A fresh load doesn't need it. |

## Where you left off (uncommitted work, Aug 25–26 2026)

After the loader was finished and committed, you loaded the full dump into a
local PostgreSQL database called `wiktionary` and started **querying it**. In
particular, you were rebuilding Wiktionary-style **etymology trees**. None of
these files are committed yet:

| File | What it is |
|---|---|
| `query_senses.sql` | Definitions, register/domain tags, examples vs quotations, topics, and links for a word. Every query starts from one `target` CTE, so you change the lookup word in one place. |
| `query_forms.sql` | Inflected forms with their tags. One query is a drop-in replacement for the queries in the Go API's `form.go`. Another filters out the ~6.9% of form rows that are table-rendering metadata, not real forms. |
| `query_etymology.sql` | Etymology prose, the templates behind it (`inh`, `bor`, `der`, `cog`, …) with their args as JSON, and "source languages of a word" browsing |
| `sizes_postgres.sql` | Read-only size report: rows, heap, TOAST, and index size for each table |
| `etymology_tree.py` | Parser for Wiktionary's `ety`/`etymon` template syntax, e.g. `frm:portemanteau<ety:af<porter><manteau<t:coat>>>`. wiktextract stores this string unexpanded, and its `etymology_text` is mangled ("Latin gratisbor."), so the parser reads the template source instead. Usable as a library (`parse`, `render`, `flatten`, `describe`) or a CLI. |
| `etymology_walk.py` | Builds the **full** ancestry tree by following hops through the database. An `ety` template only records one step (en *gratis* ← la *gratis*), so the script looks up the Latin entry, reads its template, and continues. It tolerates diacritics (*grātia* → *gratia*) and the reconstruction `*`, and stops at ancestors that have no entry of their own. |

Try it:

```bash
python etymology_tree.py 'frm:portemanteau<ety:af<porter<alt:porte><t:he carries>><manteau<t:coat>>>'
python etymology_walk.py gratis en
python etymology_walk.py portmanteau en --depth 6 --cognates
```

`etymology_walk.py` connects to
`postgresql://postgres:postgres@localhost:5432/wiktionary` by default. Use
`--uri` to point it somewhere else.

## The full load you ran (Aug 23 2026)

Reconstructed from the live database and the PostgreSQL server logs
(`C:\Program Files\PostgreSQL\18\data\log\`). No `output.log` or
`failed_lines.txt` was kept.

**Input:** `Lexica\Databases\raw-wiktextract-data.jsonl\...\raw-wiktextract-data.jsonl`
(24.5 GB, the all-languages wiktextract dump). It is not the 150 MB
English-only file next to it.

**Output:** PostgreSQL 18 (local Windows service `postgresql-x64-18`), database
**`wiktionary`**, **41 GB**, user `postgres` / password `postgres`.

**Timeline** (local time, from the server logs):

| When | What happened |
|---|---|
| 15:24 | PostgreSQL 18 installed, `wiktionary` database created |
| ~15:30–17:16 | First load attempts. The server was restarted twice. One run was stopped with Ctrl+C mid-`COPY wordcategory`. |
| 17:16 → ~19:30 | The main load ran, using the **old** schema with text tag columns (`COPY wordcategory (word_id, name)`) |
| 18:52 | `Progress.last_line` reached **10,806,865**, its final value |
| 21:32 | Explored `formtag`. A `GROUP BY tag` was too slow on 310M rows, which led to the unified tag idea. |
| 22:03–23:45 | Ran `normalize_postgres.sql`. The first tries failed because `\timing` / `\echo` psql commands were sent through a GUI client, not `psql`. There was one harmless `SenseTag_new does not exist` error from re-running a step that had already finished. |
| 22:39 | Committed "unified tag table" |
| Aug 25–26 | Query sessions: the query files and the etymology scripts |

**Result: the load is complete and clean.**

- 10,806,865 `Word` rows, matching `last_line`, so no lines failed.
- The last row (`glúteo`, es, verb) is the dump's last line.
- 5,030 language codes.
- All 9 junction tables are already normalised to `tag_id` / `category_id`
  (`Tag` 36,922 rows, `Category` 1,044,786 rows).

Biggest tables:

| Table | Rows | Size |
|---|---|---|
| formtag | 309.7M | 13 GB |
| form | 96.4M | 9.1 GB |
| sensecategory | 64.5M | 2.7 GB |
| sense | 13.0M | 2.2 GB |
| senselink | 20.5M | 1.8 GB |
| sensetag | 34.5M | 1.5 GB |
| word | 10.8M | 1.2 GB |

**Not done yet on this database:**

- **`indexes_postgres.sql` was never applied.** Only primary keys and three
  unique keys exist, and there are 0 foreign keys. Every lookup by `word_id` /
  `form_id` is a full scan. This is why `query_forms.sql` notes "15 seconds for
  one word".
- The `v_FormTag`, `v_SenseTag`, `v_SenseCategory` and `v_WordCategory` views
  are missing. Only `v_WordRelation` and `v_SenseRelation` exist.

## Possible next steps

- Run `psql -U postgres -d wiktionary -f indexes_postgres.sql` with real `psql`,
  not a GUI client. It takes a long time on `formtag`. Then create the four
  missing views from the end of `schema_postgres.sql`.

- Commit the query files and the etymology scripts. `.idea/awsToolkit.xml` and
  `__pycache__/` can stay out (consider adding them to `.gitignore`).
- Check `etymology_walk.py` on more words: compounds, words with several
  etymologies, and Proto-language ancestors. Then decide whether it goes into
  the Go API or stays a script.
- Make sure the indexes each query file lists as "REQUIRED" exist on the live
  database (`ix_formtag` on 310M rows is the slow one to build).
- Add the new files to the Layout table in `README.md`.
