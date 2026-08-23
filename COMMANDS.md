# Commands

Every command is run from the project directory. All flags are optional —
`python3 converter.py` on its own loads
`kaikki.org-dictionary-English.jsonl` into `wikitionary-kaikki-english.db`.

---

## 1. Starting a load

### SQLite

```bash
# Defaults: reads ./kaikki.org-dictionary-English.jsonl
#           writes ./wikitionary-kaikki-english.db
#           builds indexes when finished
python3 converter.py
```

```bash
# Long run, detached
nohup python3 converter.py > output.log 2>&1 &
tail -f output.log
```

### PostgreSQL

```bash
pip install "psycopg[binary]"        # one-off, driver is not bundled

createdb kaikki                      # if the database does not exist yet

python3 converter.py \
    --backend postgres \
    --target "postgresql://user:password@localhost:5432/kaikki" \
    --no-indexes
```

`--no-indexes` is recommended for the full dump: it loads unconstrained, then
you apply `indexes_postgres.sql` yourself (see section 4).

> **Starting over:** the loader only ever appends. To rebuild from scratch,
> delete the SQLite file (`rm wikitionary-kaikki-english.db*` — the `*` catches
> the `-wal` and `-shm` files) or drop and recreate the PostgreSQL database.
> Re-running against a partly-loaded database *resumes*, it does not reset.

### Reading the progress line

One line per `--log-every` lines (default 10,000), independent of how often the
loader commits:

```
[batch 11] 10,500 lines | 10,500 ok, 0 failed | 972,174 rows, 290 COPY | 3,043/s (avg 2,452/s) | 00:00:04
```

| Field | Meaning |
|---|---|
| `batch 11` | transactions committed so far — one per `--batch-size` entries |
| `10,500 lines` | position in the input file (absolute, not per-run) |
| `10,500 ok, 0 failed` | entries stored / skipped **this run** |
| `972,174 rows` | table rows actually written |
| `290 COPY` | COPY statements issued — PostgreSQL COPY mode only |
| `3,043/s` | rate for this window |
| `avg 2,452/s` | rate for the whole run — use this to extrapolate |
| `00:00:04` | elapsed |

Rows per COPY (here ~3,350) is the useful health signal on PostgreSQL: it
should stay in the thousands. If it collapses toward 1, buffering is not
working and you are back to per-row round trips.

The row count excludes the 13 `RelationType` seed rows and the `Progress`
bookmark, which the schema file writes rather than the loader — so it will read
14 lower than `SELECT count(*)` across all tables.

---

## 2. Pointing at a JSONL file

`--input` takes any path — relative, absolute, or with spaces (quote it).

```bash
# File sitting next to the script (this is the default)
python3 converter.py --input kaikki.org-dictionary-English.jsonl

# Relative path
python3 converter.py --input ../dumps/kaikki.org-dictionary-English.jsonl

# Absolute path, Linux/macOS
python3 converter.py --input /home/user/dumps/kaikki-english.jsonl

# Absolute path, Windows — quote it, and either use forward slashes
python3 converter.py --input "C:/Users/user/dumps/kaikki-english.jsonl"
# ...or escape the backslashes
python3 converter.py --input "C:\\Users\\user\\dumps\\kaikki-english.jsonl"

# Path containing spaces — always quote
python3 converter.py --input "D:/My Dumps/kaikki english.jsonl"
```

Choosing where the database goes at the same time:

```bash
python3 converter.py \
    --input "D:/dumps/kaikki-english.jsonl" \
    --target "D:/databases/english.db"
```

Downloading a dump first:

```bash
curl -O https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl
python3 converter.py --input kaikki.org-dictionary-English.jsonl
```

---

## 3. Resuming

**Re-run the exact same command.** The loader records the last committed line
in the `Progress` table inside the database, so it picks up from there.

```bash
# Interrupted (Ctrl-C, kill, crash, reboot) - just run it again
python3 converter.py --input "D:/dumps/kaikki-english.jsonl" --target "D:/databases/english.db"
```

```
Connected to SQLite database: D:/databases/english.db
Schema applied from schema_sqlite.sql
Resuming after line 412000        <- picked up where it stopped
```

PostgreSQL resumes identically:

```bash
python3 converter.py --backend postgres \
    --target "postgresql://user:password@localhost:5432/kaikki" --no-indexes
```

The bookmark is committed in the same transaction as the batch it belongs to,
so it can never point past data that was actually written — an interrupted
batch rolls back whole, bookmark included. No duplicate rows on resume.

Checking progress without stopping the run:

```bash
# SQLite
sqlite3 wikitionary-kaikki-english.db "SELECT last_line FROM Progress;"

# PostgreSQL
psql -d kaikki -c "SELECT last_line FROM Progress;"
```

Forcing a restart from a specific line (rarely needed — this does **not**
delete rows already loaded, so only go forwards):

```bash
sqlite3 wikitionary-kaikki-english.db "UPDATE Progress SET last_line = 500000;"
```

---

## 4. Applying indexes afterwards

Skipped the indexes during the load? Apply them when you are ready.

```bash
# Via the loader (works for both backends, needs no extra CLI tools)
python3 converter.py --indexes

# PostgreSQL, by hand
psql -d kaikki -f indexes_postgres.sql
```

For SQLite, prefer the loader. The `sqlite3` CLI is often built without the
FTS5 module, in which case `sqlite3 db < indexes_sqlite.sql` fails with
`no such module: fts5` and you get the indexes but no search tables. Python's
bundled SQLite has FTS5, so `--indexes` works regardless.

Both index files are safe to apply more than once, except the foreign-key
section of `indexes_postgres.sql` — PostgreSQL has no
`ADD CONSTRAINT IF NOT EXISTS`, so a second run errors on those. The indexes
themselves are all `IF NOT EXISTS`.

---

## 5. All options

| Flag | Default | Purpose |
|---|---|---|
| `--backend {sqlite,postgres}` | `sqlite` | database engine |
| `--target` | `wikitionary-kaikki-english.db` | SQLite file path, or PostgreSQL connection string |
| `--input` | `kaikki.org-dictionary-English.jsonl` | JSONL dump to read |
| `--batch-size` | `1000` | entries per transaction |
| `--failed-lines` | `failed_lines.txt` | file to append failed line numbers to |
| `--log-every` | `10000` | lines between progress messages |
| `--indexes` / `--no-indexes` | `--indexes` | apply the index file after loading |

```bash
python3 converter.py --help
```

---

## 6. Failures

Lines that fail are skipped, not fatal — each entry loads inside its own
savepoint, so one bad record never takes the batch down. Failures are appended
to `failed_lines.txt` as `line-number<TAB>error`.

```bash
wc -l failed_lines.txt        # how many failed
head failed_lines.txt         # what went wrong
```

To retry those lines, extract them into their own file:

```bash
cut -f1 failed_lines.txt | sort -n | uniq > bad_lines.txt
awk 'NR==FNR{keep[$1];next} FNR in keep' bad_lines.txt kaikki.org-dictionary-English.jsonl > retry.jsonl
```

**The bookmark counts lines of whatever file you pass**, so loading
`retry.jsonl` into the main database does nothing at all — its line numbers
(1, 2, 3…) are below the bookmark, so every line is skipped and you get
`0 entries stored` with no warning.

The clean way is a separate database, which leaves the main one untouched:

```bash
python3 converter.py --input retry.jsonl --target retries.db
```

If you specifically need the rows in the main database, clear the bookmark
first — and note the original value, because you will destroy it:

```bash
sqlite3 wikitionary-kaikki-english.db "SELECT last_line FROM Progress;"   # write this down
sqlite3 wikitionary-kaikki-english.db "UPDATE Progress SET last_line = 0;"
python3 converter.py --input retry.jsonl --no-indexes
sqlite3 wikitionary-kaikki-english.db "UPDATE Progress SET last_line = <the noted value>;"
```

Skip the last step only if you are certain you will never resume the main dump
again — leaving the bookmark at the retry file's length would make the next
resume re-insert almost the entire dump as duplicates.

Either way the `line_number` column on those rows refers to positions in
`retry.jsonl`, not the original dump.
