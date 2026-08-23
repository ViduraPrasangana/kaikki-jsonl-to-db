"""Database backends for the kaikki converter.

The insert layer in db.py is written once against a small cursor protocol:

    cur.insert(sql, params) -> int   INSERT that must return the new row id
    cur.run(sql, params)             INSERT/DDL with no id needed
    cur.run_many(sql, rows)          batched INSERT of uniform rows

and a per-entry transaction protocol used for error isolation:

    cur.begin_entry() / cur.end_entry() / cur.abort_entry()
    cur.flush()                      push anything buffered to the server

SQL is authored with SQLite-style ``?`` placeholders and no RETURNING clause;
the PostgreSQL cursors rewrite both. Everything else (SAVEPOINT, ON CONFLICT,
the schema files) is written in syntax both engines accept.

Three cursors exist:

* SqliteCursor       - executes immediately, ids from lastrowid.
* PostgresCursor     - executes immediately, ids from RETURNING. One network
                       round trip per row.
* PostgresCopyCursor - allocates ids client-side and buffers every row, then
                       writes each table with a single COPY at flush time.
                       Far fewer round trips; this is the fast path.
"""

import os
import re

SCHEMA_FILES = {'sqlite': 'schema_sqlite.sql', 'postgres': 'schema_postgres.sql'}
INDEX_FILES = {'sqlite': 'indexes_sqlite.sql', 'postgres': 'indexes_postgres.sql'}

_HERE = os.path.dirname(os.path.abspath(__file__))
_INSERT_RE = re.compile(r'INSERT\s+INTO\s+(\w+)\s*\(([^)]*)\)', re.IGNORECASE)


def read_sql(filename):
    with open(os.path.join(_HERE, filename), 'r', encoding='utf-8') as handle:
        return handle.read()


class SqliteCursor:
    """Executes immediately; generated ids come from lastrowid."""

    copies = 0

    def __init__(self, cursor):
        self._cursor = cursor
        self.rows_written = 0

    def insert(self, sql, params):
        self._cursor.execute(sql, params)
        self.rows_written += 1
        return self._cursor.lastrowid

    def run(self, sql, params=()):
        self._cursor.execute(sql, params)

    def run_many(self, sql, rows):
        if rows:
            self._cursor.executemany(sql, rows)
            self.rows_written += len(rows)

    def fetchone(self, sql, params=()):
        self._cursor.execute(sql, params)
        return self._cursor.fetchone()

    def begin_entry(self):
        self._cursor.execute("SAVEPOINT entry")

    def end_entry(self):
        self._cursor.execute("RELEASE SAVEPOINT entry")

    def abort_entry(self):
        self._cursor.execute("ROLLBACK TO SAVEPOINT entry")
        self._cursor.execute("RELEASE SAVEPOINT entry")

    def flush(self):
        pass

    def close(self):
        self._cursor.close()


class PostgresCursor:
    """Executes immediately; generated ids come from RETURNING id."""

    _translated = {}

    copies = 0

    def __init__(self, cursor):
        self._cursor = cursor
        self.rows_written = 0

    @classmethod
    def _translate(cls, sql):
        # Cached because the same handful of statements run millions of times.
        converted = cls._translated.get(sql)
        if converted is None:
            converted = sql.replace('?', '%s')
            cls._translated[sql] = converted
        return converted

    def insert(self, sql, params):
        self._cursor.execute(self._translate(sql) + ' RETURNING id', params)
        self.rows_written += 1
        return self._cursor.fetchone()[0]

    def run(self, sql, params=()):
        self._cursor.execute(self._translate(sql), params)

    def run_many(self, sql, rows):
        if rows:
            self._cursor.executemany(self._translate(sql), rows)
            self.rows_written += len(rows)

    def fetchone(self, sql, params=()):
        self._cursor.execute(self._translate(sql), params)
        return self._cursor.fetchone()

    def begin_entry(self):
        self._cursor.execute("SAVEPOINT entry")

    def end_entry(self):
        self._cursor.execute("RELEASE SAVEPOINT entry")

    def abort_entry(self):
        self._cursor.execute("ROLLBACK TO SAVEPOINT entry")
        self._cursor.execute("RELEASE SAVEPOINT entry")

    def flush(self):
        pass

    def close(self):
        self._cursor.close()


class PostgresCopyCursor:
    """Buffers rows and writes each table with one COPY per flush.

    Primary keys are allocated client-side from per-table counters, so a child
    row can reference its parent's id without the server ever being asked. That
    removes the round trip per row that RETURNING forces, and lets the whole
    batch go out as a handful of COPY streams.

    Counters are seeded lazily from MAX(id), so this resumes correctly against a
    partly-loaded database. sync_sequences() must be called once at the end so
    the identity sequences do not later hand out ids that are already in use.
    """

    def __init__(self, connection):
        self._connection = connection
        self._next_id = {}       # table -> next id to hand out
        self._buffers = {}       # (table, columns) -> list of row tuples
        self._parsed = {}        # sql -> (table, columns)
        self._mark = None        # buffer lengths at the start of the current entry
        self.rows_written = 0    # rows handed to COPY by flush()
        self.copies = 0          # COPY statements issued by flush()

    def _parse(self, sql):
        parsed = self._parsed.get(sql)
        if parsed is None:
            match = _INSERT_RE.search(sql)
            if match is None:
                raise ValueError(f"Cannot parse INSERT: {sql[:80]}")
            table = match.group(1).lower()
            columns = tuple(c.strip() for c in match.group(2).split(','))
            parsed = (table, columns)
            self._parsed[sql] = parsed
        return parsed

    def _seed(self, table):
        with self._connection.cursor() as cursor:
            cursor.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table}")
            self._next_id[table] = cursor.fetchone()[0] + 1

    def insert(self, sql, params):
        table, columns = self._parse(sql)
        if table not in self._next_id:
            self._seed(table)
        new_id = self._next_id[table]
        self._next_id[table] = new_id + 1
        key = (table, ('id',) + columns)
        self._buffers.setdefault(key, []).append((new_id,) + tuple(params))
        return new_id

    def run(self, sql, params=()):
        # Statements that are not plain INSERTs (the Progress upsert) have to go
        # straight to the server; buffering cannot express ON CONFLICT.
        if 'ON CONFLICT' in sql.upper() or not sql.lstrip().upper().startswith('INSERT'):
            with self._connection.cursor() as cursor:
                cursor.execute(sql.replace('?', '%s'), params)
            return
        table, columns = self._parse(sql)
        self._buffers.setdefault((table, columns), []).append(tuple(params))

    def run_many(self, sql, rows):
        if not rows:
            return
        table, columns = self._parse(sql)
        self._buffers.setdefault((table, columns), []).extend(tuple(r) for r in rows)

    def fetchone(self, sql, params=()):
        with self._connection.cursor() as cursor:
            cursor.execute(sql.replace('?', '%s'), params)
            return cursor.fetchone()

    # Entry isolation without SAVEPOINT: remember how long every buffer was
    # before the entry, and truncate back to that if it fails.
    def begin_entry(self):
        self._mark = {key: len(rows) for key, rows in self._buffers.items()}

    def end_entry(self):
        self._mark = None

    def abort_entry(self):
        for key, rows in self._buffers.items():
            del rows[self._mark.get(key, 0):]
        self._mark = None

    def flush(self):
        for (table, columns), rows in self._buffers.items():
            if not rows:
                continue
            column_list = ', '.join(columns)
            with self._connection.cursor() as cursor:
                with cursor.copy(f"COPY {table} ({column_list}) FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(row)
            self.rows_written += len(rows)
            self.copies += 1
            rows.clear()

    def sync_sequences(self):
        """Advance identity sequences past the ids handed out client-side."""
        with self._connection.cursor() as cursor:
            for table, next_id in self._next_id.items():
                cursor.execute(
                    "SELECT setval(pg_get_serial_sequence(%s, 'id'), %s, false)",
                    (table, max(next_id, 1)))

    def close(self):
        pass


class SqliteBackend:
    name = 'sqlite'

    def __init__(self, target, **_):
        self.target = target

    def connect(self):
        import sqlite3
        conn = sqlite3.connect(self.target)
        # Bulk-load tuning; indexes are built afterwards.
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA cache_size=-200000")
        conn.execute("PRAGMA temp_store=MEMORY")
        print(f"Connected to SQLite database: {self.target}")
        return conn

    def cursor(self, conn):
        return SqliteCursor(conn.cursor())

    def run_script(self, conn, sql):
        conn.executescript(sql)
        conn.commit()


class PostgresBackend:
    name = 'postgres'

    def __init__(self, target, use_copy=True):
        self.target = target
        self.use_copy = use_copy

    def connect(self):
        try:
            import psycopg
        except ImportError:
            raise SystemExit(
                "The postgres backend needs psycopg 3. Install it with:\n"
                "    pip install \"psycopg[binary]\""
            )
        conn = psycopg.connect(self.target)
        info = conn.info
        mode = 'COPY' if self.use_copy else 'INSERT'
        print(f"Connected to PostgreSQL: {info.dbname} on {info.host}:{info.port} ({mode} mode)")
        return conn

    def cursor(self, conn):
        if self.use_copy:
            return PostgresCopyCursor(conn)
        return PostgresCursor(conn.cursor())

    def run_script(self, conn, sql):
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def get_backend(name, target, use_copy=True):
    if name == 'sqlite':
        return SqliteBackend(target)
    if name == 'postgres':
        return PostgresBackend(target, use_copy=use_copy)
    raise SystemExit(f"Unknown backend: {name}")
