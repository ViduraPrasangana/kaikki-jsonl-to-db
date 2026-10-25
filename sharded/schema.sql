-- Schema for ONE shard. Every shard gets the identical schema; build_shards.py
-- applies it with --recreate, or run it by hand:
--     psql "$SHARD_URL" -f schema.sql
--
-- One row per JSONL line of the kaikki / wiktextract dump. `data` is the
-- original line, byte for byte, compressed with zstd using a dictionary trained
-- on the dump. The dictionary is stored once per shard in zstd_dict and is the
-- same in every shard of a build.
--
-- Indexes are created by indexes.sql AFTER the bulk load.

-- meta references zstd_dict, so it must go first.
DROP TABLE IF EXISTS meta;
DROP TABLE IF EXISTS entry;
DROP TABLE IF EXISTS zstd_dict;

CREATE TABLE entry (
    id         integer  PRIMARY KEY,  -- 1-based line number in the source JSONL, unique across shards
    word       text     NOT NULL,     -- headword exactly as in the dump
    word_key   text     NOT NULL,     -- shardkey.fold(word): the lookup and routing key
    lang_code  text     NOT NULL,
    pos        text,
    data       bytea    NOT NULL      -- zstd(original JSON line, dictionary zstd_dict.id = meta.dict_id)
);

-- The payload is already compressed. EXTERNAL stops PostgreSQL from trying to
-- compress it a second time when a large row is moved to TOAST.
ALTER TABLE entry ALTER COLUMN data SET STORAGE EXTERNAL;

CREATE TABLE zstd_dict (
    id    integer PRIMARY KEY,         -- zstd dictionary id, also written into every frame
    dict  bytea   NOT NULL
);

-- Exactly one row. The API checks it on startup so a swapped connection string
-- fails loudly instead of returning "word not found".
CREATE TABLE meta (
    shard_index  integer     NOT NULL,
    shard_count  integer     NOT NULL,
    build_id     text        NOT NULL,  -- identical on every shard of one build
    hash_scheme  text        NOT NULL,  -- 'fnv1a32(fold(word))'
    dict_id      integer     NOT NULL REFERENCES zstd_dict (id),
    source_file  text        NOT NULL,
    last_line    integer     NOT NULL DEFAULT 0,  -- resume bookmark: lines 1..last_line are committed
    complete     boolean     NOT NULL DEFAULT false,
    built_at     timestamptz NOT NULL DEFAULT now()
);
