-- Indexes for one shard, applied after the bulk load (build_shards.py does this
-- automatically at the end). Safe to re-run.

-- Exact lookup (word_key = $1) and prefix autocomplete (word_key LIKE 'ru%').
-- text_pattern_ops makes LIKE-prefix use the index whatever the database collation.
CREATE INDEX IF NOT EXISTS ix_entry_word_key ON entry (word_key text_pattern_ops);

-- Refresh planner statistics and the visibility map after the load.
VACUUM ANALYZE entry;
