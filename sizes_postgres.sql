-- Size reporting for the kaikki PostgreSQL database.
-- Read-only. Run the whole file, or copy out the query you need:
--     psql -d wiktionary -f sizes_postgres.sql
--
-- Terminology, because PostgreSQL's functions overlap confusingly:
--   pg_relation_size()       heap only (the main fork)
--   pg_table_size()          heap + TOAST + free space map + visibility map
--   pg_indexes_size()        all indexes on the table
--   pg_total_relation_size() pg_table_size + pg_indexes_size  <- the real cost

\echo ''
\echo '=== 1. Database total ==='
SELECT pg_size_pretty(pg_database_size(current_database())) AS database_size;

\echo ''
\echo '=== 2. Every table: rows, heap, toast, indexes, total ==='
SELECT
    c.relname                                            AS table,
    s.n_live_tup                                         AS rows,
    pg_size_pretty(pg_relation_size(c.oid))              AS heap,
    pg_size_pretty(COALESCE(pg_total_relation_size(c.reltoastrelid), 0)) AS toast,
    pg_size_pretty(pg_indexes_size(c.oid))               AS indexes,
    pg_size_pretty(pg_total_relation_size(c.oid))        AS total,
    round(100.0 * pg_total_relation_size(c.oid)
          / NULLIF(SUM(pg_total_relation_size(c.oid)) OVER (), 0), 1) AS pct
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE n.nspname = 'public' AND c.relkind = 'r'
ORDER BY pg_total_relation_size(c.oid) DESC;

\echo ''
\echo '=== 3. Heap vs index vs toast, whole schema ==='
SELECT
    pg_size_pretty(SUM(pg_relation_size(c.oid)))                                AS heap,
    pg_size_pretty(SUM(COALESCE(pg_total_relation_size(c.reltoastrelid), 0)))   AS toast,
    pg_size_pretty(SUM(pg_indexes_size(c.oid)))                                 AS indexes,
    pg_size_pretty(SUM(pg_total_relation_size(c.oid)))                          AS total,
    SUM(COALESCE((SELECT n_live_tup FROM pg_stat_user_tables WHERE relid = c.oid), 0)) AS rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r';

\echo ''
\echo '=== 4. Every index, largest first (idx_scan = times used) ==='
SELECT
    i.indexrelid::regclass::text                    AS index,
    i.indrelid::regclass::text                      AS table,
    pg_size_pretty(pg_relation_size(i.indexrelid))  AS size,
    s.idx_scan                                      AS scans,
    i.indisprimary                                  AS is_pk,
    i.indisunique                                   AS is_unique
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.indexrelid
WHERE n.nspname = 'public'
ORDER BY pg_relation_size(i.indexrelid) DESC;

\echo ''
\echo '=== 5. Indexes never used (candidates to drop) ==='
-- Only meaningful after the database has served real queries; a freshly loaded
-- database will list everything here.
SELECT
    s.indexrelid::regclass::text                     AS index,
    s.relid::regclass::text                          AS table,
    pg_size_pretty(pg_relation_size(s.indexrelid))   AS wasted
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.idx_scan = 0 AND NOT i.indisprimary AND NOT i.indisunique
ORDER BY pg_relation_size(s.indexrelid) DESC;

\echo ''
\echo '=== 6. Per-column bytes (what is actually taking the space) ==='
-- avg_width comes from ANALYZE, so run ANALYZE first for accurate numbers.
-- n_distinct < 0 means "fraction of rows", so it is converted back here.
SELECT
    st.tablename                                              AS table,
    st.attname                                                AS column,
    pg_size_pretty((st.avg_width::bigint * t.n_live_tup))      AS est_bytes,
    st.avg_width                                              AS avg_width,
    CASE WHEN st.n_distinct < 0
         THEN round((-st.n_distinct * t.n_live_tup)::numeric)
         ELSE round(st.n_distinct::numeric) END               AS distinct_vals,
    CASE WHEN st.n_distinct < 0 AND st.n_distinct <> 0
         THEN round((1 / -st.n_distinct)::numeric, 1)
         WHEN st.n_distinct > 0
         THEN round((t.n_live_tup / st.n_distinct)::numeric, 1)
         ELSE NULL END                                        AS repeats
FROM pg_stats st
JOIN pg_stat_user_tables t ON t.relname = st.tablename
WHERE st.schemaname = 'public' AND t.n_live_tup > 0
ORDER BY (st.avg_width::bigint * t.n_live_tup) DESC
LIMIT 40;

\echo ''
\echo '=== 7. Table bloat estimate (dead rows awaiting VACUUM) ==='
SELECT
    relname                                        AS table,
    n_live_tup                                     AS live,
    n_dead_tup                                     AS dead,
    CASE WHEN n_live_tup > 0
         THEN round(100.0 * n_dead_tup / n_live_tup, 1) ELSE 0 END AS pct_dead,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE n_dead_tup > 0
ORDER BY n_dead_tup DESC;

\echo ''
\echo '=== 8. Largest relations of any kind, including TOAST ==='
SELECT
    n.nspname                                       AS schema,
    c.relname                                       AS relation,
    CASE c.relkind WHEN 'r' THEN 'table'
                   WHEN 'i' THEN 'index'
                   WHEN 't' THEN 'toast'
                   WHEN 'm' THEN 'matview'
                   ELSE c.relkind::text END         AS kind,
    pg_size_pretty(pg_relation_size(c.oid))         AS size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname IN ('public', 'pg_toast')
ORDER BY pg_relation_size(c.oid) DESC
LIMIT 25;
