-- Get the inflected forms and their tags for a word.
--
-- Schema these run against (after normalisation):
--     word    (id, word, pos, lang, lang_code, ...)
--     form    (id, word_id, form, ipa, roman, source, head_nr)
--     formtag (form_id, tag_id, is_raw)      -- no surrogate id
--     tag     (id, name)                     -- shared vocabulary, ~37k rows
--
-- REQUIRED INDEXES. Without them every query here sequentially scans
-- form (96M rows) and formtag (310M rows). Measured on the live database with
-- no indexes: 15 seconds to fetch the 16 formtag rows for a single word.
--
--     CREATE INDEX IF NOT EXISTS ix_word_word ON Word (word, lang_code);
--     CREATE INDEX IF NOT EXISTS ix_form      ON Form (word_id);
--     CREATE INDEX IF NOT EXISTS ix_formtag   ON FormTag (form_id);
--
-- Building ix_formtag on 310M rows takes a while; run it once, in a screen.


-- ---------------------------------------------------------------------------
-- 1. One row per form, tags collapsed into a string. Best for eyeballing.
-- ---------------------------------------------------------------------------
SELECT
    w.id                                                          AS word_id,
    w.word,
    w.pos,
    w.lang_code,
    f.id                                                          AS form_id,
    f.form,
    string_agg(t.name, ', ' ORDER BY t.name)
        FILTER (WHERE ft.is_raw = 0)                              AS tags,
    string_agg(t.name, ', ' ORDER BY t.name)
        FILTER (WHERE ft.is_raw = 1)                              AS raw_tags
FROM word w
JOIN form f       ON f.word_id = w.id
LEFT JOIN formtag ft ON ft.form_id = f.id
LEFT JOIN tag t      ON t.id = ft.tag_id
WHERE w.word = 'run'
  AND w.lang_code = 'en'
GROUP BY w.id, w.word, w.pos, w.lang_code, f.id, f.form
ORDER BY w.id, f.id;


-- ---------------------------------------------------------------------------
-- 2. One row per (form, tag). Better when the client does its own grouping.
-- ---------------------------------------------------------------------------
SELECT
    f.word_id,
    f.id      AS form_id,
    f.form,
    t.id      AS tag_id,
    t.name    AS tag,
    ft.is_raw
FROM form f
LEFT JOIN formtag ft ON ft.form_id = f.id
LEFT JOIN tag t      ON t.id = ft.tag_id
WHERE f.word_id = 33984
ORDER BY f.id, t.name;


-- ---------------------------------------------------------------------------
-- 3. Same shape the API returns: forms with a nested tags array.
--    Drop-in replacement for the two queries in internal/repository/form.go.
-- ---------------------------------------------------------------------------
SELECT jsonb_agg(form_json ORDER BY form_id) AS data
FROM (
    SELECT
        f.id AS form_id,
        jsonb_build_object(
            'id',      f.id,
            'word_id', f.word_id,
            'form',    f.form,
            'tags',    COALESCE(
                jsonb_agg(
                    jsonb_build_object('form_id', ft.form_id, 'tag', t.name)
                    ORDER BY t.name
                ) FILTER (WHERE t.id IS NOT NULL),
                '[]'::jsonb)
        ) AS form_json
    FROM form f
    LEFT JOIN formtag ft ON ft.form_id = f.id
    LEFT JOIN tag t      ON t.id = ft.tag_id
    WHERE f.word_id = ANY (ARRAY[33984, 33985]::bigint[])
    GROUP BY f.id, f.word_id, f.form
) s;


-- ---------------------------------------------------------------------------
-- 4. Real forms only.
--
-- ~6.9% of form rows are wiktextract table-rendering metadata rather than word
-- forms: entries like 'no-table-tags', 'inflection-box-top', 'ro-noun-n',
-- carrying the tags 'table-tags' or 'inflection-template'. Exclude them before
-- showing forms to a user.
-- ---------------------------------------------------------------------------
SELECT
    f.id      AS form_id,
    f.form,
    string_agg(t.name, ', ' ORDER BY t.name) AS tags
FROM form f
JOIN formtag ft ON ft.form_id = f.id
JOIN tag t      ON t.id = ft.tag_id
WHERE f.word_id = 33984
  AND NOT EXISTS (
        SELECT 1
        FROM formtag ft2
        JOIN tag t2 ON t2.id = ft2.tag_id
        WHERE ft2.form_id = f.id
          AND t2.name IN ('table-tags', 'inflection-template')
  )
GROUP BY f.id, f.form
ORDER BY f.id;


-- ---------------------------------------------------------------------------
-- 5. Reverse direction: every form carrying a given tag.
--    Needs ix_formtag_tag ON FormTag (tag_id, form_id) to be quick.
-- ---------------------------------------------------------------------------
SELECT w.word, w.lang_code, f.form
FROM tag t
JOIN formtag ft ON ft.tag_id = t.id
JOIN form f     ON f.id = ft.form_id
JOIN word w     ON w.id = f.word_id
WHERE t.name = 'genitive'
  AND w.lang_code = 'en'
LIMIT 50;
