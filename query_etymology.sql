-- Etymology for a word.
--
--     word                  (id, word, pos, lang_code, etymology_text, etymology_number)
--     etymologytemplate     (id, word_id, name, expansion)
--     etymologytemplateargs (id, etymology_template_id, arg_key, arg_value)
--
-- etymology_text is the human-readable prose and is what you render.
-- The templates are the structured source behind it: name is the Wiktionary
-- template ("inh", "bor", "der", "cog"), expansion is the rendered fragment,
-- and the args carry the language codes and words it was built from.
--
-- etymology_number separates homographs with unrelated origins (bass the fish
-- vs bass the sound). Two rows with the same word and different
-- etymology_number are genuinely different etymologies, not duplicates.
--
-- REQUIRED INDEXES (see indexes_postgres.sql):
--     CREATE INDEX ix_word_word    ON Word (word, lang_code);
--     CREATE INDEX ix_etymtemplate ON EtymologyTemplate (word_id);
--     CREATE INDEX ix_etymargs     ON EtymologyTemplateArgs (etymology_template_id);


-- ---------------------------------------------------------------------------
-- 1. Just the prose. This is what most callers want.
-- ---------------------------------------------------------------------------
SELECT
    w.id            AS word_id,
    w.word,
    w.pos,
    w.lang_code,
    w.etymology_number,
    w.etymology_text
FROM word w
WHERE w.word = 'run'
  AND w.lang_code = 'en'
  AND w.etymology_text IS NOT NULL
ORDER BY w.etymology_number NULLS FIRST, w.id;


-- ---------------------------------------------------------------------------
-- 2. Prose plus the templates behind it, one row per template.
-- ---------------------------------------------------------------------------
SELECT
    w.id        AS word_id,
    w.pos,
    w.etymology_text,
    et.id       AS template_id,
    et.name     AS template,
    et.expansion
FROM word w
JOIN etymologytemplate et ON et.word_id = w.id
WHERE w.word = 'run'
  AND w.lang_code = 'en'
ORDER BY w.id, et.id;


-- ---------------------------------------------------------------------------
-- 3. Templates with their arguments folded into a JSON object.
--
--    Args are keyed by position ("1", "2", "3") for the main operands and by
--    name for the rest, exactly as in the Wiktionary source, so "1" is usually
--    the target language and "2" the source language.
-- ---------------------------------------------------------------------------
SELECT
    w.id    AS word_id,
    w.pos,
    et.name AS template,
    et.expansion,
    COALESCE(
        jsonb_object_agg(eta.arg_key, eta.arg_value)
            FILTER (WHERE eta.arg_key IS NOT NULL),
        '{}'::jsonb) AS args
FROM word w
JOIN etymologytemplate et      ON et.word_id = w.id
LEFT JOIN etymologytemplateargs eta ON eta.etymology_template_id = et.id
WHERE w.word = 'run'
  AND w.lang_code = 'en'
GROUP BY w.id, w.pos, et.id, et.name, et.expansion
ORDER BY w.id, et.id;


-- ---------------------------------------------------------------------------
-- 4. One JSON document per entry: prose + nested templates.
-- ---------------------------------------------------------------------------
SELECT jsonb_agg(entry ORDER BY word_id) AS data
FROM (
    SELECT
        w.id AS word_id,
        jsonb_build_object(
            'word_id',          w.id,
            'word',             w.word,
            'pos',              w.pos,
            'etymology_number', w.etymology_number,
            'etymology_text',   w.etymology_text,
            'templates',        COALESCE((
                SELECT jsonb_agg(
                           jsonb_build_object(
                               'name',      et.name,
                               'expansion', et.expansion,
                               'args',      COALESCE((
                                   SELECT jsonb_object_agg(a.arg_key, a.arg_value)
                                   FROM etymologytemplateargs a
                                   WHERE a.etymology_template_id = et.id
                               ), '{}'::jsonb))
                           ORDER BY et.id)
                FROM etymologytemplate et
                WHERE et.word_id = w.id
            ), '[]'::jsonb)
        ) AS entry
    FROM word w
    WHERE w.word = 'run'
      AND w.lang_code = 'en'
) s;


-- ---------------------------------------------------------------------------
-- 5. Source languages a word was borrowed or inherited from.
--    Handy for "words of Latin origin" style browsing.
-- ---------------------------------------------------------------------------
SELECT DISTINCT
    et.name AS relation,
    eta.arg_value AS source_lang_code
FROM word w
JOIN etymologytemplate et       ON et.word_id = w.id
JOIN etymologytemplateargs eta  ON eta.etymology_template_id = et.id
WHERE w.word = 'run'
  AND w.lang_code = 'en'
  AND et.name IN ('inh', 'bor', 'der', 'lbor', 'obor')
  AND eta.arg_key = '2'
ORDER BY relation, source_lang_code;
