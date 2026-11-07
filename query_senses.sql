-- Senses and their examples for a word, looked up by the word itself.
--
--     sense        (id, word_id, sense_index, gloss, glosses_json, raw_glosses_json, qualifier)
--     senseexample (id, sense_id, text, ref, type, english, translation, roman,
--                   literal_meaning, note, bold_text_offsets)
--     sensetag     (sense_id, tag_id, is_raw)  -> tag (id, name)
--     sensetopic   (id, sense_id, topic)
--     senselink    (id, sense_id, text, target)
--
-- Every query starts from the same `target` CTE, so you change the lookup in
-- one place. One word text can match several rows: 'run' is a verb, a noun and
-- an adjective in English alone, and exists in other languages too. Keep
-- word_id and pos in the output so callers can tell them apart, and keep the
-- lang_code filter unless you really want all 910 languages.
--
-- Matching is case-sensitive, the same as the backend's FindWordsByText
-- (`WHERE word = $1`). For case-insensitive lookup use the variant in the CTE
-- comment; the database already has idx_word_lower ON word (LOWER(word)) to
-- support it.
--
-- gloss is the most specific definition. glosses_json is the full hierarchical
-- path as a JSON array, so a nested sense reads
-- ["to move quickly", "of a person"] with gloss = "of a person".
--
-- senseexample.type, measured on this database:
--     quotation  879,722   attested citation, `ref` names the source
--     example    565,024   short usage example, usually no ref
--     NULL        91,241   untyped, but usually reads like a usage example
--
-- REQUIRED INDEXES (see indexes_postgres.sql):
--     CREATE INDEX ix_word_word    ON Word (word, lang_code);
--     CREATE INDEX ix_sense        ON Sense (word_id);
--     CREATE INDEX ix_senseexample ON SenseExample (sense_id);
--     CREATE INDEX ix_sensetag     ON SenseTag (sense_id);


-- ---------------------------------------------------------------------------
-- 1. Senses with their register/domain tags.
-- ---------------------------------------------------------------------------
WITH target AS (
    SELECT id, word, pos, lang_code
    FROM word
    WHERE word = 'run'          -- case-insensitive: LOWER(word) = LOWER('run')
      AND lang_code = 'en'
)
SELECT
    w.id   AS word_id,
    w.word,
    w.pos,
    s.id   AS sense_id,
    s.sense_index,
    s.gloss,
    s.qualifier,
    string_agg(t.name, ', ' ORDER BY t.name)
        FILTER (WHERE st.is_raw = 0) AS tags
FROM target w
JOIN sense s          ON s.word_id = w.id
LEFT JOIN sensetag st ON st.sense_id = s.id
LEFT JOIN tag t       ON t.id = st.tag_id
GROUP BY w.id, w.word, w.pos, s.id, s.sense_index, s.gloss, s.qualifier
ORDER BY w.id, s.sense_index;


-- ---------------------------------------------------------------------------
-- 2. Senses with their examples, one row per example.
--    LEFT JOIN so senses without examples still appear.
-- ---------------------------------------------------------------------------
WITH target AS (
    SELECT id, word, pos
    FROM word
    WHERE word = 'run' AND lang_code = 'en'
)
SELECT
    w.id        AS word_id,
    w.word,
    w.pos,
    s.id        AS sense_id,
    s.sense_index,
    s.gloss,
    e.type      AS example_type,
    e.text      AS example,
    e.ref,
    e.english,
    e.translation
FROM target w
JOIN sense s             ON s.word_id = w.id
LEFT JOIN senseexample e ON e.sense_id = s.id
ORDER BY w.id, s.sense_index, e.id;


-- ---------------------------------------------------------------------------
-- 3. Usage examples only, no quotations.
--
--    `type IS DISTINCT FROM 'quotation'` rather than `type = 'example'`, so the
--    91k untyped rows are kept: they read like usage examples, and
--    `type = 'example'` would silently drop them.
-- ---------------------------------------------------------------------------
WITH target AS (
    SELECT id, word, pos
    FROM word
    WHERE word = 'run' AND lang_code = 'en'
)
SELECT
    w.word,
    w.pos,
    s.id   AS sense_id,
    s.gloss,
    e.text AS example
FROM target w
JOIN sense s        ON s.word_id = w.id
JOIN senseexample e ON e.sense_id = s.id
WHERE e.type IS DISTINCT FROM 'quotation'
ORDER BY w.id, s.sense_index, e.id;


-- ---------------------------------------------------------------------------
-- 4. Quotations only, with their source.
-- ---------------------------------------------------------------------------
WITH target AS (
    SELECT id, word, pos
    FROM word
    WHERE word = 'run' AND lang_code = 'en'
)
SELECT
    w.word,
    s.gloss,
    e.ref,
    e.text AS quotation
FROM target w
JOIN sense s        ON s.word_id = w.id
JOIN senseexample e ON e.sense_id = s.id
WHERE e.type = 'quotation'
ORDER BY w.id, s.sense_index, e.id;


-- ---------------------------------------------------------------------------
-- 5. Gloss hierarchy, for sub-senses that need their parent's wording.
-- ---------------------------------------------------------------------------
WITH target AS (
    SELECT id, word, pos
    FROM word
    WHERE word = 'run' AND lang_code = 'en'
)
SELECT
    w.word,
    w.pos,
    s.id AS sense_id,
    s.sense_index,
    jsonb_array_length(s.glosses_json::jsonb) AS depth,
    s.gloss                                   AS leaf_gloss,
    array_to_string(
        ARRAY(SELECT jsonb_array_elements_text(s.glosses_json::jsonb)),
        ' > ')                                AS gloss_path
FROM target w
JOIN sense s ON s.word_id = w.id
ORDER BY w.id, s.sense_index;


-- ---------------------------------------------------------------------------
-- 6. Everything for the word as one JSON document per entry:
--    senses, each with tags, topics, links and examples nested.
-- ---------------------------------------------------------------------------
WITH target AS (
    SELECT id, word, pos, lang_code
    FROM word
    WHERE word = 'run' AND lang_code = 'en'
)
SELECT jsonb_agg(entry ORDER BY word_id) AS data
FROM (
    SELECT
        w.id AS word_id,
        jsonb_build_object(
            'word_id',   w.id,
            'word',      w.word,
            'pos',       w.pos,
            'lang_code', w.lang_code,
            'senses', COALESCE((
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'sense_id',   s.id,
                        'gloss',      s.gloss,
                        'gloss_path', s.glosses_json::jsonb,
                        'qualifier',  s.qualifier,
                        'tags', COALESCE((
                            SELECT jsonb_agg(t.name ORDER BY t.name)
                            FROM sensetag st JOIN tag t ON t.id = st.tag_id
                            WHERE st.sense_id = s.id AND st.is_raw = 0), '[]'::jsonb),
                        'topics', COALESCE((
                            SELECT jsonb_agg(tp.topic ORDER BY tp.topic)
                            FROM sensetopic tp WHERE tp.sense_id = s.id), '[]'::jsonb),
                        'links', COALESCE((
                            SELECT jsonb_agg(jsonb_build_object('text', l.text, 'target', l.target)
                                             ORDER BY l.id)
                            FROM senselink l WHERE l.sense_id = s.id), '[]'::jsonb),
                        'examples', COALESCE((
                            SELECT jsonb_agg(jsonb_build_object(
                                       'type',        e.type,
                                       'text',        e.text,
                                       'ref',         e.ref,
                                       'english',     e.english,
                                       'translation', e.translation)
                                   ORDER BY e.id)
                            FROM senseexample e WHERE e.sense_id = s.id), '[]'::jsonb)
                    ) ORDER BY s.sense_index)
                FROM sense s WHERE s.word_id = w.id
            ), '[]'::jsonb)
        ) AS entry
    FROM target w
) x;


-- ---------------------------------------------------------------------------
-- 7. Senses of the word filtered to a register, e.g. only the slang ones.
-- ---------------------------------------------------------------------------
SELECT w.word, w.pos, s.gloss
FROM word w
JOIN sense s     ON s.word_id = w.id
JOIN sensetag st ON st.sense_id = s.id
JOIN tag t       ON t.id = st.tag_id
WHERE w.word = 'run'
  AND w.lang_code = 'en'
  AND t.name = 'slang'
ORDER BY w.id, s.sense_index;


-- ---------------------------------------------------------------------------
-- 8. All languages for a word, so you can see what a bare text match returns.
--    The backend's FindWordsByText does no lang filtering, so this is what it
--    actually gets back.
-- ---------------------------------------------------------------------------
SELECT
    w.id AS word_id,
    w.lang,
    w.lang_code,
    w.pos,
    count(s.id) AS senses
FROM word w
LEFT JOIN sense s ON s.word_id = w.id
WHERE w.word = 'run'
GROUP BY w.id, w.lang, w.lang_code, w.pos
ORDER BY w.lang_code, w.pos;
