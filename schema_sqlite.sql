-- SQLite schema for the kaikki.org Wiktionary dump.
-- Mirrors schema_postgres.sql. Indexes live in indexes_sqlite.sql.

CREATE TABLE IF NOT EXISTS Word (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    word             TEXT,
    pos              TEXT,
    lang             TEXT,
    lang_code        TEXT,
    etymology_text   TEXT,
    etymology_number TEXT,
    line_number      INTEGER
);

-- Single-row bookmark so an interrupted run can resume in O(1).
CREATE TABLE IF NOT EXISTS Progress (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    last_line INTEGER NOT NULL
);

-- Lookup for the integer discriminator used by WordRelation / SenseRelation.
CREATE TABLE IF NOT EXISTS RelationType (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS WordCategory (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id INTEGER,
    name    TEXT
);

CREATE TABLE IF NOT EXISTS WordWikipedia (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id INTEGER,
    title   TEXT
);

CREATE TABLE IF NOT EXISTS HeadTemplate (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id   INTEGER,
    name      TEXT,
    expansion TEXT
);

CREATE TABLE IF NOT EXISTS EtymologyTemplate (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id   INTEGER,
    name      TEXT,
    expansion TEXT
);

CREATE TABLE IF NOT EXISTS EtymologyTemplateArgs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    etymology_template_id INTEGER,
    arg_key               TEXT,
    arg_value             TEXT
);

CREATE TABLE IF NOT EXISTS Form (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id INTEGER,
    form    TEXT,
    ipa     TEXT,
    roman   TEXT,
    source  TEXT,
    head_nr INTEGER
);

CREATE TABLE IF NOT EXISTS FormTag (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    form_id INTEGER,
    tag     TEXT,
    is_raw  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS Sound (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id   INTEGER,
    ipa       TEXT,
    enpr      TEXT,
    rhymes    TEXT,
    homophone TEXT,
    note      TEXT,
    audio     TEXT,
    ogg_url   TEXT,
    mp3_url   TEXT,
    wav_url   TEXT,
    oga_url   TEXT,
    opus_url  TEXT,
    other     TEXT,
    text      TEXT
);

CREATE TABLE IF NOT EXISTS SoundTag (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sound_id INTEGER,
    tag      TEXT,
    is_raw   INTEGER DEFAULT 0
);

-- Descendants nest through a `descendants` key; parent_id models that tree and
-- depth is the recursion level (0 for a top-level descendant).
CREATE TABLE IF NOT EXISTS Descendant (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id   INTEGER,
    parent_id INTEGER,
    depth     INTEGER,
    lang      TEXT,
    lang_code TEXT,
    word      TEXT,
    roman     TEXT,
    sense     TEXT
);

CREATE TABLE IF NOT EXISTS DescendantTag (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    descendant_id INTEGER,
    tag           TEXT,
    is_raw        INTEGER DEFAULT 0
);

-- Translations only ever appear at word level in kaikki output.
CREATE TABLE IF NOT EXISTS Translation (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id     INTEGER,
    lang        TEXT,
    lang_code   TEXT,
    code        TEXT,
    sense       TEXT,
    word        TEXT,
    roman       TEXT,
    alt         TEXT,
    english     TEXT,
    translation TEXT,
    note        TEXT,
    taxonomic   TEXT
);

CREATE TABLE IF NOT EXISTS TranslationTag (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    translation_id INTEGER,
    tag            TEXT,
    is_raw         INTEGER DEFAULT 0
);

-- All word-level relations (derived, synonyms, antonyms, hyponyms, ...) in one
-- table, discriminated by relation_type. See RelationType and v_WordRelation.
CREATE TABLE IF NOT EXISTS WordRelation (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id       INTEGER,
    relation_type INTEGER,
    word          TEXT,
    sense         TEXT,
    source        TEXT,
    english       TEXT,
    roman         TEXT,
    alt           TEXT,
    qualifier     TEXT,
    taxonomic     TEXT
);

CREATE TABLE IF NOT EXISTS WordRelationTag (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    relation_id INTEGER,
    tag         TEXT,
    is_raw      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS Sense (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    word_id          INTEGER,
    sense_index      INTEGER,
    gloss            TEXT,
    glosses_json     TEXT,
    raw_glosses_json TEXT,
    qualifier        TEXT
);

-- is_raw = 0 for normalised tags, 1 for wiktextract raw_tags.
CREATE TABLE IF NOT EXISTS SenseTag (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id INTEGER,
    tag      TEXT,
    is_raw   INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS SenseTopic (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id INTEGER,
    topic    TEXT
);

CREATE TABLE IF NOT EXISTS SenseCategory (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id INTEGER,
    name     TEXT
);

CREATE TABLE IF NOT EXISTS SenseLink (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id INTEGER,
    text     TEXT,
    target   TEXT
);

CREATE TABLE IF NOT EXISTS SenseExample (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id          INTEGER,
    text              TEXT,
    ref               TEXT,
    type              TEXT,
    english           TEXT,
    translation       TEXT,
    roman             TEXT,
    literal_meaning   TEXT,
    note              TEXT,
    bold_text_offsets TEXT
);

CREATE TABLE IF NOT EXISTS SenseFormOf (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id INTEGER,
    word     TEXT,
    extra    TEXT
);

CREATE TABLE IF NOT EXISTS SenseAltOf (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id INTEGER,
    word     TEXT,
    extra    TEXT
);

CREATE TABLE IF NOT EXISTS SenseWikidata (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id INTEGER,
    code     TEXT
);

CREATE TABLE IF NOT EXISTS SenseRelation (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    sense_id      INTEGER,
    relation_type INTEGER,
    word          TEXT,
    sense         TEXT,
    source        TEXT,
    english       TEXT,
    roman         TEXT,
    alt           TEXT,
    taxonomic     TEXT
);

CREATE TABLE IF NOT EXISTS SenseRelationTag (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    relation_id INTEGER,
    tag         TEXT,
    is_raw      INTEGER DEFAULT 0
);

INSERT OR IGNORE INTO RelationType (id, name) VALUES
    (0,  'derived'),
    (1,  'synonyms'),
    (2,  'antonyms'),
    (3,  'hypernyms'),
    (4,  'hyponyms'),
    (5,  'meronyms'),
    (6,  'holonyms'),
    (7,  'coordinate_terms'),
    (8,  'related'),
    (9,  'troponyms'),
    (10, 'proverbs'),
    (11, 'instances'),
    (12, 'abbreviations');

CREATE VIEW IF NOT EXISTS v_WordRelation AS
SELECT wr.id, wr.word_id, rt.name AS relation, wr.word, wr.sense,
       wr.source, wr.english, wr.roman, wr.alt, wr.qualifier, wr.taxonomic
FROM WordRelation wr JOIN RelationType rt ON rt.id = wr.relation_type;

CREATE VIEW IF NOT EXISTS v_SenseRelation AS
SELECT sr.id, sr.sense_id, rt.name AS relation, sr.word, sr.sense,
       sr.source, sr.english, sr.roman, sr.alt, sr.taxonomic
FROM SenseRelation sr JOIN RelationType rt ON rt.id = sr.relation_type;
