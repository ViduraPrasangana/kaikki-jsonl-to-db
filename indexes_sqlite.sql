-- Indexes and FTS5 search tables for the kaikki SQLite schema.
-- Apply AFTER the bulk load:
--     sqlite3 wikitionary-kaikki-english.db < indexes_sqlite.sql
-- or let the converter do it (this is the default for SQLite):
--     python converter.py --indexes

CREATE INDEX IF NOT EXISTS ix_word_word            ON Word (word, lang_code);
CREATE INDEX IF NOT EXISTS ix_word_line            ON Word (line_number);
CREATE INDEX IF NOT EXISTS ix_wordcategory         ON WordCategory (word_id);
CREATE INDEX IF NOT EXISTS ix_wordwikipedia        ON WordWikipedia (word_id);
CREATE INDEX IF NOT EXISTS ix_headtemplate         ON HeadTemplate (word_id);
CREATE INDEX IF NOT EXISTS ix_etymtemplate         ON EtymologyTemplate (word_id);
CREATE INDEX IF NOT EXISTS ix_etymargs             ON EtymologyTemplateArgs (etymology_template_id);
CREATE INDEX IF NOT EXISTS ix_form                 ON Form (word_id);
CREATE INDEX IF NOT EXISTS ix_form_form            ON Form (form);
CREATE INDEX IF NOT EXISTS ix_formtag              ON FormTag (form_id);
CREATE INDEX IF NOT EXISTS ix_sound                ON Sound (word_id);
CREATE INDEX IF NOT EXISTS ix_soundtag             ON SoundTag (sound_id);
CREATE INDEX IF NOT EXISTS ix_descendant           ON Descendant (word_id);
CREATE INDEX IF NOT EXISTS ix_descendant_parent    ON Descendant (parent_id);
CREATE INDEX IF NOT EXISTS ix_descendanttag        ON DescendantTag (descendant_id);
CREATE INDEX IF NOT EXISTS ix_translation          ON Translation (word_id);
CREATE INDEX IF NOT EXISTS ix_translation_lang     ON Translation (lang_code);
CREATE INDEX IF NOT EXISTS ix_translationtag       ON TranslationTag (translation_id);

-- The word-page query: every relation for one word in a single range scan.
CREATE INDEX IF NOT EXISTS ix_wordrelation         ON WordRelation (word_id, relation_type);
-- Supports corpus-wide scans by relation type.
CREATE INDEX IF NOT EXISTS ix_wordrelation_type    ON WordRelation (relation_type, word_id);
CREATE INDEX IF NOT EXISTS ix_wordrelation_word    ON WordRelation (word);
CREATE INDEX IF NOT EXISTS ix_wordrelationtag      ON WordRelationTag (relation_id);

CREATE INDEX IF NOT EXISTS ix_sense                ON Sense (word_id);
CREATE INDEX IF NOT EXISTS ix_sensetag             ON SenseTag (sense_id);
CREATE INDEX IF NOT EXISTS ix_sensetag_tag         ON SenseTag (tag);
CREATE INDEX IF NOT EXISTS ix_sensetopic           ON SenseTopic (sense_id);
CREATE INDEX IF NOT EXISTS ix_sensecategory        ON SenseCategory (sense_id);
CREATE INDEX IF NOT EXISTS ix_senselink            ON SenseLink (sense_id);
CREATE INDEX IF NOT EXISTS ix_senseexample         ON SenseExample (sense_id);
CREATE INDEX IF NOT EXISTS ix_senseformof          ON SenseFormOf (sense_id);
-- Lemma resolution: "ran" -> "run".
CREATE INDEX IF NOT EXISTS ix_senseformof_word     ON SenseFormOf (word);
CREATE INDEX IF NOT EXISTS ix_sensealtof           ON SenseAltOf (sense_id);
CREATE INDEX IF NOT EXISTS ix_sensealtof_word      ON SenseAltOf (word);
CREATE INDEX IF NOT EXISTS ix_sensewikidata        ON SenseWikidata (sense_id);
CREATE INDEX IF NOT EXISTS ix_senserelation        ON SenseRelation (sense_id, relation_type);
CREATE INDEX IF NOT EXISTS ix_senserelationtag     ON SenseRelationTag (relation_id);

-- ---------------------------------------------------------------------------
-- Full-text search (external-content FTS5 tables mirroring Word and Sense)
--   SELECT w.* FROM WordSearch f JOIN Word w ON w.id = f.rowid
--   WHERE WordSearch MATCH 'colour';
-- ---------------------------------------------------------------------------

CREATE VIRTUAL TABLE IF NOT EXISTS WordSearch
    USING fts5(word, content='Word', content_rowid='id');
INSERT INTO WordSearch(WordSearch) VALUES('delete-all');
INSERT INTO WordSearch(rowid, word) SELECT id, word FROM Word;

CREATE VIRTUAL TABLE IF NOT EXISTS SenseSearch
    USING fts5(gloss, content='Sense', content_rowid='id');
INSERT INTO SenseSearch(SenseSearch) VALUES('delete-all');
INSERT INTO SenseSearch(rowid, gloss) SELECT id, gloss FROM Sense;

ANALYZE;
