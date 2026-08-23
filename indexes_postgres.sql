-- Indexes, foreign keys and full-text search for the kaikki PostgreSQL schema.
-- Apply AFTER the bulk load has finished:
--     psql -d kaikki -f indexes_postgres.sql
-- or let the converter do it:
--     python converter.py --backend postgres --dsn ... --indexes
--
-- Building these before the load would slow the import down by an order of
-- magnitude, which is why they are kept out of schema_postgres.sql.

-- ---------------------------------------------------------------------------
-- Lookup indexes
-- ---------------------------------------------------------------------------

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
-- Full-text search
-- ---------------------------------------------------------------------------

-- Definition search:
--   SELECT * FROM Sense
--   WHERE to_tsvector('english', coalesce(gloss, '')) @@ plainto_tsquery('english', 'domesticated canine');
CREATE INDEX IF NOT EXISTS ix_sense_gloss_fts
    ON Sense USING GIN (to_tsvector('english', coalesce(gloss, '')));

-- Fuzzy / prefix headword lookup:
--   SELECT * FROM Word WHERE word % 'colur' ORDER BY similarity(word, 'colur') DESC;
-- Wrapped so that a server without pg_trgm available skips just this index
-- instead of aborting the whole file and leaving you with no indexes at all.
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    CREATE INDEX IF NOT EXISTS ix_word_word_trgm ON Word USING GIN (word gin_trgm_ops);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'pg_trgm unavailable, skipping fuzzy headword index (%)', SQLERRM;
END
$$;

-- ---------------------------------------------------------------------------
-- Foreign keys
--
-- Kept here rather than in the schema so the load runs without per-row parent
-- checks. Drop this section if you would rather not enforce referential
-- integrity. Adding them validates the existing rows, which takes a while on a
-- full dump; append NOT VALID to skip that and validate later.
-- ---------------------------------------------------------------------------

ALTER TABLE WordCategory          ADD CONSTRAINT fk_wordcategory_word      FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE WordWikipedia         ADD CONSTRAINT fk_wordwikipedia_word     FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE HeadTemplate          ADD CONSTRAINT fk_headtemplate_word      FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE EtymologyTemplate     ADD CONSTRAINT fk_etymtemplate_word      FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE EtymologyTemplateArgs ADD CONSTRAINT fk_etymargs_template      FOREIGN KEY (etymology_template_id) REFERENCES EtymologyTemplate (id);
ALTER TABLE Form                  ADD CONSTRAINT fk_form_word              FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE FormTag               ADD CONSTRAINT fk_formtag_form           FOREIGN KEY (form_id)               REFERENCES Form (id);
ALTER TABLE Sound                 ADD CONSTRAINT fk_sound_word             FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE SoundTag              ADD CONSTRAINT fk_soundtag_sound         FOREIGN KEY (sound_id)              REFERENCES Sound (id);
ALTER TABLE Descendant            ADD CONSTRAINT fk_descendant_word        FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE Descendant            ADD CONSTRAINT fk_descendant_parent      FOREIGN KEY (parent_id)             REFERENCES Descendant (id);
ALTER TABLE DescendantTag         ADD CONSTRAINT fk_descendanttag_desc     FOREIGN KEY (descendant_id)         REFERENCES Descendant (id);
ALTER TABLE Translation           ADD CONSTRAINT fk_translation_word       FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE TranslationTag        ADD CONSTRAINT fk_translationtag_trans   FOREIGN KEY (translation_id)        REFERENCES Translation (id);
ALTER TABLE WordRelation          ADD CONSTRAINT fk_wordrelation_word      FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE WordRelation          ADD CONSTRAINT fk_wordrelation_type      FOREIGN KEY (relation_type)         REFERENCES RelationType (id);
ALTER TABLE WordRelationTag       ADD CONSTRAINT fk_wordrelationtag_rel    FOREIGN KEY (relation_id)           REFERENCES WordRelation (id);
ALTER TABLE Sense                 ADD CONSTRAINT fk_sense_word             FOREIGN KEY (word_id)               REFERENCES Word (id);
ALTER TABLE SenseTag              ADD CONSTRAINT fk_sensetag_sense         FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseTopic            ADD CONSTRAINT fk_sensetopic_sense       FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseCategory         ADD CONSTRAINT fk_sensecategory_sense    FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseLink             ADD CONSTRAINT fk_senselink_sense        FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseExample          ADD CONSTRAINT fk_senseexample_sense     FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseFormOf           ADD CONSTRAINT fk_senseformof_sense      FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseAltOf            ADD CONSTRAINT fk_sensealtof_sense       FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseWikidata         ADD CONSTRAINT fk_sensewikidata_sense    FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseRelation         ADD CONSTRAINT fk_senserelation_sense    FOREIGN KEY (sense_id)              REFERENCES Sense (id);
ALTER TABLE SenseRelation         ADD CONSTRAINT fk_senserelation_type     FOREIGN KEY (relation_type)         REFERENCES RelationType (id);
ALTER TABLE SenseRelationTag      ADD CONSTRAINT fk_senserelationtag_rel   FOREIGN KEY (relation_id)           REFERENCES SenseRelation (id);

ANALYZE;
