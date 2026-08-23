"""Insert layer for the kaikki.org Wiktionary dump.

Backend agnostic: every statement is written with SQLite-style ``?``
placeholders against the cursor protocol in backends.py. Parent rows go through
``cur.insert`` (which returns the generated id); uniform child rows are batched
through ``cur.run_many``.

The table definitions themselves live in schema_sqlite.sql / schema_postgres.sql.
"""

import json

# Relation types are stored as small integers in WordRelation / SenseRelation.
# Keep in step with the RelationType seed rows in the schema files.
RELATION_TYPES = [
    'derived', 'synonyms', 'antonyms', 'hypernyms', 'hyponyms',
    'meronyms', 'holonyms', 'coordinate_terms', 'related', 'troponyms',
    'proverbs', 'instances', 'abbreviations',
]
RELATION_ID = {name: i for i, name in enumerate(RELATION_TYPES)}

# Which relation keys to harvest at each level (kaikki emits them in both places).
WORD_RELATIONS = RELATION_TYPES
SENSE_RELATIONS = [
    'synonyms', 'antonyms', 'hypernyms', 'hyponyms', 'meronyms',
    'holonyms', 'coordinate_terms', 'related', 'troponyms',
]


def read_last_processed_line(cur):
    row = cur.fetchone("SELECT last_line FROM Progress WHERE id = 1")
    if row is not None:
        return row[0]
    row = cur.fetchone("SELECT MAX(line_number) FROM Word")
    return row[0] if row and row[0] is not None else 0


def write_progress(cur, line_number):
    cur.run("INSERT INTO Progress (id, last_line) VALUES (1, ?) "
            "ON CONFLICT (id) DO UPDATE SET last_line = excluded.last_line",
            (line_number,))


def _insert_tags(cur, table, fk_col, fk_id, tags, raw_tags):
    rows = [(fk_id, tag, 0) for tag in tags or []]
    rows += [(fk_id, tag, 1) for tag in raw_tags or []]
    cur.run_many(f"INSERT INTO {table} ({fk_col}, tag, is_raw) VALUES (?, ?, ?)", rows)


def insert_word(cur, data, line_number):
    word_id = cur.insert("""
        INSERT INTO Word (word, pos, lang, lang_code, etymology_text, etymology_number, line_number)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get('word'),
        data.get('pos'),
        data.get('lang'),
        data.get('lang_code'),
        data.get('etymology_text'),
        data.get('etymology_number'),
        line_number,
    ))

    cur.run_many("INSERT INTO WordCategory (word_id, name) VALUES (?, ?)",
                 [(word_id, name) for name in data.get('categories') or []])
    cur.run_many("INSERT INTO WordWikipedia (word_id, title) VALUES (?, ?)",
                 [(word_id, title) for title in data.get('wikipedia') or []])
    return word_id


def insert_head_template(cur, word_id, templates):
    cur.run_many("INSERT INTO HeadTemplate (word_id, name, expansion) VALUES (?, ?, ?)",
                 [(word_id, t.get('name'), t.get('expansion')) for t in templates or []])


def insert_forms(cur, word_id, forms):
    for form in forms or []:
        form_id = cur.insert("""
            INSERT INTO Form (word_id, form, ipa, roman, source, head_nr)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            word_id,
            form.get('form'),
            form.get('ipa'),
            form.get('roman'),
            form.get('source'),
            form.get('head_nr'),
        ))
        _insert_tags(cur, 'FormTag', 'form_id', form_id,
                     form.get('tags'), form.get('raw_tags'))


def insert_sounds(cur, word_id, sounds):
    for sound in sounds or []:
        sound_id = cur.insert("""
            INSERT INTO Sound (word_id, ipa, enpr, rhymes, homophone, note, audio,
                               ogg_url, mp3_url, wav_url, oga_url, opus_url, other, text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            word_id,
            sound.get('ipa'),
            sound.get('enpr'),
            sound.get('rhymes'),
            sound.get('homophone'),
            sound.get('note'),
            sound.get('audio'),
            sound.get('ogg_url'),
            sound.get('mp3_url'),
            sound.get('wav_url'),
            sound.get('oga_url'),
            sound.get('opus_url'),
            sound.get('other'),
            sound.get('text'),
        ))
        _insert_tags(cur, 'SoundTag', 'sound_id', sound_id,
                     sound.get('tags'), sound.get('raw_tags'))


def insert_etymology_templates(cur, word_id, templates):
    for template in templates or []:
        template_id = cur.insert("""
            INSERT INTO EtymologyTemplate (word_id, name, expansion) VALUES (?, ?, ?)
        """, (word_id, template.get('name'), template.get('expansion')))
        cur.run_many("""
            INSERT INTO EtymologyTemplateArgs (etymology_template_id, arg_key, arg_value)
            VALUES (?, ?, ?)
        """, [
            (template_id, key, value if isinstance(value, str) else json.dumps(value))
            for key, value in (template.get('args') or {}).items()
        ])


def insert_descendants(cur, word_id, descendants, parent_id=None, depth=0):
    """Descendants nest via a `descendants` key; depth comes from the recursion."""
    for descendant in descendants or []:
        descendant_id = cur.insert("""
            INSERT INTO Descendant (word_id, parent_id, depth, lang, lang_code, word, roman, sense)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            word_id,
            parent_id,
            depth,
            descendant.get('lang'),
            descendant.get('lang_code'),
            descendant.get('word'),
            descendant.get('roman'),
            descendant.get('sense'),
        ))
        _insert_tags(cur, 'DescendantTag', 'descendant_id', descendant_id,
                     descendant.get('tags'), descendant.get('raw_tags'))
        insert_descendants(cur, word_id, descendant.get('descendants'), descendant_id, depth + 1)


def insert_translations(cur, word_id, translations):
    """Translations only ever appear at word level in kaikki output."""
    for translation in translations or []:
        translation_id = cur.insert("""
            INSERT INTO Translation (word_id, lang, lang_code, code, sense, word,
                                     roman, alt, english, translation, note, taxonomic)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            word_id,
            translation.get('lang'),
            translation.get('lang_code'),
            translation.get('code'),
            translation.get('sense'),
            translation.get('word'),
            translation.get('roman'),
            translation.get('alt'),
            translation.get('english'),
            translation.get('translation'),
            translation.get('note'),
            translation.get('taxonomic'),
        ))
        _insert_tags(cur, 'TranslationTag', 'translation_id', translation_id,
                     translation.get('tags'), translation.get('raw_tags'))


def insert_word_relations(cur, word_id, data):
    for name in WORD_RELATIONS:
        for item in data.get(name) or []:
            relation_id = cur.insert("""
                INSERT INTO WordRelation (word_id, relation_type, word, sense, source,
                                          english, roman, alt, qualifier, taxonomic)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                word_id,
                RELATION_ID[name],
                item.get('word'),
                item.get('sense'),
                item.get('source'),
                item.get('english'),
                item.get('roman'),
                item.get('alt'),
                item.get('qualifier'),
                item.get('taxonomic'),
            ))
            _insert_tags(cur, 'WordRelationTag', 'relation_id', relation_id,
                         item.get('tags'), item.get('raw_tags'))


def insert_senses(cur, word_id, senses):
    for index, sense in enumerate(senses or []):
        glosses = sense.get('glosses') or []
        sense_id = cur.insert("""
            INSERT INTO Sense (word_id, sense_index, gloss, glosses_json, raw_glosses_json, qualifier)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            word_id,
            index,
            glosses[-1] if glosses else None,
            json.dumps(glosses),
            json.dumps(sense.get('raw_glosses') or []),
            sense.get('qualifier'),
        ))

        _insert_tags(cur, 'SenseTag', 'sense_id', sense_id,
                     sense.get('tags'), sense.get('raw_tags'))
        cur.run_many("INSERT INTO SenseTopic (sense_id, topic) VALUES (?, ?)",
                     [(sense_id, topic) for topic in sense.get('topics') or []])
        cur.run_many("INSERT INTO SenseCategory (sense_id, name) VALUES (?, ?)",
                     [(sense_id, name) for name in sense.get('categories') or []])
        cur.run_many("INSERT INTO SenseWikidata (sense_id, code) VALUES (?, ?)",
                     [(sense_id, code) for code in sense.get('wikidata') or []])

        insert_sense_links(cur, sense_id, sense.get('links'))
        insert_sense_examples(cur, sense_id, sense.get('examples'))
        insert_sense_form_of(cur, sense_id, sense.get('form_of'), 'SenseFormOf')
        insert_sense_form_of(cur, sense_id, sense.get('alt_of'), 'SenseAltOf')
        insert_sense_relations(cur, sense_id, sense)


def insert_sense_links(cur, sense_id, links):
    cur.run_many("INSERT INTO SenseLink (sense_id, text, target) VALUES (?, ?, ?)", [
        (sense_id,
         link[0] if len(link) > 0 else None,
         link[1] if len(link) > 1 else None)
        for link in links or []
    ])


def insert_sense_examples(cur, sense_id, examples):
    cur.run_many("""
        INSERT INTO SenseExample (sense_id, text, ref, type, english, translation,
                                  roman, literal_meaning, note, bold_text_offsets)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (sense_id,
         example.get('text'),
         example.get('ref'),
         example.get('type'),
         example.get('english'),
         example.get('translation'),
         example.get('roman'),
         example.get('literal_meaning'),
         example.get('note'),
         json.dumps(example['bold_text_offsets']) if example.get('bold_text_offsets') else None)
        for example in examples or []
    ])


def insert_sense_form_of(cur, sense_id, items, table):
    cur.run_many(f"INSERT INTO {table} (sense_id, word, extra) VALUES (?, ?, ?)",
                 [(sense_id, item.get('word'), item.get('extra')) for item in items or []])


def insert_sense_relations(cur, sense_id, sense):
    for name in SENSE_RELATIONS:
        for item in sense.get(name) or []:
            relation_id = cur.insert("""
                INSERT INTO SenseRelation (sense_id, relation_type, word, sense, source,
                                           english, roman, alt, taxonomic)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sense_id,
                RELATION_ID[name],
                item.get('word'),
                item.get('sense'),
                item.get('source'),
                item.get('english'),
                item.get('roman'),
                item.get('alt'),
                item.get('taxonomic'),
            ))
            _insert_tags(cur, 'SenseRelationTag', 'relation_id', relation_id,
                         item.get('tags'), item.get('raw_tags'))


def insert_word_def(cur, json_data, line_number):
    word_id = insert_word(cur, json_data, line_number)
    insert_head_template(cur, word_id, json_data.get('head_templates'))
    insert_forms(cur, word_id, json_data.get('forms'))
    insert_descendants(cur, word_id, json_data.get('descendants'))
    insert_sounds(cur, word_id, json_data.get('sounds'))
    insert_etymology_templates(cur, word_id, json_data.get('etymology_templates'))
    insert_translations(cur, word_id, json_data.get('translations'))
    insert_word_relations(cur, word_id, json_data)
    insert_senses(cur, word_id, json_data.get('senses'))
