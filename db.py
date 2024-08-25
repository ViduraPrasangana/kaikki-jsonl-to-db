import json
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to SQLite database: {db_file}")
    except Error as e:
        print(e)
    return conn


# noinspection SqlNoDataSourceInspection
def create_tables(conn):
    """Create tables in the SQLite database."""
    create_table_queries = [
        """
          CREATE TABLE IF NOT EXISTS Word (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              pos TEXT,
              word TEXT,
              lang TEXT,
              lang_code TEXT,
              etymology_text TEXT,
              line_number INTEGER
          );
        """,
        """
        CREATE TABLE IF NOT EXISTS HeadTemplate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id INTEGER,
            name TEXT,
            expansion TEXT,
            FOREIGN KEY (word_id) REFERENCES Word(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Form (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id INTEGER,
            form TEXT,
            FOREIGN KEY (word_id) REFERENCES Word(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS FormTag (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            form_id INTEGER,
            tag TEXT,
            FOREIGN KEY (form_id) REFERENCES Form(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Descendant (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id INTEGER,
            depth INTEGER,
            text TEXT,
            FOREIGN KEY (word_id) REFERENCES Word(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DescendantTemplate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            descendant_id INTEGER,
            name TEXT,
            expansion TEXT,
            FOREIGN KEY (descendant_id) REFERENCES Descendant(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS DescendantTemplateArgs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            descendant_template_id INTEGER,
            arg_key TEXT,
            arg_value TEXT,
            FOREIGN KEY (descendant_template_id) REFERENCES DescendantTemplate(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Sound (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id INTEGER,
            ipa TEXT,
            homophone TEXT,
            rhymes TEXT,
            note TEXT,
            audio TEXT,
            ogg_url TEXT,
            mp3_url TEXT,
            enpr TEXT,
            FOREIGN KEY (word_id) REFERENCES Word(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SoundTag (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sound_id INTEGER,
            tag TEXT,
            FOREIGN KEY (sound_id) REFERENCES Sound(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS EtymologyTemplate (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id INTEGER,
            name TEXT,
            expansion TEXT,
            FOREIGN KEY (word_id) REFERENCES Word(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS EtymologyTemplateArgs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            etymology_template_id INTEGER,
            arg_key TEXT,
            arg_value TEXT,
            FOREIGN KEY (etymology_template_id) REFERENCES EtymologyTemplate(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Hyponym (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id INTEGER,
            word TEXT,
            dis1 TEXT,
            FOREIGN KEY (word_id) REFERENCES Word(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Derived (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id INTEGER,
            word TEXT,
            FOREIGN KEY (word_id) REFERENCES Word(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Sense (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            word_id INTEGER,
            raw_glosses TEXT,
            glosses TEXT,
            id_key TEXT,
            FOREIGN KEY (word_id) REFERENCES Word(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SenseLink (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sense_id INTEGER,
            link1 TEXT,
            link2 TEXT,
            FOREIGN KEY (sense_id) REFERENCES Sense(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SenseTopic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sense_id INTEGER,
            topic TEXT,
            FOREIGN KEY (sense_id) REFERENCES Sense(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SenseCategory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sense_id INTEGER,
            name TEXT,
            kind TEXT,
            source TEXT,
            orig TEXT,
            langcode TEXT,
            dis TEXT,
            FOREIGN KEY (sense_id) REFERENCES Sense(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SenseCategoryParent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER,
            parent TEXT,
            FOREIGN KEY (category_id) REFERENCES SenseCategory(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SenseTranslation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sense_id INTEGER,
            lang TEXT,
            code TEXT,
            sense TEXT,
            roman TEXT,
            word TEXT,
            dis1 TEXT,
            FOREIGN KEY (sense_id) REFERENCES Sense(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SenseSynonym (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sense_id INTEGER,
            sense TEXT,
            word TEXT,
            dis1 TEXT,
            FOREIGN KEY (sense_id) REFERENCES Sense(id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS SenseSynonymTag (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            synonym_id INTEGER,
            tag TEXT,
            FOREIGN KEY (synonym_id) REFERENCES SenseSynonym(id)
        );
        """
    ]
    try:
        c = conn.cursor()
        for query in create_table_queries:
            c.execute(query)
        print("Tables created successfully.")
    except Error as e:
        print(e)

def read_last_processed_line(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(line_number) FROM Word")
    row = cur.fetchone()
    return row[0] if row[0] is not None else 0

def insert_word(conn,data,line_number):
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO Word (pos, word, lang, lang_code, etymology_text, line_number)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        data.get('pos'),
        data.get('word'),
        data.get('lang'),
        data.get('lang_code'),
        data.get('etymology_text'),
        line_number
    ))
    conn.commit()
    return cur.lastrowid

def insert_head_template(conn,word_id, templates):
    cur = conn.cursor()
    for template in templates:
        cur.execute("""
            INSERT OR IGNORE INTO HeadTemplate (word_id, name, expansion)
            VALUES (?, ?, ?)
        """, (
            word_id,
            template.get('name'),
            template.get('expansion')
        ))
    conn.commit()

def insert_forms(conn,word_id, forms):
    cur = conn.cursor()
    for form in forms:
        cur.execute("""
            INSERT OR IGNORE INTO Form (word_id, form)
            VALUES (?, ?)
        """, (word_id, form.get('form')))
        form_id = cur.lastrowid
        insert_form_tags(conn, form_id, form.get('tags', []))
    conn.commit()

def insert_form_tags(conn,form_id, tags):
    cur = conn.cursor()
    for tag in tags:
        cur.execute("""
            INSERT OR IGNORE INTO FormTag (form_id, tag)
            VALUES (?, ?)
        """, (form_id, tag))
    conn.commit()

def insert_descendants(conn,word_id, descendants):
    cur = conn.cursor()
    for descendant in descendants:
        cur.execute("""
            INSERT OR IGNORE INTO Descendant (word_id, depth, text)
            VALUES (?, ?, ?)
        """, (
            word_id,
            descendant.get('depth'),
            descendant.get('text')
        ))
        descendant_id = cur.lastrowid
        insert_descendant_templates(conn, descendant_id, descendant.get('templates', []))
    conn.commit()

def insert_descendant_templates(conn,descendant_id, templates):
    cur = conn.cursor()
    for template in templates:
        cur.execute("""
            INSERT OR IGNORE INTO DescendantTemplate (descendant_id, name, expansion)
            VALUES (?, ?, ?)
        """, (
            descendant_id,
            template.get('name'),
            template.get('expansion')
        ))
        template_id = cur.lastrowid
        insert_descendant_template_args(conn, template_id, template.get('args', {}))
    conn.commit()

def insert_descendant_template_args(conn,template_id, args):
    cur = conn.cursor()
    for key, value in args.items():
        cur.execute("""
            INSERT OR IGNORE INTO DescendantTemplateArgs (descendant_template_id, arg_key, arg_value)
            VALUES (?, ?, ?)
        """, (template_id, key, value))
    conn.commit()

def insert_sounds(conn,word_id, sounds):
    cur = conn.cursor()
    for sound in sounds:
        cur.execute("""
            INSERT OR IGNORE INTO Sound (word_id, ipa, homophone, rhymes, note, audio, ogg_url, mp3_url, enpr)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            word_id,
            sound.get('ipa'),
            sound.get('homophone'),
            sound.get('rhymes'),
            sound.get('note'),
            sound.get('audio'),
            sound.get('ogg_url'),
            sound.get('mp3_url'),
            sound.get('enpr')
        ))
        sound_id = cur.lastrowid
        insert_sound_tags(conn, sound_id, sound.get('tags', []))
    conn.commit()

def insert_sound_tags(conn,sound_id, tags):
    cur = conn.cursor()
    for tag in tags:
        cur.execute("""
            INSERT OR IGNORE INTO SoundTag (sound_id, tag)
            VALUES (?, ?)
        """, (sound_id, tag))
    conn.commit()

def insert_etymology_templates(conn,word_id, templates):
    cur = conn.cursor()
    for template in templates:
        cur.execute("""
            INSERT OR IGNORE INTO EtymologyTemplate (word_id, name, expansion)
            VALUES (?, ?, ?)
        """, (
            word_id,
            template.get('name'),
            template.get('expansion')
        ))
        template_id = cur.lastrowid
        insert_etymology_template_args(conn, template_id, template.get('args', {}))
    conn.commit()

def insert_etymology_template_args(conn,template_id, args):
    cur = conn.cursor()
    for key, value in args.items():
        cur.execute("""
            INSERT OR IGNORE INTO EtymologyTemplateArgs (etymology_template_id, arg_key, arg_value)
            VALUES (?, ?, ?)
        """, (template_id, key, value))
    conn.commit()

def insert_hyponyms(conn,word_id, hyponyms):
    cur = conn.cursor()
    for hyponym in hyponyms:
        cur.execute("""
            INSERT OR IGNORE INTO Hyponym (word_id, word, dis1)
            VALUES (?, ?, ?)
        """, (
            word_id,
            hyponym.get('word'),
            hyponym.get('_dis1')
        ))
    conn.commit()

def insert_derived(conn,word_id, derived):
    cur = conn.cursor()
    for item in derived:
        cur.execute("""
            INSERT OR IGNORE INTO Derived (word_id, word)
            VALUES (?, ?)
        """, (word_id, item.get('word')))
    conn.commit()

def insert_senses(conn,word_id, senses):
    cur = conn.cursor()
    for sense in senses:
        cur.execute("""
            INSERT OR IGNORE INTO Sense (word_id, raw_glosses, glosses, id_key)
            VALUES (?, ?, ?, ?)
        """, (
            word_id,
            json.dumps(sense.get('raw_glosses', [])),  # Assuming glosses and raw_glosses are lists
            json.dumps(sense.get('glosses', [])),
            sense.get('id')
        ))
        sense_id = cur.lastrowid
        insert_sense_links(conn, sense_id, sense.get('links', []))
        insert_sense_topics(conn, sense_id, sense.get('topics', []))
        insert_sense_categories(conn, sense_id, sense.get('categories', []))
        insert_sense_translations(conn, sense_id, sense.get('translations', []))
        insert_sense_synonyms(conn, sense_id, sense.get('synonyms', []))
    conn.commit()

def insert_sense_links(conn,sense_id, links):
    cur = conn.cursor()
    for link in links:
        cur.execute("""
            INSERT OR IGNORE INTO SenseLink (sense_id, link1, link2)
            VALUES (?, ?, ?)
        """, (sense_id, link[0], link[1]))
    conn.commit()

def insert_sense_topics(conn,sense_id, topics):
    cur = conn.cursor()
    for topic in topics:
        cur.execute("""
            INSERT OR IGNORE INTO SenseTopic (sense_id, topic)
            VALUES (?, ?)
        """, (sense_id, topic))
    conn.commit()

def insert_sense_categories(conn,sense_id, categories):
    cur = conn.cursor()
    for category in categories:
        cur.execute("""
            INSERT OR IGNORE INTO SenseCategory (sense_id, name, kind, source, orig, langcode, dis)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            sense_id,
            category.get('name'),
            category.get('kind'),
            category.get('source'),
            category.get('orig'),
            category.get('langcode'),
            category.get('_dis')
        ))
        category_id = cur.lastrowid
        insert_sense_category_parents(conn,category_id, category.get('parents', []))

def insert_sense_category_parents(conn,category_id, parents):
    cur = conn.cursor()
    for parent in parents:
        cur.execute("""
            INSERT OR IGNORE INTO SenseCategoryParent (category_id, parent)
            VALUES (?, ?)
        """, (category_id, parent))
    conn.commit()

def insert_sense_translations(conn,sense_id, translations):
    cur = conn.cursor()
    for translation in translations:
        cur.execute("""
            INSERT OR IGNORE INTO SenseTranslation (sense_id, lang, code, sense, roman, word, dis1)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            sense_id,
            translation.get('lang'),
            translation.get('code'),
            translation.get('sense'),
            translation.get('roman'),
            translation.get('word'),
            translation.get('_dis1')
        ))
    conn.commit()

def insert_sense_synonyms(conn,sense_id, synonyms):
    cur = conn.cursor()
    for synonym in synonyms:
        cur.execute("""
            INSERT OR IGNORE INTO SenseSynonym (sense_id, sense, word, dis1)
            VALUES (?, ?, ?, ?)
        """, (
            sense_id,
            synonym.get('sense'),
            synonym.get('word'),
            synonym.get('_dis1')
        ))
        synonym_id = cur.lastrowid
        insert_sense_synonym_tags(conn, synonym_id, synonym.get('tags', []))
    conn.commit()

def insert_sense_synonym_tags(conn,synonym_id, tags):
    cur = conn.cursor()
    for tag in tags:
        cur.execute("""
            INSERT OR IGNORE INTO SenseSynonymTag (synonym_id, tag)
            VALUES (?, ?)
        """, (synonym_id, tag))
    conn.commit()
