import json
import os
import time

from db import create_connection, create_tables, insert_derived, insert_forms, \
    insert_senses, insert_sounds, insert_word, insert_head_template, \
    insert_descendants, insert_etymology_templates, insert_hyponyms, read_last_processed_line


def read_last_processed_line_from_file(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return int(file.read().strip())
    return 0

def write_last_processed_line(file_path, line_number):
    with open(file_path, 'w') as file:
        file.write(str(line_number))

def write_failed_line(file_path, line_number):
    with open(file_path, 'a') as file:
        file.write(f"{line_number}\n")

def main():
    file_path = 'kaikki.org-dictionary-English.jsonl'
    database = "wikitionary-kaikki-english.db"
    batch_size=100
    progress_file = 'progress.txt'
    failed_lines_file = 'failed_lines.txt'

    # Create a database connection
    conn = create_connection(database)

    # Create tables
    if conn is not None:
        create_tables(conn)

        last_processed_line = read_last_processed_line(conn)

        # Read JSONL file and insert data in batches
        with open(file_path, 'r') as file:
            data_batch = []
            start_time = time.time()
            for i, line in enumerate(file, 1):
                if i <= last_processed_line:
                    continue
                try:
                    data = json.loads(line)
                    data_batch.append((data, i))
                    if i % batch_size == 0:
                        insert_word_batch(conn, data_batch)
                        elapsed_time = time.time() - start_time
                        print(f"Inserted {i} records in {elapsed_time:.2f} seconds")
                        data_batch = []
                        start_time = time.time()
                        # write_last_processed_line(progress_file, i)
                except Exception as e:
                    print(f"Failed to process line {i}: {e}")
                    write_failed_line(failed_lines_file, i)

            # Insert remaining data if any
            if data_batch:
                insert_word_batch(conn, data_batch)
                elapsed_time = time.time() - start_time
                print(f"Inserted {i} records in {elapsed_time:.2f} seconds")
                # write_last_processed_line(progress_file, i)

        conn.close()
    else:
        print("Error! Cannot create the database connection.")

def insert_word_batch(conn, data_batch):
    for data, line_number in data_batch:
        insert_word_def(conn, data, line_number)

def insert_word_def(conn, json_data, line_number):
    cur = conn.cursor()
    word_id = insert_word(cur,json_data,line_number)
    insert_head_template(cur,word_id, json_data.get('head_templates', []))
    insert_forms(cur,word_id, json_data.get('forms', []))
    insert_descendants(cur,word_id, json_data.get('descendants', []))
    insert_sounds(cur,word_id, json_data.get('sounds', []))
    insert_etymology_templates(cur,word_id, json_data.get('etymology_templates', []))
    insert_hyponyms(cur,word_id, json_data.get('hyponyms', []))
    insert_derived(cur,word_id, json_data.get('derived', []))
    insert_senses(cur,word_id, json_data.get('senses', []))
    conn.commit()
    cur.close()

if __name__ == '__main__':
    main()