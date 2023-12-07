import psycopg2
from datetime import datetime
import random
import pretty_errors


def update_id_with_random_numbers(table_name):
    connection = psycopg2.connect(user='postgres', password='3312', host='localhost', port='5432', database='postgres')
    cursor = connection.cursor()

    # Create the id_mapping table within the same session
    cursor.execute(f"""
        CREATE TEMP TABLE id_mapping AS
        SELECT id, ROW_NUMBER() OVER (ORDER BY RANDOM()) AS new_id
        FROM {table_name};
    """)

    cursor.execute("SELECT id, new_id FROM id_mapping;")
    id_rows = cursor.fetchall()

    for row in id_rows:
        original_id, new_id = row
        cursor.execute(f"UPDATE {table_name} SET id = %s WHERE id = %s;", (new_id, original_id))

    connection.commit()
    cursor.close()
    connection.close()


update_id_with_random_numbers('no_usa')
