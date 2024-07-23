import psycopg2
import os
from dotenv import load_dotenv

load_dotenv(".env")
DB_URL = os.environ.get("URL_DB")
CONN = psycopg2.connect(DB_URL)
CURSOR = CONN.cursor()
LOGGER_PATH = os.environ.get("LOGGER_PATH")



def _insert_timestamp(id_value: int, embedding_model_value: str, timestamp_value: str, table: str = "last_embedding"):
    create_table_query = f""" 
        CREATE TABLE IF NOT EXISTS {table} (
        id integer UNIQUE,
        timestamp TIMESTAMP,
        embedding_model VARCHAR(100)
        );"""
    
    CURSOR.execute(create_table_query)
    
    insert_query = f"""
        INSERT INTO {table} (id, timestamp, embedding_model)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET timestamp = EXCLUDED.timestamp,
            embedding_model = EXCLUDED.embedding_model;
    """
    

    CURSOR.execute(insert_query, (id_value, timestamp_value, embedding_model_value))
    
    CONN.commit()

_insert_timestamp(id_value=3560, embedding_model_value="e5_base_v2", timestamp_value="2024-07-23 20:56:43.834562")
