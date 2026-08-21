import psycopg2
from pathlib import Path


def initialize_database(db_string: str):
    conn = psycopg2.connect(db_string)
    cursor = conn.cursor()

    conn.commit()
    conn.close()
