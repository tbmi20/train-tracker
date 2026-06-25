import sqlite3
from pathlib import Path


class Database:
    def __init__(self, db_path: str):
        self.db_file = db_path
        Path(self.db_file).parent.mkdir(parents=True, exist_ok=True)
        self.setup_production_db(self.db_file)

    def setup_production_db(self, db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. Pipeline Metadata (Tracks file version and ingestion timestamp)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                file_ref TEXT PRIMARY KEY,
                extracted_date TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. Geography Dictionary (TIPLOC to Names)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                tiploc TEXT PRIMARY KEY,
                full_name TEXT,
                crs_code TEXT
            )
        """)

        # 3. Association Graph (Train links/splits/joins)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS associations (
                main_uid TEXT,
                assoc_uid TEXT,
                start_date TEXT,
                assoc_type TEXT, -- JJ (Join), VV (Split), NP (Next Use)
                location_tiploc TEXT,
                PRIMARY KEY (main_uid, assoc_uid, start_date)
            )
        """)

        # 4. Master Schedules
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS static_schedules (
                uid TEXT,
                start_date TEXT,
                end_date TEXT,
                days_run TEXT,
                origin_tiploc TEXT,
                dest_tiploc TEXT,
                stops_json TEXT,
                PRIMARY KEY (uid, start_date)
            )
        """)

        # Indices for performance
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_sched_search ON static_schedules (origin_tiploc, dest_tiploc)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_assoc_search ON associations (main_uid, location_tiploc)"
        )

        conn.commit()
        conn.close()

    def execute_query(self, query, params=None):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()

        result = cursor.fetchall()
        conn.close()
        return result
