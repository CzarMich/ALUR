import os
import sys
import json
from decimal import Decimal
from typing import Any, Dict, List
import psycopg2
import sqlite3
from psycopg2 import sql
from psycopg2.extras import Json

# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from conf.config import (
    DB_TYPE, RESOURCE_FILES, PSEUDONYMIZATION_ENABLED,
    ELEMENTS_TO_PSEUDONYMIZE, yaml_config
)
from utils.session import get_db_connection, release_db_connection
from utils.encryption import encrypt_and_shorthand
from utils.logger import logger, verbose

# ✅ Load sanitization settings
SANITIZE_SETTINGS = yaml_config.get("sanitize", {})
SANITIZE_ENABLED = SANITIZE_SETTINGS.get("enabled", False)
SANITIZE_FIELDS = SANITIZE_SETTINGS.get("elements_to_sanitize", [])

def ensure_fetch_state_table():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fetch_state (
                resource TEXT PRIMARY KEY,
                last_run_time TIMESTAMP NOT NULL,
                next_run_time TIMESTAMP NOT NULL
            )
        """)
        conn.commit()
        verbose("✅ Ensured `fetch_state` table exists.")
    except Exception as e:
        logger.error(f"Error creating `fetch_state` table: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def get_fetch_state(resource_type: str):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT last_run_time, next_run_time FROM fetch_state WHERE resource = %s"
            if DB_TYPE == "postgres" else
            "SELECT last_run_time, next_run_time FROM fetch_state WHERE resource = ?",
            (resource_type,)
        )
        return cursor.fetchone() or (None, None)
    except Exception as e:
        logger.error(f"Error retrieving fetch state for {resource_type}: {e}")
        return None, None
    finally:
        release_db_connection(conn)

def update_fetch_state(resource_type: str, last_run, next_run):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if DB_TYPE == "postgres":
            cursor.execute("""
                INSERT INTO fetch_state (resource, last_run_time, next_run_time)
                VALUES (%s, %s, %s)
                ON CONFLICT (resource) DO UPDATE
                SET last_run_time = EXCLUDED.last_run_time,
                    next_run_time = EXCLUDED.next_run_time
            """, (resource_type, last_run, next_run))
        else:
            cursor.execute("""
                INSERT OR REPLACE INTO fetch_state (resource, last_run_time, next_run_time)
                VALUES (?, ?, ?)
            """, (resource_type, last_run, next_run))
        conn.commit()
        verbose(f"✅ Updated fetch state → {resource_type}: {last_run} → {next_run}")
    except Exception as e:
        logger.error(f"Failed to update fetch state for {resource_type}: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def get_required_fields(resource_type: str) -> List[str]:
    return RESOURCE_FILES.get(resource_type.lower(), {}).get("required_fields", [])

def create_table_if_not_exists(table_name: str, record_fields: List[str]):
    table_name = table_name.lower()
    normalized_fields = [f.lower() for f in record_fields]
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        # Step 1: Create the table if it doesn't exist
        field_defs = ", ".join([f"{f} TEXT" for f in normalized_fields]) + ", processed BOOLEAN DEFAULT FALSE"

        if DB_TYPE == "postgres":
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY, {}
                )
            """).format(sql.Identifier(table_name), sql.SQL(field_defs))
            cursor.execute(query)
        else:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, {field_defs}
                )
            """)

        conn.commit()
        verbose(f"Ensured table '{table_name}' exists.")

        # Step 2: Dynamically add missing lowercase columns
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        existing_columns = [desc[0] for desc in cursor.description]

        for field in normalized_fields:
            if field not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {field} TEXT")
                    verbose(f"Added missing column '{field}' to table '{table_name}'")
                except Exception as e:
                    logger.warning(f" Could not add column '{field}' to '{table_name}': {e}")

        conn.commit()

    except Exception as e:
        logger.error(f"Error creating or updating table '{table_name}': {e}", exc_info=True)
    finally:
        release_db_connection(conn)


def encrypt_record_fields(record: Dict[str, Any], key) -> Dict[str, Any]:
    if not PSEUDONYMIZATION_ENABLED:
        return record

    encrypted = record.copy()
    for field, value in record.items():
        config = ELEMENTS_TO_PSEUDONYMIZE.get(field, {})
        if config.get("enabled"):
            full, short = encrypt_and_shorthand(str(value), field, key)
            encrypted[field] = short
            encrypted[f"{field}_ciphertext"] = full
    return encrypted

def convert_dicts_to_json(record: Dict[str, Any]) -> Dict[str, Any]:
    def normalize(val: Any, key: str = "") -> Any:
        if isinstance(val, Decimal):
            return float(val)
        if isinstance(val, dict):
            return {k: normalize(v, k) for k, v in val.items()}
        if isinstance(val, list):
            return [normalize(v, key) for v in val]
        if isinstance(val, (int, float)) and key.endswith("_string"):
            return str(val)
        return str(val) if not isinstance(val, (str, int, float)) else val

    return {k: normalize(v, k) for k, v in record.items()}

def store_records_in_db(records: List[Dict[str, Any]], table_name: str, key):
    if not records:
        logger.warning(f"No records to store in '{table_name}'.")
        return

    records = [encrypt_record_fields(r, key) for r in records]
    records = [convert_dicts_to_json(r) for r in records]
    create_table_if_not_exists(table_name, records[0].keys())

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        columns = ", ".join(records[0].keys())
        placeholders = ", ".join(["%s"] * len(records[0])) if DB_TYPE == "postgres" else ", ".join(["?"] * len(records[0]))

        if DB_TYPE == "postgres":
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(columns),
                sql.SQL(placeholders)
            )
            values = [tuple(Json(v) if isinstance(v, dict) else v for v in r.values()) for r in records]
        else:
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            values = [tuple(r.values()) for r in records]

        cursor.executemany(query, values)
        conn.commit()
        verbose(f"✅ Inserted {len(records)} records into '{table_name}'.")
    except Exception as e:
        logger.error(f"Error inserting into '{table_name}': {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def process_records(records: List[Dict[str, Any]], resource_type: str, key):
    resource_type = resource_type.lower()
    verbose(f"Processing {len(records)} records for '{resource_type}'")

    if not records:
        logger.warning(f" No records received for {resource_type}. Skipping.")
        return

    store_records_in_db(records, resource_type, key)
    verbose(f"✅ All {len(records)} records stored for '{resource_type}'.")

def ensure_fhir_queue_table():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if DB_TYPE == "postgres":
            query = """
                CREATE TABLE IF NOT EXISTS fhir_queue (
                    id SERIAL PRIMARY KEY,
                    resource_type TEXT NOT NULL,
                    identifier TEXT NOT NULL UNIQUE,
                    resource_data JSONB NOT NULL,
                    processed BOOLEAN DEFAULT FALSE
                )
            """
        else:
            query = """
                CREATE TABLE IF NOT EXISTS fhir_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    resource_type TEXT NOT NULL,
                    identifier TEXT NOT NULL UNIQUE,
                    resource_data TEXT NOT NULL,
                    processed BOOLEAN DEFAULT FALSE
                )
            """
        cursor.execute(query)
        conn.commit()
        verbose("✅ Ensured 'fhir_queue' table exists.")
    except Exception as e:
        logger.error(f"Failed to create 'fhir_queue' table: {e}", exc_info=True)
    finally:
        release_db_connection(conn)
