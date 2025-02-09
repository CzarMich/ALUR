import os
import sys
# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import logging
import re
import json
import psycopg2
import sqlite3
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor, as_completed
from conf.config import (
    DB_TYPE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, RESOURCE_FILES,
    PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE, yaml_config
)
from utils.utils_encryption import encrypt_and_shorthand
from utils.utils_session import get_db_connection, release_db_connection

logger = logging.getLogger(__name__)

# ✅ Load sanitization settings
SANITIZE_SETTINGS = yaml_config.get('sanitize', {})
SANITIZE_ENABLED = SANITIZE_SETTINGS.get('enabled', False)
SANITIZE_FIELDS = SANITIZE_SETTINGS.get('elements_to_sanitize', [])


def get_required_fields(resource_type):
    """Retrieve required fields for a resource from RESOURCE_FILES."""
    resource_config = RESOURCE_FILES.get(resource_type, {})
    return resource_config.get("required_fields", [])


def create_table_if_not_exists(table_name, record_fields):
    """Create a table dynamically if it does not already exist for PostgreSQL & SQLite."""
    table_name = table_name.lower()  # ✅ Ensure table name is lowercase
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        field_definitions = ", ".join([f"{field} TEXT" for field in record_fields])
        field_definitions += ", processed BOOLEAN DEFAULT FALSE"

        if DB_TYPE == "postgres":
            query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    id SERIAL PRIMARY KEY, {} 
                )
            """).format(sql.Identifier(table_name), sql.SQL(field_definitions))
        else:  # SQLite
            query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT, {field_definitions}
            )
            """

        cursor.execute(query)
        conn.commit()
        logger.info(f"✅ Ensured table '{table_name}' exists.")

    except Exception as e:
        logger.error(f"🔴 Error creating table '{table_name}': {e}")
        raise
    finally:
        release_db_connection(conn)


def store_records_in_db(records, table_name):
    """Store validated and processed records into the database (PostgreSQL & SQLite)."""
    if not records:
        logger.info(f"⚠ No records to store for table '{table_name}'.")
        return

    table_name = table_name.lower()  # ✅ Ensure table name is lowercase
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # ✅ Ensure the table exists before inserting
        create_table_if_not_exists(table_name, records[0].keys())

        placeholders = ", ".join(["%s"] * len(records[0])) if DB_TYPE == "postgres" else ", ".join(["?"] * len(records[0]))
        columns = ", ".join(records[0].keys())

        if DB_TYPE == "postgres":
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(columns),
                sql.SQL(placeholders)
            )
        else:  # SQLite
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        cursor.executemany(query, [tuple(record.values()) for record in records])
        conn.commit()
        logger.info(f"✅ Stored {len(records)} records in table '{table_name}'.")

    except Exception as e:
        logger.error(f"🔴 Error storing records in '{table_name}': {e}")
        raise
    finally:
        release_db_connection(conn)


def sanitize_value(field_name, value):
    """Sanitize a value if it's listed in `elements_to_sanitize`."""
    if SANITIZE_ENABLED and field_name in SANITIZE_FIELDS and value:
        sanitized_value = value.replace('/', '-')
        sanitized_value = re.sub(r'[^\w\-.]', '', sanitized_value)
        return sanitized_value[:64]  # Truncate to 64 characters
    return value


def validate_record(record, required_fields):
    """Validate a single record by checking required fields."""
    missing_fields = [field for field in required_fields if not record.get(field)]
    if missing_fields:
        logger.warning(f"⚠ Record validation failed. Missing required fields: {missing_fields}")
        return False
    return True


def encrypt_record_fields(record, key):
    """Encrypt fields in a record dynamically based on pseudonymization settings."""
    if not PSEUDONYMIZATION_ENABLED:
        return record

    encrypted_record = record.copy()
    for field_name, value in record.items():
        element_config = ELEMENTS_TO_PSEUDONYMIZE.get(field_name, {})
        if element_config.get("enabled", False):
            full_ciphertext, short_id = encrypt_and_shorthand(str(value), field_name, key)
            encrypted_record[field_name] = short_id
            encrypted_record[f"{field_name}_ciphertext"] = full_ciphertext

    return encrypted_record


def process_record(record, resource_type, key):
    """Validate, sanitize, and encrypt a single record before storing in the database."""
    required_fields = get_required_fields(resource_type)

    if not validate_record(record, required_fields):
        logger.error(f"⚠ Record validation failed for resource '{resource_type}'. Skipping.")
        return None

    sanitized_record = {field: sanitize_value(field, value) for field, value in record.items()}
    return encrypt_record_fields(sanitized_record, key)


def process_records(records, resource_type, key, max_workers=3):
    """Process records: validate, sanitize, encrypt, and store in the database."""
    resource_type = resource_type.lower()  # ✅ Ensure table/resource name is lowercase
    logger.info(f"🔄 Processing {len(records)} records for resource: {resource_type}")
    valid_records = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(process_record, record, resource_type, key): record for record in records}

        for future in as_completed(future_map):
            record = future_map[future]
            try:
                processed_record = future.result()
                if processed_record:
                    valid_records.append(processed_record)
                else:
                    logger.warning(f"⚠ Record skipped due to validation: {record}")
            except Exception as exc:
                logger.error(f"🔴 Error processing record {record}: {exc}")

    if valid_records:
        store_records_in_db(valid_records, resource_type)
        logger.info(f"✅ Stored {len(valid_records)} records in the database for {resource_type}.")
    else:
        logger.info(f"⚠ No valid records to store for {resource_type}.")


def ensure_fhir_queue_table():
    """Ensure the `fhir_queue` table exists before inserting data."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
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
        else:  # SQLite
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
        logger.info("✅ Ensured 'fhir_queue' table exists.")
    except Exception as e:
        logger.error(f"❌ Error ensuring 'fhir_queue' table exists: {e}")
    finally:
        release_db_connection(conn)
