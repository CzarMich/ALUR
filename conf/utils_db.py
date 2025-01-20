import sqlite3
import os
import psycopg2
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from datetime import datetime
from conf.utils import get_required_fields
from conf.config import (
    PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE, 
    DB_TYPE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_FILE, 
    yaml_config
)
from utils_encryption import encrypt_and_shorthand

logger = logging.getLogger(__name__)

# Load sanitization settings
SANITIZE_SETTINGS = yaml_config.get('sanitize', {})
SANITIZE_ENABLED = SANITIZE_SETTINGS.get('enabled', False)
SANITIZE_FIELDS = SANITIZE_SETTINGS.get('elements_to_sanitize', [])

def connect_to_db():
    """
    Establish a connection to the database (SQLite or PostgreSQL) based on the config.
    """
    if DB_TYPE == "sqlite":
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        try:
            conn = sqlite3.connect(DB_FILE)
            logger.info("Connected to SQLite database.")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite: {e}")
            raise
    elif DB_TYPE == "postgres":
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            conn.autocommit = True
            logger.info("Connected to PostgreSQL database.")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    else:
        raise ValueError(f"Unsupported database type: {DB_TYPE}")

def sanitize_value(field_name, value):
    """
    Sanitize a value if its field is listed in `sanitize.elements_to_sanitize`.
    """
    if SANITIZE_ENABLED and field_name in SANITIZE_FIELDS and value:
        sanitized_value = value.replace('/', '-')
        sanitized_value = re.sub(r'[^\w\-.]', '', sanitized_value)
        return sanitized_value[:64]  # Truncate to 64 characters
    return value

def validate_record(record, required_fields):
    """
    Validate a single record by checking required fields.
    """
    missing_fields = [field for field in required_fields if field not in record]
    if missing_fields:
        logger.warning(f"Missing required fields: {missing_fields}")
        return False
    return True

def encrypt_record_fields(record, key):
    """
    Dynamically encrypt fields in a record based on pseudonymization settings.
    """
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

def create_table_if_not_exists(cursor, table_name, record_fields):
    """
    Create a table if it does not already exist.
    """
    field_definitions = ", ".join([f"{field} TEXT" for field in record_fields])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY, {field_definitions}
    )
    """
    cursor.execute(create_table_sql)

def store_records_in_db(records, table_name):
    """
    Store validated and processed records into the database.
    Create the table if it does not exist and insert new records.
    """
    conn = connect_to_db()
    cursor = conn.cursor()

    # Ensure the table exists
    if records:
        create_table_if_not_exists(cursor, table_name, records[0].keys())

    # Insert records
    if DB_TYPE == "sqlite":
        placeholders = "?"
    else:
        placeholders = "%s"

    for record in records:
        columns = ", ".join(record.keys())
        placeholders_str = ", ".join([placeholders] * len(record))
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders_str})"
        cursor.execute(insert_sql, list(record.values()))

    conn.commit()
    conn.close()

def process_record(record, required_fields, key):
    """
    Validate, sanitize, and encrypt a single record.
    """
    if not validate_record(record, required_fields):
        return None

    sanitized_record = {field: sanitize_value(field, value) for field, value in record.items()}
    return encrypt_record_fields(sanitized_record, key)

def process_records(records, resource_type, key, required_fields, max_workers=3):
    """
    Process records: validate, sanitize, encrypt, and store in the database.
    """
    logger.info(f"Processing {len(records)} records for resource: {resource_type}")
    valid_records = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_record, record, required_fields, key): record
            for record in records
        }

        for future in as_completed(future_map):
            try:
                processed_record = future.result()
                if processed_record:
                    valid_records.append(processed_record)
            except Exception as exc:
                logger.error(f"Error processing a record: {exc}")

    if valid_records:
        store_records_in_db(valid_records, resource_type)
        logger.info(f"Stored {len(valid_records)} records in the database for {resource_type}.")
    else:
        logger.info(f"No valid records to store for {resource_type}.")
