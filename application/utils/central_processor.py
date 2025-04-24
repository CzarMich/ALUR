import os
import sys
import json

# Add project root to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from utils.db_reader import (
    read_unprocessed_rows,
    read_unprocessed_rows_in_batch
)
from utils.session import get_db_connection, release_db_connection
from utils.mapper import map_and_clean_resource
from conf.config import DB_TYPE, RESOURCES, USE_BATCH, BATCH_SIZE, RESOURCE_FILES
from utils.logger import logger, verbose

def create_fhir_queue_table():
    """Ensure the fhir_queue table exists."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if DB_TYPE == "postgres":
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fhir_queue (
                    id SERIAL PRIMARY KEY,
                    resource_type TEXT NOT NULL,
                    identifier TEXT NOT NULL UNIQUE,
                    resource_data JSONB NOT NULL,
                    processed BOOLEAN DEFAULT FALSE
                )
            """)
        else:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fhir_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    resource_type TEXT NOT NULL,
                    identifier TEXT NOT NULL UNIQUE,
                    resource_data TEXT NOT NULL,
                    processed BOOLEAN DEFAULT FALSE
                )
            """)
        conn.commit()
        logger.debug("Ensured 'fhir_queue' table exists.")
    except Exception as e:
        logger.error(f"Failed to create 'fhir_queue' table: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def insert_into_fhir_queue(resource_type, identifier, resource_data, row_id):
    """Insert a FHIR resource into the fhir_queue table."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            "INSERT INTO fhir_queue (id, resource_type, identifier, resource_data, processed) "
            "VALUES (%s, %s, %s, %s, FALSE) ON CONFLICT (id) DO NOTHING"
            if DB_TYPE == "postgres" else
            "INSERT OR IGNORE INTO fhir_queue (id, resource_type, identifier, resource_data, processed) "
            "VALUES (?, ?, ?, ?, FALSE)"
        )
        params = (row_id, resource_type, identifier, json.dumps(resource_data))
        cursor.execute(query, params)
        conn.commit()
        verbose(f"Inserted {resource_type}/{identifier} into fhir_queue (row ID {row_id}).")
    except Exception as e:
        logger.error(f"Error inserting into fhir_queue: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def process_single_row(resource_name, row):
    """Process a single row and insert mapped resource into fhir_queue."""
    try:
        config = RESOURCE_FILES.get(resource_name, {})
        mappings = config.get("mappings", {})
        required_fields = config.get("required_fields", [])

        fhir_resource = map_and_clean_resource(dict(row), mappings, required_fields)
        if not fhir_resource:
            logger.warning(f"⚠ Mapping failed for row ID {row.get('id', 'UNKNOWN')}. Skipping.")
            return False

        fhir_resource.setdefault("resourceType", resource_name)
        identifier = fhir_resource.get("identifier", [{}])[0].get("value") if fhir_resource.get("identifier") else None
        if not identifier:
            logger.warning(f"No identifier found for {resource_name} row ID {row.get('id')}. Skipping.")
            return False

        insert_into_fhir_queue(resource_name, identifier, fhir_resource, row["id"])
        return True
    except Exception as e:
        logger.error(f"Failed to process {resource_name} row {row}: {e}", exc_info=True)
        return False

def process_resource(resource):
    """Process all unprocessed rows for a given resource."""
    resource_name = resource["name"]

    if resource_name.lower() == "consent":
        verbose("Skipping Consent processing in Central Processor. Handled separately.")
        return

    config = RESOURCE_FILES.get(resource_name, {})
    mappings = config.get("mappings", {})
    required_fields = config.get("required_fields", [])

    if not mappings:
        logger.warning(f"No mappings defined for '{resource_name}'. Skipping.")
        return

    try:
        fetch_func = read_unprocessed_rows_in_batch if USE_BATCH else read_unprocessed_rows
        batch = fetch_func(resource_name, BATCH_SIZE) if USE_BATCH else fetch_func(resource_name)

        if not batch:
            verbose(f"No unprocessed rows in '{resource_name}'.")
            return

        verbose(f"Processing {len(batch)} rows for {resource_name}...")

        for row in batch:
            process_single_row(resource_name, row)

    except Exception as e:
        logger.error(f"Error while processing {resource_name}: {e}", exc_info=True)

def main():
    """Main entry point to run the Central Processor."""
    verbose("🚀 Starting Central Processor...")
    create_fhir_queue_table()

    for resource in RESOURCES:
        process_resource(resource)

    verbose("✅ Central Processor completed successfully.")

if __name__ == "__main__":
    main()
