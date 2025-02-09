import os
import sys
# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import logging
import json
import psycopg2
from utils.utils_db_reader import read_unprocessed_rows, read_unprocessed_rows_in_batch, get_db_connection, release_db_connection
from utils.utils_mapper import map_and_clean_resource
from conf.config import DB_TYPE, RESOURCES, USE_BATCH, BATCH_SIZE

# ✅ Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CentralProcessor")


def create_fhir_queue_table():
    """
    Ensure the `fhir_queue` table exists before inserting data.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
            CREATE TABLE IF NOT EXISTS "fhir_queue" (
                id INTEGER PRIMARY KEY,
                resource_type TEXT NOT NULL,
                identifier TEXT NOT NULL UNIQUE,
                resource_data JSONB NOT NULL,
                processed BOOLEAN DEFAULT FALSE
            )
        """ if DB_TYPE == "postgres" else """
            CREATE TABLE IF NOT EXISTS "fhir_queue" (
                id INTEGER PRIMARY KEY,
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


def insert_into_fhir_queue(resource_type, identifier, resource_data, row_id):
    """
    Insert mapped FHIR resources into `fhir_queue` for processing.
    - Uses the same `id` from the resource table to maintain consistency.
    - Ensures `processed = FALSE` in `fhir_queue` (FHIR processor will mark TRUE after success).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
            INSERT INTO "fhir_queue" (id, resource_type, identifier, resource_data, processed)
            VALUES (%s, %s, %s, %s, FALSE)
            ON CONFLICT (id) DO NOTHING;
        """ if DB_TYPE == "postgres" else """
            INSERT OR IGNORE INTO "fhir_queue" (id, resource_type, identifier, resource_data, processed)
            VALUES (?, ?, ?, ?, FALSE)
        """

        cursor.execute(query, (row_id, resource_type, identifier, json.dumps(resource_data)))
        conn.commit()
        logger.info(f"📥 Inserted {resource_type}/{identifier} into fhir_queue with row ID {row_id}.")

    except Exception as e:
        logger.error(f"❌ Database error inserting into fhir_queue: {e}")
    finally:
        release_db_connection(conn)


def process_single_row(resource_name, table_name, row, mappings, required_fields):
    """
    Process a single row:
    - Map the row to a FHIR resource.
    - Insert into `fhir_queue` for processing.
    - Does NOT mark as processed (FHIR processor handles that).
    """
    try:
        fhir_resource = map_and_clean_resource(row, mappings, required_fields)

        # ✅ Ensure `resourceType` is included
        if "resourceType" not in fhir_resource:
            fhir_resource["resourceType"] = resource_name

        # ✅ Extract resource identifier
        resource_identifier = (
            fhir_resource.get("identifier", [{}])[0].get("value")
            if fhir_resource.get("identifier")
            else None
        )

        if not resource_identifier:
            logger.warning(f"⚠ No identifier found for resource: {fhir_resource}")
            return False

        # ✅ Insert into `fhir_queue`
        insert_into_fhir_queue(resource_name, resource_identifier, fhir_resource, row["id"])

        return True
    except Exception as e:
        logger.error(f"❌ Error processing row {row}: {e}")
        return False


def process_batch(resource_name, table_name, batch, mappings, required_fields):
    """
    Process a batch of rows:
    - Map rows to FHIR resources.
    - Insert them into `fhir_queue`.
    - Does NOT mark as processed (handled by FHIR processor).
    """
    try:
        for row in batch:
            success = process_single_row(resource_name, table_name, row, mappings, required_fields)
            if not success:
                logger.warning(f"⚠ Failed to process row ID {row['id']} in batch. Skipping.")
    except Exception as e:
        logger.error(f"❌ Error processing batch for resource '{resource_name}': {e}")


def process_resource(resource):
    """
    Process unprocessed rows for a specific resource.
    """
    resource_name = resource["name"]
    table_name = resource_name  # ✅ Use resource name as-is

    mappings = resource.get("mappings")
    required_fields = resource.get("required_fields", [])

    if not mappings:
        logger.error(f"❌ No mappings defined for resource '{resource_name}'. Skipping.")
        return

    try:
        # ✅ Fetch all unprocessed records
        batch = read_unprocessed_rows_in_batch(table_name, BATCH_SIZE) if USE_BATCH else read_unprocessed_rows(table_name)

        if not batch:
            logger.info(f"🛑 No unprocessed rows for '{resource_name}'. Skipping processing.")
            return

        # ✅ Process all rows correctly
        for row in batch:
            process_single_row(resource_name, table_name, row, mappings, required_fields)

    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted. Exiting...")
        sys.exit(0)


def main():
    """ Main execution function. Runs once and exits. """
    logger.info("🚀 Starting Central Processor...")
    create_fhir_queue_table()

    for resource in RESOURCES:
        process_resource(resource)

    logger.info("✅ Central Processor completed successfully. Exiting.")


if __name__ == "__main__":
    main()
