import os
import sys
import json
from typing import Dict, Any

# Add project root to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

# Imports
from utils.db_consent import (
    read_unprocessed_consents,
    read_unprocessed_consents_in_batch,
    mark_consent_as_processed_by_composition,
)
from utils.mapper_consent import group_provisions, clean_section, map_consent_resources
from utils.db import get_db_connection, release_db_connection
from conf.config import (
    DB_TYPE, USE_BATCH, BATCH_SIZE, CONSENT_RESOURCE_FILES
)
from utils.logger import logger, verbose

def create_fhir_queue_table():
    """Ensure the fhir_queue table exists (Postgres/SQLite)."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            """
            CREATE TABLE IF NOT EXISTS fhir_queue (
                id SERIAL PRIMARY KEY,
                resource_type TEXT NOT NULL,
                identifier TEXT NOT NULL UNIQUE,
                resource_data JSONB NOT NULL,
                processed BOOLEAN DEFAULT FALSE
            )
            """ if DB_TYPE == "postgres" else
            """
            CREATE TABLE IF NOT EXISTS fhir_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_type TEXT NOT NULL,
                identifier TEXT NOT NULL UNIQUE,
                resource_data TEXT NOT NULL,
                processed BOOLEAN DEFAULT FALSE
            )
            """
        )
        cursor.execute(query)
        conn.commit()
        logger.debug("Ensured 'fhir_queue' table exists.")
    except Exception as e:
        logger.error(f"Failed to create 'fhir_queue' table: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def insert_into_fhir_queue(resource_type: str, composition_id: str, resource_data: Dict[str, Any]):
    """Insert a Consent FHIR resource into fhir_queue."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        resource_cleaned = clean_section(resource_data)

        query = (
            """
            INSERT INTO fhir_queue (resource_type, identifier, resource_data, processed)
            VALUES (%s, %s, %s, FALSE)
            ON CONFLICT (identifier) DO NOTHING
            """ if DB_TYPE == "postgres" else
            """
            INSERT OR IGNORE INTO fhir_queue (resource_type, identifier, resource_data, processed)
            VALUES (?, ?, ?, FALSE)
            """
        )
        params = (resource_type, composition_id, json.dumps(resource_cleaned))
        cursor.execute(query, params)
        conn.commit()
        verbose(f"Consent/{composition_id} inserted into fhir_queue.")
    except Exception as e:
        logger.error(f"Error inserting Consent/{composition_id}: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def process_consent_resources():
    """Main logic to process Consent records synchronously."""
    resource_name = "Consent"
    mappings = CONSENT_RESOURCE_FILES.get(resource_name, {}).get("mappings", {})
    if not mappings:
        logger.warning(f"⚠ No mappings found for {resource_name}. Skipping.")
        return

    batch = (
        read_unprocessed_consents_in_batch(BATCH_SIZE)
        if USE_BATCH else read_unprocessed_consents()
    )

    if not batch:
        verbose("No unprocessed Consent records found. Skipping.")
        return

    verbose(f"Processing {len(batch)} Consent rows...")

    try:
        grouped_resources = group_provisions(batch)
        if not grouped_resources:
            logger.warning("No grouped Consent resources created.")
            return

        verbose(f"✅ Grouped into {len(grouped_resources)} Consent resources.")
        mapped_resources = map_consent_resources(grouped_resources)

        for resource in mapped_resources:
            composition_id = resource.get("id")
            if not composition_id:
                logger.warning("⚠ Skipping mapped resource without ID.")
                continue

            insert_into_fhir_queue(resource_name, composition_id, resource)
            mark_consent_as_processed_by_composition(composition_id)

        verbose("✅ All Consent resources processed.")

    except Exception as e:
        logger.error(f"Error during Consent processing: {e}", exc_info=True)

def main():
    verbose("🚀 Starting Consent-specific processing...")
    create_fhir_queue_table()
    process_consent_resources()
    verbose("✅ Consent processing completed successfully.")

if __name__ == "__main__":
    main()
