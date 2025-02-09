import logging
import os
import sys

# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import time
import json
import requests
import psycopg2
import sqlite3
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.utils_session import create_session, get_db_connection, release_db_connection
from utils.utils_db import ensure_fhir_queue_table
from conf.config import (
    FHIR_AUTH_METHOD, FHIR_SERVER_URL, FHIR_SERVER_PASSWORD, USE_BATCH,
    BATCH_SIZE, POLL_INTERVAL, QUERY_RETRIES_ENABLED, QUERY_RETRY_INTERVAL,
    MAX_FHIR_WORKERS, DB_TYPE
)

logger = logging.getLogger("FHIRProcessor")

# ✅ Ensure `fhir_queue` table exists before starting processing
ensure_fhir_queue_table()

# ✅ Create a reusable FHIR session
fhir_session = create_session(
    cache_name="fhir_cache",
    auth_method=FHIR_AUTH_METHOD,
    token=FHIR_SERVER_PASSWORD
)


def send_fhir_resource(resource_type, resource_identifier, resource_data):
    """
    Send a FHIR resource to the FHIR server using PUT (if exists) or POST (if new).
    If the resource already exists, it will be marked as processed and deleted.
    """
    try:
        # ✅ Ensure correct casing when posting to FHIR (first letter uppercase)
        fhir_resource_type = resource_type.capitalize()

        search_url = f"{FHIR_SERVER_URL}/{fhir_resource_type}?identifier={resource_identifier}"
        logger.info(f"🔍 Checking for existing {fhir_resource_type} with identifier: {resource_identifier}")

        search_response = fhir_session.get(search_url, timeout=10)

        if search_response.status_code == 200:
            try:
                search_results = search_response.json()
            except json.JSONDecodeError:
                logger.error(f"❌ Error decoding JSON response from FHIR server: {search_response.text}")
                return False

            total_found = search_results.get("total", 0)

            if total_found > 0:
                existing_id = search_results["entry"][0]["resource"]["id"]
                logger.info(f"🔄 Duplicate detected: {fhir_resource_type}/{existing_id} already exists.")
                return "duplicate"

            else:
                post_url = f"{FHIR_SERVER_URL}/{fhir_resource_type}"
                response = fhir_session.post(post_url, json=resource_data, timeout=10)
                logger.info(f"🆕 Creating new {fhir_resource_type}/{resource_identifier} (POST)")

        else:
            logger.error(f"❌ Failed to check existence of {fhir_resource_type}/{resource_identifier}: {search_response.text}")
            return False

        if response.status_code in [200, 201]:
            logger.info(f"✅ Successfully processed {fhir_resource_type}/{resource_identifier}.")
            return True
        else:
            logger.error(f"❌ Failed to process {fhir_resource_type}/{resource_identifier}: {response.text}")
            return False

    except requests.exceptions.Timeout:
        logger.error(f"⏳ Request timeout while checking for {fhir_resource_type}/{resource_identifier}. Skipping...")
        return False

    except requests.exceptions.RequestException as e:
        logger.warning(f"⚠ Connection to FHIR server failed: {e}")
        return False

    except Exception as e:
        logger.error(f"❌ Unexpected error while sending FHIR resource: {e}")
        return False


def mark_as_processed_and_delete(row_id, resource_type):
    """
    Mark a row as processed **before** deleting it from `fhir_queue` and the corresponding resource table.
    Also resets the PostgreSQL sequence count to avoid high counting gaps.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # ✅ Ensure table name is **lowercase** when interacting with the database
        table_name = resource_type.lower()

        # ✅ Step 1: Mark as processed in fhir_queue
        query = "UPDATE fhir_queue SET processed = TRUE WHERE id = %s" if DB_TYPE == "postgres" else "UPDATE fhir_queue SET processed = TRUE WHERE id = ?"
        cursor.execute(query, (row_id,))
        conn.commit()
        logger.info(f"✅ Marked row ID {row_id} as processed in 'fhir_queue'.")

        # ✅ Step 2: Delete from fhir_queue
        delete_query = "DELETE FROM fhir_queue WHERE id = %s" if DB_TYPE == "postgres" else "DELETE FROM fhir_queue WHERE id = ?"
        cursor.execute(delete_query, (row_id,))
        conn.commit()
        logger.info(f"🗑 Deleted row ID {row_id} from 'fhir_queue'.")

        # ✅ Step 3: Delete from resource table using `id`
        delete_resource_query = f"DELETE FROM {table_name} WHERE id = %s" if DB_TYPE == "postgres" else f"DELETE FROM \"{table_name}\" WHERE id = ?"
        cursor.execute(delete_resource_query, (row_id,))
        conn.commit()
        logger.info(f"🗑 Deleted resource row with ID {row_id} from '{table_name}'.")

        # ✅ Step 4: Reset sequence in PostgreSQL **(Corrected Query)**
        if DB_TYPE == "postgres":
            try:
                reset_sequence_query = f"""
                    SELECT setval(
                        pg_get_serial_sequence('{table_name}', 'id'),
                        COALESCE((SELECT MAX(id) FROM {table_name}), 1),
                        false
                    );
                """  # ✅ FIXED: No quotes around `{table_name}`

                cursor.execute(reset_sequence_query)
                conn.commit()
                logger.info(f"🔄 Reset sequence for table '{table_name}'.")
            except Exception as seq_err:
                logger.warning(f"⚠ Unable to reset sequence for table '{table_name}': {seq_err}. Skipping sequence reset.")

    except Exception as e:
        logger.error(f"❌ Error marking row as processed and deleting: {e}")
    finally:
        release_db_connection(conn)


def poll_and_process_fhir():
    """
    Polls and processes FHIR resources from `fhir_queue`.
    Deletes processed resources from `fhir_queue` and corresponding resource tables.
    Stops after processing all available rows.
    """
    ensure_fhir_queue_table()

    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT id, resource_type, identifier, resource_data FROM fhir_queue WHERE processed = FALSE LIMIT %s" if DB_TYPE == "postgres" else "SELECT id, resource_type, identifier, resource_data FROM fhir_queue WHERE processed = FALSE LIMIT ?"
    cursor.execute(query, (BATCH_SIZE,))
    rows = cursor.fetchall()
    release_db_connection(conn)

    if not rows:
        logger.info("✅ No unprocessed FHIR resources found. Exiting.")
        return

    logger.info(f"🚀 Processing {len(rows)} resources from fhir_queue...")

    for row in rows:
        row_id, resource_type, identifier, resource_data = row

        if isinstance(resource_data, str):
            resource_data = json.loads(resource_data)

        result = send_fhir_resource(resource_type, identifier, resource_data)

        if result in [True, "duplicate"]:
            mark_as_processed_and_delete(row_id, resource_type)

    logger.info("✅ Processing completed. Exiting.")


if __name__ == "__main__":
    logger.info("🚀 Starting FHIR processing loop...")
    poll_and_process_fhir()
