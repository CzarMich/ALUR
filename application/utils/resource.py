import os
import sys
import json
import requests
from typing import Union

# ✅ Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from utils.logger import logger, verbose
from utils.db import ensure_fhir_queue_table
from utils.session import get_db_connection, release_db_connection
from conf.config import (
    FHIR_AUTH_METHOD, FHIR_SERVER_URL, FHIR_SERVER_PASSWORD,
    BATCH_SIZE, DB_TYPE
)

def create_fhir_session() -> requests.Session:
    """Create a synchronous session for FHIR requests."""
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Basic {FHIR_SERVER_PASSWORD}" if FHIR_AUTH_METHOD == "basic" else "",
        "Content-Type": "application/fhir+json"
    })
    return session

def send_fhir_resource(session: requests.Session, resource_type: str, resource_identifier: str, resource_data: dict) -> Union[bool, str]:
    """Send FHIR resource (POST/PUT), log all errors properly."""
    try:
        fhir_type = resource_type.capitalize()
        search_url = f"{FHIR_SERVER_URL}/{fhir_type}?identifier={resource_identifier}"
        verbose(f"🔍 Checking {fhir_type} with identifier: {resource_identifier}")

        resp = session.get(search_url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("total", 0) > 0:
                existing_id = data["entry"][0]["resource"]["id"]
                verbose(f"🔄 Updating existing {fhir_type}/{existing_id} (PUT)...")

                # ✅ Ensure the resource has the correct ID for PUT
                resource_data["id"] = existing_id

                put_url = f"{FHIR_SERVER_URL}/{fhir_type}/{existing_id}"
                put_resp = session.put(put_url, json=resource_data, timeout=10)
                if put_resp.status_code in [200, 201]:
                    verbose(f"✅ Updated {fhir_type}/{existing_id}")
                    return True
                else:
                    logger.error(f"PUT failed for {fhir_type}/{existing_id} | Status: {put_resp.status_code} | Response:\n{put_resp.text}")
                    return False
            else:
                post_url = f"{FHIR_SERVER_URL}/{fhir_type}"
                verbose(f"🌟 Creating {fhir_type}/{resource_identifier} (POST)")

                # ✅ Remove ID before POST to let FHIR server assign it
                resource_data.pop("id", None)

                post_resp = session.post(post_url, json=resource_data, timeout=10)
                if post_resp.status_code in [200, 201]:
                    verbose(f"✅ Created {fhir_type}/{resource_identifier}")
                    return True
                else:
                    logger.error(f"POST failed for {fhir_type}/{resource_identifier} | Status: {post_resp.status_code} | Response:\n{post_resp.text}")
                    return False
        else:
            logger.error(f"Search failed for {fhir_type}/{resource_identifier} | Status: {resp.status_code} | Response:\n{resp.text}")
            return False

    except requests.exceptions.RequestException as e:
        logger.warning(f"Connection error for {resource_type}/{resource_identifier}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error for {resource_type}/{resource_identifier}: {e}", exc_info=True)

    return False

def mark_as_processed_and_delete(row_id: int, resource_type: str):
    """Mark a row as processed and delete from both fhir_queue and resource table."""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        table = resource_type.lower()

        cursor.execute(
            "UPDATE fhir_queue SET processed = TRUE WHERE id = %s" if DB_TYPE == "postgres" else
            "UPDATE fhir_queue SET processed = 1 WHERE id = ?", (row_id,)
        )

        cursor.execute(
            "DELETE FROM fhir_queue WHERE id = %s" if DB_TYPE == "postgres" else
            "DELETE FROM fhir_queue WHERE id = ?", (row_id,)
        )

        cursor.execute(
            f"DELETE FROM {table} WHERE id = %s" if DB_TYPE == "postgres" else
            f"DELETE FROM {table} WHERE id = ?", (row_id,)
        )

        conn.commit()
        verbose(f"🗑 Removed row ID {row_id} from fhir_queue and {table}")

    except Exception as e:
        logger.error(f"Error during delete: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def process_fhir_row(session: requests.Session, row):
    """Task for processing a single FHIR row."""
    row_id, resource_type, identifier, resource_data = row
    if resource_type.lower() == "consent":
        verbose("Skipping Consent resource (handled separately).")
        return

    if isinstance(resource_data, str):
        resource_data = json.loads(resource_data)

    result = send_fhir_resource(session, resource_type, identifier, resource_data)

    if result in [True, "duplicate"]:
        mark_as_processed_and_delete(row_id, resource_type)
    else:
        logger.warning(f"⚠ Marking invalid {resource_type} row {row_id} as processed to avoid retry loop.")
        mark_as_processed_and_delete(row_id, resource_type)

def poll_and_process_fhir():
    """Main FHIR processor."""
    ensure_fhir_queue_table()
    total_processed = 0

    while True:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            query = (
                "SELECT id, resource_type, identifier, resource_data FROM fhir_queue WHERE processed = FALSE LIMIT %s"
                if DB_TYPE == "postgres" else
                "SELECT id, resource_type, identifier, resource_data FROM fhir_queue WHERE processed = 0 LIMIT ?"
            )
            cursor.execute(query, (BATCH_SIZE,))
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch from fhir_queue: {e}", exc_info=True)
            rows = []
        finally:
            release_db_connection(conn)

        if not rows:
            break

        session = create_fhir_session()
        for row in rows:
            process_fhir_row(session, row)
            total_processed += 1
        session.close()

    verbose(f"✅ FHIR processing done. Total: {total_processed}")
    return total_processed

# ✅ Run when executed directly
if __name__ == "__main__":
    poll_and_process_fhir()
