import os
import sys
import json
import time
import requests
from typing import Union

# ✅ Add project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from utils.logger import logger, verbose
from utils.db import ensure_fhir_queue_table
from utils.session import get_db_connection, release_db_connection
from utils.db_consent import delete_consent_by_group
from conf.config import (
    FHIR_AUTH_METHOD, FHIR_SERVER_URL, FHIR_SERVER_PASSWORD,
    BATCH_SIZE, DB_TYPE, QUERY_RETRIES_ENABLED,
    QUERY_RETRY_INTERVAL, QUERY_RETRY_COUNT, RESOURCE_FILES
)

GROUP_BY = RESOURCE_FILES.get("Consent", {}).get("group_by", "composition_id")

def create_fhir_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Basic {FHIR_SERVER_PASSWORD}" if FHIR_AUTH_METHOD == "basic" else "",
        "Content-Type": "application/fhir+json"
    })
    return session

def send_fhir_consent(session: requests.Session, resource_identifier: str, resource_data: dict) -> Union[bool, str]:
    attempt = 0
    search_url = f"{FHIR_SERVER_URL}/Consent?identifier={resource_identifier}"

    while True:
        try:
            verbose(f"🔍 Searching Consent with identifier: {resource_identifier}")
            search_response = session.get(search_url, timeout=10)

            if search_response.status_code == 200:
                data = search_response.json()
                if data.get("total", 0) > 0:
                    existing_id = data["entry"][0]["resource"]["id"]
                    verbose(f"🔄 Updating existing Consent/{existing_id} (PUT)...")
                    resource_data["id"] = existing_id
                    put_url = f"{FHIR_SERVER_URL}/Consent/{existing_id}"
                    response = session.put(put_url, json=resource_data, timeout=10)
                    if response.status_code in [200, 201]:
                        verbose(f"✅ Successfully updated Consent/{existing_id}.")
                        return True
                    elif 400 <= response.status_code < 500:
                        logger.error(f"PUT failed (400) for Consent/{existing_id} | Status: {response.status_code} | Response:\n{response.text}")
                        return "invalid"
                    else:
                        logger.error(f"PUT failed for Consent/{existing_id} | Status: {response.status_code} | Response:\n{response.text}")
                        return False
                else:
                    verbose(f"🌟 Creating new Consent for identifier: {resource_identifier}")
                    resource_data.pop("id", None)
                    response = session.post(f"{FHIR_SERVER_URL}/Consent", json=resource_data, timeout=10)
                    if response.status_code in [200, 201]:
                        verbose(f"✅ Successfully created Consent for identifier: {resource_identifier}.")
                        return True
                    elif 400 <= response.status_code < 500:
                        logger.error(f"POST failed (400) for Consent | Status: {response.status_code} | Response:\n{response.text}")
                        return "invalid"
                    else:
                        logger.error(f"POST failed for Consent | Status: {response.status_code} | Response:\n{response.text}")
                        return False

            else:
                logger.warning(f"Unexpected response during GET for Consent?identifier={resource_identifier}: {search_response.status_code} - {search_response.text}")
                return False

        except requests.exceptions.RequestException as e:
            logger.warning(f"Connection error while posting Consent/{resource_identifier}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error posting Consent/{resource_identifier}: {e}", exc_info=True)

        attempt += 1
        if not QUERY_RETRIES_ENABLED or attempt >= QUERY_RETRY_COUNT:
            logger.error(f"Giving up on Consent/{resource_identifier} after {attempt} attempts.")
            return False

        verbose(f"Retrying Consent/{resource_identifier} in {QUERY_RETRY_INTERVAL} seconds...")
        time.sleep(QUERY_RETRY_INTERVAL)

def mark_consent_as_processed_and_delete(fhir_queue_id: int, group_id: str, delete_from_consent: bool = True):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            "UPDATE fhir_queue SET processed = TRUE WHERE id = %s" if DB_TYPE == "postgres"
            else "UPDATE fhir_queue SET processed = 1 WHERE id = ?", (fhir_queue_id,)
        )

        cursor.execute(
            "DELETE FROM fhir_queue WHERE id = %s" if DB_TYPE == "postgres"
            else "DELETE FROM fhir_queue WHERE id = ?", (fhir_queue_id,)
        )

        conn.commit()
        verbose(f"✅ Marked and deleted fhir_queue row ID {fhir_queue_id}")

    except Exception as e:
        logger.error(f"Error marking Consent as processed: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

    if delete_from_consent:
        deleted = delete_consent_by_group(group_id, GROUP_BY)
        if deleted:
            verbose(f"🗑 Deleted Consent records with {GROUP_BY} = {group_id}.")
        else:
            logger.warning(f"Failed to delete Consent with {GROUP_BY} = {group_id}.")

def poll_and_process_fhir_consent():
    ensure_fhir_queue_table()
    total_processed = 0

    while True:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            query = (
                "SELECT id, resource_type, identifier, resource_data FROM fhir_queue "
                "WHERE processed = FALSE AND resource_type = 'Consent' LIMIT %s"
                if DB_TYPE == "postgres" else
                "SELECT id, resource_type, identifier, resource_data FROM fhir_queue "
                "WHERE processed = 0 AND resource_type = 'Consent' LIMIT ?"
            )
            logger.debug(f"Consent polling query: {query} with limit {BATCH_SIZE}")
            cursor.execute(query, (BATCH_SIZE,))
            rows = cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching Consent from fhir_queue: {e}", exc_info=True)
            rows = []
        finally:
            release_db_connection(conn)

        if not rows:
            break

        session = create_fhir_session()
        processed_in_cycle = False

        for row in rows:
            fhir_queue_id, resource_type, identifier, resource_data = row
            if isinstance(resource_data, str):
                resource_data = json.loads(resource_data)

            result = send_fhir_consent(session, identifier, resource_data)

            if result is True:
                mark_consent_as_processed_and_delete(fhir_queue_id, identifier, delete_from_consent=True)
                total_processed += 1
                processed_in_cycle = True
            elif result == "invalid":
                logger.warning(f"Consent {identifier} is invalid. Retaining in DB for debugging.")
                # Do not delete or mark as processed
            else:
                logger.warning(f"Temporary failure for Consent {identifier}. Will retry in next cycle.")

        session.close()

        if not processed_in_cycle:
            break

    verbose(f"✅ FHIR Consent processing completed. Total processed: {total_processed}")
    return total_processed

if __name__ == "__main__":
    poll_and_process_fhir_consent()
