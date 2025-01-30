import logging
import os
import sys
import requests
import json  
from datetime import datetime, timezone
import time  # ✅ For polling interval

# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from utils.utils_db import process_records
from utils.utils import load_resource_config
from conf.config import (
    EHR_SERVER_URL, EHR_AUTH_METHOD, EHR_SERVER_USER, 
    EHR_SERVER_PASSWORD, KEY_PATH, POLL_INTERVAL, POLLING_ENABLED
)
from utils.utils_session import create_session, get_db_connection, release_db_connection
from utils.utils_state import get_last_run_time, set_last_run_time

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Create OpenEHR session (CACHING DISABLED)
ehr_session = create_session(
    cache_name="ehr_query_cache",
    expire_days=0,  # ✅ No persistent caching
    auth_method=EHR_AUTH_METHOD,
    username=EHR_SERVER_USER if EHR_AUTH_METHOD == "basic" else None,
    password=EHR_SERVER_PASSWORD if EHR_AUTH_METHOD == "basic" else None,
    token=None
)

def load_encryption_key():
    """
    Load the encryption key from the specified KEY_PATH.
    Ensures the key is in bytes format.
    """
    try:
        if KEY_PATH and os.path.exists(KEY_PATH):
            with open(KEY_PATH, "rb") as key_file:
                key = key_file.read()
                if key:
                    return key
    except Exception as e:
        logger.error(f"Error loading encryption key: {e}")
    
    logger.warning("⚠ No valid encryption key found. Proceeding without encryption.")
    return None


def construct_aql_query(resource_name: str, parameters: dict) -> str:
    """
    Load and construct an AQL query from the resource configuration,
    replacing placeholders with actual values.
    """
    try:
        resource_config = load_resource_config(resource_name)
        aql_query = resource_config.get("query_template", "")

        if not aql_query:
            raise ValueError(f"Query template missing for resource: {resource_name}")

        # ✅ Replace placeholders in the AQL query
        for key, value in parameters.items():
            placeholder = f"{{{{{key}}}}}"  # Creates {{key}} format
            if placeholder in aql_query:
                aql_query = aql_query.replace(placeholder, str(value))

        # ✅ If limit is not set, remove "LIMIT {{limit}}" from the query
        if "LIMIT {{limit}}" in aql_query and "limit" not in parameters:
            aql_query = aql_query.replace("LIMIT {{limit}}", "")

        return " ".join(aql_query.split())  # ✅ Remove excess whitespace

    except Exception as e:
        logger.error(f"Error constructing AQL query for {resource_name}: {e}")
        raise


def query_resource(resource_name: str):
    """
    Query a resource from the OpenEHR server using AQL and store results in the database.
    """
    try:
        conn = get_db_connection()

        # ✅ Load parameters dynamically from the YAML resource file
        resource_config = load_resource_config(resource_name)
        default_parameters = resource_config.get("parameters", {})

        parameters = {
            "last_run_time": get_last_run_time(resource_name) or default_parameters.get("last_run_time", "2025-01-01T00:00:00"),
            "composition_name": default_parameters.get("composition_name", "Diagnose"),
            "offset": default_parameters.get("offset", 0),
        }

        # ✅ Add limit ONLY IF it's specified in YAML
        if "limit" in default_parameters:
            parameters["limit"] = default_parameters["limit"]

        logger.info(f"✅ Loaded parameters for {resource_name}: {parameters}")

        batch_size = parameters.get("limit", None)  # Can be None if limit is not set

        while True:
            # ✅ Construct AQL query with correct pagination
            aql_query = construct_aql_query(resource_name, parameters)

            url = f"{EHR_SERVER_URL}/rest/v1/query"
            query_payload = json.dumps({"aql": aql_query}, ensure_ascii=False)

            # ✅ Send query request (NO CACHING)
            response = ehr_session.post(
                url,
                data=query_payload,
                headers={"Content-Type": "application/json"},
                params={"_": str(os.urandom(16))}  # Bypass cache with random query param
            )

            if response.status_code == 200:
                result_set = response.json().get("resultSet", [])
                if not result_set:
                    logger.info(f"✅ No more records found for {resource_name}. Stopping pagination.")
                    break

                logger.info(f"✅ Retrieved {len(result_set)} records for {resource_name}.")

                # ✅ Load encryption key and ensure it is bytes
                encryption_key = load_encryption_key()
                if encryption_key and isinstance(encryption_key, str):
                    encryption_key = encryption_key.encode()

                process_records(records=result_set, resource_type=resource_name, key=encryption_key)

                # ✅ Use system time as last_run_time AFTER committing records
                latest_time = datetime.now(timezone.utc).isoformat()
                set_last_run_time(resource_name, latest_time)

                # ✅ Stop fetching if fewer records than batch size are received
                if batch_size and len(result_set) < batch_size:
                    logger.info(f"✅ No more records available for {resource_name}. Stopping pagination.")
                    break 

                # ✅ Update offset for next batch
                parameters["offset"] += batch_size if batch_size else len(result_set)

            else:
                logger.error(f"🔴 Error querying {resource_name}: {response.status_code} - {response.text}")
                break  # ✅ Stop if an error occurs

            if not POLLING_ENABLED:
                logger.info("✅ Polling is disabled. Running query only once.")
                break

            logger.info(f"⏳ Waiting for {POLL_INTERVAL} seconds before next query...")
            time.sleep(POLL_INTERVAL)

    except requests.RequestException as e:
        logger.error(f"🔴 Request failed for {resource_name}: {e}")
    except Exception as e:
        logger.error(f"🔴 General error occurred while querying {resource_name}: {e}")
    finally:
        if conn:
            release_db_connection(conn)


# Run if executed directly
if __name__ == "__main__":
    query_resource('Condition')
