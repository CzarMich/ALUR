import os
import sys
# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)  # Add project root to module search path

import logging
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from utils.utils_db import process_records
from utils.utils import load_resource_config
from utils.utils_encryption import load_aes_key
from conf.config import (
    EHR_SERVER_URL, EHR_AUTH_METHOD, EHR_SERVER_USER, EHR_SERVER_PASSWORD,
    POLL_INTERVAL, POLLING_ENABLED, RESOURCES, FETCH_BY_DATE_ENABLED,
    FETCH_START_DATE, FETCH_END_DATE, FETCH_INTERVAL_HOURS, PRIORITY_BASED_FETCHING,
    MAX_PARALLEL_FETCHES
)
from utils.utils_session import create_session, get_db_connection, release_db_connection
from utils.utils_state import get_last_run_time, set_last_run_time
from utils.utils_healthcheck import server_heartbeat_check  # ✅ Health check

# ✅ Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("OpenEHRProcessor")

# ✅ Create OpenEHR session
ehr_session = create_session(
    cache_name="ehr_query_cache",
    expire_days=0,  # Disable cache persistence
    auth_method=EHR_AUTH_METHOD,
    username=EHR_SERVER_USER if EHR_AUTH_METHOD == "basic" else None,
    password=EHR_SERVER_PASSWORD if EHR_AUTH_METHOD == "basic" else None,
    token=None
)

# ✅ Load encryption key (Ensure it is bytes-like)
aes_key = load_aes_key()
if isinstance(aes_key, str):
    logger.warning("⚠ AES key is in string format. Converting to bytes.")
    aes_key = aes_key.encode()


def construct_aql_query(resource_name: str, parameters: dict) -> str:
    """Load and construct AQL query dynamically."""
    try:
        resource_config = load_resource_config(resource_name)
        aql_query = resource_config.get("query_template", "")

        if not aql_query:
            raise ValueError(f"Query template missing for {resource_name}")

        # ✅ Replace placeholders dynamically
        for key, value in parameters.items():
            placeholder = f"{{{{{key}}}}}"
            if placeholder in aql_query:
                aql_query = aql_query.replace(placeholder, str(value))

        # ✅ Remove last_run_time filter if fetch_by_date is disabled
        if not FETCH_BY_DATE_ENABLED:
            aql_query = aql_query.replace("AND v/commit_audit/time_committed/value >= '{{last_run_time}}'", "")

        return " ".join(aql_query.split())  # ✅ Remove excess whitespace

    except Exception as e:
        logger.error(f"❌ Error constructing AQL query for {resource_name}: {e}")
        raise


def query_resource(resource_name: str):
    """Query a resource dynamically based on configuration."""
    if not server_heartbeat_check():
        return  # Abort if server is unreachable

    try:
        conn = get_db_connection()

        # ✅ Load parameters dynamically
        resource_config = load_resource_config(resource_name)
        default_parameters = resource_config.get("parameters", {})

        # ✅ Fetch interval & priority-based fetching
        fetch_interval = int(FETCH_INTERVAL_HOURS) * 3600  # Convert hours to seconds
        priority_fetch_interval = int(resource_config.get("priority", 0)) * 1800  # P1=30min, P2=2h, etc.

        if PRIORITY_BASED_FETCHING and priority_fetch_interval > 0:
            fetch_interval = priority_fetch_interval  # Override with priority setting

        # ✅ Load last_run_time
        last_run_time = get_last_run_time(resource_name) or FETCH_START_DATE

        # ✅ Load parameters, ensure correct types
        parameters = {
            "last_run_time": last_run_time,
            "composition_name": default_parameters.get("composition_name", ""),
            "offset": int(default_parameters.get("offset", 0)),  # Ensure integer
        }

        if "limit" in default_parameters:
            parameters["limit"] = int(default_parameters["limit"])  # Ensure integer

        logger.info(f"🚀 Fetching {resource_name} | Interval: {fetch_interval // 60} min")

        retries = 3  # ✅ Retry failed queries up to 3 times
        for attempt in range(retries):
            try:
                # ✅ Construct AQL Query
                aql_query = construct_aql_query(resource_name, parameters)
                url = f"{EHR_SERVER_URL}/rest/v1/query"
                query_payload = {"aql": aql_query}

                response = ehr_session.post(url, json=query_payload, headers={"Content-Type": "application/json"})

                logger.debug(f"🔍 DEBUG: Response Status: {response.status_code}")

                if response.status_code == 200:
                    result_set = response.json().get("resultSet", [])

                    if not result_set:
                        logger.info(f"✅ No new records found for {resource_name}.")
                        return

                    logger.info(f"✅ Retrieved {len(result_set)} records for {resource_name}.")

                    # ✅ Ensure process_records is being called
                    process_records(records=result_set, resource_type=resource_name, key=aes_key)

                    # ✅ Update last_run_time
                    new_run_time = time.strftime("%Y-%m-%dT%H:%M:%S")
                    set_last_run_time(resource_name, new_run_time)

                    break  # ✅ Exit retry loop if successful

                elif response.status_code == 204:  # ✅ Handle No Content
                    logger.warning(f"⚠ No data available for {resource_name}. Response 204 (No Content). Skipping.")
                    return

                else:
                    logger.warning(f"⚠ Query failed (attempt {attempt+1}/{retries}): {response.status_code} - {response.text}")
                    if attempt == retries - 1:
                        logger.error(f"🔴 Final attempt failed for {resource_name}. Skipping this fetch.")
                    time.sleep(5)  # Wait before retrying

            except requests.RequestException as e:
                logger.error(f"❌ Request failed for {resource_name}: {e}")
                time.sleep(5)  # Wait before retrying

    except Exception as e:
        logger.error(f"❌ General error occurred while querying {resource_name}: {e}", exc_info=True)
    finally:
        if conn:
            release_db_connection(conn)


def query_all_resources():
    """Fetch all resources dynamically based on configuration."""
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FETCHES) as executor:
        futures = {executor.submit(query_resource, resource["name"]): resource for resource in RESOURCES}
        
        for future in futures:
            resource = futures[future]
            try:
                future.result()  # Wait for completion
            except Exception as exc:
                logger.error(f"⚠ Error fetching {resource['name']}: {exc}")

    # ✅ Only wait if polling is enabled
    if POLLING_ENABLED:
        logger.info(f"✅ Cycle complete. Waiting for {POLL_INTERVAL} seconds before the next run.")
        time.sleep(POLL_INTERVAL)
    else:
        logger.info("✅ Polling is disabled. Exiting after this fetch cycle.")
        sys.exit(0)  # ✅ Gracefully exit instead of waiting


if __name__ == "__main__":
    while True:
        logger.info("🚀 Starting a new fetch-and-process cycle.")
        query_all_resources()
        logger.info(f"✅ Cycle complete. Waiting for {POLL_INTERVAL} seconds before the next run.")
        time.sleep(POLL_INTERVAL)
