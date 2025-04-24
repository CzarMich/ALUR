import os
import sys
import logging
import time
import yaml
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from utils.logger import logger
from utils.db import process_records
from utils.utils import load_resource_config
from utils.encryption import load_aes_key
from conf.config import (
    EHR_SERVER_URL, EHR_AUTH_METHOD, EHR_SERVER_USER, EHR_SERVER_PASSWORD,
    POLL_INTERVAL, POLLING_ENABLED, RESOURCES, FETCH_BY_DATE_ENABLED,
    FETCH_START_DATE, FETCH_END_DATE, PRIORITY_BASED_FETCHING, FETCH_INTERVAL_HOURS,
    MAX_PARALLEL_FETCHES, PRIORITY_LEVELS, RESOURCE_FILES
)
from utils.session import create_session, get_db_connection, release_db_connection
from utils.state import get_fetch_state, update_fetch_state, calculate_next_run_time

ehr_session = create_session(
    cache_name="ehr_query_cache",
    expire_days=0,
    auth_method=EHR_AUTH_METHOD,
    username=EHR_SERVER_USER if EHR_AUTH_METHOD == "basic" else None,
    password=EHR_SERVER_PASSWORD if EHR_AUTH_METHOD == "basic" else None,
    token=None
)

aes_key = load_aes_key()
if isinstance(aes_key, str):
    aes_key = aes_key.encode()

def construct_aql_query(resource_name: str, parameters: dict) -> str:
    try:
        # Load from the correct YAML key with case-insensitive fallback
        mapping_path = RESOURCE_FILES.get(resource_name, {}).get("mapping_path")
        if not mapping_path or not os.path.exists(mapping_path):
            raise FileNotFoundError(f"Mapping file not found for resource: {resource_name}")

        with open(mapping_path, 'r') as f:
            yaml_data = yaml.safe_load(f)

        # Try loading the AQL template using case-insensitive key lookup
        yaml_keys = {k.lower(): k for k in yaml_data}
        key = yaml_keys.get(resource_name.lower())
        if not key:
            raise ValueError(f"No query template found in YAML for resource '{resource_name}'")

        aql_query = yaml_data[key].get("query_template", "")
        if not aql_query:
            raise ValueError(f"Query template missing for {resource_name}")

        if FETCH_BY_DATE_ENABLED:
            if "{{last_run_time}}" not in aql_query or "{{end_run_time}}" not in aql_query:
                raise ValueError(f"Missing date placeholders in AQL for {resource_name}")
        else:
            # Strip date filtering
            aql_query = aql_query.replace("AND c/context/start_time/value < '{{end_run_time}}'", "")

        for key, value in parameters.items():
            aql_query = aql_query.replace(f"{{{{{key}}}}}", str(value))

        return " ".join(aql_query.split())

    except Exception as e:
        logger.error(f"❌ Error constructing AQL for {resource_name}: {e}")
        raise

def query_resource(resource_name: str):
    conn = None
    try:
        conn = get_db_connection()
        resource_config = load_resource_config(resource_name)
        default_params = resource_config.get("parameters", {})

         # Step 1: Load resource YAML config
        resource_config = load_resource_config(resource_name)
        default_params = resource_config.get("parameters", {})

        # Step 2: Resolve last_run_time depending on fetch_by_date setting
        if FETCH_BY_DATE_ENABLED:
            last_run_time, _ = get_fetch_state(resource_name)
            start_dt = datetime.strptime(last_run_time or FETCH_START_DATE, "%Y-%m-%dT%H:%M:%S")
            end_dt = start_dt + timedelta(hours=FETCH_INTERVAL_HOURS)
            end_time = FETCH_END_DATE if FETCH_END_DATE and end_dt.strftime("%Y-%m-%dT%H:%M:%S") > FETCH_END_DATE else end_dt.strftime("%Y-%m-%dT%H:%M:%S")
            last_run_time_str = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
            logger.info(f"🚀 Fetching {resource_name} | {last_run_time_str} → {end_time}")
        else:
            # ✅ Use last_run_time from resource YAML if polling
            last_run_time_str = default_params.get("last_run_time", FETCH_START_DATE)
            end_time = ""  # No end
            logger.info(f"🚀 Polling {resource_name} from {last_run_time_str}")

        # Step 3: Inject all parameters
        parameters = {
            "last_run_time": last_run_time_str,
            "end_run_time": end_time,
            "composition_name": default_params.get("composition_name", ""),  # ✅ THIS LINE!
            "offset": int(default_params.get("offset", 0)),
            "limit": int(default_params.get("limit", 100)),
        }
        logger.debug(f"🧪 Final parameters for {resource_name}: {parameters}")

        aql_query = construct_aql_query(resource_name, parameters)
        #logger.info(f"📄 AQL for {resource_name}:\n{aql_query}")

        response = ehr_session.post(
            f"{EHR_SERVER_URL}/rest/v1/query",
            json={"aql": aql_query},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result_set = response.json().get("resultSet", [])
            if not result_set:
                logger.info(f"ℹ️ No new records for {resource_name}.")
            else:
                logger.info(f"✅ Retrieved {len(result_set)} records for {resource_name}.")
                process_records(records=result_set, resource_type=resource_name, key=aes_key)

            next_run_time = calculate_next_run_time(last_run_time_str)
            update_fetch_state(resource_name, last_run_time_str, next_run_time)

        elif response.status_code == 204:
            logger.warning(f"⚠️ No content for {resource_name}. HTTP 204.")

        else:
            logger.error(f"❌ Failed querying {resource_name}. HTTP {response.status_code}: {response.text}")

    except Exception as e:
        logger.error(f"❌ General error querying {resource_name}: {e}", exc_info=True)
    finally:
        if conn:
            release_db_connection(conn)

def query_all_resources():
    try:
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FETCHES) as executor:
            futures = {}
            for resource in RESOURCES:
                resource_name = resource["name"]
                priority = resource.get("priority", 1)

                if PRIORITY_BASED_FETCHING:
                    interval_minutes = PRIORITY_LEVELS.get(priority)
                    if interval_minutes is not None:
                        last_run, _ = get_fetch_state(resource_name)
                        if last_run:
                            elapsed = (datetime.now() - datetime.strptime(last_run, "%Y-%m-%dT%H:%M:%S")).total_seconds() / 60
                            if elapsed < interval_minutes:
                                logger.info(f"⏩ Skipping {resource_name}, only {int(elapsed)} min since last run")
                                continue

                futures[executor.submit(query_resource, resource_name)] = resource_name

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"❌ Error processing {futures[future]}: {e}")

        if POLLING_ENABLED:
            logger.info(f"✅ Cycle complete. Sleeping {POLL_INTERVAL} seconds.")
            time.sleep(POLL_INTERVAL)
        else:
            logger.info("✅ Polling disabled. Exiting after one run.")

    except KeyboardInterrupt:
        logger.info("🛑 Interrupted. Exiting.")
        sys.exit(0)
