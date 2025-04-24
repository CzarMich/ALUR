import sys
import os
import time
from datetime import datetime

# -----------------------------
# Bootstrap Project Paths
# -----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

# -----------------------------
# Shared Logger
# -----------------------------
from utils.logger import logger, verbose

logger.info(f"🧪 Logger handler count: {len(logger.handlers)}")

# -----------------------------
# Imports After Logger Setup
# -----------------------------
from conf import config  # Triggers config loading
from utils.openehr_query import query_resource
from utils.central_processor import main as process_standard_resources
from utils.central_processor_consent import main as process_consent_resources
from utils.resource import poll_and_process_fhir
from utils.resource_consent import poll_and_process_fhir_consent
from conf.config import RESOURCES, POLL_INTERVAL as DEFAULT_POLL_INTERVAL, EHR_SERVER_URL
from utils.healthcheck import server_heartbeat_check

# -----------------------------
# Folder Checks
# -----------------------------
for folder in ["logs", "data", "application"]:
    os.makedirs(os.path.join(BASE_DIR, folder), exist_ok=True)

# -----------------------------
# Polling Interval
# -----------------------------
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", DEFAULT_POLL_INTERVAL))

# -----------------------------
# Utility Wrapper with Logging
# -----------------------------
def run_step(description: str, func, *args, **kwargs):
    start_time = time.time()
    logger.info(f"🚀 Starting: {description}")
    try:
        result = func(*args, **kwargs)
        duration = time.time() - start_time
        logger.info(f"✅ Completed: {description} in {duration:.2f} seconds")
        return result
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"❌ Error during: {description} after {duration:.2f}s | {e}", exc_info=True)

# -----------------------------
# Main ALUR Cycle Logic
# -----------------------------
def alur_cycle():
    cycle_start = time.time()
    cycle_timestamp = datetime.utcnow().isoformat()
    logger.info(f"🚀 ALUR started a new cycle at {cycle_timestamp}")

    # ✅ Run heartbeat check once
    if not run_step("Heartbeat check", server_heartbeat_check, f"{EHR_SERVER_URL}/rest/v1/ehr", "OpenEHR"):
        logger.error(" Skipping cycle due to failed heartbeat.")
        return

    # STEP 1: Fetch OpenEHR Standard Resources (Skip Consent)
    for resource in RESOURCES:
        resource_name = resource.get("name", "").lower()
        if resource_name == "consent":
            verbose(" Skipping Consent fetch in standard OpenEHR processing.")
            continue
        run_step(f"Fetching OpenEHR resource '{resource_name}'", query_resource, resource_name)

    # STEP 2: Process Standard Resources
    run_step("Processing standard resources (central processor)", process_standard_resources)

    # STEP 3: Process FHIR Queue for Standard Resources
    run_step("Posting standard resources to FHIR queue", poll_and_process_fhir)

    # STEP 4: Conditionally Fetch & Process Consent Resources
    consent_active = any(r.get("name", "").lower() == "consent" for r in RESOURCES)

    if consent_active:
        run_step("Fetching Consent resource from OpenEHR", query_resource, "Consent")
        run_step("Processing Consent resources (central processor)", process_consent_resources)
        run_step("Posting Consent resources to FHIR queue", poll_and_process_fhir_consent)
    else:
        verbose("Consent resource not active in configuration. Skipping Consent steps.")

    cycle_duration = time.time() - cycle_start
    logger.info(f"✅ ALUR cycle completed in {cycle_duration:.2f} seconds.")

# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    try:
        logger.info("🟢 ALUR processing started.")
        while True:
            alur_cycle()
            logger.info(f"⏳ Waiting {POLL_INTERVAL}s before next cycle.")
            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Interrupted by user (CTRL+C). Exiting gracefully.")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        time.sleep(10)
