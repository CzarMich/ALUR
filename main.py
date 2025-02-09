import logging
import time
import sys
from utils.utils_openehr_query import query_resource
from utils.utils_central_processor import main as process_resources
from utils.utils_resource import poll_and_process_fhir  # ✅ Ensure FHIR processing starts
from conf.config import RESOURCES, POLL_INTERVAL
from conf.config import logger

def fetch_and_process_resources():
    """
    Fetch data from OpenEHR, process it, and push to FHIR.
    """
    try:
        logger.info("🚀 Starting a new fetch-and-process cycle.")

        # ✅ Step 1: Fetch data from OpenEHR
        for resource in RESOURCES:
            resource_name = resource["name"]
            logger.info(f"🔍 Fetching data for resource '{resource_name}' from OpenEHR.")
            query_resource(resource_name)
            logger.info(f"✅ Data fetching complete for resource '{resource_name}'.")

        # ✅ Step 2: Process data and send to FHIR
        logger.info("⚡ Starting central processor to process resources.")
        process_resources()
        logger.info("✅ Central processor run complete.")

        # ✅ Step 3: Process FHIR queue after central processor is done
        logger.info("📡 Starting FHIR queue processing...")
        poll_and_process_fhir()  # 🔥 Now runs AFTER resources are processed
        logger.info("✅ FHIR queue processing complete.")

    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted by user (CTRL+C). Exiting gracefully...")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ An unexpected error occurred: {e}", exc_info=True)
        time.sleep(10)  # Prevent spamming logs, wait before retrying


if __name__ == "__main__":
    try:
        while True:
            fetch_and_process_resources()
            logger.info(f"⏳ Cycle complete. Waiting for {POLL_INTERVAL} seconds before the next run.")
            time.sleep(POLL_INTERVAL)  # ✅ Wait before next cycle

    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted. Exiting...")
        sys.exit(0)
