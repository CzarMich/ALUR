import logging
import time
from conf.open_ehr_query import query_resource
from processors.central_processor import main as process_resources
from conf.config import RESOURCES, POLL_INTERVAL

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("Main")

def fetch_and_process_resources():
    """
    Fetch data from OpenEHR and process it into FHIR resources.
    """
    try:
        # Step 1: Fetch data from OpenEHR
        for resource in RESOURCES:
            resource_name = resource["name"]
            logger.info(f"Fetching data for resource '{resource_name}' from OpenEHR.")
            query_resource(resource_name)
            logger.info(f"Data fetching complete for resource '{resource_name}'.")

        # Step 2: Process data and send to FHIR
        logger.info("Starting central processor to process resources.")
        process_resources()
        logger.info("Central processor run complete.")
    except Exception as e:
        logger.error(f"An error occurred during execution: {e}")

if __name__ == "__main__":
    while True:
        logger.info("Starting a new fetch-and-process cycle.")
        fetch_and_process_resources()
        logger.info(f"Cycle complete. Waiting for {POLL_INTERVAL} seconds before the next run.")
        time.sleep(POLL_INTERVAL)  # Wait before the next cycle
