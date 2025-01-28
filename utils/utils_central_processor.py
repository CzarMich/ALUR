import logging
import time
from conf.config import RESOURCES, POLL_INTERVAL, USE_BATCH, BATCH_SIZE
from utils.utils_db_reader import read_unprocessed_rows, read_unprocessed_rows_in_batch
from utils.utils_resource import send_fhir_resource, delete_row_from_db
from utils.utils_mapper import map_and_clean_resource

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CentralProcessor")


def process_single_row(resource_name, table_name, row, mappings, required_fields):
    """
    Process a single row:
    1. Map the row to a FHIR resource.
    2. Send the resource to the FHIR server.
    3. Delete the row if successful.
    """
    try:
        # Map the row dynamically based on mappings and required fields
        fhir_resource = map_and_clean_resource(row, mappings, required_fields)

        # Print or log the mapped FHIR resource
        logger.info(f"Generated FHIR resource: {fhir_resource}")

        # Ensure resourceType is included
        if "resourceType" not in fhir_resource:
            fhir_resource["resourceType"] = resource_name

        # Extract the identifier for the FHIR resource
        resource_identifier = (
            fhir_resource.get("identifier", [{}])[0].get("value")
            if fhir_resource.get("identifier")
            else None
        )

        if not resource_identifier:
            logger.warning(f"No identifier found for resource: {fhir_resource}")
            return False

        # Send the FHIR resource
        if send_fhir_resource(resource_name, resource_identifier, fhir_resource):
            delete_row_from_db(table_name, row["id"])
            logger.info(f"Successfully processed and deleted row ID {row['id']} for '{resource_name}'.")
            return True
        else:
            logger.error(f"Failed to process resource {resource_name}/{resource_identifier}")
            return False
    except Exception as e:
        logger.error(f"Error processing row {row}: {e}")
        return False


def process_batch(resource_name, table_name, batch, mappings, required_fields):
    """
    Process a batch of rows:
    1. Map rows to FHIR resources.
    2. Send resources to the FHIR server.
    3. Delete rows if successful.
    """
    try:
        for row in batch:
            success = process_single_row(resource_name, table_name, row, mappings, required_fields)
            if not success:
                logger.warning(f"Failed to process row ID {row['id']} in batch. Skipping.")
    except Exception as e:
        logger.error(f"Error processing batch for resource '{resource_name}': {e}")


def process_resource(resource):
    """
    Process unprocessed rows for a specific resource:
    - In batch mode, process rows in batches.
    - In single-row mode, process rows one at a time.
    """
    resource_name = resource["name"]
    table_name = resource_name.lower()

    # Retrieve mappings and required fields from the resource configuration
    mappings = resource.get("mappings")
    required_fields = resource.get("required_fields", [])
    if not mappings:
        logger.error(f"No mappings defined for resource '{resource_name}'. Skipping.")
        return

    while True:
        if USE_BATCH:
            # Fetch and process in batches
            batch = read_unprocessed_rows_in_batch(table_name, BATCH_SIZE)
            if not batch:
                logger.info(f"No unprocessed rows for resource '{resource_name}'. Pausing for {POLL_INTERVAL} seconds.")
                time.sleep(POLL_INTERVAL)
                continue
            process_batch(resource_name, table_name, batch, mappings, required_fields)
        else:
            # Fetch and process one row at a time
            row = read_unprocessed_rows(table_name)
            if not row:
                logger.info(f"No unprocessed rows for resource '{resource_name}'. Pausing for {POLL_INTERVAL} seconds.")
                time.sleep(POLL_INTERVAL)
                continue
            process_single_row(resource_name, table_name, row[0], mappings, required_fields)


def main():
    """
    Main entry point for the central processor:
    1. Iterate over resources defined in settings.yml (via config.py).
    2. Continuously process each resource dynamically.
    """
    while True:
        for resource in RESOURCES:
            process_resource(resource)


if __name__ == "__main__":
    main()
