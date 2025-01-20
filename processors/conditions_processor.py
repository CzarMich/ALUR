import os
import logging
import json
import concurrent.futures
from conf.utils_session import fhir_session  # Use the session to handle FHIR communication
from conf.utils_db_reader import read_table, connect_to_db
from fhir_mapping.condition_mapper import map_condition
from conf.config import RESOURCES

# Set up logging
log_file_path = os.path.join(os.path.dirname(__file__), 'conditions_processing.log')
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def send_fhir_resource(resource_type, resource_identifier, resource_data, session):
    """
    Send the FHIR resource to the FHIR server using PUT based on identifier.
    If PUT fails with 404, fall back to POST for resource creation.
    """
    # Construct the FHIR search query for the identifier
    search_url = f"{session.base_url}/{resource_type}?identifier={resource_identifier}"
    
    # Check if the resource exists using the identifier
    search_response = session.get(search_url)
    if search_response.status_code == 200:
        search_results = search_response.json()
        if search_results.get('total', 0) > 0:
            # Resource exists, extract the `id` and perform a PUT
            existing_id = search_results['entry'][0]['resource']['id']
            put_url = f"{session.base_url}/{resource_type}/{existing_id}"
            response = session.put(put_url, json=resource_data)
        else:
            # Resource not found, perform a POST
            post_url = f"{session.base_url}/{resource_type}"
            response = session.post(post_url, json=resource_data)
    else:
        logger.error(f"Failed to query resource identifier {resource_identifier}: {search_response.text}")
        return False

    if response.status_code in [200, 201]:
        logger.info(f"Resource {resource_type}/{resource_identifier} processed successfully.")
        return True
    else:
        logger.error(f"Failed to process resource {resource_type}/{resource_identifier}: {response.text}")
        return False


def delete_row_from_db(table_name, row_id):
    """
    Delete a row from the database after successful processing.
    """
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (row_id,))
        conn.commit()
        conn.close()
        logger.info(f"Deleted row with ID {row_id} from table {table_name}.")
    except Exception as e:
        logger.error(f"Error deleting row ID {row_id} from {table_name}: {e}")


def process_resource(resource_type, table_name, mapper_func):
    """
    Read rows from the database, map them to FHIR resources, and send them to the FHIR server.
    """
    rows = read_table(table_name)
    if not rows:
        logger.info(f"No rows to process for {resource_type}.")
        return

    with fhir_session as session:
        for row in rows:
            try:
                fhir_resource = mapper_func(row)
                resource_identifier = fhir_resource.get("identifier", {}).get("value")
                if not resource_identifier:
                    logger.warning(f"No identifier found for resource: {fhir_resource}")
                    continue
                if send_fhir_resource(resource_type, resource_identifier, fhir_resource, session):
                    delete_row_from_db(table_name, row["id"])  # Delete row after successful POST/PUT
                else:
                    logger.warning(f"Failed to process row with ID: {row['id']}")
            except Exception as e:
                logger.error(f"Error processing row: {row}. Error: {e}")



def main():
    """
    Main entry point for processing resources.
    """
    resource_mappers = {
        "Condition": map_condition,
        # Add other resource-specific mappers here
    }

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(process_resource, res["name"], res["name"].lower(), resource_mappers[res["name"]])
            for res in RESOURCES if res["name"] in resource_mappers
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred during resource processing: {e}")


if __name__ == "__main__":
    main()
