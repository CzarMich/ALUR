import xml.etree.ElementTree as ET
import urllib.parse
import logging
from utils_db import process_records
from conf.config import EHR_SERVER_URL, RESOURCES
from utils.utils_session import create_session

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Create a reusable session
ehr_session = create_session(
    cache_name='ehr_query_cache',
    auth_method='basic',
    username='EHR_SERVER_USER',
    password='EHR_SERVER_PASSWORD',
)


def read_aql_query(file_path: str) -> str:
    """
    Read and return the AQL query from the specified XML file.

    :param file_path: Path to the XML file containing the AQL query.
    :return: The AQL query as a string.
    """
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        return root.text.strip()
    except Exception as e:
        logger.error(f"Error reading AQL query file '{file_path}': {e}")
        raise


def query_resource(resource_type: str, last_run_time: str = None):
    """
    Query a resource from the OpenEHR server using AQL and store results in the database.

    :param resource_type: The name of the resource (e.g., 'Condition').
    :param last_run_time: The last run time to replace placeholders in the AQL query.
    """
    file_path = f'openehr_aql/{resource_type}.xml'

    try:
        # Read the AQL query
        aql_query = read_aql_query(file_path)

        # Replace placeholders in the AQL query
        if last_run_time:
            aql_query = aql_query.replace('{{last_run_time}}', last_run_time)
        else:
            aql_query = aql_query.replace('{{last_run_time}}', '2024-03-06')

        logger.info(f"Constructed AQL Query for {resource_type}: {aql_query}")

        # Encode the AQL query for the URL
        encoded_query = urllib.parse.quote(aql_query)
        url = f"{EHR_SERVER_URL}/query?aql={encoded_query}"

        # Use the reusable session from `utils_sessions`
        response = ehr_session.get(url)
        response.raise_for_status()

        if response.status_code == 200:
            result_set = response.json().get('resultSet', [])
            if not result_set:
                logger.info(f"No records found for the query on {resource_type}.")
                return

            logger.info(f"Retrieved {len(result_set)} records for {resource_type}.")

            # Retrieve resource-specific configurations
            resource_config = next((r for r in RESOURCES if r["name"] == resource_type), None)
            if not resource_config:
                logger.warning(f"Resource configuration not found for {resource_type}.")
                return

            required_fields = resource_config.get("required_fields", [])

            # Process and store the results in the database
            process_records(
                records=result_set,
                resource_type=resource_type,
                key=None,  # Encryption key, if needed, can be passed here
                required_fields=required_fields
            )
            logger.info(f"Successfully processed and stored records for {resource_type}.")
        elif response.status_code == 204:
            logger.info(f"No content found for the query on {resource_type}.")
        else:
            logger.error(f"Unexpected response: {response.status_code} - {response.text}")

    except Exception as e:
        logger.error(f"Error occurred while querying {resource_type}: {e}")
        raise


# Example usage
if __name__ == "__main__":
    # Replace 'Condition' with the desired resource type and provide a last run time if needed
    query_resource('Condition', last_run_time='2025-01-01T00:00:00Z')
