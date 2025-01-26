import logging
from conf.utils_db_reader import connect_to_db
from conf.utils_session import create_session
from conf.config import FHIR_AUTH_METHOD, FHIR_SERVER_PASSWORD

logger = logging.getLogger("ResourceUtils")

# Create a reusable FHIR session
fhir_session = create_session(
    cache_name="fhir_cache",
    auth_method=FHIR_AUTH_METHOD,
    token=FHIR_SERVER_PASSWORD  # Use token for bearer auth if applicable
)


def send_fhir_resource(resource_type, resource_identifier, resource_data):
    """
    Send a FHIR resource to the FHIR server using PUT/POST.
    """
    try:
        # Construct the search URL
        search_url = f"{fhir_session.base_url}/{resource_type}?identifier={resource_identifier}"
        search_response = fhir_session.get(search_url)

        if search_response.status_code == 200:
            search_results = search_response.json()
            if search_results.get('total', 0) > 0:
                # Resource exists, perform a PUT
                existing_id = search_results['entry'][0]['resource']['id']
                put_url = f"{fhir_session.base_url}/{resource_type}/{existing_id}"
                response = fhir_session.put(put_url, json=resource_data)
            else:
                # Resource does not exist, perform a POST
                post_url = f"{fhir_session.base_url}/{resource_type}"
                response = fhir_session.post(post_url, json=resource_data)
        else:
            logger.error(f"Failed to query resource {resource_identifier}: {search_response.text}")
            return False

        if response.status_code in [200, 201]:
            logger.info(f"Resource {resource_type}/{resource_identifier} processed successfully.")
            return True
        else:
            logger.error(f"Failed to process resource {resource_type}/{resource_identifier}: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending FHIR resource {resource_identifier}: {e}")
        return False


def delete_row_from_db(table_name, row_id):
    """
    Delete a row from the database after successful processing.
    """
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        if conn.__class__.__name__ == "Connection":  # SQLite
            query = f"DELETE FROM {table_name} WHERE id = ?"
        else:  # PostgreSQL
            query = f"DELETE FROM {table_name} WHERE id = %s"
        cursor.execute(query, (row_id,))
        conn.commit()
        conn.close()
        logger.info(f"Deleted row with ID {row_id} from table {table_name}.")
    except Exception as e:
        logger.error(f"Error deleting row ID {row_id} from {table_name}: {e}")
