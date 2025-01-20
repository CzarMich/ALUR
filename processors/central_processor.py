import logging
import concurrent.futures
from conf.utils_db_reader import read_table, dynamic_import
from conf.config import RESOURCES

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def process_resource(resource_name, mapper_module, mapper_function, table_name):
    """
    Process a specific resource by reading from the database, mapping, and sending it to the FHIR server.
    """
    try:
        mapper_func = dynamic_import(mapper_module, mapper_function)
    except ImportError as e:
        logger.error(f"Error importing {mapper_function} from {mapper_module}: {e}")
        return

    rows = read_table(table_name)
    if not rows:
        logger.info(f"No rows to process for {resource_name}.")
        return

    for row in rows:
        try:
            fhir_resource = mapper_func(row)
            logger.info(f"Mapped resource: {fhir_resource}")
            # Add logic for sending the resource to the FHIR server if required
        except Exception as e:
            logger.error(f"Error processing row: {row}. Error: {e}")


def main():
    """
    Main function to process all resources defined in settings.yml.
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for resource in RESOURCES:
            try:
                resource_name = resource["name"]
                mapper_module = resource["mapper_module"]
                mapper_function = resource["mapper_function"]
                table_name = resource_name.lower()

                futures.append(
                    executor.submit(process_resource, resource_name, mapper_module, mapper_function, table_name)
                )
            except KeyError as e:
                logger.error(f"Missing key in resource definition: {e}")
                continue

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"An error occurred: {e}")

logger.info(f"Resource definitions: {RESOURCES}")


if __name__ == "__main__":
    main()
