import csv
import logging
from conf.utils_aql import execute_aql_query

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define your AQL query
AQL_QUERY = """
SELECT c/context/start_time/value AS MSH10Timesatmp, v/commit_audit/time_committed/value
FROM VERSION v
CONTAINS COMPOSITION c  
WHERE c/archetype_details/template_id/value = 'KDS_Diagnose_Extended' AND c/context/start_time/value >"2022-12-01"  AND c/context/start_time/value <"2024-12-01" 
AND NOT EXISTS c/content[openEHR-EHR-EVALUATION.problem_diagnosis.v1]/data/items[at0002]/value/defining_code
ORDER BY c/context/start_time/value DESC
LIMIT 10
"""

# Define resource type for state tracking
RESOURCE_TYPE = "Condition"

# Path for saving results to CSV
CSV_OUTPUT_PATH = "aql_results.csv"


def save_results_to_csv(results, output_path):
    """
    Save the result set to a CSV file.

    :param results: List of dictionaries containing the query results.
    :param output_path: Path to save the CSV file.
    """
    if not results:
        logger.info("No results to save.")
        return

    # Extract column names from the first row
    column_names = results[0].keys()

    # Write to CSV
    with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()
        writer.writerows(results)

    logger.info(f"Results saved to {output_path}")


def test_execute_aql_query():
    """
    Test function to execute the AQL query and save results to a CSV file.
    """
    logger.info("Executing AQL query...")
    response = execute_aql_query(AQL_QUERY, RESOURCE_TYPE)

    if response and response.get('resultSet'):
        results = response['resultSet']
        logger.info(f"Fetched {len(results)} records.")
        save_results_to_csv(results, CSV_OUTPUT_PATH)
    else:
        logger.error("No results fetched or query execution failed.")


if __name__ == "__main__":
    test_execute_aql_query()
