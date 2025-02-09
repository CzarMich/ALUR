import os
import sys
# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Add the project root to Python's module search path
sys.path.insert(0, BASE_DIR)
import json
import yaml
from datetime import datetime, timedelta
import logging
from conf.config import (  # Import settings from the config file
    STATE_FILE, TEMP_FOLDER, DB_FILE, LOG_FOLDER, PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE,
    GPAS_ENABLED, GPAS_BASE_URL, GPAS_ROOT_DOMAIN, GPAS_CLIENT_CERT, GPAS_CLIENT_KEY, GPAS_CA_CERT,
    EHR_SERVER_URL, FHIR_SERVER_URL, FHIR_SERVER_USER, FHIR_SERVER_PASSWORD, RESOURCE_FILES, FETCH_BY_DATE_ENABLED, 
    FETCH_START_DATE, FETCH_END_DATE
)

# Ensure log folder exists
os.makedirs(LOG_FOLDER, exist_ok=True)

def get_path(key):
    """Get the absolute path for a given config key."""
    paths = {
        'ehr_server_url': EHR_SERVER_URL,
        'state_file': STATE_FILE,
        'temp_folder': TEMP_FOLDER,
        'db_file': DB_FILE,
        'log_folder': LOG_FOLDER,
        'fetch_start_date': FETCH_START_DATE,
        'fetch_end_date': FETCH_END_DATE,
    }
    return paths.get(key, '')

def get_required_fields(resource_name):
    """
    Get the required fields for a given resource.
    :param resource_name: Name of the resource (e.g., 'Condition').
    :return: List of required fields or raise an error if the resource is not found.
    """
    resource = RESOURCE_FILES.get(resource_name)
    if not resource:
        raise ValueError(f"No configuration found for resource '{resource_name}'. Check your settings.yml.")
    return resource.get('required_fields', [])

def generate_date_range(start_date, end_date=None):
    """
    Generate a range of dates between start_date and end_date.
    :param start_date: The start date (string in 'YYYY-MM-DD' format).
    :param end_date: The end date (string in 'YYYY-MM-DD' format). Defaults to today.
    :return: A generator yielding dates as strings in 'YYYY-MM-DD' format.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()

    while start <= end:
        yield start.strftime("%Y-%m-%d")
        start += timedelta(days=1)

def is_fetch_by_date_enabled():
    """Check if date-based fetching is enabled."""
    return FETCH_BY_DATE_ENABLED

def fetch_by_start_date():
    """Get the start date for date-based fetching."""
    return FETCH_START_DATE

def fetch_by_end_date():
    """Get the end date for date-based fetching."""
    return FETCH_END_DATE

def resources():
    """Get the list of resources to fetch."""
    return list(RESOURCE_FILES.keys())

def load_resource_config(resource_name):
    """
    Load the combined configuration (AQL + mappings) for a given resource.
    Assumes the YAML structure has the resource name as the root key.
    """
    if resource_name not in RESOURCE_FILES:
        raise ValueError(f"No configuration found for resource: {resource_name}")

    mapping_file = RESOURCE_FILES[resource_name]['mapping_path']

    if not os.path.exists(mapping_file):
        raise FileNotFoundError(f"Mapping file for resource '{resource_name}' not found: {mapping_file}")

    with open(mapping_file, 'r') as map_f:
        resource_data = yaml.safe_load(map_f)

    # Extract the resource-specific configuration (rooted at the resource name)
    if resource_name not in resource_data:
        raise ValueError(f"Missing root key '{resource_name}' in mapping file: {mapping_file}")

    return resource_data[resource_name]


def perform_gpas_pseudonymization(data):
    """
    Perform pseudonymization using GPAS, if enabled.
    """
    if not GPAS_ENABLED:
        logging.info("GPAS pseudonymization is disabled.")
        return data  # Return unmodified data

    # Proceed with GPAS pseudonymization logic here if enabled
    if not GPAS_BASE_URL:
        raise ValueError("GPAS_BASE_URL is not set but GPAS is enabled.")
    
    # Example GPAS pseudonymization logic
    logging.info("Performing GPAS pseudonymization...")
    # Placeholder logic for pseudonymization
    # data['pseudonymized'] = True
    return data

def pseudonymize_field(field_name, value):
    """
    Pseudonymize a specific field if enabled.
    """
    if not PSEUDONYMIZATION_ENABLED:
        logging.info("Pseudonymization is globally disabled.")
        return value

    config = ELEMENTS_TO_PSEUDONYMIZE.get(field_name, {})
    if not config.get("enabled", False):
        return value  # Return unmodified if pseudonymization is disabled for this field

    prefix = config.get("prefix", "")
    pseudonymized_value = f"{prefix}{hash(value)}"
    logging.info(f"Pseudonymized {field_name}: {value} -> {pseudonymized_value}")
    return pseudonymized_value
