import os
import sys
import json
import yaml
from datetime import datetime, timedelta
import logging
from utils.logger import logger, verbose
from conf.config import (
    BASE_DIR, STATE_FILE, TEMP_FOLDER, DB_FILE, LOG_FOLDER,
    PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE,
    GPAS_ENABLED, GPAS_BASE_URL, FETCH_BY_DATE_ENABLED,
    FETCH_START_DATE, FETCH_END_DATE, RESOURCE_CONFIG_PATH,
    BASE_MAPPING_DIR
)

# Logging setup
logger = logging.getLogger("Utils")

# Ensure log folder exists
os.makedirs(LOG_FOLDER, exist_ok=True)

# Path to resource.yml (loaded from config)
RESOURCE_YML_PATH = RESOURCE_CONFIG_PATH


def get_path(key):
    paths = {
        'state_file': STATE_FILE,
        'temp_folder': TEMP_FOLDER,
        'db_file': DB_FILE,
        'log_folder': LOG_FOLDER,
        'fetch_start_date': FETCH_START_DATE,
        'fetch_end_date': FETCH_END_DATE,
    }
    return paths.get(key, '')


def generate_date_range(start_date, end_date=None):
    """Generate daily date range between start and end."""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
    except ValueError as e:
        logger.error(f"Invalid date format in generate_date_range: {e}")
        return

    while start <= end:
        yield start.strftime("%Y-%m-%d")
        start += timedelta(days=1)


def is_fetch_by_date_enabled():
    return FETCH_BY_DATE_ENABLED


def fetch_by_start_date():
    return FETCH_START_DATE


def fetch_by_end_date():
    return FETCH_END_DATE


def load_all_active_resources():
    """Load all active resources from resource.yml."""
    if not os.path.exists(RESOURCE_YML_PATH):
        raise FileNotFoundError(f"Resource file not found at {RESOURCE_YML_PATH}")

    with open(RESOURCE_YML_PATH, 'r') as file:
        config = yaml.safe_load(file)

    resources = config.get("resources", [])
    return {res["name"].lower(): res for res in resources if "name" in res}


def load_resource_config(resource_name):
    """
    Loads and returns the resource config (e.g. query_template and parameters)
    from the resource YAML file, handling case-insensitive keys.
    """
    import yaml
    from conf.config import RESOURCE_FILES

    normalized_name = resource_name.lower()

    if normalized_name not in RESOURCE_FILES:
        raise ValueError(f"No RESOURCE_FILES entry for resource: {normalized_name}")

    mapping_path = RESOURCE_FILES[normalized_name].get("mapping_path")
    if not mapping_path or not mapping_path.exists():
        raise FileNotFoundError(f"AQL/mapping YAML file not found: {mapping_path}")

    with open(mapping_path, 'r') as f:
        yaml_data = yaml.safe_load(f) or {}

    # Normalize top-level keys to lowercase for case-insensitive matching
    normalized_data = {key.lower(): value for key, value in yaml_data.items()}

    if normalized_name not in normalized_data:
        raise ValueError(f"Query template missing for resource: {resource_name}")

    return normalized_data[normalized_name]



def get_required_fields(resource_name):
    """Get required fields for a specific resource."""
    resource_config = load_resource_config(resource_name)
    return resource_config.get('required_fields', [])


def get_all_required_fields():
    """Return required fields for all active resources."""
    active_resources = load_all_active_resources()
    return {
        name: res.get("required_fields", [])
        for name, res in active_resources.items()
    }


def get_aql_for_resource(resource_name: str, aql_file: str) -> str:
    """Load AQL query template for a resource from the given file (case-insensitive)."""
    aql_path = BASE_MAPPING_DIR / aql_file
    if not aql_path.exists():
        raise FileNotFoundError(f"AQL file not found: {aql_path}")

    with open(aql_path, "r") as file:
        content = yaml.safe_load(file)

    # Normalize all top-level keys to lowercase
    lower_content = {k.lower(): v for k, v in content.items()}
    resource_key = resource_name.lower()

    if resource_key not in lower_content:
        raise ValueError(f"AQL content not found under key '{resource_key}' in {aql_file}")

    query_template = lower_content[resource_key].get("query_template")
    if not query_template:
        raise ValueError(f"Missing 'query_template' in {aql_file} for resource '{resource_name}'")

    return query_template



# --- Pseudonymization Helpers ---

def perform_gpas_pseudonymization(data):
    if not GPAS_ENABLED:
        verbose("GPAS pseudonymization is disabled.")
        return data

    if not GPAS_BASE_URL:
        raise ValueError("GPAS_BASE_URL is not set but GPAS is enabled.")

    verbose("Performing GPAS pseudonymization...")
    return data


def pseudonymize_field(field_name, value):
    if not PSEUDONYMIZATION_ENABLED:
        verbose("Pseudonymization is globally disabled.")
        return value

    config = ELEMENTS_TO_PSEUDONYMIZE.get(field_name, {})
    if not config.get("enabled", False):
        return value

    prefix = config.get("prefix", "")
    pseudonymized_value = f"{prefix}{hash(value)}"
    verbose(f"Pseudonymized {field_name}: {value} -> {pseudonymized_value}")
    return pseudonymized_value
