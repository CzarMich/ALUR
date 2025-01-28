import os
import yaml
from dotenv import load_dotenv

# Load environment variables from a .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Base directory for the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load YAML configuration
def load_yaml_config(yaml_path):
    with open(yaml_path, 'r') as file:
        return yaml.safe_load(file)

yaml_config = load_yaml_config(os.path.join(BASE_DIR, 'conf', 'settings.yml'))

# Paths for AQL and mappings
BASE_MAPPING_DIR = os.path.join(BASE_DIR, 'resources')

# Paths derived from `settings.yml`
STATE_FILE = os.path.join(BASE_DIR, yaml_config['paths']['state_file'])
TEMP_FOLDER = os.path.join(BASE_DIR, yaml_config['paths']['temp_folder'])
DB_FILE = os.path.join(BASE_DIR, yaml_config['paths']['db_file'])
LOG_FOLDER = os.path.join(BASE_DIR, yaml_config['paths']['log_folder'])

# EHR server configurations
EHR_SERVER_URL = os.getenv("EHR_SERVER_URL", "http://localhost/ehr/rest/v1")  # Default fallback URL
EHR_SERVER_USER = os.getenv("EHR_SERVER_USER", "admin")
EHR_SERVER_PASSWORD = os.getenv("EHR_SERVER_PASSWORD", "password")
EHR_AUTH_METHOD = os.getenv("EHR_AUTH_METHOD", "basic").lower()  # Options: basic, bearer, api_key

# FHIR server configurations
FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL", "http://localhost:8080/fhir")  # Default fallback URL
FHIR_SERVER_USER = os.getenv("FHIR_SERVER_USER", "")
FHIR_SERVER_PASSWORD = os.getenv("FHIR_SERVER_PASSWORD", "")
FHIR_AUTH_METHOD = os.getenv("FHIR_AUTH_METHOD", "basic").lower()  # Options: basic, bearer, api_key


# Pseudonymization configuration
PSEUDONYMIZATION_SETTINGS = yaml_config.get('pseudonymization', {})
PSEUDONYMIZATION_ENABLED = PSEUDONYMIZATION_SETTINGS.get('enabled', False)
GPAS_ENABLED = PSEUDONYMIZATION_SETTINGS.get('GPAS', False)  # Load GPAS-specific flag

ELEMENTS_TO_PSEUDONYMIZE = {
    element: {
        "enabled": config.get("enabled", False),
        "prefix": config.get("prefix", "")
    }
    for element, config in PSEUDONYMIZATION_SETTINGS.get('elements_to_pseudonymize', {}).items()
}

# GPAS server configurations (conditionally loaded based on GPAS_ENABLED)
GPAS_BASE_URL = os.getenv("GPAS_BASE_URL") if GPAS_ENABLED else None
GPAS_ROOT_DOMAIN = os.getenv("GPAS_ROOT_DOMAIN") if GPAS_ENABLED else None
GPAS_CLIENT_CERT = os.getenv("GPAS_CLIENT_CERT") if GPAS_ENABLED else None
GPAS_CLIENT_KEY = os.getenv("GPAS_CLIENT_KEY") if GPAS_ENABLED else None
GPAS_CA_CERT = os.getenv("GPAS_CA_CERT") if GPAS_ENABLED else None

# Database configuration
DB_SETTINGS = yaml_config.get("database", {})
DB_TYPE = DB_SETTINGS.get("name", "sqlite").lower()
DB_HOST = DB_SETTINGS.get("host", "localhost")
DB_PORT = DB_SETTINGS.get("port", 5432)
DB_USER = DB_SETTINGS.get("user", "user")
DB_PASSWORD = DB_SETTINGS.get("password", "password")
DB_NAME = DB_SETTINGS.get("database", "aql2fhir")

# Ensure DB_FILE has the correct extension
if DB_TYPE == "sqlite" and not DB_FILE.endswith(".db"):
    DB_FILE += ".db"

# Resources, AQL, and mapping files
# Resources, AQL, and mapping files
RESOURCES = yaml_config.get('resources', [])
RESOURCE_FILES = {
    resource['name']: {
        "mapping": os.path.join(BASE_MAPPING_DIR, resource['mapping_file']),
        "required_fields": resource.get('required_fields', [])  # Include required_fields
    }
    for resource in RESOURCES
}


# Date-based fetching configuration
FETCH_BY_DATE_ENABLED = yaml_config.get('fetch_by_date', {}).get('enabled', False)
FETCH_START_DATE = yaml_config.get('fetch_by_date', {}).get('start_date', "2025-01-01") or None
FETCH_END_DATE = yaml_config.get('fetch_by_date', {}).get('end_date', "") or None

# Polling Interval
USE_BATCH = yaml_config.get('processing', {}).get('use_batch', False)
BATCH_SIZE = yaml_config.get('processing', {}).get('batch_size', 1)  # Default batch size is 1 if not set
POLL_INTERVAL = yaml_config.get('polling', {}).get('interval_seconds', 60)  # Default poll interval is 60 seconds

# Debugging
#if __name__ == "__main__":
#   print(f"RESOURCE_FILES: {RESOURCE_FILES}")

