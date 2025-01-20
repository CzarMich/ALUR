import os
import yaml
from dotenv import load_dotenv

# Load environment variables from a .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Base directory for the config file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables from settings.yml
def load_yaml_config(yaml_path):
    with open(yaml_path, 'r') as file:
        return yaml.safe_load(file)

yaml_config = load_yaml_config(os.path.join(BASE_DIR, 'settings.yml'))

# Pseudonymization configuration
PSEUDONYMIZATION_ENABLED = yaml_config.get('pseudonymization', {}).get('enabled', False)
ELEMENTS_TO_PSEUDONYMIZE = {
    element: {
        "enabled": config.get("enabled", False),
        "prefix": config.get("prefix", "")
    }
    for element, config in yaml_config.get('pseudonymization', {}).get('elements_to_pseudonymize', {}).items()
}

# GPAS server configurations
GPAS_BASE_URL = os.getenv("GPAS_BASE_URL")
GPAS_ROOT_DOMAIN = os.getenv("GPAS_ROOT_DOMAIN")

# Certificate paths
GPAS_CLIENT_CERT = os.getenv("GPAS_CLIENT_CERT")
GPAS_CLIENT_KEY = os.getenv("GPAS_CLIENT_KEY")
GPAS_CA_CERT = os.getenv("GPAS_CA_CERT")

# OpenEHR server configuration
EHR_AUTH_METHOD = os.getenv("EHR_AUTH_METHOD", "basic").lower()  # Default to 'basic' if not set
EHR_SERVER_URL = os.getenv("EHR_SERVER_URL")
EHR_SERVER_USER = os.getenv("EHR_SERVER_USER")
EHR_SERVER_PASSWORD = os.getenv("EHR_SERVER_PASSWORD")

# FHIR server configuration
FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL")
FHIR_SERVER_USER = os.getenv("FHIR_SERVER_USER")
FHIR_SERVER_PASSWORD = os.getenv("FHIR_SERVER_PASSWORD")
FHIR_AUTH_METHOD = os.getenv("FHIR_AUTH_METHOD", "basic").lower()  # Default to 'basic' if not set

# Define paths
BASE_AQL_DIR = yaml_config.get('paths', {}).get('aql_folder', os.path.join(BASE_DIR, '..', 'openehr_aql'))
STATE_FILE = yaml_config.get('paths', {}).get('state_file', os.path.join(BASE_DIR, '..', 'application', 'state.json'))
TEMP_FOLDER = yaml_config.get('paths', {}).get('temp_folder', os.path.join(BASE_DIR, '..', 'application', 'temp'))
LOG_FOLDER = yaml_config.get('paths', {}).get('log_folder', os.path.join(BASE_DIR, '..', 'application', 'logs'))

# Key file path
KEY_PATH = os.getenv("KEY_PATH")


# Database configuration
# Existing database configuration
DB_SETTINGS = yaml_config.get("database", {})
DB_TYPE = DB_SETTINGS.get("name", "sqlite").lower()
DB_HOST = DB_SETTINGS.get("host", "localhost")
DB_PORT = DB_SETTINGS.get("port", 5432)
DB_USER = DB_SETTINGS.get("user", "user")
DB_PASSWORD = DB_SETTINGS.get("password", "password")
DB_NAME = DB_SETTINGS.get("database", "aql2fhir")

# Ensure DB_FILE has the correct extension
BASE_DIR = os.path.dirname(__file__)
DB_FILE = os.path.join(BASE_DIR, "..", "application", DB_NAME)

# Append '.db' if not present and using SQLite
if DB_TYPE == "sqlite" and not DB_FILE.endswith(".db"):
    DB_FILE += ".db"

# Resources and required fields
RESOURCES = yaml_config.get('resources', [])
RESOURCE_FILES = {resource['name']: os.path.join(BASE_AQL_DIR, resource['file']) for resource in RESOURCES}
REQUIRED_FIELDS = {resource['name']: resource.get('required_fields', []) for resource in RESOURCES}

# Date-based fetching configuration
FETCH_BY_DATE_ENABLED = yaml_config.get('fetch_by_date', {}).get('enabled', False)
FETCH_START_DATE = yaml_config.get('fetch_by_date', {}).get('start_date', "2025-01-01") or None
FETCH_END_DATE = yaml_config.get('fetch_by_date', {}).get('end_date', "") or None

# Debugging
#if __name__ == "__main__":
#   #print(f"BASE_DIR: {BASE_DIR}")
#   #print(f"DB_FILE: {DB_FILE}")