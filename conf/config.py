import os
import yaml
from dotenv import load_dotenv

# Load environment variables from a .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')

# Ensure comments in .env are ignored by manually parsing
def load_env_vars(dotenv_path):
    """ Load environment variables manually while ignoring comments. """
    if os.path.exists(dotenv_path):
        with open(dotenv_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):  # Ignore comments
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.split("#")[0].strip()  # Ignore inline comments

# Load environment variables safely
load_env_vars(dotenv_path)

# Base directory for the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load YAML configuration
def load_yaml_config(yaml_path):
    """Load YAML settings safely with error handling."""
    try:
        with open(yaml_path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise Exception(f"🔴 ERROR: Configuration file not found at {yaml_path}")
    except yaml.YAMLError as e:
        raise Exception(f"🔴 ERROR: Failed to parse YAML file: {e}")

yaml_config = load_yaml_config(os.path.join(BASE_DIR, 'conf', 'settings.yml'))

# Paths for AQL and mappings
BASE_MAPPING_DIR = os.path.join(BASE_DIR, 'resources')

# Retrieve secure key path
KEY_PATH = os.getenv("KEY_PATH", os.path.join(os.path.dirname(__file__), "conf", "key", "key.bin"))

# Paths derived from `settings.yml`
STATE_FILE = os.path.join(BASE_DIR, yaml_config.get('paths', {}).get('state_file', 'state.json'))
TEMP_FOLDER = os.path.join(BASE_DIR, yaml_config.get('paths', {}).get('temp_folder', 'temp'))
DB_FILE = os.path.join(BASE_DIR, yaml_config.get('paths', {}).get('db_file', 'data.db'))
LOG_FOLDER = os.path.join(BASE_DIR, yaml_config.get('paths', {}).get('log_folder', 'logs'))

# Ensure DB_FILE has the correct extension
if not DB_FILE.endswith(".db"):
    DB_FILE += ".db"

# EHR server configurations
EHR_SERVER_URL = os.getenv("EHR_SERVER_URL", "http://localhost/ehr/rest/v1")  # Default fallback URL
EHR_SERVER_USER = os.getenv("EHR_SERVER_USER", "admin")
EHR_SERVER_PASSWORD = os.getenv("EHR_SERVER_PASSWORD", "password")
EHR_AUTH_METHOD = os.getenv("EHR_AUTH_METHOD", "basic").strip().lower()  # Ensure lowercase and strip spaces

# Validate authentication method
if EHR_AUTH_METHOD not in ["basic", "bearer", "api_key"]:
    raise ValueError(f"🔴 ERROR: Invalid EHR_AUTH_METHOD '{EHR_AUTH_METHOD}'. Allowed: basic, bearer, api_key")

# FHIR server configurations
FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL", "http://localhost:8080/fhir")  # Default fallback URL
FHIR_SERVER_USER = os.getenv("FHIR_SERVER_USER", "")
FHIR_SERVER_PASSWORD = os.getenv("FHIR_SERVER_PASSWORD", "")
FHIR_AUTH_METHOD = os.getenv("FHIR_AUTH_METHOD", "basic").strip().lower()  # Ensure lowercase and strip spaces

# Validate authentication method for FHIR
if FHIR_AUTH_METHOD not in ["basic", "bearer", "api_key"]:
    raise ValueError(f"🔴 ERROR: Invalid FHIR_AUTH_METHOD '{FHIR_AUTH_METHOD}'. Allowed: basic, bearer, api_key")

# Fetch by date configuration
FETCH_BY_DATE_ENABLED = yaml_config.get('fetch_by_date', {}).get('enabled', False)
FETCH_START_DATE = yaml_config.get('fetch_by_date', {}).get('start_date', "2025-01-01T00:00:00") or None
FETCH_END_DATE = yaml_config.get('fetch_by_date', {}).get('end_date', "") or None
FETCH_INTERVAL_HOURS = yaml_config.get('fetch_by_date', {}).get('fetch_interval_hours', 6)  # Default to 6 hours

# Polling configuration
POLLING_ENABLED = yaml_config.get('polling', {}).get('enabled', False)
POLL_INTERVAL = yaml_config.get('polling', {}).get('interval_seconds', 1800)  # Default to 30 minutes
MAX_PARALLEL_FETCHES = yaml_config.get('polling', {}).get('max_parallel_fetches', 3)

# Priority-based fetching

fetching_config = yaml_config.get("fetching", {})
PRIORITY_BASED_FETCHING = fetching_config.get("priority_based", False)  # Default is False


# Database configuration
DB_SETTINGS = yaml_config.get("database", {})
DB_TYPE = DB_SETTINGS.get("name", "sqlite").strip().lower()
DB_HOST = DB_SETTINGS.get("host", "localhost")
DB_PORT = DB_SETTINGS.get("port", 5432)
DB_USER = DB_SETTINGS.get("user", "user")
DB_PASSWORD = DB_SETTINGS.get("password", "password")
DB_NAME = DB_SETTINGS.get("database", "aql2fhir")

# Pseudonymization configuration
PSEUDONYMIZATION_SETTINGS = yaml_config.get('pseudonymization', {})
PSEUDONYMIZATION_ENABLED = PSEUDONYMIZATION_SETTINGS.get('enabled', False)
PSEUDONYMIZATION_DETERMINISTIC_AES = PSEUDONYMIZATION_SETTINGS.get('use_deterministic_aes', True)  # Default to True
# GPAS Configuration
GPAS_ENABLED = yaml_config.get("pseudonymization", {}).get("GPAS", False)

if GPAS_ENABLED:
    GPAS_BASE_URL = os.getenv("GPAS_BASE_URL", None)
    GPAS_ROOT_DOMAIN = os.getenv("GPAS_ROOT_DOMAIN", None)
    GPAS_CLIENT_CERT = os.getenv("GPAS_CLIENT_CERT", None)
    GPAS_CLIENT_KEY = os.getenv("GPAS_CLIENT_KEY", None)
    GPAS_CA_CERT = os.getenv("GPAS_CA_CERT", None)
else:
    GPAS_BASE_URL = None
    GPAS_ROOT_DOMAIN = None
    GPAS_CLIENT_CERT = None
    GPAS_CLIENT_KEY = None
    GPAS_CA_CERT = None


ELEMENTS_TO_PSEUDONYMIZE = {
    element: {
        "enabled": config.get("enabled", False),
        "prefix": config.get("prefix", "")
    }
    for element, config in PSEUDONYMIZATION_SETTINGS.get('elements_to_pseudonymize', {}).items()
}

# Sanitization configuration
SANITIZE_SETTINGS = yaml_config.get('sanitize', {})
SANITIZE_ENABLED = SANITIZE_SETTINGS.get('enabled', False)
SANITIZE_FIELDS = SANITIZE_SETTINGS.get('elements_to_sanitize', [])

# Query retries configuration
QUERY_RETRIES_ENABLED = yaml_config.get('query_retries', {}).get('enabled', True)
QUERY_RETRY_COUNT = yaml_config.get('query_retries', {}).get('retry_count', 3)
QUERY_RETRY_INTERVAL = yaml_config.get('query_retries', {}).get('retry_interval_seconds', 10)

# Server health check configuration
HEALTH_CHECK_CONFIG = yaml_config.get("server_health_check", {})
HEALTH_CHECK_ENABLED = HEALTH_CHECK_CONFIG.get("enabled", True)
HEALTH_CHECK_RETRY_INTERVAL = HEALTH_CHECK_CONFIG.get("retry_interval_seconds", 20)
HEALTH_CHECK_MAX_RETRIES = HEALTH_CHECK_CONFIG.get("max_retries", None)  # None means unlimited retries

# Processing settings
USE_BATCH = yaml_config.get('processing', {}).get('use_batch', False)
BATCH_SIZE = yaml_config.get('processing', {}).get('batch_size', 100)

# Resources, AQL, and mapping files
RESOURCES = yaml_config.get('resources', [])
RESOURCE_FILES = {
    resource['name']: {
        "mapping": os.path.join(BASE_MAPPING_DIR, resource.get('mapping_file', 'default_mapping.json')),
        "required_fields": resource.get('required_fields', [])
    }
    for resource in RESOURCES
}

# Debugging
if __name__ == "__main__":
    print(f"🔍 CONFIGURATION LOADED SUCCESSFULLY")
    print(f"🔹 EHR_AUTH_METHOD: {EHR_AUTH_METHOD}")
    print(f"🔹 FHIR_AUTH_METHOD: {FHIR_AUTH_METHOD}")
    print(f"🔹 DB_TYPE: {DB_TYPE}")
    print(f"🔹 GPAS_ENABLED: {GPAS_ENABLED}")
    print(f"🔹 FETCH_BY_DATE_ENABLED: {FETCH_BY_DATE_ENABLED}")
    print(f"🔹 FETCH_INTERVAL_HOURS: {FETCH_INTERVAL_HOURS}")
    print(f"🔹 POLLING_ENABLED: {POLLING_ENABLED}")
    print(f"🔹 POLL_INTERVAL: {POLL_INTERVAL}")
    print(f"🔹 MAX_PARALLEL_FETCHES: {MAX_PARALLEL_FETCHES}")
    print(f"🔹 PRIORITY_BASED_FETCHING: {PRIORITY_BASED_FETCHING}")
    print(f"🔹 QUERY_RETRIES_ENABLED: {QUERY_RETRIES_ENABLED}")
    print(f"🔹 HEALTH_CHECK_ENABLED: {HEALTH_CHECK_ENABLED}")
    print(f"🔹 RESOURCE_FILES: {RESOURCE_FILES}")
