import os
import yaml
from dotenv import load_dotenv
import logging

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

# Ensure log folder exists
os.makedirs(LOG_FOLDER, exist_ok=True)

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
# Database configuration
DB_SETTINGS = yaml_config.get("database", {})
DB_TYPE = os.getenv("DB_TYPE", "postgres").strip().lower()  # ✅ Default to PostgreSQL
DB_HOST = os.getenv("DB_HOST", "192.168.2.32")
DB_PORT = int(os.getenv("DB_PORT", 5432))  # ✅ Ensure integer conversion
DB_NAME = os.getenv("DB_NAME", "touch")
DB_USER = os.getenv("DB_USER", "touch")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_secure_password")

if DB_TYPE not in ["postgres", "mysql"]:
    raise ValueError(f"🔴 ERROR: Unsupported database type: {DB_TYPE}")

# Log database type
logger = logging.getLogger(__name__)
logger.info(f"📢 Using {DB_TYPE.upper()} as database backend")


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

# ✅ NEW: Load MAX_FHIR_WORKERS from settings.yml
MAX_FHIR_WORKERS = yaml_config.get('processing', {}).get('max_fhir_workers', 5)

# Resources, AQL, and mapping files
def load_mapping_file(mapping_path):
    """Load mappings from a YAML file."""
    try:
        if mapping_path and os.path.exists(mapping_path):
            #print(f"✅ Found mapping file: {mapping_path}")  # Debugging
            with open(mapping_path, 'r') as file:
                mapping_data = yaml.safe_load(file)
             #   print(f"✅ Loaded mappings from {mapping_path}")  # Debugging
                return mapping_data
        else:
            print(f"❌ ERROR: Mapping file not found: {mapping_path}")
            return {}
    except yaml.YAMLError as e:
        raise Exception(f"❌ ERROR: Failed to parse mapping file {mapping_path}: {e}")

# ✅ Use BASE_MAPPING_DIR for mapping file paths
RESOURCES = []
RESOURCE_FILES = {}

for resource in yaml_config.get("resources", []):
    resource_name = resource["name"]  # ✅ Get dynamic resource name
    mapping_file = resource.get("mapping_file")
    mapping_path = os.path.join(BASE_MAPPING_DIR, mapping_file) if mapping_file else None  # ✅ Dynamic path

    # ✅ Load mappings dynamically for the current resource
    mappings = load_mapping_file(mapping_path).get(resource_name, {}).get("mappings", {}) if mapping_path else {}

    # Debugging to ensure mappings are loaded correctly
    print(f"🔍 Processing resource: {resource_name}")
    #print(f"📂 Using mapping file: {mapping_path}")
    #print(f"🔄 Mappings Loaded: {mappings}")

    # ✅ Add dynamically loaded mappings to RESOURCES
    resource_entry = {
        "name": resource_name,
        "priority": resource.get("priority", 1),
        "aql_file": resource.get("aql_file"),
        "required_fields": resource.get("required_fields", []),
        "mapper_module": resource.get("mapper_module"),
        "mapper_function": resource.get("mapper_function"),
        "mappings": mappings  # ✅ Now dynamically loads correct mappings
    }
    RESOURCES.append(resource_entry)

    # ✅ Add to RESOURCE_FILES for other usages
    RESOURCE_FILES[resource_name] = {
        "mapping_path": mapping_path,
        "mappings": mappings,
        "required_fields": resource.get("required_fields", [])
    }

# Load Logging Settings
# Logging settings
LOG_SETTINGS = yaml_config.get('logging', {})
LOG_LEVEL = LOG_SETTINGS.get('level', 'INFO').upper()
ENABLE_CONSOLE_LOG = LOG_SETTINGS.get('enable_console', True)
ENABLE_FILE_LOG = LOG_SETTINGS.get('enable_file', True)


# Set up logging configuration
log_handlers = []

# Console Logger
if ENABLE_CONSOLE_LOG:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    log_handlers.append(console_handler)

# File Logger
if ENABLE_FILE_LOG:
    log_file_path = os.path.join(LOG_FOLDER, 'application.log')  # ✅ Now using `LOG_FOLDER`
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    log_handlers.append(file_handler)

# Apply logging configuration
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=log_handlers
)


logger = logging.getLogger(__name__)
logger.info(f"📢 Logging initialized at {LOG_LEVEL} level.")  # Removed log folder path



# Debugging output
if __name__ == "__main__":
    #print(f"✅ RESOURCES Loaded: {RESOURCES}")
    print(f"✅ RESOURCE_FILES Loaded: {RESOURCE_FILES}") 