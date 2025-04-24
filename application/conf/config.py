import os
import yaml
from dotenv import load_dotenv
from pathlib import Path

# -------------------------------
# Environment Setup
# -------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
CONF_DIR = BASE_DIR / "conf"
ENV_DIR = CONF_DIR / "environment"
KEY_FOLDER = ENV_DIR / "key"
ENV_PATH = ENV_DIR / ".env"

# Load environment variables manually, allowing inline comments
def load_env_vars(dotenv_path):
    if dotenv_path.exists():
        with dotenv_path.open("r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.split("#")[0].strip()

load_env_vars(ENV_PATH)

# -------------------------------
# Paths
# -------------------------------
CONFIG_PATH = CONF_DIR / "settings.yml"
RESOURCE_CONFIG_PATH = CONF_DIR / "resource.yml"
CONSENT_RESOURCE_CONFIG_PATH = CONF_DIR / "consent_resource.yml"
RESOURCE_DIR = Path(os.getenv("RESOURCE_DIR", BASE_DIR / "resources"))

# -------------------------------
# Load YAML Configurations
# -------------------------------
def load_yaml_config(yaml_path):
    try:
        with open(yaml_path, 'r') as file:
            data = yaml.safe_load(file)
            if data is None:
                raise ValueError(f"❌ YAML config file is empty: {yaml_path}")
            return data
    except FileNotFoundError:
        raise Exception(f"🔴 Configuration file not found: {yaml_path}")
    except yaml.YAMLError as e:
        raise Exception(f"🔴 Failed to parse YAML file: {e}")

# -------------------------------
# Mapping Loader
# -------------------------------
def load_mapping_file(mapping_path):
    try:
        if mapping_path and os.path.exists(mapping_path):
            with open(mapping_path, 'r') as file:
                mapping_data = yaml.safe_load(file)
                return mapping_data
        else:
            print(f"❌ Mapping file not found: {mapping_path}")
            return {}
    except yaml.YAMLError as e:
        raise Exception(f"❌ Failed to parse mapping file {mapping_path}: {e}")

def load_mapping_file_case_insensitive(mapping_path):
    try:
        if mapping_path and os.path.exists(mapping_path):
            with open(mapping_path, 'r') as file:
                raw_data = yaml.safe_load(file)
                return {k.lower(): v for k, v in raw_data.items()} if raw_data else {}
        else:
            print(f"❌ Mapping file not found: {mapping_path}")
            return {}
    except yaml.YAMLError as e:
        raise Exception(f"❌ Failed to parse mapping file {mapping_path}: {e}")

# -------------------------------
# Load YAMLs
# -------------------------------
yaml_config = load_yaml_config(CONFIG_PATH)
resource_config = load_yaml_config(RESOURCE_CONFIG_PATH)
consent_resource_config = load_yaml_config(CONSENT_RESOURCE_CONFIG_PATH)

# -------------------------------
# File Paths
# -------------------------------
LOG_FOLDER = BASE_DIR / yaml_config.get('paths', {}).get('log_folder', 'logs')
BASE_MAPPING_DIR = RESOURCE_DIR
KEY_PATH = Path(os.getenv("KEY_PATH", KEY_FOLDER / "key.bin"))
STATE_FILE = BASE_DIR / yaml_config.get('paths', {}).get('state_file', 'state.json')
TEMP_FOLDER = BASE_DIR / yaml_config.get('paths', {}).get('temp_folder', 'temp')
DB_FILE = BASE_DIR / yaml_config.get('paths', {}).get('db_file', 'data.db')
if not str(DB_FILE).endswith(".db"):
    DB_FILE = Path(f"{DB_FILE}.db")

# -------------------------------
# Server Settings
# -------------------------------
EHR_SERVER_URL = os.getenv("EHR_SERVER_URL", "http://localhost/ehr/rest/v1")
EHR_SERVER_USER = os.getenv("EHR_SERVER_USER", "admin")
EHR_SERVER_PASSWORD = os.getenv("EHR_SERVER_PASSWORD", "password")
EHR_AUTH_METHOD = os.getenv("EHR_AUTH_METHOD", "basic").strip().lower()
FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL", "http://localhost:8080/fhir")
FHIR_SERVER_USER = os.getenv("FHIR_SERVER_USER", "")
FHIR_SERVER_PASSWORD = os.getenv("FHIR_SERVER_PASSWORD", "")
FHIR_AUTH_METHOD = os.getenv("FHIR_AUTH_METHOD", "basic").strip().lower()

if EHR_AUTH_METHOD not in ["basic", "bearer", "api_key"]:
    raise ValueError(f"🔴 Invalid EHR_AUTH_METHOD '{EHR_AUTH_METHOD}'")
if FHIR_AUTH_METHOD not in ["basic", "bearer", "api_key"]:
    raise ValueError(f"🔴 Invalid FHIR_AUTH_METHOD '{FHIR_AUTH_METHOD}'")

# -------------------------------
# Database Settings
# -------------------------------
DB_TYPE = os.getenv("DB_TYPE", "postgres").strip().lower()
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "touch")
DB_USER = os.getenv("DB_USER", "touch")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_secure_password")
if DB_TYPE not in ["postgres", "mysql"]:
    raise ValueError(f"🔴 Unsupported database type: {DB_TYPE}")

# -------------------------------
# Logging
# -------------------------------
LOGGING_VERBOSE = yaml_config.get("logging", {}).get("verbose", False)

# -------------------------------
# GPAS / Pseudonymization
# -------------------------------
PSEUDONYMIZATION_SETTINGS = yaml_config.get('pseudonymization', {})
PSEUDONYMIZATION_ENABLED = PSEUDONYMIZATION_SETTINGS.get('enabled', False)
GPAS_ENABLED = PSEUDONYMIZATION_SETTINGS.get("GPAS", False)
GPAS_DOMAIN = PSEUDONYMIZATION_SETTINGS.get("GPAS_Domain", "")
PSEUDONYMIZATION_DETERMINISTIC_AES = PSEUDONYMIZATION_SETTINGS.get("use_deterministic_aes", True)

GPAS_BASE_URL = os.getenv("GPAS_BASE_URL") if GPAS_ENABLED else None
GPAS_ROOT_DOMAIN = os.getenv("GPAS_ROOT_DOMAIN") if GPAS_ENABLED else None
GPAS_CLIENT_CERT = os.getenv("GPAS_CLIENT_CERT") if GPAS_ENABLED else None
GPAS_CLIENT_KEY = os.getenv("GPAS_CLIENT_KEY") if GPAS_ENABLED else None
GPAS_CA_CERT = os.getenv("GPAS_CA_CERT") if GPAS_ENABLED else None

ELEMENTS_TO_PSEUDONYMIZE = {
    element: {
        "enabled": config.get("enabled", False),
        "prefix": config.get("prefix", ""),
        "domain": config.get("domain", GPAS_DOMAIN)
    }
    for element, config in PSEUDONYMIZATION_SETTINGS.get("elements_to_pseudonymize", {}).items()
}

# -------------------------------
# Sanitize
# -------------------------------
SANITIZE_SETTINGS = yaml_config.get('sanitize', {})
SANITIZE_ENABLED = SANITIZE_SETTINGS.get('enabled', False)
SANITIZE_FIELDS = SANITIZE_SETTINGS.get('elements_to_sanitize', [])

# -------------------------------
# Fetch Settings
# -------------------------------
FETCH_BY_DATE_ENABLED = yaml_config.get('fetch_by_date', {}).get('enabled', False)
FETCH_START_DATE = yaml_config.get('fetch_by_date', {}).get('start_date', "2025-01-01T00:00:00") or None
FETCH_END_DATE = yaml_config.get('fetch_by_date', {}).get('end_date', "") or None
FETCH_INTERVAL_HOURS = yaml_config.get('fetch_by_date', {}).get('fetch_interval_hours', 6)

POLLING_ENABLED = yaml_config.get('polling', {}).get('enabled', False)
POLL_INTERVAL = yaml_config.get('polling', {}).get('interval_seconds', 1800)
MAX_PARALLEL_FETCHES = yaml_config.get('polling', {}).get('max_parallel_fetches', 3)

# -------------------------------
# Priority Fetching
# -------------------------------
PRIORITY_FETCHING_SETTINGS = yaml_config.get("priority_fetching", {})
PRIORITY_BASED_FETCHING = PRIORITY_FETCHING_SETTINGS.get("enabled", False)
PRIORITY_LEVELS = PRIORITY_FETCHING_SETTINGS.get("priority_levels", {})

# -------------------------------
# Retry Settings
# -------------------------------
QUERY_RETRIES_ENABLED = yaml_config.get('query_retries', {}).get('enabled', True)
QUERY_RETRY_COUNT = yaml_config.get('query_retries', {}).get('retry_count', 3)
QUERY_RETRY_INTERVAL = yaml_config.get('query_retries', {}).get('retry_interval_seconds', 10)

USE_BATCH = yaml_config.get('processing', {}).get('use_batch', False)
BATCH_SIZE = yaml_config.get('processing', {}).get('batch_size', 100)
MAX_FHIR_WORKERS = yaml_config.get('processing', {}).get('max_fhir_workers', 5)

# -------------------------------
# Health Check
# -------------------------------
HEALTH_CHECK_CONFIG = yaml_config.get("server_health_check", {})
HEALTH_CHECK_ENABLED = HEALTH_CHECK_CONFIG.get("enabled", True)
HEALTH_CHECK_RETRY_INTERVAL = HEALTH_CHECK_CONFIG.get("retry_interval_seconds", 20)
HEALTH_CHECK_MAX_RETRIES = HEALTH_CHECK_CONFIG.get("max_retries", None)

# -------------------------------
# Resource Definitions
# -------------------------------
RESOURCES = []
RESOURCE_FILES = {}
for resource in resource_config.get("resources", []):
    resource_name = resource["name"]
    mapping_file = resource.get("mapping_file")
    mapping_path = BASE_MAPPING_DIR / mapping_file if mapping_file else None
    yaml_data = load_mapping_file_case_insensitive(mapping_path)
    mappings = yaml_data.get(resource_name.lower(), {}).get("mappings", {})

    resource_entry = {
        "name": resource_name,
        "priority": resource.get("priority", 1),
        "aql_file": resource.get("aql_file"),
        "required_fields": resource.get("required_fields", []),
        "mapper_module": resource.get("mapper_module"),
        "mapper_function": resource.get("mapper_function"),
        "mappings": mappings
    }
    RESOURCES.append(resource_entry)

    RESOURCE_FILES[resource_name] = {
        "mapping_path": mapping_path,
        "mappings": mappings,
        "required_fields": resource.get("required_fields", []),
        "group_by": resource.get("group_by", "composition_id")
    }

# -------------------------------
# Consent Resource Definitions
# -------------------------------
CONSENT_RESOURCE_FILES = {}
for resource in consent_resource_config.get("resources", []):
    resource_name = resource["name"]
    mapping_file = resource.get("mapping_file")
    mapping_path = BASE_MAPPING_DIR / mapping_file if mapping_file else None
    yaml_data = load_mapping_file_case_insensitive(mapping_path)
    mappings = yaml_data.get(resource_name.lower(), {}).get("mappings", {})

    CONSENT_RESOURCE_FILES[resource_name] = {
        "mapping_path": mapping_path,
        "mappings": mappings,
        "required_fields": resource.get("required_fields", []),
        "group_by": resource.get("group_by", "composition_id")
    }

if __name__ == "__main__":
    from utils.logger import logger
    logger.info(f"✅ Loaded {len(RESOURCES)} standard and {len(CONSENT_RESOURCE_FILES)} consent resources.")
