import base64
import os
from dotenv import load_dotenv
from requests_cache import CachedSession
from datetime import timedelta
import yaml
import logging

# Load environment variables from a .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

# Load environment variables from config_files.yml file
def load_yaml_config(yaml_path):
    with open(yaml_path, 'r') as file:
        return yaml.safe_load(file)

yaml_config = load_yaml_config(os.path.join(os.path.dirname(__file__), 'settings.yml'))

# Pseudonymization configuration
PSEUDONYMIZATION_ENABLED = yaml_config.get('pseudonymization', {}).get('enabled', False)
ELEMENTS_TO_PSEUDONYMIZE = yaml_config.get('elements_to_pseudonymize', {})

# GPAS server configurations
GPAS_BASE_URL = os.getenv('GPAS_BASE_URL')
GPAS_ROOT_DOMAIN = os.getenv('GPAS_ROOT_DOMAIN')

# Certificate paths
GPAS_CLIENT_CERT = os.getenv('GPAS_CLIENT_CERT')
GPAS_CLIENT_KEY = os.getenv('GPAS_CLIENT_KEY')
GPAS_CA_CERT = os.getenv('GPAS_CA_CERT')

# OpenEHR server configuration
EHR_SERVER_URL = os.getenv("EHR_SERVER_URL")
EHR_SERVER_USER = os.getenv("EHR_SERVER_USER")
EHR_SERVER_PASSWORD = os.getenv("EHR_SERVER_PASSWORD")

# FHIR server configuration
FHIR_SERVER_URL = os.getenv("FHIR_SERVER_URL")
FHIR_SERVER_USER = os.getenv("FHIR_SERVER_USER")
FHIR_SERVER_PASSWORD = os.getenv("FHIR_SERVER_PASSWORD")
AUTH_METHOD = os.getenv("FHIR_AUTH_METHOD", "basic").lower()  # Default to 'basic' if not set

# Define the base directory for AQL files
BASE_AQL_DIR = yaml_config.get('paths', {}).get('aql_folder', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'openehr_aql'))

# Define paths for individual AQL files
RESOURCES = yaml_config.get('resources', [])
RESOURCE_FILES = {resource['name']: os.path.join(BASE_AQL_DIR, resource['file']) for resource in RESOURCES}

STATE_FILE = yaml_config.get('paths', {}).get('state_file', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'application', 'state.json'))
TEMP_FOLDER = yaml_config.get('paths', {}).get('temp_folder', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'application', 'temp'))
DB_FILE = yaml_config.get('paths', {}).get('db_file', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'application', 'Aql2FHIR.db'))
LOG_FOLDER = yaml_config.get('paths', {}).get('log_folder', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'application', 'logs'))

# Ensure log folder exists
os.makedirs(LOG_FOLDER, exist_ok=True)

# Set up logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_FOLDER, 'app.log')),
        logging.StreamHandler()  # Optional: also log to console
    ]
)

logger = logging.getLogger(__name__)

# Example usage of logger
logger.info('Logging setup complete.')