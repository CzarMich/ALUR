import logging
import os
import yaml
from dotenv import load_dotenv

# Load environment variables from a .env file
dotenv_path = os.path.join(os.path.dirname(__file__), 'environments', '.env')
load_dotenv(dotenv_path)

# Load configuration from config_files.yml
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config_files.yml')

def load_config():
    """Load configuration from YAML file."""
    with open(CONFIG_PATH, 'r') as file:
        config = yaml.safe_load(file)
    return config

config = load_config()

# Define BASE_DIR as the directory where the config file is located
BASE_DIR = os.path.dirname(CONFIG_PATH)

def get_path(key):
    """Get the absolute path for a given config key."""
    relative_path = config['paths'].get(key, '')
    return os.path.join(BASE_DIR, relative_path)

def get_temp_folder_path():
    """Get the path to the temporary folder."""
    return get_path('temp_folder')

def get_log_folder_path():
    """Get the path to the log folder."""
    return get_path('log_folder')

# Define the path for the log file
LOG_FOLDER_PATH = get_log_folder_path()
LOG_FILE_PATH = os.path.join(LOG_FOLDER_PATH, 'application.log')

def setup_logging():
    """Set up logging configuration."""
    os.makedirs(LOG_FOLDER_PATH, exist_ok=True)
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # File handler
    file_handler = logging.FileHandler(LOG_FILE_PATH)
    file_handler.setLevel(logging.DEBUG)
    
    # Define the format for logs
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
