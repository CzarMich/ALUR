import logging
import os
from logging.handlers import RotatingFileHandler
from conf.config import LOG_FOLDER  # Import the log folder path from config.py

# Define the log file path
LOG_FILE = os.path.join(LOG_FOLDER, 'application.log')

# Ensure the log folder exists
os.makedirs(LOG_FOLDER, exist_ok=True)

def get_log_level():
    """
    Retrieve log level from environment variables or default to DEBUG.
    Environment variable: LOG_LEVEL
    """
    log_level = os.getenv('LOG_LEVEL', 'DEBUG').upper()
    return getattr(logging, log_level, logging.DEBUG)

def setup_logging():
    """
    Set up logging configuration.
    """
    logger = logging.getLogger()
    logger.setLevel(get_log_level())

    # Define log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File handler with log rotation
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5)  # 5MB per file, 5 backups
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHa
