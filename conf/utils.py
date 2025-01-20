import os
import json
import yaml
import tempfile
import sqlite3
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
import re
from datetime import datetime, timedelta
import logging
from requests_cache import CachedSession
from conf.config import (# Import the settings from the config file
    STATE_FILE, TEMP_FOLDER, DB_FILE, LOG_FOLDER,BASE_AQL_DIR, PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE,BASE_AQL_DIR,
    GPAS_BASE_URL, GPAS_ROOT_DOMAIN, GPAS_CLIENT_CERT, GPAS_CLIENT_KEY, GPAS_CA_CERT, EHR_SERVER_URL, EHR_SERVER_USER, EHR_SERVER_PASSWORD, FHIR_SERVER_URL, FHIR_SERVER_USER, FHIR_SERVER_PASSWORD, FHIR_AUTH_METHOD, RESOURCES, RESOURCE_FILES, KEY_PATH, FETCH_BY_DATE_ENABLED, FETCH_END_DATE, FETCH_START_DATE, REQUIRED_FIELDS)


# Define BASE_DIR as the directory where the config file is located
BASE_DIR = os.path.dirname(__file__)

# Ensure log folder exists
os.makedirs(LOG_FOLDER, exist_ok=True)

def get_path(key):
    """Get the absolute path for a given config key."""
    paths = {
        'ehr_server_url': EHR_SERVER_URL,
        'state_file': STATE_FILE,
        'temp_folder': TEMP_FOLDER,
        'db_file': DB_FILE,
        'aql_folder': BASE_AQL_DIR,
        'log_folder': LOG_FOLDER,
        'encryption_key': KEY_PATH,
        'required_fields': REQUIRED_FIELDS,
        'fetch_start_date': FETCH_START_DATE,
        'fetch_by_date_enabled': FETCH_BY_DATE_ENABLED,
        'fetch_end_date': FETCH_END_DATE,
        'env_file': os.path.join(BASE_DIR, 'environments', '.env')
    }
    return paths.get(key, '')

def get_required_fields(resource_name):
    """
    Get the required fields for a given resource.
    
    :param resource_name: Name of the resource (e.g., 'Condition').
    :return: List of required fields or raise an error if the resource is not found.
    """
    required_fields = REQUIRED_FIELDS.get(resource_name)
    if not required_fields:
        raise ValueError(f"No required fields defined for resource '{resource_name}'. Check your settings.yml.")
    return required_fields

def get_state_file_path():
    """Get the path to the state file."""
    return get_path('state_file')

def get_temp_folder_path():
    """Get the path to the temporary folder."""
    return get_path('temp_folder')

def get_db_path():
    """Get the path to the SQLite database file."""
    return get_path('db_file')

def get_aql_folder_path():
    """Get the path to the AQL folder."""
    return get_path('aql_folder')

def get_encryption_key_path():
    """Get the path to the encryption key file."""
    return get_path('encryption_key')

def get_log_folder_path():
    """Get the path to the log folder."""
    return get_path('log_folder')

from datetime import datetime, timedelta

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
    return RESOURCES