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
from config import (# Import the settings from the config file
    STATE_FILE, TEMP_FOLDER, DB_FILE, LOG_FOLDER,BASE_AQL_DIR, PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE,BASE_AQL_DIR,
    GPAS_BASE_URL, GPAS_ROOT_DOMAIN, GPAS_CLIENT_CERT, GPAS_CLIENT_KEY, GPAS_CA_CERT, EHR_SERVER_URL, EHR_SERVER_USER, EHR_SERVER_PASSWORD, FHIR_SERVER_URL, FHIR_SERVER_USER, FHIR_SERVER_PASSWORD, AUTH_METHOD, RESOURCES, RESOURCE_FILES, KEY_PATH)


# Define BASE_DIR as the directory where the config file is located
BASE_DIR = os.path.dirname(__file__)

# Ensure log folder exists
os.makedirs(LOG_FOLDER, exist_ok=True)

def get_path(key):
    """Get the absolute path for a given config key."""
    paths = {
        'state_file': STATE_FILE,
        'temp_folder': TEMP_FOLDER,
        'db_file': DB_FILE,
        'aql_folder': BASE_AQL_DIR,
        'log_folder': LOG_FOLDER,
        'encryption_key': KEY_PATH,
        'env_file': os.path.join(BASE_DIR, 'environments', '.env')
    }
    return paths.get(key, '')
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

def load_env_variables():
    """Load environment variables from a .env file."""
    env_file_path = get_path('env_file')
    if not os.path.exists(env_file_path):
        raise FileNotFoundError(f"{env_file_path} not found.")
    
    with open(env_file_path) as file:
        for line in file:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

def get_gpas_certificate_paths():
    """Retrieve certificate paths for GPAS."""
    return (
        os.path.join(BASE_DIR, GPAS_CLIENT_CERT),
        os.path.join(BASE_DIR, GPAS_CLIENT_KEY),
        os.path.join(BASE_DIR, GPAS_CA_CERT)
    )

def create_session():
    """Create and return a cached session."""
    return CachedSession(
        'gpas_cache',
        use_cache_dir=True,
        cache_control=True,
        expire_after=timedelta(days=1),
        allowable_codes=[200, 400],
        allowable_methods=['GET', 'POST'],
        ignored_parameters=['api_key'],
        match_headers=['Accept-Language'],
        stale_if_error=True,
    )

def get_last_run_time(resource_type):
    """Retrieve the last run time for a specific resource type."""
    state_file_path = get_state_file_path()
    if not os.path.exists(state_file_path):
        return None
    with open(state_file_path, 'r') as file:
        state = json.load(file)
    return state.get(resource_type)

def set_last_run_time(resource_type, run_time):
    """Set the last run time for a specific resource type."""
    state_file_path = get_state_file_path()
    if not os.path.exists(state_file_path):
        state = {}
    else:
        with open(state_file_path, 'r') as file:
            state = json.load(file)
    
    formatted_time = datetime.fromisoformat(run_time).strftime('%Y-%m-%dT%H')
    state[resource_type] = formatted_time
    
    os.makedirs(os.path.dirname(state_file_path), exist_ok=True)

    with open(state_file_path, 'w') as file:
        json.dump(state, file)

def store_temp_file(data):
    """Store data in a temporary file and return its path."""
    temp_folder_path = get_temp_folder_path()
    os.makedirs(temp_folder_path, exist_ok=True)
    temp_file_path = os.path.join(temp_folder_path, f'{next(tempfile._get_candidate_names())}.json')
    with open(temp_file_path, 'w', encoding='utf-8') as temp_file:
        json.dump(data, temp_file, ensure_ascii=False)
    return temp_file_path

def load_data_into_db(temp_file_path, table_name):
    """Load JSON data from a file into a specified table in an SQLite database."""
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(temp_file_path, 'r', encoding='utf-8') as temp_file:
        data = json.load(temp_file)

        if not data:
            print("No data found in the temporary file.")
            return conn, cursor

        if isinstance(data, list):
            for record in data:
                create_or_update_table(cursor, table_name, record)
                insert_record(cursor, table_name, record)
        else:
            create_or_update_table(cursor, table_name, data)
            insert_record(cursor, table_name, data)
        
    conn.commit()
    return conn, cursor

def create_or_update_table(cursor, table_name, record):
    """Create or update a table schema based on the fields in the record, including pseudonymized fields."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY'
    
    for field in record.keys():
        if field not in existing_columns:
            create_table_sql += f', {field} TEXT'
    
    # Add pseudonymized fields if pseudonymization is enabled
    if PSEUDONYMIZATION_ENABLED:
        if ELEMENTS_TO_PSEUDONYMIZE.get('patient_id', False):
            create_table_sql += ', patient_id_pseudonym TEXT'
        if ELEMENTS_TO_PSEUDONYMIZE.get('encounter_id', False):
            create_table_sql += ', encounter_id_pseudonym TEXT'
        if ELEMENTS_TO_PSEUDONYMIZE.get('patient_dob', False):
            create_table_sql += ', patient_dob_pseudonym TEXT'
    
    create_table_sql += ')'
    cursor.execute(create_table_sql)

def insert_record(cursor, table_name, record):
    """Insert a record into a table, handling missing fields with default values."""
    columns = ', '.join(record.keys())
    placeholders = ', '.join(['?'] * len(record))
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    cursor.execute(insert_sql, list(record.values()))

def execute_aql_query(aql_query):
    """Execute an AQL query and return the result using a cached session."""
    session = create_session()
    url = f"{EHR_SERVER_URL}/rest/v1/query/aql"
    try:
        response = session.post(url, data=aql_query, auth=(os.getenv('EHR_SERVER_USER'), os.getenv('EHR_SERVER_PASSWORD')))
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"AQL query failed with error: {e}")
        return None

def get_aql_query(query_name):
    """Load the AQL query from an XML file."""
    aql_folder = get_aql_folder_path()
    query_file_path = os.path.join(aql_folder, f"{query_name}.xml")
    
    if not os.path.exists(query_file_path):
        raise FileNotFoundError(f"AQL query file {query_file_path} not found.")
    
    tree = ET.parse(query_file_path)
    return tree.getroot().text.strip()

def extract_aql_variables(aql_query):
    """Extract the variables from an AQL query string."""
    return re.findall(r"\$([a-zA-Z_]\w*)", aql_query)

def replace_aql_variables(aql_query, variables):
    """Replace variables in an AQL query with their values."""
    for var, value in variables.items():
        aql_query = aql_query.replace(f"${var}", quote(value))
    return aql_query