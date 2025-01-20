import os
import json
import xml.etree.ElementTree as ET
import re
from urllib.parse import quote
from datetime import datetime, timedelta
import requests
from utils_session import ehr_session
from utils_state import get_last_run_time, set_last_run_time
from utils import get_path, generate_date_range, is_fetch_by_date_enabled, fetch_by_end_date, fetch_by_start_date
from utils_db import process_records
from config import ELEMENTS_TO_PSEUDONYMIZE

# -----------------------------------------------------------
# AQL Query Management
# -----------------------------------------------------------

def get_aql_query(query_name, resource_type):
    """
    Load the AQL query from an XML file and replace placeholders.

    :param query_name: The name of the query file (without extension).
    :param resource_type: The resource type (e.g., 'Condition') for state tracking.
    :return: The AQL query string with placeholders replaced.
    """
    aql_folder = get_path('aql_folder')
    query_file_path = os.path.join(aql_folder, f"{query_name}.xml")

    if not os.path.exists(query_file_path):
        raise FileNotFoundError(f"AQL query file {query_file_path} not found.")

    try:
        tree = ET.parse(query_file_path)
        root = tree.getroot()
        raw_query = root.text.strip()

        last_run_time = get_last_run_time(resource_type)
        if not last_run_time:
            last_run_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

        return raw_query.replace("{{last_run_time}}", last_run_time)
    except ET.ParseError as e:
        raise ValueError(f"Error parsing XML file {query_file_path}: {e}")


def replace_date_placeholders(aql_query, current_date):
    """
    Replace dynamic date placeholders in the AQL query.

    :param aql_query: The AQL query string.
    :param current_date: The current date to replace placeholders.
    :return: AQL query with date placeholders replaced.
    """
    next_date = (datetime.strptime(current_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    aql_query = aql_query.replace("{{current_date}}", current_date)
    aql_query = aql_query.replace("{{next_date}}", next_date)
    return aql_query


def fetch_records_by_date(query_name, resource_type, key, required_fields):
    """
    Fetch records for a resource by iterating through dates.
    """
    if not is_fetch_by_date_enabled():
        print("fetch_by_date is disabled. Using last run time from state.json.")
        current_date = get_last_run_time(resource_type)
        end_date = None  # Open-ended
    else:
        current_date = fetch_by_start_date()
        end_date = fetch_by_end_date()

    print(f"Fetching records for {resource_type} from {current_date} to {end_date or 'today'}")

    for day_str in generate_date_range(current_date, end_date):
        print(f"Fetching records for date: {day_str}")
        aql_query = get_aql_query(query_name, resource_type)
        aql_query = replace_date_placeholders(aql_query, day_str)

        response = execute_aql_query(aql_query, resource_type)
        if response and response.get('resultSet'):
            print(f"Fetched {len(response['resultSet'])} records for {day_str}.")
            process_records(response['resultSet'], resource_type, key, required_fields)
        else:
            print(f"No records fetched for {day_str}. Moving to next date.")



def execute_aql_query(aql_query, resource_type):
    ehr_server_url = get_path('ehr_server_url').rstrip('/')
    encoded_query = quote(aql_query)
    url = f"{ehr_server_url}/query?aql={encoded_query}"

    print(f"Executing AQL query for {resource_type}. URL: {url}")

    try:
        response = ehr_session.get(url)
        print(f"HTTP Response Status Code: {response.status_code}")
        response.raise_for_status()

        response_body = response.json()
        result_set = response_body.get('resultSet', [])
        print(f"ResultSet contains {len(result_set)} records.")

        current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        set_last_run_time(resource_type, current_time)
        print(f"Updated last run time for {resource_type}: {current_time}")

        return response_body
    except requests.RequestException as e:
        print(f"AQL query failed with error: {e}")
        return None



def extract_aql_variables(aql_query):
    """
    Extract variables from an AQL query string.

    :param aql_query: The AQL query string.
    :return: A list of variable names (e.g., ['patient_id']).
    """
    return re.findall(r"\$([a-zA-Z_]\w*)", aql_query)


def replace_aql_variables(aql_query, variables):
    """
    Replace variables in an AQL query with their values.

    :param aql_query: The AQL query string.
    :param variables: A dictionary of variables and their values.
    :return: The AQL query with variables replaced.
    """
    for var, value in variables.items():
        aql_query = aql_query.replace(f"${var}", quote(value))
    return aql_query


def set_last_run_time(resource_type, run_time):
    state_file_path = get_path('state_file')

    print(f"Setting last run time for {resource_type} to {run_time}")

    if not os.path.exists(state_file_path):
        state = {}
    else:
        with open(state_file_path, 'r') as file:
            state = json.load(file)

    state[resource_type] = run_time

    with open(state_file_path, 'w') as file:
        json.dump(state, file)
    print(f"Last run time for {resource_type} saved in {state_file_path}")
