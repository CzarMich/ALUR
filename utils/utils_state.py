import os
import json
from datetime import datetime
from utils.utils import get_path
from conf.config import yaml_config

def get_last_run_time(resource_type):
    """
    Get the last run time for a resource, prioritizing fetch_by_date if enabled.
    """
    # Check fetch_by_date settings
    fetch_by_date = yaml_config.get('fetch_by_date', {})
    if fetch_by_date.get('enabled', False):
        start_date = fetch_by_date.get('start_date')
        if start_date:
            print(f"Using fetch_by_date start_date: {start_date}")
            return start_date  # Use start_date if fetch_by_date is enabled

    # Fallback to state.json
    state_file_path = get_path('state_file')
    if not os.path.exists(state_file_path):
        return None

    with open(state_file_path, 'r') as file:
        state = json.load(file)

    return state.get(resource_type)

def set_last_run_time(resource_type, run_time):
    """
    Set the last run time for a specific resource type.
    """
    state_file_path = get_path('state_file')  # Use get_path to get the state file path
    if not os.path.exists(state_file_path):
        state = {}
    else:
        with open(state_file_path, 'r') as file:
            state = json.load(file)

    # Format time as 'YYYY-MM-DD HH:MM:SS'
    formatted_time = datetime.fromisoformat(run_time).strftime('%Y-%m-%d %H:%M:%S')
    state[resource_type] = formatted_time

    # Ensure the directory exists
    os.makedirs(os.path.dirname(state_file_path), exist_ok=True)

    # Write the updated state back to the file
    with open(state_file_path, 'w') as file:
        json.dump(state, file)

def clear_last_run_time(resource_type):
    """
    Clear the last run time for a specific resource type.
    """
    state_file_path = get_path('state_file')
    if not os.path.exists(state_file_path):
        print(f"State file not found: {state_file_path}")
        return

    with open(state_file_path, 'r') as file:
        state = json.load(file)

    if resource_type in state:
        del state[resource_type]

    with open(state_file_path, 'w') as file:
        json.dump(state, file)

    print(f"Cleared last run time for resource: {resource_type}")
