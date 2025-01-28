# tests/test_config.py

import os
from conf.config import RESOURCE_FILES, STATE_FILE, TEMP_FOLDER, DB_FILE, LOG_FOLDER
import yaml

def test_config_paths():
    """
    Test to ensure that all paths in config.py are correctly resolved.
    If a directory does not exist, create it.
    """
    print("Testing configuration paths...")
    
    paths_to_check = [
        ("STATE_FILE", os.path.dirname(STATE_FILE)),
        ("TEMP_FOLDER", TEMP_FOLDER),
        ("DB_FILE", os.path.dirname(DB_FILE)),
        ("LOG_FOLDER", LOG_FOLDER),
    ]

    for path_name, path in paths_to_check:
        if not os.path.exists(path):
            print(f"{path_name}: {path} does not exist. Creating...")
            os.makedirs(path, exist_ok=True)
        assert os.path.exists(path), f"{path_name}: {path} still does not exist!"
        print(f"{path_name}: {path} ✅")



def test_load_condition_mapping():
    """
    Test if condition.yml can be loaded successfully and validate its content.
    """
    print("Testing condition.yml loading...")
    try:
        # Retrieve the path to the condition.yml from RESOURCE_FILES
        condition_mapping_path = RESOURCE_FILES['Condition']['mapping']
        
        # Ensure the file exists
        assert os.path.exists(condition_mapping_path), f"Mapping file does not exist: {condition_mapping_path}"

        # Load the YAML file
        with open(condition_mapping_path, 'r') as f:
            data = yaml.safe_load(f)

        # Access the nested 'Condition' key if present
        condition_data = data.get('Condition', {})
        assert 'query_template' in condition_data, "Missing 'query_template' key in condition.yml"
        assert 'parameters' in condition_data, "Missing 'parameters' key in condition.yml"
        assert 'mappings' in condition_data, "Missing 'mappings' key in condition.yml"

        # Validate query template
        assert isinstance(condition_data['query_template'], str), "'query_template' should be a string"
        print(f"'query_template' loaded successfully: {condition_data['query_template'][:50]}...")

        # Validate mappings
        assert isinstance(condition_data['mappings'], dict), "'mappings' should be a dictionary"
        print(f"'mappings' loaded successfully: {list(condition_data['mappings'].keys())}")

        print("Condition.yml loaded and validated successfully! ✅")

    except Exception as e:
        print(f"Failed to load condition.yml: {e} ❌")


if __name__ == "__main__":
    print("Testing Configurations...")
    test_config_paths()
    test_load_condition_mapping()
