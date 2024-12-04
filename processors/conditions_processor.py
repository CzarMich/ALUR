import sys
import os
import yaml
import base64
import re
from datetime import datetime
import concurrent.futures
import requests
import logging
import json

# Add the parent directory (project_root) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ..conf.utils import (
    store_temp_file, load_data_into_db, clean_up, set_last_run_time,
    get_last_run_time, execute_aql_query, get_log_folder_path
)
from conf.pseudonymization import get_or_create_pseudonyms
from conf.config import (
    BASE_AQL_DIR, PSEUDONYMIZATION_ENABLED, ANONYMIZER_SERVER_URL,
    FHIR_SERVER_URL, FHIR_SERVER_USER, FHIR_SERVER_PASSWORD, AUTH_METHOD
)
from fhir_mapping.condition_mapping import map_condition

# Ensure the log folder exists
log_folder_path = get_log_folder_path()
os.makedirs(log_folder_path, exist_ok=True)

# Configure logging
log_file_path = os.path.join(log_folder_path, 'conditions_mapping.log')
failed_log_file_path = os.path.join(log_folder_path, 'failed_resources.log')
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_auth_headers():
    """Generate authentication headers based on the AUTH_METHOD."""
    if AUTH_METHOD == 'basic':
        credentials = f"{FHIR_SERVER_USER}:{FHIR_SERVER_PASSWORD}"
        auth_header = f"Basic {base64.b64encode(credentials.encode('utf-8')).decode('utf-8')}"
        return {'Authorization': auth_header, 'Content-Type': 'application/fhir+json'}
    elif AUTH_METHOD == 'bearer':
        token = os.getenv("FHIR_BEARER_TOKEN")
        if not token:
            raise ValueError("FHIR_BEARER_TOKEN is not set for 'bearer' authentication.")
        return {'Authorization': f'Bearer {token}', 'Content-Type': 'application/fhir+json'}
    elif AUTH_METHOD == 'api_key':
        api_key = os.getenv("FHIR_API_KEY")
        if not api_key:
            raise ValueError("FHIR_API_KEY is not set for 'api_key' authentication.")
        return {'Authorization': f'ApiKey {api_key}', 'Content-Type': 'application/fhir+json'}
    else:
        raise ValueError("Unsupported authentication method specified in AUTH_METHOD.")

def log_failed_resource(resource_type, resource_id, error_message):
    """Log failed resource and error message to a file."""
    with open(failed_log_file_path, 'a') as file:
        file.write(f"Resource Type: {resource_type}\n")
        file.write(f"Resource ID: {resource_id}\n")
        file.write(f"Error Message: {error_message}\n")
        file.write("-" * 80 + "\n")

def send_fhir_resource(resource_type, resource_id, resource_data):
    """Send the FHIR resource to the FHIR server using a PUT request."""
    url = f"{FHIR_SERVER_URL}/{resource_type}/{resource_id}"
    headers = get_auth_headers()
    
    #Print and log the request details, including the FHIR resource data
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    
    if resource_data:
        print(f"FHIR Resource Data: {json.dumps(resource_data, indent=2)}")
        logging.info(f"FHIR Resource Data: {json.dumps(resource_data, indent=2)}")
    else:
        print("Warning: The FHIR resource data is empty.")
        logging.warning("The FHIR resource data is empty.")

    response = requests.put(url, json=resource_data, headers=headers)
    
    if response.status_code in [200, 201]:
        logging.info("Resource updated successfully: %s/%s", resource_type, resource_id)
        logging.info("Response: %s", response.json())
        print(f"Success: Resource {resource_type}/{resource_id} updated.")
        print(f"Response: {response.json()}")
    else:
        error_message = response.text
        logging.error("Failed to update resource: %s/%s", resource_type, resource_id)
        logging.error("Response: %s", error_message)
        print(f"Error: Failed to update resource {resource_type}/{resource_id}.")
        print(f"Response: {error_message}")

        # Log the failed resource details
        log_failed_resource(resource_type, resource_id, resource_data, error_message)


def get_aql_query(resource_type, last_run_time):
    """Get AQL query from XML file with the last run time placeholder replaced."""
    config_file_path = os.path.join(os.path.dirname(__file__), '..', 'conf', 'config_files.yml')
    
    with open(config_file_path, 'r') as file:
        config = yaml.safe_load(file)

    for resource in config['resources']:
        if resource['name'] == resource_type:
            xml_file_path = os.path.join(BASE_AQL_DIR, resource['file'])
            break
    else:
        raise ValueError(f"No configuration found for resource type: {resource_type}")

    with open(xml_file_path, 'r') as file:
        xml_content = file.read()

    if last_run_time:
        aql_query = xml_content.replace('{{last_run_time}}', last_run_time)
    else:
        logging.info("No last run time provided, defaulting to no time condition.")
        aql_query = xml_content.replace("AND v/commit_audit/time_committed/value >= '{{last_run_time}}'", "")

    aql_query = re.sub(r'<\/?query>', '', aql_query, flags=re.IGNORECASE).strip()

    return aql_query

def sanitize_id(id_value):
    """Sanitize ID to ensure it meets FHIR requirements."""
    if id_value:
        # Replace slashes with hyphens
        sanitized_id = id_value.replace('/', '-')
        # Remove any other invalid characters
        sanitized_id = re.sub(r'[^\w\-.]', '', sanitized_id)
        # Truncate to 64 characters if necessary
        if len(sanitized_id) > 64:
            sanitized_id = sanitized_id[:64]
        return sanitized_id
    return id_value

def format_datetime(dt_value):
    """Format datetime to FHIR-compliant dateTime format."""
    if dt_value:
        try:
            dt_obj = datetime.fromisoformat(dt_value)
            # Use ISO 8601 format with 'Z' for UTC timezone
            return dt_obj.isoformat() + 'Z'
        except ValueError:
            print(f"Invalid datetime format: {dt_value}")
    return dt_value

def process_resource(resource_type):
    last_run_time = get_last_run_time(resource_type)
    
    if last_run_time is None:
        last_run_time = '2023-08-02'  # Default to a past date for initial run
    
    aql_query = get_aql_query(resource_type, last_run_time)
    
    query_results = execute_aql_query(aql_query)
    
    if not query_results:
        logging.info("No results returned for resource type: %s", resource_type)
        return

    result_set = query_results.get('resultSet', [])
    temp_file_path = store_temp_file(result_set)
    
    table_name = resource_type.lower()
    conn, cursor = load_data_into_db(temp_file_path, table_name)
    
    cursor.execute(f'SELECT * FROM {table_name}')
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    for row in rows:
        ehr_record = dict(zip(column_names, row))
        
        try:
            fhir_profile = map_condition(ehr_record)
        except KeyError as e:
            logging.error("Mapping error for record %s: %s", ehr_record, e)
            continue
##The format here Matters. Either you are using reference or logical referencing for the patient ID and encounter.
        #if PSEUDONYMIZATION_ENABLED:
        # Pseudonymize the subject ID
          #  if 'subject' in fhir_profile and 'identifier' in fhir_profile['subject']:
           #     subject_id = fhir_profile['subject']['identifier']['value']
            #    subject_pseudonym = get_pseudonym(subject_id, ANONYMIZER_SERVER_URL)
             #   fhir_profile['subject']['identifier']['value'] = subject_pseudonym
            #Pseudonymize the subject (patient) ID using  reference
            #subject_id = fhir_profile['subject']['reference'].split('/')[-1]
            #pseudonym = get_pseudonym(subject_id, ANONYMIZER_SERVER_URL)
            #fhir_profile['subject']['reference'] = f"Patient/{pseudonym}"
   
   #         # Pseudonymize the encounter ID
    #        if 'encounter' in fhir_profile and 'identifier' in fhir_profile['encounter']:
     #           encounter_id = fhir_profile['encounter']['identifier']['value']
      #          encounter_pseudonym = get_pseudonym(encounter_id, ANONYMIZER_SERVER_URL)
       #         fhir_profile['encounter']['identifier']['value'] = encounter_pseudonym
            #encounter_id = fhir_profile['encounter']['reference'].split('/')[-1]
            #encounter_pseudonym = get_pseudonym(encounter_id, ANONYMIZER_SERVER_URL)
            #fhir_profile['encounter']['reference'] = f"Encounter/{encounter_pseudonym}"
            
        if PSEUDONYMIZATION_ENABLED:
            # Get subject and encounter IDs
            subject_id = fhir_profile.get('subject', {}).get('identifier', {}).get('value', None)
            encounter_id = fhir_profile.get('encounter', {}).get('identifier', {}).get('value', None)
            
            if subject_id or encounter_id:
                # Get pseudonyms for both IDs
                patient_pseudonym, encounter_pseudonym = get_or_create_pseudonyms(
                    patient_id=subject_id if subject_id else '',
                    encounter_id=encounter_id if encounter_id else '',
                    domain_name=ANONYMIZER_SERVER_URL.split('/')[-1]  # Extract domain name from URL
                )

                # Update FHIR profile with pseudonyms
                if subject_id:
                    fhir_profile['subject']['identifier']['value'] = patient_pseudonym
                if encounter_id:
                    fhir_profile['encounter']['identifier']['value'] = encounter_pseudonym
        if 'recordedDate' in fhir_profile:
            fhir_profile['recordedDate'] = format_datetime(fhir_profile['recordedDate'])
        if 'onsetDateTime' in fhir_profile:
            fhir_profile['onsetDateTime'] = format_datetime(fhir_profile['onsetDateTime'])
        if 'id' in fhir_profile:
            fhir_profile['id'] = sanitize_id(fhir_profile['id'])
        
        send_fhir_resource(resource_type, fhir_profile['id'], fhir_profile)
   
    clean_up(temp_file_path, conn)
    
    formatted_time = datetime.now().strftime('%Y-%m-%dT%H')
    set_last_run_time(resource_type, formatted_time)

def main():
    config_file_path = os.path.join(os.path.dirname(__file__), '..', 'conf', 'config_files.yml')
    
    with open(config_file_path, 'r') as file:
        config = yaml.safe_load(file)

    resources = [res['name'] for res in config['resources'] if 'file' in res]
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_resource, resource) for resource in resources]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error("An error occurred: %s", e)

if __name__ == "__main__":
    main()
