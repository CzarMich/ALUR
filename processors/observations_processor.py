from conf.open_ehr_query import query_resource
from fhir_mapping import map_observation_to_fhir
from conf.utils import store_temp_file, load_data_into_db, send_to_fhir_server, clean_up

def process_observations(fhir_server_url):
    resource_type = 'Observations'
    
    # Query resource data
    query_results = query_resource(resource_type)
    
    # Store data in a temp file
    temp_file_path = store_temp_file(query_results)
    
    # Load data into an in-memory database
    table_name = resource_type.lower()
    conn, cursor = load_data_into_db(temp_file_path, table_name)
    
    # Map to FHIR profiles
    fhir_profiles = map_observation_to_fhir(cursor, table_name)
    
    # Send to FHIR server
    send_to_fhir_server(fhir_profiles, fhir_server_url)
    
    # Clean up
    clean_up(temp_file_path, conn)
