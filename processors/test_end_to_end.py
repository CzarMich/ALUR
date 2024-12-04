import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import unittest
import yaml  # Import yaml for parsing YAML files
import json
from datetime import datetime
from conf.open_ehr_query import query_resource
from conf.utils import store_temp_file, load_data_into_db, clean_up, set_last_run_time, create_fhir_transaction_bundle
from conf.pseudonymization import get_pseudonym
from conf.config import PSEUDONYMIZATION_ENABLED, ANONYMIZER_SERVER_URL
from fhir_mapping.test import map_test

# Mock functions for testing purposes
def mock_query_resource(aql_query):
    # Simulate AQL query results based on the query content
    return [
        {
            'EHRID': '12345',
            'verificationStatusCode': 'confirmed',
            'SubjectID': 'patient123',
            'onsetStart': '2024-03-01',
            'onsetEnd': '2024-03-05',
            'recordedDate': '2024-03-06'
        }
    ]

class TestEndToEndProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Ensure state file exists and has a valid last_run_time
        cls.state_file = 'conf/state.json'
        if not os.path.exists(cls.state_file):
            with open(cls.state_file, 'w') as f:
                json.dump({'last_run_time': '1970-01-01T00:00:00'}, f)
        cls.resource_type = 'test'
    
    def setUp(self):
        # Ensure configuration files are in place
        self.config_file = 'conf/config_files.yml'
        self.aql_file = 'openehr_aql/test.xml'
        self.state_file = 'conf/state.json'
        self.temp_file_path = 'temp/temp_test_data.csv'
    
    def test_end_to_end(self):
        # Step 1: Read and replace placeholder in AQL query
        last_run_time = self.read_last_run_time(self.state_file)
        aql_query = self.get_aql_query(self.resource_type, last_run_time)
        
        # Mock the query_resource function
        query_results = mock_query_resource(aql_query)
        
        # Step 2: Store query results into a temporary file
        temp_file_path = store_temp_file(query_results)
        
        # Step 3: Load data into an in-memory database
        table_name = self.resource_type.lower()
        conn, cursor = load_data_into_db(temp_file_path, table_name)
        
        # Step 4: Process rows and create FHIR transaction bundles
        cursor.execute(f'SELECT * FROM {table_name}')
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        for row in rows:
            ehr_record = dict(zip(column_names, row))
            fhir_profile = map_test(ehr_record)
            
            # Expected FHIR profile
            expected_fhir_profile = {
                "resourceType": "Test",
                "id": '12345',
                "verificationStatus": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                            "code": 'confirmed'
                        }
                    ]
                },
                "subject": {
                    "reference": "Patient/patient123"
                },
                "onsetPeriod": {
                    "start": '2024-03-01',
                    "end": '2024-03-05'
                },
                "recordedDate": '2024-03-06'
            }
            
            # Check that the FHIR profile matches the expected result
            self.assertEqual(fhir_profile, expected_fhir_profile)
        
        # Step 5: Clean up
        clean_up(temp_file_path, conn)
        
        # Step 6: Update last run time
        set_last_run_time(self.state_file, datetime.now().isoformat())
    
    @staticmethod
    def read_last_run_time(state_file):
        if os.path.exists(state_file):
            with open(state_file, 'r') as file:
                state = json.load(file)
                return state.get('last_run_time', '1970-01-01T00:00:00')
        return '1970-01-01T00:00:00'

    @staticmethod
    def get_aql_query(resource_type, last_run_time):
        # Load the XML file path from the config
        with open('conf/config_files.yml', 'r') as file:
            config = yaml.safe_load(file)
        
        for resource in config['resources']:
            if resource['name'] == resource_type:
                xml_file_path = os.path.join('openehr_aql', resource['file'])
                break
        else:
            raise ValueError(f"No configuration found for resource type: {resource_type}")
        
        # Read the AQL from XML file
        with open(xml_file_path, 'r') as file:
            xml_content = file.read()
        
        # Replace the placeholder with the last run time
        aql_query = xml_content.replace('{{last_run_time}}', last_run_time)
        
        return aql_query

if __name__ == "__main__":
    unittest.main()
