import xml.etree.ElementTree as ET
import requests
import urllib.parse
from conf.config import EHR_SERVER_URL, EHR_SERVER_USER, EHR_SERVER_PASSWORD
import base64

def read_aql_query(file_path):
    """Read and return the AQL query from the XML file."""
    tree = ET.parse(file_path)
    root = tree.getroot()
    return root.text.strip()

def query_resource(resource_type, last_run_time=None):
    """Query a resource from the OpenEHR server using AQL."""
    file_path = f'openehr_aql/{resource_type}.xml'
    aql_query = read_aql_query(file_path)
    
    # Replace placeholder with actual last run time or a default date
    if last_run_time:
        aql_query = aql_query.replace('{{last_run_time}}', last_run_time)
    else:
        aql_query = aql_query.replace('{{last_run_time}}', '2024-03-06')
    
    # Print the constructed AQL query
    print(f"Constructed AQL Query: {aql_query}")
    
    # Encode the AQL query for inclusion in the URL
    encoded_query = urllib.parse.quote(aql_query)
    url = f"{EHR_SERVER_URL}/query?aql={encoded_query}"
    
    # Print the request URL for debugging
    print(f"Request URL: {url}")
    
    # Manually encode the credentials
    credentials = f"{EHR_SERVER_USER}:{EHR_SERVER_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    # Set the Authorization header
    headers = {
        'Authorization': f'Basic {encoded_credentials}'
    }
    
    # Print the encoded credentials for debugging
    print(f"Encoded Credentials: {encoded_credentials}")
    
    # Send the HTTP GET request with headers
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP error responses
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        print(f"Other error occurred: {err}")
        raise
    
    # Print the response status code for debugging
    print(f"Response Status Code: {response.status_code}")
    
    # Handle the response based on status code
    if response.status_code == 200:
        result_set = response.json().get('resultSet', [])
        if not result_set:
            print("No records found for the query.")
        return result_set
    elif response.status_code == 204:
        print("No content found for the query.")
        return []
    else:
        print(f"Response Content: {response.text}")
        raise Exception(f"Failed to execute AQL query: {response.status_code}, {response.text}")

