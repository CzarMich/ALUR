import os
import sys
# Add the parent directory (project root) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
import urllib3
import ssl
from urllib3.util.ssl_ import create_urllib3_context
from conf.config import GPAS_BASE_URL, GPAS_ROOT_DOMAIN, GPAS_CLIENT_CERT, GPAS_CLIENT_KEY, GPAS_CA_CERT


# Use absolute paths
GPAS_CA_CERT = os.path.abspath('conf/certs/ca.crt')
GPAS_CLIENT_CERT = os.path.abspath('conf/certs/client.crt')
GPAS_CLIENT_KEY = os.path.abspath('conf/certs/client.key')

# Debugging: Print paths
print("GPAS_BASE_URL:", GPAS_BASE_URL)
print("CA Cert Path:", GPAS_CA_CERT)
print("Client Cert Path:", GPAS_CLIENT_CERT)
print("Client Key Path:", GPAS_CLIENT_KEY)

# Create a custom SSL context
# Create a custom SSL context
def create_ssl_context():
    context = create_urllib3_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2  # Use TLS 1.2
    return context

# Create an adapter with custom SSL context
class TLSAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.ssl_context = create_ssl_context()
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)

# Create a session with the adapter
session = requests.Session()
adapter = TLSAdapter()
session.mount('https://', adapter)

# Make a GET request to the GPAS base URL with client certificate and CA certificate for verification
try:
    response = session.get(GPAS_BASE_URL, cert=(GPAS_CLIENT_CERT, GPAS_CLIENT_KEY), verify=GPAS_CA_CERT)
    print(response.status_code)
    print(response.text)
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")