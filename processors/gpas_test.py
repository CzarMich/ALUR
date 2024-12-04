import sys
import os
import logging

# Set logging level for Zeep to warning (this suppresses debug messages)
logging.getLogger('zeep').setLevel(logging.WARNING)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from gpas_test_2 import Client, AdminClient
from conf.config import GPAS_BASE_URL, GPAS_ROOT_DOMAIN

if __name__ == '__main__':
    # Configurations from config.py
    base_url = GPAS_BASE_URL
    root_domain = GPAS_ROOT_DOMAIN
    sub_domain = 'Dental_AI'

    # Ensure that GPAS_BASE_URL and GPAS_ROOT_DOMAIN are properly set
    if not base_url or not root_domain:
        raise ValueError("GPAS_BASE_URL or GPAS_ROOT_DOMAIN is not set in the configuration")

    # Create an AdminClient
    gPas_admin_client = AdminClient(base_url=base_url, port=8080)
    
    # Create root domain
    print("Creating root domain...")
    gPas_admin_client.create_domain(
        name=root_domain,
        alphabet=AdminClient.Alphabets.Symbol31,
        generator=AdminClient.Generators.NoCheckDigits
    )
    
    # Create subdomain below the root domain
    print("Creating subdomain...")
    gPas_admin_client.create_domain(
        name=sub_domain,
        alphabet=AdminClient.Alphabets.Symbol32,
        generator=AdminClient.Generators.HammingCode,
        parent_domain=root_domain
    )

    # Create GPAS client for pseudonymization
    print('Creating GPAS client...')
    gPas_client = Client(base_url=base_url, port=8080, domain=sub_domain)

    # Pseudonymize a patient ID
    patient_id = '1234567890_HannesUlrich'
    print(f"Creating pseudonym for patient ID: {patient_id}")
    pseudo = gPas_client.get_pseudonym(patient_id)

    if pseudo:
        print("Pseudo: " + pseudo)

        # Resolve the pseudonym back to the original ID
        resolved_name = gPas_client.get_name(pseudo)
        print(f"Resolved ID: {resolved_name}")
    else:
        print("Failed to create pseudonym")

    # Test error handling for invalid pseudonyms
    print("Testing invalid pseudonyms:")
    print(gPas_client.get_name("GJNW1G5080C8"))  # Unknown pseudonym
    print(gPas_client.get_name("GJNW"))          # Too short pseudonym
