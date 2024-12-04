import os
import requests
from lxml import etree
from config import GPAS_BASE_URL, CERT_PATH, KEY_PATH, CA_CERT_PATH

# Load environment variables
GPAS_BASE_URL = os.getenv('GPAS_BASE_URL')  # The base URL for GPAS
CERT_PATH = os.path.join(os.getcwd(), 'conf/environments/certs/client.crt')
KEY_PATH = os.path.join(os.getcwd(), 'conf/environments/certs/client.key')
CA_CERT_PATH = os.path.join(os.getcwd(), 'conf/environments/certs/ca.crt')

def create_soap_request(patient_id, encounter_id, domain_name):
    """Create the SOAP XML request for both patient and encounter IDs."""
    envelope = etree.Element('{http://schemas.xmlsoap.org/soap/envelope/}Envelope')
    body = etree.SubElement(envelope, '{http://schemas.xmlsoap.org/soap/envelope/}Body')

    # Create the first pseudonym request for the patient ID
    get_or_create_psuedonym_for_patient = etree.SubElement(body, '{http://psn.ttp.ganimed.icmvc.emau.org/}getOrCreatePseudonymFor')
    value_patient = etree.SubElement(get_or_create_psuedonym_for_patient, 'value')
    value_patient.text = patient_id
    domain_patient = etree.SubElement(get_or_create_psuedonym_for_patient, 'domainName')
    domain_patient.text = domain_name

    # Create the second pseudonym request for the encounter ID
    get_or_create_psuedonym_for_encounter = etree.SubElement(body, '{http://psn.ttp.ganimed.icmvc.emau.org/}getOrCreatePseudonymFor')
    value_encounter = etree.SubElement(get_or_create_psuedonym_for_encounter, 'value')
    value_encounter.text = encounter_id
    domain_encounter = etree.SubElement(get_or_create_psuedonym_for_encounter, 'domainName')
    domain_encounter.text = domain_name

    return etree.tostring(envelope, pretty_print=True, xml_declaration=True, encoding='UTF-8')

def get_or_create_pseudonyms(patient_id, encounter_id, domain_name):
    """Send a SOAP request to get or create pseudonyms for the given patient ID and encounter ID."""
    # Create SOAP request XML
    soap_request = create_soap_request(patient_id, encounter_id, domain_name)
    
    # Log the request being sent
    print("Sending SOAP request with the following XML:")
    print(soap_request.decode())

    # Send the request to the GPAS pseudonymization service
    try:
        response = requests.post(
            f"{GPAS_BASE_URL}/ttp-fhir/fhir/gpas/metadata",
            data=soap_request,
            cert=(CERT_PATH, KEY_PATH),
            verify=CA_CERT_PATH,
            headers={'Content-Type': 'text/xml'}
        )
        
        # Check if the request was successful
        if response.status_code == 200:
            print("SOAP request successful. Parsing response.")
            # Parse the response XML to get the pseudonyms
            tree = etree.fromstring(response.content)
            pseudonym_elements = tree.findall('.//{http://psn.ttp.ganimed.icmvc.emau.org/}return')
            
            if len(pseudonym_elements) == 2:
                patient_pseudonym = pseudonym_elements[0].text
                encounter_pseudonym = pseudonym_elements[1].text
                return patient_pseudonym, encounter_pseudonym
            else:
                raise RuntimeError("Failed to parse pseudonyms from the SOAP response.")
        else:
            print(f"SOAP request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            response.raise_for_status()
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to communicate with GPAS service: {e}")

def main():
    # Example patient ID and encounter ID
    patient_id = '123456'
    encounter_id = '223025411'
    domain_name = 'MeDIC'

    patient_pseudonym, encounter_pseudonym = get_or_create_pseudonyms(patient_id, encounter_id, domain_name)
    print(f"Received patient pseudonym: {patient_pseudonym}")
    print(f"Received encounter pseudonym: {encounter_pseudonym}")

if __name__ == "__main__":
    main()
