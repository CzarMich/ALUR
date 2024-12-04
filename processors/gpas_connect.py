import zeep
import logging
import sys
from enum import Enum
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep.transports import Transport
from conf.config import GPAS_BASE_URL, CERT_PATH, KEY_PATH, CA_CERT_PATH

class Client:
    def __init__(self, port, domain):
        self.base_url = GPAS_BASE_URL
        self.port = port
        self.domain = domain
        wsdl = self.base_url + ':' + str(self.port) + '/gpas/gpasService?wsdl'
        
        # Setup a session with client certificates
        session = Session()
        session.cert = (CERT_PATH, KEY_PATH)
        session.verify = CA_CERT_PATH  # Path to the CA cert file
        
        self.client = None
        try:
            # Use the session with zeep Transport
            transport = Transport(session=session)
            self.client = zeep.Client(wsdl=wsdl, transport=transport)
        except Exception as e:
            logging.error("gPAS.Client.init: Could not retrieve WSDL. Check connection.")
            sys.exit()

    def get_pseudonym(self, value):
        try:
            return self.client.service.getOrCreatePseudonymFor(value, self.domain)
        except Exception as e:
            logging.error("{0}".format(e))
            return None

    def get_name(self, psn):
        try:
            return self.client.service.getValueFor(psn, self.domain)
        except Exception as e:
            logging.error("{0}".format(e))
            return None

class AdminClient:
    class Alphabets(Enum):
        Hex = "org.emau.icmvc.ganimed.ttp.psn.alphabets.Hex",
        Numbers = "org.emau.icmvc.ganimed.ttp.psn.alphabets.Numbers",
        # Add others as required...

    class Generators(Enum):
        NoCheckDigits = "org.emau.icmvc.ganimed.ttp.psn.generator.NoCheckDigits",
        # Add others as required...

    def __init__(self, port):
        self.base_url = GPAS_BASE_URL
        self.port = port
        wsdl = self.base_url + ':' + str(self.port) + '/gpas/DomainService?wsdl'
        
        # Setup a session with client certificates
        session = Session()
        session.cert = (CERT_PATH, KEY_PATH)
        session.verify = CA_CERT_PATH
        
        self.admin_client = None
        try:
            transport = Transport(session=session)
            self.admin_client = zeep.Client(wsdl=wsdl, transport=transport)
        except Exception as e:
            logging.error("gPAS.Client.init: Could not retrieve WSDL. Check connection.")
            sys.exit()

    def create_domain(self, name, alphabet: Alphabets, generator: Generators, comment="", parent_domain=""):
        payload = {"name": name, "alphabet": alphabet.value[0],
                   "checkDigitClass": generator.value[0], "comment": comment,
                   "parentDomainName": parent_domain}
        try:
            self.admin_client.service.addDomain(payload)
        except Exception as e:
            logging.error("{0}".format(e))
