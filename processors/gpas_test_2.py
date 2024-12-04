import zeep
import logging
import os
import sys
# Add the parent directory (project root) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from enum import Enum
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from conf.config import GPAS_BASE_URL, GPAS_ROOT_DOMAIN, GPAS_CLIENT_CERT, GPAS_CLIENT_KEY, GPAS_CA_CERT

class GPASSession(Session):
    def __init__(self):
        super().__init__()
        self.cert = (GPAS_CLIENT_CERT, GPAS_CLIENT_KEY)
        self.verify = GPAS_CA_CERT
        retries = Retry(total=5, backoff_factor=0.1)
        adapter = HTTPAdapter(max_retries=retries)
        self.mount('https://', adapter)

class Client:
    def __init__(self, port=8080):  # Removed base_url and replaced with GPAS_BASE_URL
        self.base_url = GPAS_BASE_URL  # Using GPAS_BASE_URL from config
        self.port = port
        self.domain = GPAS_ROOT_DOMAIN
        wsdl = f"{self.base_url}:{self.port}/gpas/gpasService?wsdl"
        self.client = None
        try:
            # Create and use the custom session
            session = GPASSession()
            self.client = zeep.Client(wsdl=wsdl, transport=zeep.Transport(session=session))
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
        NumbersWithoutZero = "org.emau.icmvc.ganimed.ttp.psn.alphabets.NumbersWithoutZero",
        NumbersX = "org.emau.icmvc.ganimed.ttp.psn.alphabets.NumbersX",
        Symbol31 = "org.emau.icmvc.ganimed.ttp.psn.alphabets.Symbol31",
        Symbol32 = "org.emau.icmvc.ganimed.ttp.psn.alphabets.Symbol32",

    class Generators(Enum):
        NoCheckDigits = "org.emau.icmvc.ganimed.ttp.psn.generator.NoCheckDigits",
        HammingCode = "org.emau.icmvc.ganimed.ttp.psn.generator.HammingCode",
        Verhoeff = "org.emau.icmvc.ganimed.ttp.psn.generator.Verhoeff",
        VerhoeffGumm = "org.emau.icmvc.ganimed.ttp.psn.generator.VerhoeffGumm",
        Damm = "org.emau.icmvc.ganimed.ttp.psn.generator.Damm",
        ReedSolomonLagrange = "org.emau.icmvc.ganimed.ttp.psn.generator.ReedSolomonLagrange",

    def __init__(self, base_url, port):
        self.base_url = base_url
        self.port = port
        wsdl = f"{base_url}:{port}/gpas/DomainService?wsdl"
        self.admin_client = None
        try:
            # Create and use the custom session
            session = GPASSession()
            self.admin_client = zeep.Client(wsdl=wsdl, transport=zeep.Transport(session=session))
        except Exception as e:
            logging.error("gPAS.AdminClient.init: Could not retrieve WSDL. Check connection.")
            sys.exit()

    def create_domain(self, name, alphabet: Alphabets, generator: Generators, comment="", parent_domain=""):
        payload = {"name": name, "alphabet": alphabet.value[0],
                   "checkDigitClass": generator.value[0], "comment": comment,
                   "parentDomainName": parent_domain}
        try:
            self.admin_client.service.addDomain(payload)
        except Exception as e:
            logging.error("{0}".format(e))
