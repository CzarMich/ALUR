import os
import sys

# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import base64
import hashlib
import requests
import xml.etree.ElementTree as ET
from typing import Tuple
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend

from utils.key import load_or_generate_key
from conf.config import (
    PSEUDONYMIZATION_ENABLED,
    PSEUDONYMIZATION_DETERMINISTIC_AES,
    GPAS_ENABLED,
    GPAS_BASE_URL,
    GPAS_DOMAIN,
    GPAS_CLIENT_CERT,
    GPAS_CLIENT_KEY,
    GPAS_CA_CERT,
    ELEMENTS_TO_PSEUDONYMIZE
)
from utils.logger import logger

# ✅ Load or generate AES key
def load_aes_key() -> bytes:
    return load_or_generate_key()


# ✅ Deterministic IV using SHA256
def derive_iv(plaintext: str) -> bytes:
    return hashlib.sha256(plaintext.encode("utf-8")).digest()[:16]


# ✅ AES Encryption
def aes_encrypt(plaintext: str, aes_key: bytes) -> str:
    iv = derive_iv(plaintext) if PSEUDONYMIZATION_DETERMINISTIC_AES else os.urandom(16)
    padder = PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(plaintext.encode("utf-8")) + padder.finalize()
    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()
    combined = iv + ciphertext
    return base64.b64encode(combined).decode("utf-8")


# ✅ AES Decryption
def aes_decrypt(ciphertext_b64: str, aes_key: bytes, plaintext_hint: str = None) -> str:
    data = base64.b64decode(ciphertext_b64)
    if PSEUDONYMIZATION_DETERMINISTIC_AES:
        if not plaintext_hint:
            logger.error("Decryption failed: plaintext_hint required for deterministic AES.")
            return ciphertext_b64
        iv = derive_iv(plaintext_hint)
        ct = data
    else:
        iv, ct = data[:16], data[16:]

    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded = decryptor.update(ct) + decryptor.finalize()
    unpadder = PKCS7(algorithms.AES.block_size).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


# ✅ GPAS Fallback Pseudonymization
def gpas_pseudonymize(value: str) -> str:
    if not GPAS_ENABLED or not GPAS_BASE_URL:
        logger.warning("⚠ GPAS is not enabled or configured. Skipping GPAS.")
        return value

    try:
        payload = f"""
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                          xmlns:psn="http://psn.ttp.ganimed.icmwc.emau.org/">
            <soapenv:Body>
                <psn:getOrCreatePseudonymFor>
                    <value>{value}</value>
                    <domainName>{GPAS_DOMAIN}</domainName>
                </psn:getOrCreatePseudonymFor>
            </soapenv:Body>
        </soapenv:Envelope>
        """
        headers = {"Content-Type": "text/xml"}
        response = requests.post(
            GPAS_BASE_URL,
            data=payload,
            headers=headers,
            cert=(GPAS_CLIENT_CERT, GPAS_CLIENT_KEY),
            verify=GPAS_CA_CERT,
            timeout=10
        )
        response.raise_for_status()
        pseudonym = parse_gpas_response(response.text)
        return pseudonym if pseudonym else value

    except requests.exceptions.RequestException as e:
        logger.error(f"GPAS request failed: {e}")
        logger.warning("Falling back to AES encryption.")
        return aes_encrypt(value, load_aes_key())


# ✅ GPAS Response XML Parser
def parse_gpas_response(response_text: str) -> str:
    try:
        root = ET.fromstring(response_text)
        ns = {"ns2": "http://psn.ttp.ganimed.icmwc.emau.org/"}
        pseudonym_element = root.find(".//ns2:getOrCreatePseudonymForResponse/ns2:psn", ns)
        return pseudonym_element.text if pseudonym_element is not None else None
    except ET.ParseError:
        logger.error("Failed to parse GPAS response XML.")
        return None


# ✅ Unified Encryption Handler
def encrypt_and_shorthand(plaintext: str, element_name: str, aes_key: bytes) -> Tuple[str, str]:
    if (
        not PSEUDONYMIZATION_ENABLED
        or element_name not in ELEMENTS_TO_PSEUDONYMIZE
        or not ELEMENTS_TO_PSEUDONYMIZE[element_name].get("enabled", False)
    ):
        return plaintext, plaintext

    if GPAS_ENABLED:
        pseudonym = gpas_pseudonymize(plaintext)
        return pseudonym, pseudonym

    raw_ciphertext = aes_encrypt(plaintext, aes_key)
    short_handle = hashlib.sha256(raw_ciphertext.encode("utf-8")).hexdigest()[:16]
    prefix = ELEMENTS_TO_PSEUDONYMIZE[element_name].get("prefix", "")
    short_id = f"{prefix}{short_handle}" if prefix else short_handle

    return raw_ciphertext, short_id


# ✅ Unified Decryption Handler
def decrypt_with_ciphertext(ciphertext_b64: str, aes_key: bytes, plaintext_hint: str = None, element_name: str = None) -> str:
    if (
        not PSEUDONYMIZATION_ENABLED
        or not element_name
        or element_name not in ELEMENTS_TO_PSEUDONYMIZE
        or not ELEMENTS_TO_PSEUDONYMIZE[element_name].get("enabled", False)
    ):
        return ciphertext_b64

    try:
        return aes_decrypt(ciphertext_b64, aes_key, plaintext_hint)
    except Exception:
        logger.error(f"Decryption failed for {element_name}. Returning ciphertext.")
        return ciphertext_b64
