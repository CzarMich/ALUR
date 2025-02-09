import os
import sys
# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Add the project root to Python's module search path
sys.path.insert(0, BASE_DIR)
import base64
import hashlib
from typing import Tuple
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend
from utils.utils_key import load_key
from conf.config import KEY_PATH, PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE

import logging

logger = logging.getLogger(__name__)

# ✅ Load deterministic AES setting from config
from conf.config import PSEUDONYMIZATION_DETERMINISTIC_AES

def load_aes_key():
    """Load the AES key from the configured KEY_PATH."""
    return load_key(KEY_PATH)

def derive_iv(plaintext: str) -> bytes:
    """
    Generate a deterministic IV from a SHA-256 hash of the input.
    Ensures that encrypting the same input always results in the same ciphertext.
    """
    return hashlib.sha256(plaintext.encode("utf-8")).digest()[:16]  # First 16 bytes

def aes_encrypt(plaintext: str, aes_key: bytes) -> str:
    """
    Encrypt a plaintext string using AES-CBC.
    If deterministic AES is enabled, uses a derived IV for consistency.
    Otherwise, generates a random IV.
    """
    iv = derive_iv(plaintext) if PSEUDONYMIZATION_DETERMINISTIC_AES else os.urandom(16)  # ✅ Choose IV mode

    padder = PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(plaintext.encode("utf-8")) + padder.finalize()

    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()

    combined = iv + ciphertext  # Include IV in the output for safety
    return base64.b64encode(combined).decode("utf-8")

def aes_decrypt(ciphertext_b64: str, aes_key: bytes, plaintext_hint: str = None) -> str:
    """
    Decrypt AES-CBC ciphertext.
    If deterministic AES is enabled, derives the IV from plaintext_hint.
    Otherwise, extracts IV from the ciphertext itself.
    """
    data = base64.b64decode(ciphertext_b64)
    
    if PSEUDONYMIZATION_DETERMINISTIC_AES:
        if not plaintext_hint:
            logger.error("❌ Decryption failed: plaintext_hint is required for deterministic AES.")
            return ciphertext_b64  # Fallback to ciphertext

        iv = derive_iv(plaintext_hint)  # ✅ Ensure IV is derived the same way
        ct = data  # No IV stored in deterministic mode
    else:
        iv = data[:16]  # First 16 bytes are the IV
        ct = data[16:]

    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded = decryptor.update(ct) + decryptor.finalize()

    unpadder = PKCS7(algorithms.AES.block_size).unpadder()
    plaintext = unpadder.update(padded) + unpadder.finalize()

    return plaintext.decode("utf-8")

def encrypt_and_shorthand(
    plaintext: str,
    element_name: str,
    aes_key: bytes,
    max_len: int = 64
) -> Tuple[str, str]:
    """
    Encrypts and creates a shorthand for the given plaintext.
    Uses **deterministic AES** if enabled in settings.
    :return: (raw_ciphertext_b64, shortID_with_prefix)
    """
    if (
        not PSEUDONYMIZATION_ENABLED
        or element_name not in ELEMENTS_TO_PSEUDONYMIZE
        or not ELEMENTS_TO_PSEUDONYMIZE[element_name].get("enabled", False)
    ):
        return plaintext, plaintext

    # 1) Encrypt (deterministic or standard based on settings)
    raw_ciphertext = aes_encrypt(plaintext, aes_key)

    # 2) Create short handle
    short_handle = hashlib.sha256(raw_ciphertext.encode("utf-8")).hexdigest()[:16]  # Short hash

    # 3) Append dynamic prefix from config
    prefix = ELEMENTS_TO_PSEUDONYMIZE[element_name].get("prefix", "")
    short_id = f"{prefix}{short_handle}" if prefix else short_handle

    return raw_ciphertext, short_id

def decrypt_with_ciphertext(ciphertext_b64: str, aes_key: bytes, plaintext_hint: str = None, element_name: str = None) -> str:
    """
    Decrypt AES-CBC ciphertext using **deterministic or standard mode**.
    If deterministic AES is enabled, requires plaintext_hint.
    """
    if (
        not PSEUDONYMIZATION_ENABLED
        or not element_name
        or element_name not in ELEMENTS_TO_PSEUDONYMIZE
        or not ELEMENTS_TO_PSEUDONYMIZE[element_name].get("enabled", False)
    ):
        return ciphertext_b64  # No encryption applied, return as is

    try:
        return aes_decrypt(ciphertext_b64, aes_key, plaintext_hint)
    except Exception:
        logger.error(f"❌ Decryption failed for {element_name}. Returning ciphertext as fallback.")
        return ciphertext_b64
