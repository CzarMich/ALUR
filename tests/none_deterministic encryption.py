import os
import sys
import logging
import base64
import hashlib
from typing import Tuple
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend
from utils.utils_key import load_key
from conf.config import KEY_PATH, PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE

# ✅ Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("Encryption")

def load_aes_key():
    """Load the AES key from the configured KEY_PATH."""
    aes_key = load_key(KEY_PATH)

    # ✅ Debugging - Log key type before returning
    logger.debug(f"🔍 DEBUG: Loaded AES Key Type: {type(aes_key)}")

    # ✅ Ensure it is bytes
    if isinstance(aes_key, str):
        logger.warning("⚠ AES key was loaded as a string. Converting to bytes.")
        aes_key = aes_key.encode()

    return aes_key


def aes_encrypt(plaintext: str, aes_key: bytes) -> str:
    """
    Encrypt a plaintext string using AES-CBC (with random IV),
    returning a Base64 string of (IV + ciphertext), without any prefix.
    """
    if not isinstance(aes_key, bytes):
        logger.error(f"🔴 Encryption key must be bytes, but received: {type(aes_key)}")
        aes_key = aes_key.encode()  # Convert to bytes if loaded incorrectly

    iv = os.urandom(16)
    padder = PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(plaintext.encode("utf-8")) + padder.finalize()

    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    ciphertext = encryptor.update(padded_data) + encryptor.finalize()

    combined = iv + ciphertext
    return base64.b64encode(combined).decode("utf-8")


def aes_decrypt(ciphertext_b64: str, aes_key: bytes) -> str:
    """
    Decrypt Base64(IV + ciphertext) from aes_encrypt(),
    returning the original plaintext string.
    """
    if not isinstance(aes_key, bytes):
        logger.error(f"🔴 Decryption key must be bytes, but received: {type(aes_key)}")
        aes_key = aes_key.encode()

    data = base64.b64decode(ciphertext_b64)
    iv = data[:16]
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
    1) If the field is not configured or pseudonymization is disabled, return (plaintext, plaintext).
    2) Otherwise, AES-encrypt the plaintext -> raw ciphertext in Base64 (no prefix).
    3) Create a short handle from that ciphertext.
    4) Dynamically append prefix from config if present.
    :return: (raw_ciphertext_b64, shortID_with_prefix)
    """
    # Skip encryption if globally disabled or element not in config / not enabled
    if (
        not PSEUDONYMIZATION_ENABLED
        or element_name not in ELEMENTS_TO_PSEUDONYMIZE
        or not ELEMENTS_TO_PSEUDONYMIZE[element_name].get("enabled", False)
    ):
        return plaintext, plaintext

    # ✅ Log before encryption
    logger.debug(f"🔍 DEBUG: Encrypting {element_name}: {plaintext}")

    # 1) Raw AES encryption
    raw_ciphertext = aes_encrypt(plaintext, aes_key)

    # 2) Create short handle
    short_handle = hashlib.sha256(raw_ciphertext.encode("utf-8")).digest()
    b64_url = base64.urlsafe_b64encode(short_handle).decode("utf-8").rstrip("=")

    # 3) Append dynamic prefix from config
    prefix = ELEMENTS_TO_PSEUDONYMIZE[element_name].get("prefix", "")
    short_id = prefix + b64_url[:max_len - len(prefix)]

    logger.debug(f"✅ Encrypted {element_name}: {short_id}")

    return raw_ciphertext, short_id
