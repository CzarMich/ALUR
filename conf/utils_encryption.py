import os
import base64
import hashlib
from typing import Tuple
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from cryptography.hazmat.backends import default_backend
from utils_key import load_key
from config import KEY_PATH, PSEUDONYMIZATION_ENABLED, ELEMENTS_TO_PSEUDONYMIZE


def load_aes_key():
    """Load the AES key from the configured KEY_PATH."""
    return load_key(KEY_PATH)


def aes_encrypt(plaintext: str, aes_key: bytes) -> str:
    """
    Encrypt a plaintext string using AES-CBC (with random IV),
    returning a Base64 string of (IV + ciphertext), without any prefix.
    """
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
    data = base64.b64decode(ciphertext_b64)
    iv = data[:16]
    ct = data[16:]

    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded = decryptor.update(ct) + decryptor.finalize()

    unpadder = PKCS7(algorithms.AES.block_size).unpadder()
    plaintext = unpadder.update(padded) + unpadder.finalize()

    return plaintext.decode("utf-8")


def create_short_handle(ciphertext_b64: str) -> str:
    """
    Create a short handle (hash-based) from the raw AES ciphertext.
    Ensures no invalid characters or length issues for FHIR or other constraints.
    """
    sha_digest = hashlib.sha256(ciphertext_b64.encode("utf-8")).digest()

    # Base64-URL encode to remove +,/ etc.; then remove '='
    b64_url = base64.urlsafe_b64encode(sha_digest).decode("utf-8").rstrip("=")

    # Replace '_' with '.' to fully meet [A-Za-z0-9-.] if needed
    b64_url = b64_url.replace("_", ".")

    return b64_url


def append_dynamic_prefix(element_name: str, short_handle: str, max_len: int = 64) -> str:
    """
    Check PSEUDONYMIZATION config for the element to see if a prefix is defined.
    If so, append it to the short handle, ensuring total length doesn't exceed max_len.
    """
    element_config = ELEMENTS_TO_PSEUDONYMIZE.get(element_name, {})
    prefix = element_config.get("prefix", "") if element_config.get("enabled", False) else ""
    if not prefix:
        return short_handle  # No prefix to append

    # Truncate if total length would exceed max_len
    allowed_after_prefix = max_len - len(prefix)
    truncated_handle = short_handle[:allowed_after_prefix]

    return prefix + truncated_handle


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

    # 1) Raw AES encryption
    raw_ciphertext = aes_encrypt(plaintext, aes_key)

    # 2) Create short handle
    short_handle = create_short_handle(raw_ciphertext)

    # 3) Append dynamic prefix from config
    short_id = append_dynamic_prefix(element_name, short_handle, max_len)

    return raw_ciphertext, short_id


def decrypt_with_ciphertext(ciphertext_b64: str, aes_key: bytes, element_name: str = None) -> str:
    """
    Decrypt the raw AES ciphertext (no prefix) to get the original plaintext.
    If the field is not configured for encryption or pseudonymization is disabled,
    return the string as-is (skip base64 decoding).
    """
    # If element_name is unknown or we are not enabled, skip
    if (
        not PSEUDONYMIZATION_ENABLED
        or not element_name
        or element_name not in ELEMENTS_TO_PSEUDONYMIZE
        or not ELEMENTS_TO_PSEUDONYMIZE[element_name].get("enabled", False)
    ):
        # Attempt to detect if ciphertext_b64 is truly base64 or just plaintext
        # We can do a quick check:
        # If it fails b64decode => return as is
        try:
            base64.b64decode(ciphertext_b64)
        except Exception:
            return ciphertext_b64
        # If it does decode, but we never intended to encrypt it, let's just return as is
        return ciphertext_b64

    # Otherwise, decrypt
    try:
        return aes_decrypt(ciphertext_b64, aes_key)
    except Exception:
        # In case it's not valid base64, fallback to as-is
        return ciphertext_b64
