import os
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
from config import KEY_PATH

def generate_key(password: str, salt: bytes) -> bytes:
    """
    Generate a 256-bit AES key using PBKDF2HMAC.
    :param password: The password to derive the key.
    :param salt: A random salt for derivation.
    :return: A 256-bit AES key.
    """
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend(),
    )
    return kdf.derive(password.encode())

def store_key(key: bytes, file_path: str) -> None:
    """
    Store a key in a file.
    :param key: The AES key to store.
    :param file_path: The file path to save the key.
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "wb") as key_file:
        key_file.write(key)

def load_key(file_path: str) -> bytes:
    """
    Load a key from a file.
    :param file_path: The file path to load the key from.
    :return: The loaded AES key.
    """
    with open(file_path, "rb") as key_file:
        return key_file.read()

def key_exists(file_path: str = KEY_PATH) -> bool:
    """
    Check if the key file exists.
    :param file_path: Path to the key file. Defaults to KEY_PATH from config.
    :return: True if the key file exists, False otherwise.
    """
    return os.path.exists(file_path)


