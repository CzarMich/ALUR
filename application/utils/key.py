import os
import sys
import base64
import secrets
import string
import psycopg2

from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

# Add project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from conf.config import (
    KEY_PATH, DB_TYPE, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
)
from utils.logger import logger, verbose

# --- AES Key Derivation ---
def generate_key(password: str, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    return kdf.derive(password.encode())


def generate_salt(length: int = 16) -> bytes:
    return secrets.token_bytes(length)


def generate_secure_password(length: int = 32) -> str:
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(secrets.choice(characters) for _ in range(length))


def store_key(key: bytes, path: str = KEY_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(key)


def load_key(path: str = KEY_PATH) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def key_exists(path: str = KEY_PATH) -> bool:
    return os.path.exists(path)


# --- PostgreSQL Config Table Storage ---
def store_password_and_salt(password: str, salt: bytes):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        cur.execute("""
            INSERT INTO config (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, ('aes_key_password', password))
        cur.execute("""
            INSERT INTO config (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, ('aes_key_salt', base64.b64encode(salt).decode()))
        conn.commit()
        verbose("🔐 Stored AES key password and salt in DB.")
    finally:
        conn.close()


def retrieve_password_and_salt() -> tuple[str, bytes]:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM config WHERE key = 'aes_key_password'")
        password_row = cur.fetchone()
        cur.execute("SELECT value FROM config WHERE key = 'aes_key_salt'")
        salt_row = cur.fetchone()
        if not password_row or not salt_row:
            raise ValueError("Password or salt not found in config table.")
        return password_row[0], base64.b64decode(salt_row[0])
    finally:
        conn.close()


# --- High-Level Key Loader ---
def load_or_generate_key() -> bytes:
    if key_exists(KEY_PATH):
        verbose("🔐 AES key file found. Loading...")
        key = load_key()
        if key:
            verbose("AES key successfully loaded into memory.")
            return key
        else:
            logger.warning(" AES key file exists but could not be loaded. Regenerating...")
    
    verbose("🔐 AES key file not found or invalid. Generating new key...")
    password = generate_secure_password()
    salt = generate_salt()
    aes_key = generate_key(password, salt)

    store_key(aes_key)
    store_password_and_salt(password, salt)

    verbose("AES key generated and stored.")
    return aes_key


# --- Debug Runner ---
if __name__ == "__main__":
    load_or_generate_key()
