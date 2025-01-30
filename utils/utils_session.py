import os
import sqlite3
import logging
import requests
from requests_cache import CachedSession
from datetime import timedelta
from dotenv import load_dotenv
from psycopg2 import pool

# ✅ Load `.env` before importing `config.py`
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'conf', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f"✅ Loaded environment variables from {dotenv_path}")
else:
    print(f"⚠ WARNING: `.env` file not found at {dotenv_path}")

from conf.config import (
    EHR_AUTH_METHOD, EHR_SERVER_USER, EHR_SERVER_PASSWORD, EHR_SERVER_URL,
    DB_TYPE, DB_FILE, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
)

# Initialize logger
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ✅ Debugging: Ensure `EHR_AUTH_METHOD` is correctly loaded
print(f"🔍 DEBUG: EHR_AUTH_METHOD in utils_sessions.py: {repr(EHR_AUTH_METHOD)}")

# ✅ Ensure `EHR_AUTH_METHOD` is correctly set
if not EHR_AUTH_METHOD or EHR_AUTH_METHOD.strip() == "":
    raise ValueError("🔴 ERROR: `EHR_AUTH_METHOD` is missing or empty. Check your `.env` file and config.py.")

# ✅ Normalize authentication method
EHR_AUTH_METHOD = EHR_AUTH_METHOD.strip().lower()
if EHR_AUTH_METHOD not in ["basic", "bearer", "api_key"]:
    raise ValueError(f"🔴 ERROR: Invalid `EHR_AUTH_METHOD`: {EHR_AUTH_METHOD}. Allowed: basic, bearer, api_key")

# ✅ Ensure database connection pool is initialized if using PostgreSQL
pg_pool = None
if DB_TYPE == "postgres":
    try:
        pg_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        logger.info("✅ Initialized PostgreSQL connection pool.")
    except Exception as e:
        logger.error(f"🔴 ERROR: Failed to initialize PostgreSQL pool: {e}")


import base64

def create_session(
    cache_name='default_cache',
    expire_days=1,
    auth_method='basic',
    username=None,
    password=None,
    token=None
):
    """
    Create and return a cached requests session with authentication.
    """
    print("🔍 DEBUG: Inside create_session function.")
    print(f"🔍 DEBUG: auth_method={repr(auth_method)}, username={repr(username)}, password={'****' if password else None}, token={repr(token)}")

    session = CachedSession(
        cache_name=cache_name,
        use_cache_dir=True,
        cache_control=False,
        expire_after=0,
        allowable_codes=[200, 400, 401],
        allowable_methods=['GET', 'POST', 'PUT', 'DELETE'],
        ignored_parameters=['api_key'],
        match_headers=['Accept-Language'],
        stale_if_error=False,
    )

    session.cache.clear()

    if auth_method == "basic" and username and password:
        print("✅ Using Basic Authentication for session.")
        session.auth = (username, password)

        # ✅ Manually set the Authorization header
        auth_string = f"{username}:{password}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        session.headers.update({"Authorization": f"Basic {encoded_auth}"})
        print(f"✅ Authorization header set: Basic {encoded_auth[:10]}...")

    elif auth_method == "bearer" and token:
        print("✅ Using Bearer Token Authentication for session.")
        session.headers.update({"Authorization": f"Bearer {token}"})

    else:
        print(f"⚠ WARNING: No valid authentication method provided: {auth_method}. Requests may be unauthorized.")

    return session

def get_db_connection():
    """
    Get a database connection from the pool (PostgreSQL) or create a new one (SQLite).
    """
    try:
        if DB_TYPE == "postgres" and pg_pool:
            conn = pg_pool.getconn()
            logger.info("✅ Retrieved connection from PostgreSQL pool.")
            return conn
        elif DB_TYPE == "sqlite":
            conn = sqlite3.connect(DB_FILE)
            logger.info("✅ Connected to SQLite database.")
            return conn
        else:
            raise ValueError(f"⚠ Unsupported database type: {DB_TYPE}")
    except Exception as e:
        logger.error(f"🔴 ERROR: Failed to get database connection: {e}")
        raise


def release_db_connection(conn):
    """
    Release the database connection back to the pool or close it (SQLite).
    """
    try:
        if DB_TYPE == "postgres" and pg_pool:
            pg_pool.putconn(conn)
            logger.info("✅ Returned connection to PostgreSQL pool.")
        elif DB_TYPE == "sqlite":
            conn.close()
            logger.info("✅ Closed SQLite database connection.")
    except Exception as e:
        logger.error(f"🔴 ERROR: Failed to release database connection: {e}")
        raise


def close_connection_pool():
    """
    Close all connections in the PostgreSQL pool.
    """
    if DB_TYPE == "postgres" and pg_pool:
        try:
            pg_pool.closeall()
            logger.info("✅ Closed all PostgreSQL connections in the pool.")
        except Exception as e:
            logger.error(f"🔴 ERROR: Failed to close PostgreSQL connection pool: {e}")


# ✅ Manual Test Code
if __name__ == "__main__":
    print("🔍 DEBUG: Manually testing `create_session` in utils_sessions.py...")

    # Explicitly print credentials to verify they exist
    print(f"🔍 DEBUG: EHR_AUTH_METHOD={repr(EHR_AUTH_METHOD)}")
    print(f"🔍 DEBUG: EHR_SERVER_USER={repr(EHR_SERVER_USER)}")
    print(f"🔍 DEBUG: EHR_SERVER_PASSWORD={'****' if EHR_SERVER_PASSWORD else None}")

    test_session = create_session(
        cache_name="test_cache",
        auth_method=EHR_AUTH_METHOD,
        username=EHR_SERVER_USER if EHR_AUTH_METHOD == "basic" else None,
        password=EHR_SERVER_PASSWORD if EHR_AUTH_METHOD == "basic" else None,
        token=None
    )

    print(f"✅ TEST SESSION CREATED: {test_session}")
    print(f"✅ TEST SESSION HEADERS: {test_session.headers}")
