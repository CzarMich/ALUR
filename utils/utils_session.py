import os
import logging
import sqlite3
import psycopg2
import requests
import base64
from psycopg2 import pool, OperationalError
from requests_cache import CachedSession
from dotenv import load_dotenv

# ✅ Load .env file
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'conf', '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

# ✅ Import DB settings from config
from conf.config import DB_TYPE, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_FILE
from conf.config import EHR_AUTH_METHOD, EHR_SERVER_USER, EHR_SERVER_PASSWORD, EHR_SERVER_URL

# ✅ Initialize Logger
logger = logging.getLogger(__name__)

# ✅ PostgreSQL Connection Pool
pg_pool = None

if DB_TYPE == "postgres":
    try:
        pg_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=10,
        )
        logger.info(f"✅ PostgreSQL Connection Pool Initialized at {DB_HOST}:{DB_PORT}")
    except OperationalError as e:
        logger.error(f"🔴 ERROR: Could not initialize PostgreSQL connection pool: {e}")
        pg_pool = None  # Ensure it's set to None if connection fails


def get_db_connection():
    """
    Get a database connection.
    - Uses a connection pool for PostgreSQL.
    - Creates a new connection for SQLite.
    - Ensures only active connections are returned.
    """
    try:
        if DB_TYPE == "postgres" and pg_pool:
            conn = pg_pool.getconn()
            if conn.closed:
                logger.warning("⚠️ Retrieved a closed PostgreSQL connection. Recreating...")
                conn = psycopg2.connect(
                    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
                )
            conn.autocommit = True  # ✅ Ensure autocommit is enabled for efficiency
            logger.info("✅ Retrieved PostgreSQL connection from pool.")
            return conn
        elif DB_TYPE == "sqlite":
            conn = sqlite3.connect(DB_FILE, check_same_thread=False)  # ✅ Thread-safe for multi-threading
            conn.row_factory = sqlite3.Row  # ✅ Enables dictionary-like access to rows
            logger.info("✅ Connected to SQLite database.")
            return conn
        else:
            raise ValueError(f"⚠ Unsupported database type: {DB_TYPE}")
    except Exception as e:
        logger.error(f"🔴 ERROR: Failed to get database connection: {e}")
        raise


def release_db_connection(conn):
    """
    Release a database connection back to the pool (PostgreSQL) or close it (SQLite).
    """
    try:
        if DB_TYPE == "postgres" and pg_pool:
            if conn and not conn.closed:
                pg_pool.putconn(conn)
                logger.info("✅ Returned PostgreSQL connection to pool.")
            else:
                logger.warning("⚠️ Attempted to return a closed or invalid connection.")
        elif DB_TYPE == "sqlite" and conn:
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
    - Supports Basic and Bearer Token authentication.
    - Uses requests_cache to cache responses for efficiency.
    """
    logger.info("🔍 DEBUG: Creating HTTP session...")

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

    session.cache.clear()  # ✅ Ensure cache is cleared before starting

    # ✅ Handle Authentication Methods
    if auth_method == "basic" and username and password:
        logger.info("✅ Using Basic Authentication for session.")
        session.auth = (username, password)

        # ✅ Manually set the Authorization header
        auth_string = f"{username}:{password}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        session.headers.update({"Authorization": f"Basic {encoded_auth}"})

    elif auth_method == "bearer" and token:
        logger.info("✅ Using Bearer Token Authentication for session.")
        session.headers.update({"Authorization": f"Bearer {token}"})

    else:
        logger.warning(f"⚠ WARNING: No valid authentication method provided: {auth_method}. Requests may be unauthorized.")

    return session


# ✅ Debugging/Test Code (Only runs when executed directly)
if __name__ == "__main__":
    logger.info("🔍 DEBUG: Testing database connection...")

    try:
        conn = get_db_connection()
        if conn:
            logger.info("✅ Test Connection Successful.")
            release_db_connection(conn)
        else:
            logger.error("🔴 Test Connection Failed.")
    except Exception as e:
        logger.error(f"❌ Exception during test: {e}")

    logger.info("🔍 DEBUG: Testing session creation...")

    test_session = create_session(
        cache_name="test_cache",
        auth_method=EHR_AUTH_METHOD,
        username=EHR_SERVER_USER if EHR_AUTH_METHOD == "basic" else None,
        password=EHR_SERVER_PASSWORD if EHR_AUTH_METHOD == "basic" else None,
        token=None
    )

    logger.info(f"✅ TEST SESSION CREATED: {test_session}")
    logger.info(f"✅ TEST SESSION HEADERS: {test_session.headers}")
