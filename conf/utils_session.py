import os
from datetime import timedelta
from requests_cache import CachedSession
import sqlite3
from psycopg2 import pool
import logging
from config import (
    EHR_AUTH_METHOD, EHR_SERVER_USER, EHR_SERVER_PASSWORD,
    FHIR_SERVER_PASSWORD, FHIR_AUTH_METHOD, DB_TYPE, DB_FILE, 
    DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
)

logger = logging.getLogger(__name__)

# Initialize PostgreSQL connection pool
pg_pool = None
if DB_TYPE == "postgres":
    try:
        pg_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,  # Adjust based on expected workload
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        logger.info("Initialized PostgreSQL connection pool.")
    except Exception as e:
        logger.error(f"Error initializing PostgreSQL connection pool: {e}")


def create_session(
    cache_name='default_cache',
    expire_days=1,
    auth_method='basic',
    username=None,
    password=None,
    token=None
):
    """
    Create and return a cached requests session.

    :param cache_name:   Name of the cache directory/file (default 'default_cache').
    :param expire_days:  Number of days after which cached responses expire.
    :param auth_method:  Authentication method ('basic', 'bearer', or 'none').
    :param username:     Username for basic auth.
    :param password:     Password for basic auth.
    :param token:        Bearer token if using token-based auth.
    :return:             A requests_cache.CachedSession object with appropriate auth & caching.
    """
    session = CachedSession(
        cache_name=cache_name,
        use_cache_dir=True,
        cache_control=True,
        expire_after=timedelta(days=expire_days),
        allowable_codes=[200, 400],
        allowable_methods=['GET', 'POST', 'PUT', 'DELETE'],
        ignored_parameters=['api_key'],
        match_headers=['Accept-Language'],
        stale_if_error=True,
    )

    # Normalize auth_method to lowercase for consistency
    auth_method = (auth_method or 'basic').lower()

    if auth_method == 'basic' and username and password:
        # Attach HTTP Basic Auth
        session.auth = (username, password)
    elif auth_method == 'bearer' and token:
        # Attach Bearer token as Authorization header
        session.headers.update({"Authorization": f"Bearer {token}"})
    else:
        # 'none' or unrecognized method → no auth
        pass

    return session


def get_db_connection():
    """
    Get a database connection from the pool (PostgreSQL) or create a new one (SQLite).
    """
    try:
        if DB_TYPE == "postgres" and pg_pool:
            conn = pg_pool.getconn()
            logger.info("Retrieved connection from PostgreSQL pool.")
            return conn
        elif DB_TYPE == "sqlite":
            conn = sqlite3.connect(DB_FILE)
            logger.info("Connected to SQLite database.")
            return conn
        else:
            raise ValueError(f"Unsupported database type: {DB_TYPE}")
    except Exception as e:
        logger.error(f"Error getting database connection: {e}")
        raise


def release_db_connection(conn):
    """
    Release the database connection back to the pool or close it (SQLite).
    """
    try:
        if DB_TYPE == "postgres" and pg_pool:
            pg_pool.putconn(conn)
            logger.info("Returned connection to PostgreSQL pool.")
        elif DB_TYPE == "sqlite":
            conn.close()
            logger.info("Closed SQLite database connection.")
    except Exception as e:
        logger.error(f"Error releasing database connection: {e}")
        raise


def close_connection_pool():
    """
    Close all connections in the PostgreSQL pool.
    """
    if DB_TYPE == "postgres" and pg_pool:
        try:
            pg_pool.closeall()
            logger.info("Closed all PostgreSQL connections in the pool.")
        except Exception as e:
            logger.error(f"Error closing PostgreSQL connection pool: {e}")


# Example: Creating sessions for openEHR and FHIR servers.
ehr_session = create_session(
    cache_name='ehr_cache',
    auth_method=EHR_AUTH_METHOD,
    username=EHR_SERVER_USER,
    password=EHR_SERVER_PASSWORD
)

fhir_session = create_session(
    cache_name='fhir_cache',
    auth_method=FHIR_AUTH_METHOD,   # 'basic', 'bearer', or 'none'
    token=FHIR_SERVER_PASSWORD      # If FHIR_AUTH_METHOD == 'bearer', set token here
)
