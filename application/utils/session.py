import os
import sys
import logging
import sqlite3
import psycopg2
import base64
import requests
from typing import Optional
from psycopg2 import pool, OperationalError
from requests_cache import CachedSession
from utils.logger import logger, verbose
from conf.config import (
    DB_TYPE, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_FILE,
    EHR_AUTH_METHOD, EHR_SERVER_USER, EHR_SERVER_PASSWORD, EHR_SERVER_URL,
    BASE_DIR, CONF_DIR
)

# Ensure the project root is in Python's module search path
BASE_MAPPING_DIR = os.path.join(CONF_DIR, "resources")

# PostgreSQL Connection Pool (initialized once)
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
        verbose(f"PostgreSQL Connection Pool Initialized at {DB_HOST}:{DB_PORT}")
    except OperationalError as e:
        logger.error(f"🔴 ERROR: Could not initialize PostgreSQL pool: {e}")
        pg_pool = None


# ---------------------------
# SYNC DB FUNCTIONS
# ---------------------------
def get_db_connection():
    try:
        if DB_TYPE == "postgres":
            if not pg_pool:
                raise RuntimeError("ERROR: PostgreSQL pool is not initialized.")
            conn = pg_pool.getconn()
            if conn.closed:
                logger.warning("Retrieved closed PostgreSQL connection. Reconnecting...")
                conn = psycopg2.connect(
                    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                    host=DB_HOST, port=DB_PORT
                )
            conn.autocommit = True
            logger.debug("PostgreSQL connection acquired.")
            return conn
        elif DB_TYPE == "sqlite":
            conn = sqlite3.connect(DB_FILE, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            logger.debug("SQLite connection established.")
            return conn
        else:
            logger.critical(f"Unsupported database type: {DB_TYPE}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to get DB connection: {e}", exc_info=True)
        raise


def release_db_connection(conn):
    try:
        if DB_TYPE == "postgres" and pg_pool and conn:
            if not conn.closed:
                pg_pool.putconn(conn)
                logger.debug("PostgreSQL connection returned to pool.")
        elif DB_TYPE == "sqlite" and conn:
            conn.close()
            logger.debug("SQLite connection closed.")
    except Exception as e:
        logger.error(f"Error releasing DB connection: {e}", exc_info=True)


def close_connection_pool():
    if DB_TYPE == "postgres" and pg_pool:
        try:
            pg_pool.closeall()
            verbose("PostgreSQL connection pool closed.")
        except Exception as e:
            logger.error(f"Failed to close PostgreSQL connection pool: {e}")


# ---------------------------
# SYNC SESSION
# ---------------------------
def create_session(
    cache_name='default_cache',
    expire_days=1,
    auth_method='basic',
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None
):
    logger.debug("🔧 Creating HTTP session...")
    session = CachedSession(
        cache_name=cache_name,
        use_cache_dir=True,
        cache_control=False,
        expire_after=0,
        allowable_codes=[200, 400, 401],
        allowable_methods=['GET', 'POST', 'PUT', 'DELETE'],
        match_headers=['Accept-Language'],
        stale_if_error=False,
    )
    session.cache.clear()
    verbose("Cleared HTTP cache before sending request.")

    # ❌ Disable HTTP caching explicitly
    session.headers.update({"Cache-Control": "no-cache"})

    if auth_method == "basic" and username and password:
        logger.debug("🔑 Using Basic Auth.")
        session.auth = (username, password)
    elif auth_method == "bearer" and token:
        logger.debug("🔑 Using Bearer Token Auth.")
        session.headers.update({"Authorization": f"Bearer {token}"})
    else:
        logger.warning(f"No valid authentication method configured: {auth_method}")

    return session


# ---------------------------
# DEBUG ENTRY POINT
# ---------------------------
if __name__ == "__main__":
    verbose("Testing DB and HTTP session...")

    try:
        conn = get_db_connection()
        if conn:
            verbose(" Test DB connection successful.")
            release_db_connection(conn)
    except Exception as e:
        logger.error(f" DB Test Failed: {e}")

    test_session = create_session(
        cache_name="test_cache",
        auth_method=EHR_AUTH_METHOD,
        username=EHR_SERVER_USER if EHR_AUTH_METHOD == "basic" else None,
        password=EHR_SERVER_PASSWORD if EHR_AUTH_METHOD == "basic" else None,
        token=None
    )
    verbose(f"HTTP Session Created with headers: {test_session.headers}")
