import os
import json
from datetime import datetime, timedelta
from conf.config import (
    yaml_config, FETCH_BY_DATE_ENABLED, FETCH_INTERVAL_HOURS,
    POLL_INTERVAL
)
from utils.logger import logger, verbose
from utils.utils import get_path
from utils.session import get_db_connection, release_db_connection

def ensure_fetch_state_table():
    """
    Create the `fetch_state` table if it doesn't exist.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fetch_state (
                resource TEXT PRIMARY KEY,
                last_run_time TIMESTAMP NOT NULL,
                next_run_time TIMESTAMP NOT NULL
            )
        """)
        conn.commit()
        verbose("Ensured `fetch_state` table exists.")
    except Exception as e:
        logger.error(f"Error creating `fetch_state` table: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

def get_fetch_state(resource_type: str):
    """
    Retrieve `last_run_time` and `next_run_time` from the `fetch_state` table.
    If none exists, fallback to config `start_date` if enabled.
    """
    fetch_start_date = yaml_config.get('fetch_by_date', {}).get('start_date', None)

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT last_run_time, next_run_time FROM fetch_state WHERE resource = %s",
            (resource_type,)
        )
        result = cursor.fetchone()
        if result:
            last_run, next_run = result
            # Convert datetime to ISO 8601 with 'T'
            if isinstance(last_run, datetime):
                last_run = last_run.strftime("%Y-%m-%dT%H:%M:%S")
            if isinstance(next_run, datetime):
                next_run = next_run.strftime("%Y-%m-%dT%H:%M:%S")
            logger.debug(f"🧭 Found fetch state for {resource_type}: {last_run}, {next_run}")
            return last_run, next_run

    except Exception as e:
        logger.error(f"Error retrieving fetch state for {resource_type}: {e}", exc_info=True)

    finally:
        release_db_connection(conn)

    if FETCH_BY_DATE_ENABLED and fetch_start_date:
        verbose(f"Using start_date from config for {resource_type}: {fetch_start_date}")
        return fetch_start_date, None

    return None, None

def calculate_next_run_time(last_run_time):
    """
    Calculate next_run_time using either interval_hours (if fetch_by_date) or poll_interval (default).
    """
    if isinstance(last_run_time, str):
        last_run_dt = datetime.strptime(last_run_time, "%Y-%m-%dT%H:%M:%S")
    else:
        last_run_dt = last_run_time

    next_run = (last_run_dt + timedelta(hours=FETCH_INTERVAL_HOURS)) if FETCH_BY_DATE_ENABLED \
        else (last_run_dt + timedelta(seconds=POLL_INTERVAL))

    return next_run.strftime("%Y-%m-%dT%H:%M:%S")

def update_fetch_state(resource_type: str, last_run_time: str, next_run_time: str):
    """
    Update both `last_run_time` and `next_run_time` in the fetch_state table.
    """
    try:
        last_dt = datetime.strptime(last_run_time.split(".")[0].replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
        next_dt = datetime.strptime(next_run_time.split(".")[0].replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
    except ValueError as e:
        logger.error(f"Invalid datetime format for fetch state update: {e}", exc_info=True)
        return

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO fetch_state (resource, last_run_time, next_run_time)
            VALUES (%s, %s, %s)
            ON CONFLICT (resource) DO UPDATE
            SET last_run_time = EXCLUDED.last_run_time,
                next_run_time = EXCLUDED.next_run_time
        """, (resource_type, last_dt, next_dt))
        conn.commit()
        verbose(f"✅ Updated fetch state for {resource_type}: {last_run_time} → {next_run_time}")

    except Exception as e:
        logger.error(f"Failed to update fetch state for {resource_type}: {e}", exc_info=True)

    finally:
        release_db_connection(conn)

def clear_fetch_state(resource_type: str):
    """
    Clear fetch state from DB and local state.json.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fetch_state WHERE resource = %s", (resource_type,))
        conn.commit()
        verbose(f"Cleared fetch_state table entry for {resource_type}")
    except Exception as e:
        logger.error(f"Error clearing fetch state from DB for {resource_type}: {e}", exc_info=True)
    finally:
        release_db_connection(conn)

    state_file_path = get_path("state_file")
    if os.path.exists(state_file_path):
        try:
            with open(state_file_path, "r") as f:
                state = json.load(f)
            if resource_type in state:
                del state[resource_type]
                with open(state_file_path, "w") as f:
                    json.dump(state, f, indent=2)
                verbose(f"Removed {resource_type} from local state.json")
        except Exception as e:
            logger.error(f"Failed to update state.json: {e}", exc_info=True)
