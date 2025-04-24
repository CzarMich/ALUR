import logging
from typing import List, Dict, Any, Optional
import os
import sys
# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from conf.config import DB_TYPE
from utils.session import get_db_connection, release_db_connection
from utils.logger import logger, verbose

def fetch_as_dict(cursor, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    try:
        logger.debug(f"Executing query: {query.strip()} | Params: {params}")
        cursor.execute(query, params or ())
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Error executing query: {query.strip()} | {e}", exc_info=True)
        return []


def read_unprocessed_consents() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            "SELECT * FROM consent WHERE processed = FALSE ORDER BY composition_id"
            if DB_TYPE == "postgres" else
            "SELECT * FROM consent WHERE processed = 0 ORDER BY composition_id"
        )
        rows = fetch_as_dict(cursor, query)
        if rows:
            verbose(f"Retrieved {len(rows)} unprocessed Consent records.")
        else:
            verbose("No unprocessed Consent records found.")
        return rows
    except Exception as e:
        logger.error(f"Error reading unprocessed Consent records: {e}", exc_info=True)
        return []
    finally:
        release_db_connection(conn)


def read_unprocessed_consents_in_batch(batch_size: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            """
            SELECT id, composition_id, subject_id, consent_status, start_time, end_time, 
                   uri_einwilligungsnachweis, consent_code, consent_code_system, provision_type, consent 
            FROM consent 
            WHERE processed = FALSE 
            ORDER BY composition_id 
            LIMIT %s
            """ if DB_TYPE == "postgres" else
            """
            SELECT id, composition_id, subject_id, consent_status, start_time, end_time, 
                   uri_einwilligungsnachweis, consent_code, consent_code_system, provision_type, consent 
            FROM consent 
            WHERE processed = 0 
            ORDER BY composition_id 
            LIMIT ?
            """
        )
        rows = fetch_as_dict(cursor, query, (batch_size,))
        if rows:
            verbose(f"Retrieved {len(rows)} unprocessed Consent records in batch.")
        else:
            verbose("No unprocessed Consent records found in batch.")
        return rows
    except Exception as e:
        logger.error(f"Error reading unprocessed Consent batch: {e}", exc_info=True)
        return []
    finally:
        release_db_connection(conn)


def mark_consent_as_processed_by_composition(composition_id: str) -> bool:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            "UPDATE consent SET processed = TRUE WHERE composition_id = %s"
            if DB_TYPE == "postgres" else
            "UPDATE consent SET processed = 1 WHERE composition_id = ?"
        )
        cursor.execute(query, (composition_id,))
        conn.commit()
        verbose(f"Marked Consent rows with composition_id '{composition_id}' as processed.")
        return True
    except Exception as e:
        logger.error(f"Error marking Consent composition_id '{composition_id}' as processed: {e}", exc_info=True)
        return False
    finally:
        release_db_connection(conn)


def delete_consent_by_composition(composition_id: str) -> bool:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            "DELETE FROM consent WHERE composition_id = %s"
            if DB_TYPE == "postgres" else
            "DELETE FROM consent WHERE composition_id = ?"
        )
        cursor.execute(query, (composition_id,))
        conn.commit()
        verbose(f"Deleted Consent records with composition_id '{composition_id}'.")
        return True
    except Exception as e:
        logger.error(f"Error deleting Consent records with composition_id '{composition_id}': {e}", exc_info=True)
        return False
    finally:
        release_db_connection(conn)
