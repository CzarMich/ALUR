import os
import sys
from typing import List, Dict, Any, Optional

# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from utils.session import get_db_connection, release_db_connection
from conf.config import DB_TYPE, RESOURCE_FILES
from utils.logger import logger, verbose

# ---------------------------
# UTILITY: Fetch rows as dictionaries
# ---------------------------
def _fetch_as_dict(cursor, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    try:
        logger.debug(f"Executing query: {query.strip()} | Params: {params}")
        cursor.execute(query, params or ())
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Error executing query: {query.strip()} | {e}", exc_info=True)
        return []

# ---------------------------
# READ: Unprocessed Consent Records
# ---------------------------
def read_unprocessed_consents() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            "SELECT * FROM consent WHERE processed = FALSE ORDER BY composition_id"
            if DB_TYPE == "postgres" else
            "SELECT * FROM consent WHERE processed = 0 ORDER BY composition_id"
        )
        rows = _fetch_as_dict(cursor, query)
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

# ---------------------------
# READ: Unprocessed Consent Records in Batch
# ---------------------------
def read_unprocessed_consents_in_batch(batch_size: int) -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            "SELECT * FROM consent WHERE processed = FALSE ORDER BY composition_id LIMIT %s"
            if DB_TYPE == "postgres" else
            "SELECT * FROM consent WHERE processed = 0 ORDER BY composition_id LIMIT ?"
        )
        rows = _fetch_as_dict(cursor, query, (batch_size,))
        if rows:
            verbose(f"Retrieved {len(rows)} unprocessed Consent records (batch size: {batch_size}).")
        else:
            verbose("No unprocessed Consent records found in batch.")
        return rows
    except Exception as e:
        logger.error(f"Error reading unprocessed Consent batch: {e}", exc_info=True)
        return []
    finally:
        release_db_connection(conn)

# ---------------------------
# UPDATE: Mark Consent by composition_id as processed
# ---------------------------
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

# ---------------------------
# DELETE: Consent by arbitrary group_by field (default: composition_id)
# ---------------------------
def delete_consent_by_group(value: str, group_by: str = "composition_id") -> bool:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = (
            f"DELETE FROM consent WHERE {group_by} = %s"
            if DB_TYPE == "postgres" else
            f"DELETE FROM consent WHERE {group_by} = ?"
        )
        cursor.execute(query, (value,))
        conn.commit()
        verbose(f"Deleted Consent records where {group_by} = '{value}'.")
        return True
    except Exception as e:
        logger.error(f"Error deleting Consent records with {group_by} = '{value}': {e}", exc_info=True)
        return False
    finally:
        release_db_connection(conn)
