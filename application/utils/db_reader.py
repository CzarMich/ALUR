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
# -------------------------------
# Generic Fetch as Dictionary
# -------------------------------
def fetch_as_dict(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute a SQL query and return rows as a list of dictionaries.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"❌ Error executing query: {query} | {e}", exc_info=True)
        return []
    finally:
        release_db_connection(conn)


# -------------------------------
# Fetch All Unprocessed Records
# -------------------------------
def read_unprocessed_rows(table_name: str) -> List[Dict[str, Any]]:
    """
    Fetch all unprocessed rows from a given table.
    """
    table_name = table_name.lower()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name} WHERE processed = FALSE"
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if rows:
            verbose(f"Retrieved {len(rows)} unprocessed rows from '{table_name}'.")
        else:
            verbose(f"No unprocessed rows found in '{table_name}'.")
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"❌ Error reading unprocessed rows from '{table_name}': {e}", exc_info=True)
        return []
    finally:
        release_db_connection(conn)


# -------------------------------
# Fetch Unprocessed Rows in Batch
# -------------------------------
def read_unprocessed_rows_in_batch(table_name: str, batch_size: int) -> List[Dict[str, Any]]:
    """
    Fetch a batch of unprocessed rows from a given table.
    """
    table_name = table_name.lower()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = f"SELECT * FROM {table_name} WHERE processed = FALSE LIMIT %s" if DB_TYPE == "postgres" \
            else f"SELECT * FROM {table_name} WHERE processed = 0 LIMIT ?"
        cursor.execute(query, (batch_size,))
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if rows:
            verbose(f"Retrieved {len(rows)} unprocessed rows from '{table_name}' (batch size: {batch_size}).")
        else:
            verbose(f"No unprocessed rows found in batch for '{table_name}'.")
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"❌ Error reading batch from '{table_name}': {e}", exc_info=True)
        return []
    finally:
        release_db_connection(conn)


# -------------------------------
# Mark Row as Processed
# -------------------------------
def mark_row_as_processed(table_name: str, row_id: Any) -> bool:
    """
    Mark a specific row as processed in the given table.
    """
    table_name = table_name.lower()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = f"UPDATE {table_name} SET processed = TRUE WHERE id = %s" if DB_TYPE == "postgres" \
            else f"UPDATE {table_name} SET processed = 1 WHERE id = ?"
        cursor.execute(query, (row_id,))
        conn.commit()
        verbose(f"Marked row ID {row_id} as processed in '{table_name}'.")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to mark row ID {row_id} as processed in '{table_name}': {e}", exc_info=True)
        return False
    finally:
        release_db_connection(conn)


# -------------------------------
# Local Test Entry Point
# -------------------------------
if __name__ == "__main__":
    table = "fhir_queue"
    rows = read_unprocessed_rows(table)
    print(f"→ Retrieved {len(rows)} rows from '{table}'")

    query = f"SELECT * FROM {table} WHERE processed = FALSE"
    rows_generic = fetch_as_dict(query)
    print(f"→ Retrieved {len(rows_generic)} rows using fetch_as_dict.")
