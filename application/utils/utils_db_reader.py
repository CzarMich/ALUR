import os
import sys
import logging
import psycopg2
import psycopg2.extras
import sqlite3
from typing import List, Dict, Any, Optional
from psycopg2 import sql
from utils.utils_session import get_db_connection, release_db_connection
from conf.config import DB_TYPE

# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

logger = logging.getLogger("DBReader")


def fetch_as_dict(cursor, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a SQL query and return results as a list of dictionaries."""
    try:
        cursor.execute(query, params or ())

        # ✅ Extract column names dynamically
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    except Exception as e:
        logger.error(f"❌ Error executing query: {query} | {e}", exc_info=True)
        return []


def read_unprocessed_rows(table_name: str) -> List[Dict[str, Any]]:
    """
    Read unprocessed rows from the specified table.
    - Converts table name to lowercase for consistency.
    """
    conn = get_db_connection()
    rows = []

    try:
        cursor_factory = psycopg2.extras.RealDictCursor if DB_TYPE == "postgres" else None
        cursor = conn.cursor(cursor_factory=cursor_factory)

        # ✅ Convert table name to lowercase
        table_name = table_name.lower()

        # ✅ Query unprocessed rows safely using sql.Identifier
        query = sql.SQL("SELECT * FROM {} WHERE processed = FALSE").format(sql.Identifier(table_name))
        cursor.execute(query)

        rows = cursor.fetchall()

        if rows:
            logger.info(f"✅ {len(rows)} unprocessed rows found in '{table_name}'.")
        else:
            logger.info(f"🛑 No unprocessed rows found in '{table_name}'.")

        return rows

    except Exception as e:
        logger.error(f"❌ Error reading unprocessed rows from '{table_name}': {e}", exc_info=True)
        return []
    
    finally:
        if conn:
            release_db_connection(conn)


def read_unprocessed_rows_in_batch(table_name: str, batch_size: int) -> List[Dict[str, Any]]:
    """
    Read unprocessed rows from the specified table in batches.
    - Uses lowercase table names for consistency.
    """
    conn = get_db_connection()
    rows = []

    try:
        cursor_factory = psycopg2.extras.RealDictCursor if DB_TYPE == "postgres" else None
        cursor = conn.cursor(cursor_factory=cursor_factory)

        # ✅ Convert table name to lowercase
        table_name = table_name.lower()

        # ✅ Query unprocessed rows safely with batch size
        query = sql.SQL("SELECT * FROM {} WHERE processed = FALSE LIMIT %s").format(sql.Identifier(table_name))
        cursor.execute(query, (batch_size,))

        rows = cursor.fetchall()

        if rows:
            logger.info(f"✅ {len(rows)} rows retrieved in batch from '{table_name}'.")
        else:
            logger.info(f"🛑 No unprocessed rows in batch for '{table_name}'.")

        return rows

    except Exception as e:
        logger.error(f"❌ Error reading unprocessed rows in batch from '{table_name}': {e}", exc_info=True)
        return []
    
    finally:
        if conn:
            release_db_connection(conn)


def mark_row_as_processed(table_name: str, row_id: Any) -> bool:
    """
    Mark a row as processed in the database.
    - Converts table name to lowercase for consistency.
    """
    conn = get_db_connection()

    try:
        cursor = conn.cursor()

        # ✅ Convert table name to lowercase
        table_name = table_name.lower()

        # ✅ Update query for processed rows
        query = sql.SQL("UPDATE {} SET processed = TRUE WHERE id = %s").format(sql.Identifier(table_name))
        cursor.execute(query, (row_id,))

        conn.commit()
        logger.info(f"✅ Marked row ID {row_id} as processed in '{table_name}'.")
        return True

    except Exception as e:
        logger.error(f"❌ Error marking row ID {row_id} as processed in '{table_name}': {e}", exc_info=True)
        return False

    finally:
        if conn:
            release_db_connection(conn)


if __name__ == "__main__":
    """
    Test the database reading functionality.
    """
    print("Tables in database:", read_unprocessed_rows("fhir_queue"))
    print("Unprocessed rows in 'fhir_queue':", read_unprocessed_rows("fhir_queue"))
