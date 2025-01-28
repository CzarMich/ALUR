import unittest
from utils.utils_session import (
    create_session, get_db_connection, release_db_connection, close_connection_pool
)

class TestUtilsSessions(unittest.TestCase):

    def test_create_session_basic_auth(self):
        """Test session creation with basic authentication."""
        session = create_session(
            cache_name="test_cache",
            auth_method="basic",
            username="test_user",
            password="test_pass"
        )
        self.assertEqual(session.auth, ("test_user", "test_pass"))
        session.close()

    def test_create_session_bearer_auth(self):
        """Test session creation with bearer authentication."""
        session = create_session(
            cache_name="test_cache",
            auth_method="bearer",
            token="test_token"
        )
        self.assertEqual(session.headers.get("Authorization"), "Bearer test_token")
        session.close()

    def test_create_session_no_auth(self):
        """Test session creation with no authentication."""
        session = create_session(cache_name="test_cache", auth_method="none")
        self.assertIsNone(session.auth)
        session.close()

    def test_get_db_connection_sqlite(self):
        """Test SQLite database connection."""
        conn = get_db_connection()
        self.assertIsNotNone(conn)
        self.assertEqual(conn.__class__.__name__, "Connection")  # SQLite connection
        release_db_connection(conn)

    def test_postgres_connection(self):
        """Test PostgreSQL connection if DB_TYPE is postgres."""
        from conf.config import DB_TYPE
        if DB_TYPE == "postgres":
            conn = get_db_connection()
            self.assertIsNotNone(conn)
            self.assertEqual(conn.__class__.__name__, "connection")  # PostgreSQL connection
            release_db_connection(conn)

    def test_close_postgres_pool(self):
        """Test closing the PostgreSQL connection pool."""
        from conf.config import DB_TYPE
        if DB_TYPE == "postgres":
            try:
                close_connection_pool()
            except Exception as e:
                self.fail(f"close_connection_pool failed with error: {e}")

if __name__ == "__main__":
    unittest.main()
