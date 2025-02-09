import pytest
import json
import requests
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from utils.utils_openehr_query import query_resource
from utils.utils_state import get_last_run_time, set_last_run_time

# Sample OpenEHR API Response
SAMPLE_RESPONSE = {
    "resultSet": [
        {
            "TIMECommitted": "2025-01-30T12:00:00.000Z",
            "subjectID": "12345",
            "encounter_id": "enc-6789",
            "Condition_code": "A01",
            "Condition_Display": "Sample Condition"
        }
    ]
}

@pytest.fixture
def mock_requests_post():
    """ Mock the requests.post method to return a predefined response. """
    with patch("utils.utils_openehr_query.ehr_session.post") as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_RESPONSE
        mock_post.return_value = mock_response
        yield mock_post

@pytest.fixture
def mock_db_operations():
    """ Mock database operations to prevent actual inserts. """
    with patch("utils.utils_db.process_records") as mock_process_records, \
         patch("utils.utils_session.get_db_connection") as mock_get_db, \
         patch("utils.utils_session.release_db_connection") as mock_release_db:

        mock_process_records.return_value = None  # Mock DB insert as a no-op
        mock_get_db.return_value = MagicMock()    # Mock DB connection
        yield mock_process_records, mock_get_db, mock_release_db

@pytest.fixture
def mock_state_operations():
    """ Mock state operations to prevent file changes. """
    with patch("utils.utils_state.get_last_run_time") as mock_get_last, \
         patch("utils.utils_state.set_last_run_time") as mock_set_last:
        
        mock_get_last.return_value = "2025-01-29T00:00:00"
        mock_set_last.return_value = None
        yield mock_get_last, mock_set_last

def test_query_resource_success(mock_requests_post, mock_db_operations, mock_state_operations):
    """
    ✅ Test successful querying of OpenEHR and processing of results.
    """
    mock_process_records, _, _ = mock_db_operations
    _, mock_set_last_run_time = mock_state_operations

    # Call the function under test
    query_resource("Condition")

    # ✅ Ensure API was called
    mock_requests_post.assert_called_once()

    # ✅ Ensure records were processed
    mock_process_records.assert_called_once_with(
        records=SAMPLE_RESPONSE["resultSet"], 
        resource_type="Condition", 
        key=None  # Mocked key
    )

    # ✅ Ensure last run time was updated
    mock_set_last_run_time.assert_called_once_with(
        "Condition", datetime.now(timezone.utc).isoformat()
    )

def test_query_resource_no_results(mock_requests_post, mock_db_operations, mock_state_operations):
    """
    ✅ Test handling when OpenEHR API returns no results.
    """
    mock_requests_post.return_value.json.return_value = {"resultSet": []}

    # Call the function
    query_resource("Condition")

    # ✅ Ensure API was called
    mock_requests_post.assert_called_once()

    # ✅ Ensure process_records was NOT called
    mock_db_operations[0].assert_not_called()

def test_query_resource_api_error(mock_requests_post, mock_db_operations):
    """
    ✅ Test handling of API errors (HTTP 500).
    """
    mock_requests_post.return_value.status_code = 500
    mock_requests_post.return_value.text = "Internal Server Error"

    # Call function
    query_resource("Condition")

    # ✅ Ensure API was called
    mock_requests_post.assert_called_once()

    # ✅ Ensure process_records was NOT called
    mock_db_operations[0].assert_not_called()

def test_query_resource_state_update(mock_requests_post, mock_db_operations, mock_state_operations):
    """
    ✅ Test that last_run_time updates correctly.
    """
    mock_set_last_run_time = mock_state_operations[1]

    query_resource("Condition")

    # ✅ Ensure last_run_time is updated correctly
    mock_set_last_run_time.assert_called_once_with(
        "Condition", datetime.now(timezone.utc).isoformat()
    )
