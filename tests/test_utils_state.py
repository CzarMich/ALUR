from utils.utils_session import ehr_session, EHR_SERVER_URL
import logging

logger = logging.getLogger(__name__)

def query_openehr(aql_query):
    """
    Query the OpenEHR server using the provided AQL query.
    """
    query_url = f"{EHR_SERVER_URL}/rest/v1/query"

    payload = {"q": aql_query}
    logger.info(f"Sending OpenEHR Query: {payload}")

    # ✅ Print session headers to verify authentication
    logger.info(f"EHR Session Headers Before Request: {ehr_session.headers}")

    response = ehr_session.post(query_url, json=payload)

    if response.status_code == 401:
        logger.error("🔴 401 Unauthorized: Check authentication credentials.")
        logger.error(f"Response: {response.text}")
        raise Exception("Authentication failed. Please check OpenEHR credentials.")

    response.raise_for_status()
    return response.json()
