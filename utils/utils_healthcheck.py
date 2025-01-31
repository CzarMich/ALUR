import time
import logging
import requests
from conf.config import EHR_SERVER_URL, HEALTH_CHECK_ENABLED, HEALTH_CHECK_RETRY_INTERVAL, HEALTH_CHECK_MAX_RETRIES

logger = logging.getLogger(__name__)

def server_heartbeat_check():
    """
    Check if the EHR server is online before making queries.
    Uses the OpenEHR OPTIONS request to test connectivity.
    Retries until the server is reachable if enabled.
    """
    if not HEALTH_CHECK_ENABLED:
        logger.info("🔹 Server health check is disabled. Proceeding with queries.")
        return True  # Skip check if health check is disabled

    health_check_url = f"{EHR_SERVER_URL}/rest/v1/template"  # ✅ OpenEHR health check endpoint
    retries = 0

    while True:
        try:
            logger.info(f"🔍 Checking EHR Server Health: {health_check_url} (Attempt {retries+1})")
            response = requests.options(health_check_url, timeout=10)

            if response.status_code == 200:
                logger.info("✅ EHR Server is online. Proceeding with data fetch.")
                return True

            logger.warning(f"⚠ Unexpected response from EHR Server: {response.status_code} - {response.text}")

        except requests.RequestException as e:
            logger.error(f"❌ Server health check failed: {e}")

        retries += 1

        if HEALTH_CHECK_MAX_RETRIES and retries >= HEALTH_CHECK_MAX_RETRIES:
            logger.error(f"🔴 Maximum retries reached ({HEALTH_CHECK_MAX_RETRIES}). Server unreachable.")
            return False  # Stop retrying

        logger.info(f"⏳ Retrying in {HEALTH_CHECK_RETRY_INTERVAL} seconds...")
        time.sleep(HEALTH_CHECK_RETRY_INTERVAL)
