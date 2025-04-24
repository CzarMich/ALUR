import time
import os
import sys
import requests

# Add project root to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from conf.config import (
    EHR_SERVER_URL,
    FHIR_SERVER_URL,
    HEALTH_CHECK_RETRY_INTERVAL,
    EHR_AUTH_METHOD,
    EHR_SERVER_USER,
    EHR_SERVER_PASSWORD
)
from utils.logger import logger, verbose
from utils.session import create_session

# ------------------------------------
# Shared Heartbeat Logic (Sync)
# ------------------------------------
def server_heartbeat_check(
    url: str,
    label: str,
    method: str = "OPTIONS",
    expected_statuses=(200, 204)
) -> bool:
    """
    Authenticated heartbeat check using requests and proper headers.
    Retries until success or timeout.
    """
    attempt = 0
    session = create_session(
        cache_name="heartbeat_cache",
        auth_method=EHR_AUTH_METHOD,
        username=EHR_SERVER_USER,
        password=EHR_SERVER_PASSWORD,
    )
    session.cache.clear()  # Clear stale results

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }

    while True:
        attempt += 1
        try:
            verbose(f"[Heartbeat] 🔍 Checking {label} at {url} (Attempt {attempt})")
            response = session.request(method=method, url=url, headers=headers, timeout=10)
            if response.status_code in expected_statuses:
                verbose(f"[Heartbeat] {label} is online.")
                return True
            else:
                logger.warning(f"[Heartbeat] ⚠ Unexpected response from {label}: {response.status_code} - {response.text[:120]}")
        except requests.exceptions.RequestException as e:
            logger.error(f"[Heartbeat] {label} unreachable: {e}")
        except Exception as e:
            logger.error(f"[Heartbeat] Unexpected error while checking {label}: {e}", exc_info=True)

        verbose(f"[Heartbeat] ⏳ Retrying {label} in {HEALTH_CHECK_RETRY_INTERVAL} seconds...")
        time.sleep(HEALTH_CHECK_RETRY_INTERVAL)

# ------------------------------------
# Specific Server Check Wrappers
# ------------------------------------
def ehr_server_heartbeat_check() -> bool:
    """Sync health check for the EHR server."""
    ehr_health_url = f"{EHR_SERVER_URL}/rest/v1/template"
    return server_heartbeat_check(ehr_health_url, label="EHR Server")

def fhir_server_heartbeat_check() -> bool:
    """Sync health check for the FHIR server using GET /metadata."""
    fhir_health_url = f"{FHIR_SERVER_URL}/metadata"
    return server_heartbeat_check(
        fhir_health_url,
        label="FHIR Server",
        method="GET",
        expected_statuses=(200,)
    )

# ------------------------------------
# Combined Startup Check (Optional)
# ------------------------------------
def heartbeat_check_all_services():
    """Run EHR and FHIR server health checks sequentially."""
    verbose("Starting health checks for EHR and FHIR servers...")
    ehr_server_heartbeat_check()
    fhir_server_heartbeat_check()
    verbose("All servers are reachable.")
