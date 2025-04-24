import os
import sys
import requests
from datetime import datetime, timedelta

# Setup path to project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from utils.session import create_session
from conf.config import (
    EHR_SERVER_URL,
    EHR_AUTH_METHOD,
    EHR_SERVER_USER,
    EHR_SERVER_PASSWORD,
    FETCH_START_DATE,
    FETCH_INTERVAL_HOURS,
    FETCH_END_DATE
)
from utils.logger import logger

RESOURCE_NAME = "diagnose"

# ✅ AQL Template (fixed and trimmed to LIMIT 3)
AQL_TEMPLATE = """
SELECT
    c/uid/value AS composition_id, 
    e/ehr_status/subject/external_ref/id/value AS subject_id,
    c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.case_identification.v0]/items[at0001]/value/value AS encounter_id,
    c/content[openEHR-EHR-EVALUATION.problem_diagnosis.v1]/data[at0001]/items[openEHR-EHR-CLUSTER.problem_qualifier.v2]/items[at0004]/value/value AS condition_category,
    c/content[openEHR-EHR-EVALUATION.problem_diagnosis.v1]/data[at0001]/items[openEHR-EHR-CLUSTER.problem_qualifier.v2]/items[at0004]/value/defining_code/code_string AS condition_category_code,
    c/content[openEHR-EHR-EVALUATION.problem_diagnosis.v1]/data[at0001]/items[at0002]/value/defining_code/code_string AS condition_code
FROM EHR e
CONTAINS COMPOSITION c
WHERE 
    c/name/value = 'Diagnose' AND
    c/context/start_time/value >= '{{last_run_time}}' AND
    c/context/start_time/value < '{{end_run_time}}'
ORDER BY c/context/start_time/value ASC
LIMIT 3
"""

def construct_test_aql(start_date: str, end_date: str) -> str:
    aql = AQL_TEMPLATE
    aql = aql.replace("{{last_run_time}}", start_date)
    aql = aql.replace("{{end_run_time}}", end_date)
    return " ".join(aql.strip().split())

def test_query_resource():
    # Setup time window
    start_date = FETCH_START_DATE
    end_date = (
        datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S") +
        timedelta(hours=FETCH_INTERVAL_HOURS)
    ).strftime("%Y-%m-%dT%H:%M:%S")

    if FETCH_END_DATE and end_date > FETCH_END_DATE:
        end_date = FETCH_END_DATE

    aql_query = construct_test_aql(start_date, end_date)
    payload = {"aql": aql_query}

    logger.info(f"📤 Sending test AQL to EHR: {start_date} → {end_date}")
    logger.debug(f"🧪 Payload:\n{payload['aql']}")

    session = create_session(
        cache_name="test_cache",
        auth_method=EHR_AUTH_METHOD,
        username=EHR_SERVER_USER,
        password=EHR_SERVER_PASSWORD
    )

    try:
        url = f"{EHR_SERVER_URL}/rest/v1/query"
        response = session.post(url, json=payload)
        logger.info(f"📥 Status Code: {response.status_code}")
        print("📄 Response Body:\n", response.text)
    except Exception as e:
        logger.error(f"❌ Exception during AQL fetch test: {e}", exc_info=True)

if __name__ == "__main__":
    test_query_resource()
