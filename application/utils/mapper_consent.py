import json
import os
import sys
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from collections import defaultdict

# Project root setup
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from conf.config import RESOURCE_FILES
from utils.logger import logger, verbose
from utils.mapper import resolve_value, fix_system_uris, clean_section

def fix_fhir_datetime(date_str: str) -> Optional[str]:
    """Convert a datetime string to FHIR-compliant UTC format."""
    try:
        if not date_str or date_str.lower() in ["none", "null"]:
            return None
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            logger.warning(f"Invalid datetime format: {date_str}")
            return None
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def group_provisions(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Group Consent records by composition_id and build provision_list."""
    group_by = RESOURCE_FILES.get("Consent", {}).get("group_by", "composition_id")
    grouped = defaultdict(list)

    for record in records:
        key = record.get(group_by, "").strip()
        if not key:
            logger.warning(f"⚠ Skipping record with missing {group_by}: {record}")
            continue
        grouped[key].append(record)

    merged = []
    for group_key, group in grouped.items():
        base = {group_by: group_key, "provision_list": []}

        # Copy all non-provision fields from the first record
        for k, v in group[0].items():
            if k not in ("provision_type", "consent_code", "consent_code_system",
                         "start_time", "end_time", "consent", "uri_einwilligungsnachweis"):
                base[k] = v

        for item in group:
            provision = {
                "type": item.get("provision_type"),
                "period": {
                    "start": fix_fhir_datetime(item.get("start_time"))
                },
                "code": {
                    "coding": [{
                        #"system": item.get("consent_code_system"),
                        "system": "https://www.medizininformatik-initiative.de/fhir/modul-consent/CodeSystem/mii-cs-consent-consent_code",
                        "code": item.get("consent_code"),
                        "display": item.get("consent")
                    }]
                },
                "sourceAttachment": {
                    "url": item.get("uri_einwilligungsnachweis")
                }
            }

            end = fix_fhir_datetime(item.get("end_time"))
            if end:
                provision["period"]["end"] = end

            base["provision_list"].append(provision)

        verbose(f"Built provision_list for group {group_key}:")
        verbose(json.dumps(base["provision_list"], indent=2, ensure_ascii=False))
        merged.append(base)

    #logger.info(f"✅ Grouped into {len(merged)} Consent resources.")
    return merged

def map_consent_resources(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform grouped consent data into FHIR Consent resources."""
    config = RESOURCE_FILES.get("Consent", {})
    template = config.get("mappings")
    required_fields = config.get("required_fields", [])
    group_by = config.get("group_by", "composition_id")

    if not template:
        logger.error("❌ No mapping template found for Consent.")
        return []

    grouped_records = group_provisions(records)
    output = []

    for data in grouped_records:
        if "consent_type" in data:
            data["consent_type"] = data["consent_type"].strip().lower().replace(" ", "-")

        provision_list = data.get("provision_list", [])

        # Prepare outer provision wrapper
        top_provision = {
            "type": data.get("provision_type", "permit"),
            "provision": provision_list
        }

        start = fix_fhir_datetime(data.get("start_time"))
        if start:
            top_provision["period"] = {"start": start}
            end = fix_fhir_datetime(data.get("end_time"))
            if end:
                top_provision["period"]["end"] = end

        data["provision"] = json.dumps(top_provision, ensure_ascii=False)

        missing = [f for f in required_fields if not data.get(f)]
        if missing:
            logger.warning(f"Missing required fields: {missing} for {data.get(group_by)}")
            continue

        resource = resolve_value(template, data)

        #logger.info(f"Rendered FHIR Consent (raw) for {data.get(group_by)}:")
        #logger.info(json.dumps(resource, indent=2, ensure_ascii=False))

        if "dateTime" in resource:
            resource["dateTime"] = fix_fhir_datetime(resource["dateTime"])

        if "provision" in resource:
            try:
                resource["provision"] = json.loads(resource["provision"])
            except Exception as e:
                logger.error(f"❌ Failed to parse provision JSON: {e}")

        resource = fix_system_uris(resource)
        resource = clean_section(resource)

        if not resource:
            logger.warning(f"⚠ Empty FHIR Consent after cleaning for {data.get(group_by)}")
            continue

        verbose(f"✅ Final FHIR Consent resource for {data.get(group_by)}:")
        verbose(json.dumps(resource, indent=2, ensure_ascii=False))

        output.append(resource)

    return output
