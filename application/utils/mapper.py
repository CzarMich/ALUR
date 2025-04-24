import os
import sys
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from jinja2 import Template, TemplateSyntaxError, UndefinedError

# Ensure project root is in sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from conf.config import CONF_DIR, RESOURCE_FILES
from utils.logger import logger
BASE_MAPPING_DIR = os.path.join(CONF_DIR, "resources")


# ---------------------------
# Utility: Fix FHIR DateTime
# ---------------------------
def fix_fhir_datetime(date_str: str) -> Optional[str]:
    """Convert ISO timestamp to UTC FHIR dateTime format."""
    try:
        if not date_str or date_str.lower() in ["none", "null"]:
            return None
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        logger.warning(f"⚠ Invalid datetime format: {date_str}")
        return None


# ---------------------------
# Utility: Ensure Valid URIs
# ---------------------------
def ensure_valid_uri(system_value: Optional[str]) -> str:
    """Ensure system URIs are well-formed."""
    if not system_value or system_value.lower() in ["none", "null"]:
        return ""

    system_mappings = {
        "SNOMED Clinical Terms": "http://snomed.info/sct",
        "LOINC": "http://loinc.org",
        "RxNorm": "http://www.nlm.nih.gov/research/umls/rxnorm",
        "OPS": "http://fhir.de/CodeSystem/bfarm/ops",
        "ICD-10": "http://hl7.org/fhir/sid/icd-10",
        "ICD-10-GM": "http://fhir.de/CodeSystem/bfarm/icd-10-gm",
        "ATC": "http://www.whocc.no/atc",
        "UCUM": "http://unitsofmeasure.org",
    }

    if system_value in system_mappings:
        return system_mappings[system_value]

    if not system_value.startswith(("http://", "https://")):
        return f"http://{system_value}"

    return system_value


# ---------------------------
# Utility: Validate Required Fields
# ---------------------------
def validate_required_fields(resource_data: Dict[str, Any], required_fields: List[str]) -> bool:
    """Check if required fields exist and are non-empty."""
    missing = []
    for field in required_fields:
        value = resource_data.get(field)
        if isinstance(value, list):
            if not value or all(not bool(v) for v in value):
                missing.append(field)
        elif value in [None, "None", "null", "", {}]:
            missing.append(field)

    if missing:
        logger.warning(f"⚠ Missing required fields: {missing}")
        return False
    return True


# ---------------------------
# Utility: Clean Resource Fields
# ---------------------------
def clean_section(section: Any) -> Any:
    """Recursively clean a section by removing empty/null entries."""
    if isinstance(section, dict):
        cleaned = {
            k: clean_section(v)
            for k, v in section.items()
            if clean_section(v) not in [None, "", "None", "null", {}, [], [{}]]
        }
        return cleaned if cleaned else None
    elif isinstance(section, list):
        cleaned_list = [
            clean_section(item)
            for item in section
            if clean_section(item) not in [None, "", "None", "null", {}, [], [{}]]
        ]
        return cleaned_list if cleaned_list else None
    return section if section not in [None, "", "None", "null", {}] else None


# ---------------------------
# Utility: Resolve Jinja2 Template
# ---------------------------
def resolve_value(template: Any, resource_data: Dict[str, Any]) -> Any:
    """Resolve mapping templates using Jinja2."""
    try:
        if isinstance(template, str):
            return Template(template).render(resource_data) or None
        elif isinstance(template, dict):
            return {k: resolve_value(v, resource_data) for k, v in template.items()}
        elif isinstance(template, list):
            return [resolve_value(item, resource_data) for item in template]
        return template
    except (TemplateSyntaxError, UndefinedError) as e:
        logger.error(f"Jinja2 Template Error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in resolve_value: {e}")
        return None


# ---------------------------
# Utility: Fix Coding Systems
# ---------------------------
def fix_system_uris(resource: Dict[str, Any]) -> Dict[str, Any]:
    """Fix system URIs in coding fields."""
    def fix_coding_system(coding_list: List[Dict[str, Any]]):
        for coding in coding_list:
            if "system" in coding:
                coding["system"] = ensure_valid_uri(coding["system"])

    for key in ["code", "category", "reasonCode", "severity", "outcome", "statusReason"]:
        if key in resource:
            item = resource[key]
            if isinstance(item, dict) and "coding" in item:
                fix_coding_system(item["coding"])
            elif isinstance(item, list):
                for elem in item:
                    if isinstance(elem, dict) and "coding" in elem:
                        fix_coding_system(elem["coding"])

    return resource


# ---------------------------
# Utility: Enforce Field Order
# ---------------------------
def enforce_field_order(resource_type: str, resource: Dict[str, Any]) -> Dict[str, Any]:
    """Reorder fields in a FHIR resource based on YAML mappings order."""
    mappings = RESOURCE_FILES.get(resource_type, {}).get("mappings", {})
    if not mappings:
        return resource  # No mappings found, skip ordering

    ordered = {}
    for field in mappings.keys():  # Preserve YAML order
        if field in resource:
            ordered[field] = resource[field]

    # Preserve additional fields at the end
    for field in resource:
        if field not in ordered:
            ordered[field] = resource[field]

    return ordered



# ---------------------------
# MAIN: Map and Clean Resource
# ---------------------------
def map_and_clean_resource(
    resource_data: Dict[str, Any],
    mappings: Dict[str, Any],
    required_fields: List[str]
) -> Dict[str, Any]:
    """Map a single record to FHIR, clean, validate and return."""
    if not validate_required_fields(resource_data, required_fields):
        logger.warning("Skipping due to missing required fields.")
        return {}

    mapped = {key: resolve_value(value, resource_data) for key, value in mappings.items()}

    # Fix known FHIR date fields
    for date_field in ["recordedDate", "onsetDateTime", "abatementDateTime", "effectiveDateTime", "performedDateTime"]:
        if date_field in mapped:
            mapped[date_field] = fix_fhir_datetime(mapped[date_field])

    mapped = fix_system_uris(mapped)
    cleaned = clean_section(mapped)

    return enforce_field_order(cleaned.get("resourceType", ""), cleaned) if cleaned else {}
