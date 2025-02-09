import logging
import os
import sys
from typing import Any, Dict, List, Optional
from datetime import datetime
from jinja2 import Template, TemplateSyntaxError, UndefinedError
from conf.config import RESOURCE_FILES  # ✅ Import resource.yml definitions

# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

logger = logging.getLogger("FHIRMapper")


def fix_fhir_datetime(date_str: str) -> Optional[str]:
    """Convert date strings to proper FHIR-compliant dateTime format."""
    try:
        if not date_str or date_str.lower() in ["none", "null"]:
            return None
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")  # UTC format
    except ValueError:
        logger.warning(f"⚠ Invalid datetime format: {date_str}")
        return None  # If invalid, return None


def validate_required_fields(resource_data: Dict[str, Any], required_fields: List[str]) -> bool:
    """Ensure all required fields are present in the resource data."""
    missing_fields = []
    for field in required_fields:
        if resource_data.get(field) in [None, "None", "null", "", {}]:
            missing_fields.append(field)

    if missing_fields:
        logger.warning(f"⚠ Missing required fields: {missing_fields}")
        return False
    return True


def clean_section(section: Any) -> Any:
    """Recursively clean a section by removing empty or invalid entries."""
    if isinstance(section, dict):
        cleaned = {}
        for k, v in section.items():
            cleaned_value = clean_section(v)

            # ✅ Remove empty or None fields
            if cleaned_value not in [None, "None", "null", "", {}, [], [{}]]:
                cleaned[k] = cleaned_value

        # ✅ Special case for extensions: Remove if `code` is missing
        if "extensions" in cleaned:
            cleaned["extensions"] = [
                ext for ext in cleaned["extensions"]
                if ext.get("valueCoding", {}).get("code") not in [None, "None", ""]
            ]
            if not cleaned["extensions"]:
                del cleaned["extensions"]

        return cleaned if cleaned else None

    elif isinstance(section, list):
        cleaned_list = [clean_section(item) for item in section if clean_section(item) not in [None, {}, [{}], "None", "null"]]
        return cleaned_list if cleaned_list else None

    return section if section not in [None, "None", "null", "", {}] else None


def resolve_value(template: Any, resource_data: Dict[str, Any]) -> Any:
    """Resolve a value from a mapping definition using Jinja2 templates."""
    try:
        if isinstance(template, str):
            return Template(template).render(resource_data) or None
        elif isinstance(template, dict):
            return {k: resolve_value(v, resource_data) for k, v in template.items()}
        elif isinstance(template, list):
            return [resolve_value(item, resource_data) for item in template]
        return template
    except (TemplateSyntaxError, UndefinedError) as e:
        logger.error(f"❌ Jinja2 Template Error: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error in resolve_value: {e}")
        return None


def enforce_field_order(resource_type: str, resource: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure resource fields follow the order in `resource.yml`."""
    if resource_type not in RESOURCE_FILES:
        logger.warning(f"⚠ No field order defined for {resource_type}. Returning as is.")
        return resource  # No changes if resource type is unknown

    ordered_resource = {}
    field_order = RESOURCE_FILES[resource_type].get("mappings", {}).keys()  # Get ordered fields from resource.yml

    for field in field_order:
        if field in resource:
            ordered_resource[field] = resource[field]

    return ordered_resource


def map_and_clean_resource(
    resource_data: Dict[str, Any],
    mappings: Dict[str, Any],
    required_fields: List[str]
) -> Dict[str, Any]:
    """
    Maps resource data to a FHIR resource using provided mappings.
    - Uses Jinja2 templates for value resolution.
    - Cleans the final resource by removing empty/null values.
    - Ensures all required fields exist before proceeding.
    - Converts date fields to FHIR `dateTime` format.
    - Enforces field order.
    """
    if not validate_required_fields(resource_data, required_fields):
        return {}

    logger.info("🚀 Mapping resource with provided mappings.")
    mapped_resource = {key: resolve_value(value, resource_data) for key, value in mappings.items()}

    # ✅ Convert all date fields to FHIR `dateTime` format
    date_fields = ["recordedDate", "onsetDateTime", "abatementDateTime", "effectiveDateTime"]
    for date_field in date_fields:
        if date_field in mapped_resource:
            original_date = mapped_resource[date_field]
            fixed_date = fix_fhir_datetime(original_date)
            if fixed_date:
                mapped_resource[date_field] = fixed_date
            else:
                logger.error(f"❌ Failed to format {date_field}: {original_date}")

    # 🔥 Clean mapped resource and remove empty fields
    cleaned_resource = clean_section(mapped_resource)

    # 🔥 Enforce field order from `resource.yml`
    ordered_resource = enforce_field_order(cleaned_resource.get("resourceType", ""), cleaned_resource)

    logger.debug(f"✅ Mapped and cleaned resource: {ordered_resource}")
    return ordered_resource
