import logging
from typing import Any, Dict, List, Union, Optional, Callable
from jinja2 import Template
import os
import sys
# Ensure the project root is in Python's module search path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Add the project root to Python's module search path
sys.path.insert(0, BASE_DIR)

logger = logging.getLogger("utils.utils_mapper")
logger.setLevel(logging.INFO)


def validate_required_fields(resource_data: Dict[str, Any], required_fields: List[str]) -> bool:
    """
    Validate that all required fields are present in the resource data.
    """
    missing_fields = []
    for field in required_fields:
        # Check nested fields like 'code.coding[0].code'
        parts = field.split(".")
        value = resource_data
        for part in parts:
            if isinstance(value, list):
                try:
                    index = int(part.strip("[]"))
                    value = value[index] if len(value) > index else None
                except ValueError:
                    value = None
            elif isinstance(value, dict):
                value = value.get(part)
            else:
                value = None
            if value is None:
                break
        if not value:
            missing_fields.append(field)

    if missing_fields:
        logger.error(f"Missing required fields: {missing_fields}")
        return False
    return True


def clean_section_with_items(
    section: Dict[str, Any],
    items_key: str,
    required_fields: Optional[List[str]] = None,
    filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> Union[Dict[str, Any], None]:
    """
    Clean a section containing a list of items (e.g., `coding` in `code`).
    """
    if not section or not isinstance(section, dict):
        return None

    items = section.get(items_key, [])
    if not isinstance(items, list):
        return None

    cleaned_items = []
    for item in items:
        if not isinstance(item, dict):
            continue

        if filter_func and not filter_func(item):
            continue

        # Validate fields
        if required_fields and not any(item.get(field) for field in required_fields):
            continue

        cleaned_items.append(item)

    if cleaned_items:
        section[items_key] = cleaned_items
        return section
    return None


def clean_section(section: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Union[Dict[str, Any], None]:
    """
    Recursively clean a section by removing empty or invalid entries.
    """
    if isinstance(section, dict):
        cleaned_section = {key: clean_section(value) for key, value in section.items()}
        return {k: v for k, v in cleaned_section.items() if v} or None
    elif isinstance(section, list):
        return [clean_section(item) for item in section if clean_section(item)] or None
    return section if section else None


def map_and_clean_resource(
    resource_data: Dict[str, Any],
    mappings: Dict[str, Any],
    required_fields: List[str]
) -> Dict[str, Any]:
    """
    Map and clean a resource based on mappings and required fields.
    """
    if not validate_required_fields(resource_data, required_fields):
        return {}

    def resolve_value(template: Any) -> Any:
        """
        Resolve a value from a mapping definition using Jinja2 templates.
        """
        try:
            if isinstance(template, str):
                return Template(template).render(resource_data) or None
            elif isinstance(template, dict):
                return {k: resolve_value(v) for k, v in template.items()}
            elif isinstance(template, list):
                return [resolve_value(item) for item in template]
            return template
        except Exception as e:
            logger.error(f"Error resolving template: {e}")
            return None

    logger.info("Mapping resource with provided mappings.")
    mapped_resource = {key: resolve_value(value) for key, value in mappings.items()}

    # Clean mapped resource
    cleaned_resource = clean_section(mapped_resource) or {}
    logger.debug(f"Mapped and cleaned resource: {cleaned_resource}")
    return {k: v for k, v in cleaned_resource.items() if v is not None}
