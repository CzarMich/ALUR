import logging
from typing import Callable, Optional, List, Dict, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def clean_section_with_items(
    resource: dict,
    section_key: str,
    items_key: str,
    top_fields_required: Optional[List[str]] = None,
    extension_required_code: bool = False,
    filter_func: Optional[Callable[[Dict[str, Any]], bool]] = None
) -> None:
    """
    Clean a section of a resource that contains a list of items, such as 'coding' in 'code' or 'severity'.

    :param resource: The resource dictionary to clean.
    :param section_key: The key of the section to clean (e.g., 'code').
    :param items_key: The key of the list within the section (e.g., 'coding').
    :param top_fields_required: List of top-level fields required for each item to be kept.
    :param extension_required_code: If True, ensure extensions have valid 'valueCoding.code'.
    :param filter_func: Optional callable to apply custom filtering logic to each item.
    """
    logger.info(f"Cleaning section '{section_key}' with key '{items_key}' in resource.")
    section_block = resource.get(section_key)
    if not isinstance(section_block, dict):
        logger.warning(f"Section '{section_key}' is not a dictionary. Skipping.")
        return

    items_list = section_block.get(items_key)
    if not isinstance(items_list, list):
        logger.warning(f"Items in section '{section_key}' are not a list. Skipping.")
        return

    cleaned_items = []

    for item in items_list:
        if not isinstance(item, dict):
            continue

        # Apply custom filtering logic
        if filter_func and not filter_func(item):
            continue

        # Handle extensions if needed
        if extension_required_code and 'extension' in item and isinstance(item['extension'], list):
            item['extension'] = [
                ext for ext in item['extension']
                if ext.get('valueCoding', {}).get('code')
            ] or None

        # Check top-level fields for validity
        keep_item = any(item.get(field) for field in (top_fields_required or []))

        # Keep the item if extensions are valid or top fields are valid
        if keep_item or (item.get('extension')):
            cleaned_items.append(item)

    if cleaned_items:
        section_block[items_key] = cleaned_items
        logger.info(f"Section '{section_key}' cleaned. Retained {len(cleaned_items)} items.")
    else:
        resource.pop(section_key, None)
        logger.info(f"Section '{section_key}' removed from resource due to no valid items.")

def clean_section_with_list(
    resource: dict,
    section_key: str,
    top_fields_required: Optional[List[str]] = None
) -> None:
    """
    Clean a section of a resource that is a list, such as 'identifier'.

    :param resource: The resource dictionary to clean.
    :param section_key: The key of the section to clean (e.g., 'identifier').
    :param top_fields_required: List of top-level fields required for each item to be kept.
    """
    logger.info(f"Cleaning section '{section_key}' in resource.")
    items_list = resource.get(section_key)
    if not isinstance(items_list, list):
        logger.warning(f"Section '{section_key}' is not a list. Skipping.")
        return

    cleaned_items = [
        item for item in items_list
        if any(item.get(field) for field in (top_fields_required or []))
    ]

    if cleaned_items:
        resource[section_key] = cleaned_items
        logger.info(f"Section '{section_key}' cleaned. Retained {len(cleaned_items)} items.")
    else:
        resource.pop(section_key, None)
        logger.info(f"Section '{section_key}' removed from resource due to no valid items.")

def clean_extensions(extensions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean a list of extensions recursively to ensure validity.

    :param extensions: The list of extensions to clean.
    :return: A cleaned list of extensions.
    """
    cleaned_extensions = []
    for ext in extensions:
        if isinstance(ext, dict) and ext.get("valueCoding", {}).get("code"):
            cleaned_extensions.append(ext)
        elif isinstance(ext, dict) and "nestedExtension" in ext:
            ext["nestedExtension"] = clean_extensions(ext["nestedExtension"])
            if ext["nestedExtension"]:
                cleaned_extensions.append(ext)
    return cleaned_extensions

def map_resource(resource_data: Dict[str, Any], mappings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply generic mapping logic to resource data.

    :param resource_data: Raw resource data to map.
    :param mappings: A dictionary defining the mapping rules.
    :return: A mapped resource.
    """
    logger.info("Mapping resource with provided mappings.")
    mapped_resource = {}
    for key, value in mappings.items():
        if isinstance(value, str):  # Direct mapping
            mapped_resource[key] = resource_data.get(value)
        elif callable(value):  # Custom function for mapping
            mapped_resource[key] = value(resource_data)
        elif isinstance(value, dict):  # Nested mapping
            mapped_resource[key] = map_resource(resource_data, value)
    return mapped_resource
