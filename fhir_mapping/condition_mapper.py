import logging
from typing import Dict, Any

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def clean_section_with_items(
    resource: dict,
    section_key: str,
    items_key: str,
    top_fields_required: list = None,
    extension_required_code: bool = False
) -> None:
    """
    Clean a section of a resource that contains a list of items, such as 'coding' in 'code' or 'severity'.
    """
    section_block = resource.get(section_key)
    if not isinstance(section_block, dict):
        return

    items_list = section_block.get(items_key)
    if not isinstance(items_list, list):
        return

    cleaned_items = []

    for item in items_list:
        if not isinstance(item, dict):
            continue

        # Handle extensions if needed
        if extension_required_code and 'extension' in item and isinstance(item['extension'], list):
            exts = [
                ext for ext in item['extension']
                if ext.get('valueCoding', {}).get('code')
            ]
            item['extension'] = exts if exts else None

        # Check top-level fields for validity
        keep_item = any(item.get(field) for field in (top_fields_required or []))

        # Keep the item if extensions are valid or top fields are valid
        if keep_item or (item.get('extension')):
            cleaned_items.append(item)

    # Update or remove the section based on cleaned items
    if cleaned_items:
        section_block[items_key] = cleaned_items
    else:
        resource.pop(section_key, None)

def clean_section_with_list(resource: dict, section_key: str, top_fields_required: list = None) -> None:
    """
    Clean a section of a resource that is a list, such as 'identifier'.
    """
    items_list = resource.get(section_key)
    if not isinstance(items_list, list):
        return

    cleaned_items = [
        item for item in items_list
        if any(item.get(field) for field in (top_fields_required or []))
    ]

    if cleaned_items:
        resource[section_key] = cleaned_items
    else:
        resource.pop(section_key, None)

def map_condition(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a row from the Condition table to a FHIR Condition resource.
    """
    resource = {
        "resourceType": "Condition",
            "note": [
        {
            "text": row.get('BerichtID')
        }
    ],
        "id": row.get('BerichtID'),
        "meta": {
            "profile": [
                "https://www.medizininformatik-initiative.de/fhir/core/modul-diagnose/StructureDefinition/Diagnose"
            ]
        },
        "clinicalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                    "code": "active"
                }
            ]
        },
        "verificationStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                    "code": "confirmed"
                }
            ]
        },
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                        "code": row.get('Kategorie_der_Diagnose_Code'),
                        "display": row.get('Kategorie_der_Diagnose')
                    }
                ]
            }
        ],
        "severity": {
            "coding": [
                {
                    "system": row.get('Schweregrad_terminology_id'),
                    "code": row.get('Schweregrad_Code'),
                    "display": row.get('Schweregrad')
                }
            ]
        },
        "code": {
            "coding": [
                {
                    "extension": [
                        {
                            "url": "http://fhir.de/StructureDefinition/icd-10-gm-manifestationscode",
                            "valueCoding": {
                                "system": "http://fhir.de/CodeSystem/dimdi/icd-10-gm",
                                "code": row.get('SekondaryDiagnosis_CodeString'),
                                "display": row.get('SekondaryDiagnosis_Display')
                            }
                        },
                        {
                            "url": "http://fhir.de/StructureDefinition/icd-10-gm-diagnosesicherheit",
                            "valueCoding": {
                                "system": "https://fhir.kbv.de/CodeSystem/KBV_CS_SFHIR_ICD_DIAGNOSESICHERHEIT",
                                "code": row.get('PrimärcodeDiagnosesicherheit_Code'),
                                "display": row.get('PrimärcodeDiagnosesicherheit')
                            }
                        },
                        {
                            "url": "http://fhir.de/StructureDefinition/seitenlokalisation",
                            "valueCoding": {
                                "system": row.get('Seitenlokalisation_terminology_id'),
                                "code": row.get('Seitenlokalisation_code_string'),
                                "display": row.get('Seitenlokalisation_Value')
                            }
                        }
                    ],
                    "system": row.get('Kodierte_Diagnose_terminology_id'),
                    "code": row.get('Kodierte_Diagnose_code_string'),
                    "display": row.get('Kodierte_Diagnose')
                }
            ]
        },
        "subject": {
            "identifier": {
                "system": "urn:UKSH.ext.ReferenceID",
                "value": row.get('SubjectID')
            }
        },
        "encounter": {
            "identifier": {
                "system": "urn:UKSH.ext.Fallnummer",
                "value": row.get('Fall_Kennung')
            }
        },
        "onsetDateTime": row.get('Feststellungsdatum'),
        "recordedDate": row.get('Berichtsdatum')
    }

    # Clean up sections dynamically
    clean_section_with_items(resource, 'code', 'coding', ["system", "code", "display"], extension_required_code=True)
    clean_section_with_items(resource, 'severity', 'coding', ["system", "code", "display"])
    clean_section_with_list(resource, 'identifier', ["value"])

    return resource
