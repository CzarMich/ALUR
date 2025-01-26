import logging
from typing import Dict, Any
from conf.utils_mapping import clean_section_with_items, clean_section_with_list  # Reuse utility functions

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def map_condition(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map a row from the Condition table to a FHIR Condition resource.
    """
    try:
        # Construct the FHIR Condition resource
        resource = {
            "resourceType": "Condition",
            "note": [{"text": row.get('BerichtID')}],
            "identifier": [
                {
                    "system": "http://USKH.ext.DiagnoseID",
                    "value": row.get('CompositionID')
                }
            ],
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

        logger.info(f"Successfully mapped Condition resource: {resource['identifier'][0]['value']}")
        return resource

    except Exception as e:
        logger.error(f"Error mapping row to Condition resource: {e}")
        raise
