import json
from conf.utils import get_db_path  # Import the function to get the path to the database

def map_condition(row):
    def clean_dict(d):
        """Recursively remove keys with None or empty values from a dictionary."""
        if isinstance(d, dict):
            cleaned = {k: clean_dict(v) for k, v in d.items() if v not in [None, '', [], {}]}
            return {k: v for k, v in cleaned.items() if v not in [None, '', [], {}]}
        elif isinstance(d, list):
            return [clean_dict(item) for item in d if item not in [None, '', [], {}]]
        else:
            return d

    def clean_code_section(resource):
        """Remove the entire 'code' field if it or its children are empty or if extensions lack 'code'."""
        code = resource.get('code')
        if isinstance(code, dict) and 'coding' in code:
            coding = code['coding']
            cleaned_coding = []
            for item in coding:
                if isinstance(item, dict):
                    extensions = item.get('extension', [])
                    # Clean each extension, removing those where 'valueCoding' or 'code' is None
                    cleaned_extensions = []
                    for ext in extensions:
                        value_coding = ext.get('valueCoding', {})
                        # Only keep extensions where 'valueCoding' and 'code' are not empty
                        if value_coding.get('code'):
                            cleaned_extensions.append({**ext, 'valueCoding': clean_dict(value_coding)})
                    # Only add coding items that have valid extensions
                    if cleaned_extensions:
                        cleaned_coding.append({**item, 'extension': cleaned_extensions})
            # Remove the 'code' section if no valid coding entries remain
            if not cleaned_coding:
                resource.pop('code', None)
            else:
                resource['code']['coding'] = cleaned_coding

    def clean_severity_section(resource):
        """Remove the entire 'severity' field if it or its children are empty."""
        severity = resource.get('severity')
        if isinstance(severity, dict) and 'coding' in severity:
            coding = severity['coding']
            cleaned_coding = [item for item in coding if item not in [None, '', {}, []]]
            if cleaned_coding:
                resource['severity']['coding'] = cleaned_coding
            else:
                resource.pop('severity', None)

    # Build the dictionary structure
    resource = {
        "resourceType": "Condition",
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
    
        "onsetDateTime": row.get('Feststellungsdatum')
        ,
        "recordedDate": row.get('Berichtsdatum')
    }

    # Clean up the dictionary to remove None or empty values
    cleaned_resource = clean_dict(resource)
    
    # Further clean up the 'code' and 'severity' sections if necessary
    clean_code_section(cleaned_resource)
    clean_severity_section(cleaned_resource)
    
    return cleaned_resource
