def map_test(row):
    # Initialize the base resource with required fields
    fhir_resource = {
        "resourceType": "Test",
        "id": row.get('EHRID')
    }

    # Add optional fields only if they have values
    if row.get('verificationStatusCode'):
        fhir_resource["verificationStatus"] = {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                    "code": row['verificationStatusCode']
                }
            ]
        }

    if row.get('SubjectID'):
        fhir_resource["subject"] = {
            "reference": f"Patient/{row['SubjectID']}"
        }

    if row.get('onsetStart') and row.get('onsetEnd'):
        fhir_resource["onsetPeriod"] = {
            "start": row['onsetStart'],
            "end": row['onsetEnd']
        }

    if row.get('recordedDate'):
        fhir_resource["recordedDate"] = row['recordedDate']
    
    # Return the resource, which will omit fields that are not present
    return fhir_resource
