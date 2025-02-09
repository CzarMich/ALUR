import unittest
from unittest.mock import patch
from utils.utils_central_processor import process_single_row
from utils.utils_resource import delete_row_from_db
from utils.utils_resource import send_fhir_resource


class TestFHIRResourceGeneration(unittest.TestCase):
    def setUp(self):
        # Define test data
        self.resource_name = "Condition"
        self.table_name = "condition"
        self.row = {
            "id": 1,
            "Composition_ID": "12345",
            "Condition_code": "A01",
            "Condition_Display": "Test Condition",
            "Condition_category_code": "diagnosis",
            "Condition_category": "Diagnosis",
        }
        self.mappings = {
            "resourceType": "Condition",
            "id": "{{Composition_ID}}",
            "identifier": [{"value": "{{Composition_ID}}"}],
            "code": {
                "coding": [
                    {"code": "{{Condition_code}}", "display": "{{Condition_Display}}"}
                ]
            },
            "category": {
                "coding": [
                    {"code": "{{Condition_category_code}}", "display": "{{Condition_category}}"}
                ]
            },
        }
        self.required_fields = ["Composition_ID", "Condition_code"]

    @patch("utils.utils_resource.send_fhir_resource")
    @patch("utils.utils_resource.delete_row_from_db")
    def test_process_single_row(self, mock_delete_row, mock_send_fhir_resource):
        """
        Test that a single row is processed, mapped, sent to the FHIR server,
        and the corresponding database row is deleted.
        """
        # Mock the FHIR resource sending and row deletion
        mock_send_fhir_resource.return_value = True

        # Call the function
        result = process_single_row(self.resource_name, self.table_name, self.row, self.mappings, self.required_fields)

        # Validate results
        self.assertTrue(result, "The row should be processed successfully.")

        # Assert the resource was sent to the FHIR server
        mock_send_fhir_resource.assert_called_once_with(
            self.resource_name,
            "12345",
            {
                "resourceType": "Condition",
                "id": "12345",
                "identifier": [{"value": "12345"}],
                "code": {"coding": [{"code": "A01", "display": "Test Condition"}]},
                "category": {"coding": [{"code": "diagnosis", "display": "Diagnosis"}]},
            },
        )

        # Assert the row was deleted from the database
        mock_delete_row.assert_called_once_with(self.table_name, self.row["id"])


if __name__ == "__main__":
    unittest.main()
