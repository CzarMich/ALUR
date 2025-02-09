import unittest
from utils.utils_mapper import map_and_clean_resource


class TestUtilsMapper(unittest.TestCase):
    def setUp(self):
        self.resource_data = {
            "Composition_ID": "12345",
            "Condition_code": "A01",
            "Condition_Display": "Test Condition",
            "Condition_category": "Diagnosis",
        }

        self.mappings = {
            "identifier": [{"value": "{{Composition_ID}}"}],
            "code": {
                "coding": [
                    {"code": "{{Condition_code}}", "display": "{{Condition_Display}}"}
                ]
            },
            "category": {"text": "{{Condition_category}}"},
        }

        self.required_fields = ["Composition_ID", "Condition_code"]

    def test_valid_mapping(self):
        """
        Test a valid mapping where all required fields exist.
        """
        result = map_and_clean_resource(self.resource_data, self.mappings, self.required_fields)
        self.assertEqual(result["identifier"][0]["value"], "12345")
        self.assertEqual(result["code"]["coding"][0]["code"], "A01")
        self.assertEqual(result["code"]["coding"][0]["display"], "Test Condition")
        self.assertEqual(result["category"]["text"], "Diagnosis")

    def test_missing_required_fields(self):
        """
        Test when required fields are missing in the resource data.
        """
        incomplete_data = {
            "Condition_code": "A01",
            "Condition_Display": "Test Condition",
        }
        result = map_and_clean_resource(incomplete_data, self.mappings, self.required_fields)
        self.assertEqual(result, {}, "Result should be empty when required fields are missing.")

    def test_clean_empty_sections(self):
        """
        Test that incomplete sections are removed during mapping.
        """
        self.resource_data["Condition_code"] = ""
        result = map_and_clean_resource(self.resource_data, self.mappings, self.required_fields)
        self.assertNotIn("code", result, "Code section should be omitted if incomplete.")

    def test_resolve_template_errors(self):
        """
        Test behavior when mappings contain non-existent fields.
        """
        faulty_mappings = {
            "identifier": [{"value": "{{ NonExistentField }}"}],
        }
        result = map_and_clean_resource(self.resource_data, faulty_mappings, self.required_fields)

        # Identifier value will be None if the field doesn't exist
        self.assertIsNone(
            result.get("identifier", [{}])[0].get("value"),
            "Identifier should be None for missing fields."
        )

    def test_nested_cleaning(self):
        """
        Test cleaning of nested sections within the resource.
        """
        nested_data = {
            "Composition_ID": "12345",
            "nested_section": {
                "key1": "",
                "key2": {"subkey": "value"},
                "key3": None,
            },
        }
        nested_mappings = {
            "identifier": [{"value": "{{Composition_ID}}"}],
            "nested_section": {
                "key1": "{{nested_section.key1}}",
                "key2": "{{nested_section.key2.subkey}}",
            },
        }
        result = map_and_clean_resource(nested_data, nested_mappings, ["Composition_ID"])
        self.assertIn("nested_section", result)
        self.assertNotIn("key1", result["nested_section"], "Empty fields should be omitted.")
        self.assertIn("key2", result["nested_section"])


if __name__ == "__main__":
    unittest.main()
