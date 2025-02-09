from utils.utils import load_resource_config, generate_date_range, get_required_fields, is_fetch_by_date_enabled

# Test the function
try:
    condition_config = load_resource_config('Condition')
    print("Condition Config Loaded Successfully:")
    print(condition_config)
except Exception as e:
    print(f"Error loading Condition Config: {e}")


def test_utils():
    print("Testing utils.py...")

    # Test resource config loading
    try:
        resource_config = load_resource_config("Condition")
        assert "query_template" in resource_config, "Query template missing in resource config!"
        assert "mappings" in resource_config, "Mappings missing in resource config!"
        print("load_resource_config passed ✅")
    except Exception as e:
        print(f"load_resource_config failed ❌: {e}")

    # Test date range generation
    try:
        dates = list(generate_date_range("2025-01-01", "2025-01-05"))
        assert len(dates) == 5, "Date range generation failed!"
        print(f"generate_date_range passed ✅: {dates}")
    except Exception as e:
        print(f"generate_date_range failed ❌: {e}")

    # Test required fields
    try:
        fields = get_required_fields("Condition")
        print(f"get_required_fields passed ✅: {fields}")
    except Exception as e:
        print(f"get_required_fields failed ❌: {e}")

    # Test fetch by date
    print(f"is_fetch_by_date_enabled: {is_fetch_by_date_enabled()} ✅")

if __name__ == "__main__":
    test_utils()
