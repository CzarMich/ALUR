import os
import json
from datetime import datetime
from utils.utils_state import get_last_run_time, set_last_run_time, clear_last_run_time
from utils.utils import get_path
from conf.config import yaml_config


def setup_test_state_file():
    """Create a test state file with predefined values."""
    state_file = get_path('state_file')
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    state_data = {
        "Condition": "2025-01-01 10:00:00"
    }
    with open(state_file, 'w') as file:
        json.dump(state_data, file)


def cleanup_test_state_file():
    """Remove the test state file."""
    state_file = get_path('state_file')
    if os.path.exists(state_file):
        os.remove(state_file)


def test_utils_state(fetch_by_date_enabled=False, start_date="2025-01-08 00:00:00"):
    """Test utils_state functions with or without fetch_by_date enabled."""
    # Dynamically modify yaml_config
    yaml_config['fetch_by_date']['enabled'] = fetch_by_date_enabled
    yaml_config['fetch_by_date']['start_date'] = start_date

    setup_test_state_file()

    # Test: get_last_run_time
    resource = "Condition"
    if fetch_by_date_enabled:
        expected_time = start_date
    else:
        expected_time = "2025-01-01 10:00:00"

    last_run_time = get_last_run_time(resource)
    assert last_run_time == expected_time, f"Expected '{expected_time}', got {last_run_time}"
    print(f"get_last_run_time passed ✅ for {resource}: {last_run_time}")

    # Test: set_last_run_time
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    set_last_run_time("NewResource", now)

    # Ensure fetch_by_date is temporarily disabled for this part of the test
    yaml_config['fetch_by_date']['enabled'] = False
    updated_time = get_last_run_time("NewResource")
    assert updated_time == now, f"Expected '{now}', got {updated_time}"
    print(f"set_last_run_time passed ✅ for NewResource: {updated_time}")

    # Test: clear_last_run_time
    clear_last_run_time("Condition")
    cleared_time = get_last_run_time("Condition")
    assert cleared_time is None, f"Expected None, got {cleared_time}"
    print(f"clear_last_run_time passed ✅ for Condition")

    # Test: get_last_run_time when state.json is missing
    cleanup_test_state_file()
    missing_time = get_last_run_time("MissingResource")
    assert missing_time is None, f"Expected None, got {missing_time}"
    print(f"get_last_run_time passed ✅ for MissingResource with no state file")

    # Cleanup
    cleanup_test_state_file()
    print("All tests passed for utils_state.py! 🎉")


# Run tests for both scenarios
print("Testing utils_state without fetch_by_date...")
test_utils_state(fetch_by_date_enabled=False)

print("\nTesting utils_state with fetch_by_date enabled...")
test_utils_state(fetch_by_date_enabled=True, start_date="2025-01-08 00:00:00")
