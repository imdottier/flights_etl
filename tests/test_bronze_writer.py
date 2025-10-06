import pytest
from unittest.mock import patch, mock_open

import json

# Import the function we want to test
from utils.io_utils import write_json_to_bronze
import os
from pathlib import Path

base = Path(os.getenv("WORKSPACE_BASE", "./"))
BRONZE_RAW_BASE = base / "bronze_raw"

# --- Test Case 1: The Happy Path ---
# We use @patch to replace real functions with mocks during the test
@patch("pathlib.Path.mkdir")
@patch("builtins.open", new_callable=mock_open)
@patch("json.dump")
def test_write_json_to_bronze_success(mock_json_dump, mock_file_open, mock_mkdir):
    """
    Tests that the function correctly calls mkdir, open, and json.dump
    without actually touching the filesystem.
    """
    # 1. Arrange: Set up our test data
    test_data = {"key": "value", "flights": [1, 2, 3]}
    test_path = "flights/ingestion_hour=2025-09-22-14/airport=LHR"
    test_filename = "payload.json"
    expected_full_path = BRONZE_RAW_BASE / test_path
    expected_file_path = expected_full_path / test_filename

    # 2. Act: Call the function we are testing
    write_json_to_bronze(test_data, test_path, test_filename)

    # 3. Assert: Check if the mocks were called as we expected
    # Did it try to create the directory?
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    # Did it try to open the correct file in write mode?
    mock_file_open.assert_called_once_with(expected_file_path, "w")

    # Did it try to dump the correct data into the file handle?
    mock_json_dump.assert_called_once_with(test_data, mock_file_open(), indent=2)


# --- Test Case 2: The Sad Path (File System Error) ---
@patch("pathlib.Path.mkdir")
@patch("builtins.open", new_callable=mock_open)
def test_write_json_to_bronze_raises_io_error(mock_file_open, mock_mkdir):
    """
    Tests that the function correctly catches and re-raises an IOError
    if the file cannot be written.
    """
    # 1. Arrange: Configure the mock to simulate an error
    # When open() is called, it will raise an IOError
    mock_file_open.side_effect = IOError("Permission denied")

    # 2. Act & Assert: Use pytest.raises to confirm that an exception is thrown
    with pytest.raises(IOError, match="Permission denied"):
        write_json_to_bronze({"data": "test"}, "some/path", "file.json")

    # We can also assert that mkdir was still called before the error
    mock_mkdir.assert_called_once()