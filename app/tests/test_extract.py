import json
import os
from unittest.mock import MagicMock, mock_open, patch

import pytest

from app.etl.extract import DATA_FOLDER, drop_data, extract_data, fetch_data

"""
Explanation of @pytest.fixture:

The @pytest.fixture decorator is used to define a fixture function in pytest. Fixtures are a way to provide a fixed baseline upon which tests can reliably and repeatedly execute. 
They are used to set up some context for the tests, such as creating mock objects, preparing test data, or configuring the environment. 
Fixtures are defined using functions, and they can return values that are then injected into test functions that depend on them.
"""
@pytest.fixture
def mock_get():
    with patch("app.etl.extract.requests.get") as mock:
        yield mock


@pytest.fixture
def mock_makedirs():
    with patch("app.etl.extract.os.makedirs") as mock:
        yield mock


@pytest.fixture
def mock_open_fixture():
    with patch("builtins.open", new_callable=mock_open) as mock:
        yield mock


@pytest.fixture
def mock_sleep():
    with patch("app.etl.extract.time.sleep") as mock:
        yield mock


@pytest.fixture
def mock_scandir():
    with patch("app.etl.extract.os.scandir") as mock:
        yield mock


@pytest.fixture
def mock_rmtree():
    with patch("app.etl.extract.shutil.rmtree") as mock:
        yield mock


@pytest.fixture
def mock_remove():
    with patch("app.etl.extract.os.remove") as mock:
        yield mock


@pytest.fixture
def mock_fetch_data():
    with patch("app.etl.extract.fetch_data") as mock:
        yield mock


@pytest.fixture
def mock_file_data():
    return {
        "competitions": [
            {
                "id": 2000,
                "name": "Test League"
            }
        ]
    }

def test_fetch_data_success(mock_get, mock_makedirs, mock_open_fixture, mock_file_data):
    """
    Test case for the fetch_data function to ensure it successfully fetches data from a given URL and writes it to a file.
    Args:
        mock_get (MagicMock): Mock object for the requests.get function.
        mock_makedirs (MagicMock): Mock object for the os.makedirs function.
        mock_open_fixture (MagicMock): Mock object for the built-in open function.
        mock_file_data (dict): Mock data to be returned by the mocked response.
    Test Steps:
    1. Create a mock response object with a status code of 200 and mock data.
    2. Set the mock_get return value to the mock response.
    3. Call the fetch_data function with a fake URL and filename.
    4. Assert that os.makedirs was called once with the correct parameters.
    5. Assert that the open function was called once with the correct parameters.
    6. Assert that requests.get was called once with the correct URL and headers.
    7. Assert that the result of fetch_data matches the mock data.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_file_data
    mock_response.text = json.dumps(mock_file_data)
    mock_get.return_value = mock_response

    result = fetch_data("http://fakeurl.com", "competitions.json")

    mock_makedirs.assert_called_once_with(DATA_FOLDER, exist_ok=True)
    mock_open_fixture.assert_called_once_with(
        os.path.join(DATA_FOLDER, "competitions.json"), "w", encoding="utf-8"
    )
    mock_get.assert_called_once_with(
        "http://fakeurl.com", headers={"X-Auth-Token": os.getenv("API_KEY")}
    )
    assert result == mock_file_data



def test_fetch_data_rate_limit(mock_get, mock_sleep, mock_open_fixture, mock_file_data):
    """
    Test the `fetch_data` function for handling rate limit (HTTP 429) responses.
    This test simulates a scenario where the first request to the API returns a 
    rate limit response (HTTP 429), and the function should handle this by 
    waiting (sleeping) for a specified duration before retrying the request. 
    The second request should return a successful response (HTTP 200) with the 
    expected data.
    Mocks:
        mock_get: Mock for the `requests.get` function to simulate API responses.
        mock_sleep: Mock for the `time.sleep` function to simulate waiting.
        mock_makedirs: Mock for the `os.makedirs` function to simulate directory creation.
        mock_open_fixture: Mock for the `open` function to simulate file operations.
        mock_file_data: Mock data to be returned by the successful API response.
    Assertions:
        - The `requests.get` function is called twice.
        - The `time.sleep` function is called once with the correct duration.
        - The result of the `fetch_data` function matches the expected mock data.
        - The `open` function is called with the correct file path and mode.
    """
    """Test handling of rate limit (429) response with recursion"""
    # Setup responses
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = mock_file_data
    mock_response_200.text = json.dumps(mock_file_data)
    
    # Configure mocks
    mock_get.side_effect = [mock_response_429, mock_response_200]

    # Call function
    result = fetch_data("http://fakeurl.com", "competitions.json")

    # Verify behavior
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(60)
    assert result == mock_file_data
    mock_open_fixture.assert_called_with(
        os.path.join(DATA_FOLDER, "competitions.json"), 
        "w", 
        encoding="utf-8"
    )

def test_fetch_data_error(mock_get):
    """
    Test case for fetch_data function to handle HTTP error response.
    This test simulates a scenario where the fetch_data function receives a 
    500 Internal Server Error response from the server. It verifies that the 
    function correctly handles the error by returning an empty dictionary.
    Args:
        mock_get (MagicMock): Mock object for the requests.get method.
    Asserts:
        The result of fetch_data is an empty dictionary when the server 
        responds with a 500 status code.
    """
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_get.return_value = mock_response

    result = fetch_data("http://fakeurl.com", "competitions.json")
    assert result == {}


def test_drop_data(mock_remove, mock_rmtree, mock_scandir, mock_makedirs):
    """
    Test the drop_data function.
    This test verifies that the drop_data function correctly removes files and directories
    and recreates the data folder. It uses mock objects to simulate the behavior of the
    os.remove, shutil.rmtree, and os.scandir functions.
    Args:
        mock_remove (MagicMock): Mock object for os.remove.
        mock_rmtree (MagicMock): Mock object for shutil.rmtree.
        mock_scandir (MagicMock): Mock object for os.scandir.
        mock_makedirs (MagicMock): Mock object for os.makedirs.
    Assertions:
        - os.remove is called once with the path of the file.
        - shutil.rmtree is called once with the path of the directory.
        - os.makedirs is called once with the DATA_FOLDER path and exist_ok=True.
    """
    mock_entry_file = MagicMock()
    mock_entry_file.is_file.return_value = True
    mock_entry_file.is_dir.return_value = False
    mock_entry_file.path = "file_path"

    mock_entry_dir = MagicMock()
    mock_entry_dir.is_file.return_value = False
    mock_entry_dir.is_dir.return_value = True
    mock_entry_dir.path = "dir_path"

    mock_scandir.return_value = [mock_entry_file, mock_entry_dir]

    drop_data()

    mock_remove.assert_called_once_with("file_path")
    mock_rmtree.assert_called_once_with("dir_path")
    mock_makedirs.assert_called_once_with(DATA_FOLDER, exist_ok=True)


def test_extract_data(mock_fetch_data):
    """
    Test the extract_data function.
    This test uses a mock object to simulate the fetch_data function, providing
    predefined responses for competitions and teams. It then calls the extract_data
    function and asserts that the returned competitions and all_teams match the
    expected values.
    Args:
        mock_fetch_data (Mock): A mock object for the fetch_data function.
    Assertions:
        - The competitions list should contain a dictionary with the competition
          details.
        - The all_teams list should contain a dictionary with the combined
          competition and team details.
    """
    mock_fetch_data.side_effect = [
        {"competitions": [{"id": 1, "name": "Competition 1", "code": "C1"}]},
        {"teams": [{"id": 101, "name": "Team 1"}]},
    ]

    competitions, all_teams = extract_data()

    assert competitions == [{"id": 1, "name": "Competition 1", "code": "C1"}]
    assert all_teams == [
        {
            "competition_id": 1,
            "competition_name": "Competition 1",
            "team_id": 101,
            "team_name": "Team 1",
        }
    ]
