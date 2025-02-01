import json
import os
from unittest.mock import MagicMock, mock_open, patch

import pytest

from app.etl.extract import DATA_FOLDER, drop_data, extract_data, fetch_data


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



def test_fetch_data_rate_limit(mock_get, mock_sleep, mock_makedirs, mock_open_fixture, mock_file_data):
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
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_get.return_value = mock_response

    result = fetch_data("http://fakeurl.com", "competitions.json")
    assert result == {}


def test_drop_data(mock_remove, mock_rmtree, mock_scandir, mock_makedirs):
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
