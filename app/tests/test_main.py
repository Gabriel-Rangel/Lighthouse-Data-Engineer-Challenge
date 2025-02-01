import pytest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from app.main import export_summary, main

@pytest.fixture
def mock_makedirs(monkeypatch):
    with patch('main.os.makedirs') as mock:
        yield mock

@pytest.fixture
def mock_connect(monkeypatch):
    with patch('main.sqlite3.connect') as mock:
        yield mock

@pytest.fixture
def mock_read_sql_query(monkeypatch):
    with patch('main.pd.read_sql_query') as mock:
        yield mock

@pytest.fixture
def mock_to_csv(monkeypatch):
    with patch('main.pd.DataFrame.to_csv') as mock:
        yield mock

def test_export_summary(mock_makedirs, mock_connect, mock_read_sql_query, mock_to_csv):
    # Mock the database connection and query result
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_df = pd.DataFrame({
        'Competition': ['Competition1', 'Competition2'],
        'Number_of_Teams': [10, 8]
    })
    mock_read_sql_query.return_value = mock_df

    # Call the function
    export_summary()

    # Assertions
    mock_makedirs.assert_called_with("output", exist_ok=True)
    mock_connect.assert_called_with("db/football_data.sqlite")
    mock_read_sql_query.assert_called_once()
    mock_to_csv.assert_called_once_with("output/summary.csv", index=False)

@pytest.fixture
def mock_etl_functions():
    with patch('app.main.drop_data') as mock_drop, \
         patch('app.main.extract_data') as mock_extract, \
         patch('app.main.transform_data') as mock_transform, \
         patch('app.main.create_tables') as mock_create, \
         patch('app.main.load_data') as mock_load, \
         patch('app.main.export_summary') as mock_export:
        yield {
            'drop_data': mock_drop,
            'extract_data': mock_extract,
            'transform_data': mock_transform,
            'create_tables': mock_create,
            'load_data': mock_load,
            'export_summary': mock_export
        }

@pytest.fixture
def mock_logger():
    with patch('app.main.logger') as mock:
        yield mock

def test_main_success(mock_etl_functions, mock_logger):
    # Setup mock return values
    mock_etl_functions['extract_data'].return_value = (
        [{'id': 1, 'name': 'Competition1'}],
        [{'team_id': 1, 'team_name': 'Team1'}]
    )
    mock_etl_functions['transform_data'].return_value = (
        pd.DataFrame({'id': [1], 'name': ['Competition1']}),
        pd.DataFrame({'id': [1], 'name': ['Team1']}),
        pd.DataFrame({'competition_id': [1], 'team_id': [1]})
    )

    # Execute main function
    main()

    # Verify all ETL functions were called in order
    mock_etl_functions['drop_data'].assert_called_once()
    mock_etl_functions['extract_data'].assert_called_once()
    mock_etl_functions['transform_data'].assert_called_once()
    mock_etl_functions['create_tables'].assert_called_once()
    mock_etl_functions['load_data'].assert_called_once()
    mock_etl_functions['export_summary'].assert_called_once()

    # Verify logging calls
    assert mock_logger.info.call_args_list == [
        call("Starting ETL process"),
        call("Cleaning data folder"),
        call("Extracting data"),
        call("Transforming data"),
        call("Loading data to database"),
        call("Exporting summary"),
        call("ETL process completed successfully")
    ]

def test_main_error_handling(mock_etl_functions, mock_logger):
    # Setup mock to raise an exception
    mock_etl_functions['extract_data'].side_effect = Exception("Test error")

    # Execute main function and verify it raises the exception
    with pytest.raises(Exception, match="Test error"):
        main()

    # Verify error was logged
    mock_logger.error.assert_called_once_with(
        "ETL process failed: Test error",
        exc_info=True
    )
