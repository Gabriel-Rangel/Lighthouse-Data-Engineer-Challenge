import pytest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from app.main import export_summary, main

"""
Explanation of @pytest.fixture:

The @pytest.fixture decorator is used to define a fixture function in pytest. Fixtures are a way to provide a fixed baseline upon which tests can reliably and repeatedly execute. 
They are used to set up some context for the tests, such as creating mock objects, preparing test data, or configuring the environment. 
Fixtures are defined using functions, and they can return values that are then injected into test functions that depend on them.
"""

@pytest.fixture
def mock_makedirs():
    with patch('main.os.makedirs') as mock:
        yield mock

@pytest.fixture
def mock_connect():
    with patch('main.sqlite3.connect') as mock:
        yield mock

@pytest.fixture
def mock_read_sql_query():
    with patch('main.pd.read_sql_query') as mock:
        yield mock

@pytest.fixture
def mock_to_csv():
    with patch('main.pd.DataFrame.to_csv') as mock:
        yield mock

def test_export_summary(mock_makedirs, mock_connect, mock_read_sql_query, mock_to_csv):
    """
    Test the export_summary function.
    This test verifies that the export_summary function performs the following actions:
    1. Creates the output directory if it does not exist.
    2. Connects to the SQLite database located at "db/football_data.sqlite".
    3. Executes a SQL query to retrieve the competition summary data.
    4. Exports the retrieved data to a CSV file at "output/summary.csv".
    Mocks:
    - mock_makedirs: Mock for os.makedirs to avoid creating actual directories.
    - mock_connect: Mock for sqlite3.connect to avoid making actual database connections.
    - mock_read_sql_query: Mock for pandas.read_sql_query to avoid executing actual SQL queries.
    - mock_to_csv: Mock for pandas.DataFrame.to_csv to avoid writing actual files.
    Assertions:
    - Verifies that os.makedirs is called with the correct arguments.
    - Verifies that sqlite3.connect is called with the correct database path.
    - Verifies that pandas.read_sql_query is called once.
    - Verifies that pandas.DataFrame.to_csv is called once with the correct file path and parameters.
    """
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

@pytest.fixture
def mock_logging_setup():
    with patch('app.main.logging.basicConfig') as mock_basic_config, \
         patch('app.main.logging.FileHandler') as mock_file_handler, \
         patch('app.main.logging.StreamHandler') as mock_stream_handler, \
         patch('app.main.os.makedirs') as mock_makedirs:
        yield {
            'basic_config': mock_basic_config,
            'file_handler': mock_file_handler,
            'stream_handler': mock_stream_handler,
            'makedirs': mock_makedirs
        }

def test_main_success(mock_etl_functions, mock_logger):
    """
    Test the main function for a successful ETL process.
    This test verifies that the ETL process runs successfully by mocking the ETL functions and logger.
    It sets up mock return values for the extract and transform functions, executes the main function,
    and checks that all ETL functions are called in the correct order. Additionally, it verifies that
    the appropriate logging calls are made.
    Args:
        mock_etl_functions (dict): A dictionary of mocked ETL functions.
        mock_logger (Mock): A mocked logger object.
    Assertions:
        - All ETL functions (drop_data, extract_data, transform_data, create_tables, load_data, export_summary)
          are called exactly once.
        - The logger's info method is called with the expected messages in the correct order.
    """
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
