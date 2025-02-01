import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from app.etl.load import load_data, create_tables 

"""
Explanation of @pytest.fixture:

The @pytest.fixture decorator is used to define a fixture function in pytest. Fixtures are a way to provide a fixed baseline upon which tests can reliably and repeatedly execute. 
They are used to set up some context for the tests, such as creating mock objects, preparing test data, or configuring the environment. 
Fixtures are defined using functions, and they can return values that are then injected into test functions that depend on them.
"""
@pytest.fixture
def mock_sqlite():
    with patch('sqlite3.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_connect

@pytest.fixture
def sample_data():
    return {
        'dim_competitions': pd.DataFrame({
            'id': [1, 2],
            'name': ['Competition 1', 'Competition 2']
        }),
        'dim_teams': pd.DataFrame({
            'id': [1, 2],
            'name': ['Team 1', 'Team 2']
        }),
        'fact_competitions': pd.DataFrame({
            'competition_id': [1, 2],
            'team_id': [1, 2]
        })
    }


@pytest.fixture
def mock_sqlite_connection():
    """
    A context manager that mocks the sqlite3 connection and cursor for testing purposes.

    This function uses the `patch` function from the `unittest.mock` module to replace the 
    `sqlite3.connect` method with a mock object. It also creates mock objects for the 
    connection and cursor, and sets up the necessary return values.

    Yields:
        tuple: A tuple containing the mock connection and mock cursor objects.
    """
    with patch('sqlite3.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_connect, mock_cursor

def test_create_tables(mock_sqlite_connection):
    """
    Test the create_tables function to ensure that the necessary tables are created
    without performing actual database operations.
    Args:
        mock_sqlite_connection (tuple): A tuple containing mock objects for the 
        SQLite connection and cursor.
    Test Steps:
    1. Call the create_tables function.
    2. Verify that the database connection was attempted with the correct database file.
    3. Verify that all expected table creation SQL commands were executed.
    4. Ensure that the commit and close methods were called on the database connection.
    """
    mock_connect, mock_cursor = mock_sqlite_connection

    # Call create_tables
    create_tables()

    # Verify database connection was attempted
    mock_connect.assert_called_once_with("db/football_data.sqlite")

    # Verify all table creation commands were executed
    expected_calls = [
        "DROP TABLE IF EXISTS dim_teams",
        "DROP TABLE IF EXISTS dim_competitions",
        "DROP TABLE IF EXISTS fact_competitions",
        "CREATE TABLE IF NOT EXISTS dim_teams",
        "CREATE TABLE IF NOT EXISTS dim_competitions",
        "CREATE TABLE IF NOT EXISTS fact_competitions"
    ]

    # Verify each SQL command was executed
    for call in mock_cursor.execute.call_args_list:
        assert any(expected in str(call) for expected in expected_calls)

    # Verify commit and close were called
    mock_connect.return_value.commit.assert_called_once()
    mock_connect.return_value.close.assert_called_once()

def test_load_data(mock_sqlite, sample_data):
    """
    Test the load_data function to ensure it correctly calls the DataFrame.to_sql method
    for each table without performing actual database operations.
    Args:
        mock_sqlite (MagicMock): Mock object for the SQLite connection.
        sample_data (dict): Dictionary containing sample data for 'dim_competitions',
                            'dim_teams', and 'fact_competitions' tables.
    Test Steps:
    1. Mock the pd.DataFrame.to_sql method.
    2. Call the load_data function with sample data.
    3. Verify that the database connection was attempted once with the correct database path.
    4. Verify that the to_sql method was called three times, once for each table, with the correct parameters.
    """

    # Mock DataFrame.to_sql method
    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        # Call load_data with sample data
        load_data(
            sample_data['dim_competitions'],
            sample_data['dim_teams'],
            sample_data['fact_competitions']
        )

        # Verify database connection was attempted
        mock_sqlite.assert_called_once_with("db/football_data.sqlite")

        # Verify to_sql was called for each table
        assert mock_to_sql.call_count == 3
        mock_to_sql.assert_any_call('dim_competitions', mock_sqlite.return_value, if_exists='append', index=False)
        mock_to_sql.assert_any_call('dim_teams', mock_sqlite.return_value, if_exists='append', index=False)
        mock_to_sql.assert_any_call('fact_competitions', mock_sqlite.return_value, if_exists='append', index=False)