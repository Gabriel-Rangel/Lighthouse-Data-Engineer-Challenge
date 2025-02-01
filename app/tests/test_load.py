import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from app.etl.load import load_data, create_tables 


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
    with patch('sqlite3.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_connect, mock_cursor

def test_create_tables(mock_sqlite_connection):
    """Test that the tables are created without actual database operations"""
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
    """Test that data loading is called correctly without actual database operations"""
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