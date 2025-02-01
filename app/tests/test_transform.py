import pytest
from app.etl.transform import transform_data

"""
Explanation of @pytest.fixture:

The @pytest.fixture decorator is used to define a fixture function in pytest. Fixtures are a way to provide a fixed baseline upon which tests can reliably and repeatedly execute. 
They are used to set up some context for the tests, such as creating mock objects, preparing test data, or configuring the environment. 
Fixtures are defined using functions, and they can return values that are then injected into test functions that depend on them.
"""
@pytest.fixture
def setup_data():
    competitions = [
        {"id": 1, "name": "Premier League"},
        {"id": 2, "name": "La Liga"},
        {"id": 3, "name": None}  # Include a None value to test dropna
    ]
    all_teams = [
        {"team_id": 1, "team_name": "Team A", "competition_id": 1},
        {"team_id": 2, "team_name": "Team B", "competition_id": 1},
        {"team_id": 3, "team_name": "Team C", "competition_id": 2},
        {"team_id": 1, "team_name": "Team A", "competition_id": 1}  # Duplicate entry to test drop_duplicates
    ]
    return competitions, all_teams

def test_transform_data(setup_data):
    """
    Test the transform_data function.
    This test verifies the following:
    - The shape and columns of the dim_competitions DataFrame.
    - The shape and columns of the dim_teams DataFrame.
    - The uniqueness of the 'id' column in the dim_teams DataFrame.
    - The shape and columns of the fact_competitions DataFrame.
    Args:
        setup_data (tuple): A tuple containing the competitions and all_teams data.
    Asserts:
        - The dim_competitions DataFrame has the correct shape and columns, and no None values in the 'name' column.
        - The dim_teams DataFrame has the correct shape and columns, and unique 'id' values.
        - The fact_competitions DataFrame has the correct shape and columns.
    """
    competitions, all_teams = setup_data
    dim_competitions, dim_teams, fact_competitions = transform_data(competitions, all_teams)

    # Test dim_competitions DataFrame
    assert dim_competitions.shape == (2, 2)
    assert list(dim_competitions.columns) == ["id", "name"]
    assert None not in dim_competitions["name"].values

    # Test dim_teams DataFrame
    assert dim_teams.shape == (3, 2)
    assert list(dim_teams.columns) == ["id", "name"]
    assert dim_teams["id"].nunique() == 3

    # Test fact_competitions DataFrame
    assert fact_competitions.shape == (4, 2)
    assert list(fact_competitions.columns) == ["competition_id", "team_id"]

def test_transform_data_empty_input():
    """
    Test the transform_data function with empty input lists.
    This test ensures that when the transform_data function is provided with empty
    lists as input, it returns empty DataFrames with the correct column names.
    Assertions:
    - The dim_competitions DataFrame should be empty and have columns ["id", "name"].
    - The dim_teams DataFrame should be empty and have columns ["id", "name"].
    - The fact_competitions DataFrame should be empty and have columns ["competition_id", "team_id"].
    """
    dim_competitions, dim_teams, fact_competitions = transform_data([], [])

    # Test dim_competitions DataFrame
    assert dim_competitions.empty
    assert list(dim_competitions.columns) == ["id", "name"]

    # Test dim_teams DataFrame
    assert dim_teams.empty
    assert list(dim_teams.columns) == ["id", "name"]

    # Test fact_competitions DataFrame
    assert fact_competitions.empty
    assert list(fact_competitions.columns) == ["competition_id", "team_id"]
