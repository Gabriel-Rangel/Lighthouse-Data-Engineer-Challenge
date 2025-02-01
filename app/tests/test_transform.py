import pytest
from app.etl.transform import transform_data

class TestTransformData:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.competitions = [
            {"id": 1, "name": "Premier League"},
            {"id": 2, "name": "La Liga"},
            {"id": 3, "name": None}  # Include a None value to test dropna
        ]
        self.all_teams = [
            {"team_id": 1, "team_name": "Team A", "competition_id": 1},
            {"team_id": 2, "team_name": "Team B", "competition_id": 1},
            {"team_id": 3, "team_name": "Team C", "competition_id": 2},
            {"team_id": 1, "team_name": "Team A", "competition_id": 1}  # Duplicate entry to test drop_duplicates
        ]

    def test_transform_data(self):
        dim_competitions, dim_teams, fact_competitions = transform_data(self.competitions, self.all_teams)

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

    def test_transform_data_empty_input(self):
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
