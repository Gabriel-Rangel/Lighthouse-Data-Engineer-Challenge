import pandas as pd

def transform_data(competitions, all_teams):
    """Transforms the extracted data into DataFrames."""
    # Create DataFrame for competitions dimension
    dim_competitions = pd.DataFrame(competitions, columns=["id", "name"])
    dim_competitions.dropna(inplace=True)

    # Create DataFrame for teams dimension
    dim_teams = pd.DataFrame(all_teams, columns=["team_id", "team_name"])
    dim_teams.rename(columns={"team_id": "id"}, inplace=True)
    dim_teams.rename(columns={"team_name": "name"}, inplace=True)

    pd.to_csv("output/dim_teams.csv", index=False)

    # Create DataFrame for fact_competitions (relationships between teams and competitions)
    fact_competitions = pd.DataFrame(all_teams, columns=["competition_id", "team_id"])

    return dim_competitions, dim_teams, fact_competitions
