import pandas as pd
import sqlite3

from extract import drop_data, extract_data
from transform import transform_data
from load import create_tables, load_data

def export_summary():
    conn = sqlite3.connect("db/football_data.sqlite")
    print("\nExporting summary to output/summary.csv")
    query = """
    SELECT c.name AS Competition, COUNT(f.team_id) AS Number_of_Teams
    FROM dim_competitions c
    JOIN fact_competitions f ON c.id = f.competition_id
    GROUP BY c.name
    ORDER BY COUNT(f.team_id) DESC;
    """
    df = pd.read_sql_query(query, conn)
    df.to_csv("output/summary.csv", index=False)
    conn.close()
    print("\nSummary exported.")


def main():
    drop_data() # Clean the data folder before starting the process
    
    competitions, all_teams = extract_data()
    dim_competitions, dim_teams, fact_competitions = transform_data(competitions, all_teams)
    
    create_tables() # Create tables in the database
    load_data(dim_competitions, dim_teams, fact_competitions) # Load data into the database

    print("Transformation completed.")
    print("Competitions DataFrame:")
    print(dim_competitions.head())
    print("Teams DataFrame:")
    print(dim_teams.head())
    print("Fact Table DataFrame:")
    print(fact_competitions.head())

    export_summary() #extract the summary asked in the challenge

if __name__ == "__main__":
    main()
