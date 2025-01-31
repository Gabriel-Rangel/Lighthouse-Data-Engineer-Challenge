import sqlite3
import os

def create_tables():
    # Ensure the db directory exists
    os.makedirs("db", exist_ok=True)

    conn = sqlite3.connect("db/football_data.sqlite")
    cursor = conn.cursor()

    # Drop tables if they exist
    cursor.execute("DROP TABLE IF EXISTS dim_teams")
    cursor.execute("DROP TABLE IF EXISTS dim_competitions")
    cursor.execute("DROP TABLE IF EXISTS fact_competitions")

    # Create tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_teams (
        id INTEGER,
        name TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_competitions (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_competitions (
        competition_id INTEGER,
        team_id INTEGER
    )
    """)
    conn.commit()
    conn.close()

def load_data(dim_competitions, dim_teams, fact_competitions):
    conn = sqlite3.connect("db/football_data.sqlite")

    # Insert data into dim_competitions
    dim_competitions.to_sql('dim_competitions', conn, if_exists='append', index=False)

    # Insert data into dim_teams
    dim_teams.to_sql('dim_teams', conn, if_exists='append', index=False)

    # Insert data into fact_competitions
    fact_competitions.to_sql('fact_competitions', conn, if_exists='append', index=False)

    conn.commit()
    conn.close()