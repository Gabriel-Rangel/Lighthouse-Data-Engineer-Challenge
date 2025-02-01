import sqlite3
import os
import logging

logger = logging.getLogger(__name__)

def create_tables():
    """
    Creates the necessary tables for the football data in an SQLite database.
    This function ensures that the 'db' directory exists, connects to the SQLite database
    'football_data.sqlite', and creates the following tables if they do not already exist:
    - dim_teams: Stores team information with columns 'id' (INTEGER PRIMARY KEY) and 'name' (TEXT).
    - dim_competitions: Stores competition information with columns 'id' (INTEGER PRIMARY KEY) and 'name' (TEXT).
    - fact_competitions: Stores the relationship between competitions and teams with columns 'competition_id' (INTEGER) and 'team_id' (INTEGER).
    If the tables already exist, they are dropped and recreated.
    """

    logger.info("Starting database tables creation")
    try:
        # Ensure the db directory exists
        os.makedirs("db", exist_ok=True)
        logger.debug("Database directory checked/created")

        conn = sqlite3.connect("db/football_data.sqlite")
        cursor = conn.cursor()
        logger.info("Successfully connected to database")

        # Drop tables if they exist
        logger.debug("Dropping existing tables")
        cursor.execute("DROP TABLE IF EXISTS dim_teams")
        cursor.execute("DROP TABLE IF EXISTS dim_competitions")
        cursor.execute("DROP TABLE IF EXISTS fact_competitions")

        # Create tables
        logger.debug("Creating new tables")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_teams (
            id INTEGER PRIMARY KEY,
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
        logger.info("Tables created successfully")

    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")


def load_data(dim_competitions, dim_teams, fact_competitions):
    """
    Load data into the SQLite database.
    This function inserts data into three tables: dim_competitions, dim_teams, 
    and fact_competitions in the SQLite database located at 'db/football_data.sqlite'.
    Parameters:
    dim_competitions (DataFrame): DataFrame containing data for the dim_competitions table.
    dim_teams (DataFrame): DataFrame containing data for the dim_teams table.
    fact_competitions (DataFrame): DataFrame containing data for the fact_competitions table.
    Returns:
    None
    """
    logger.info("Starting data loading process")
    try:
        conn = sqlite3.connect("db/football_data.sqlite")
        logger.debug("Connected to database")

        # Load dim_competitions
        dim_competitions.to_sql('dim_competitions', conn, if_exists='append', index=False)
        logger.info(f"Loaded {len(dim_competitions)} rows into dim_competitions")

        # Load dim_teams
        dim_teams.to_sql('dim_teams', conn, if_exists='append', index=False)
        logger.info(f"Loaded {len(dim_teams)} rows into dim_teams")

        # Load fact_competitions
        fact_competitions.to_sql('fact_competitions', conn, if_exists='append', index=False)
        logger.info(f"Loaded {len(fact_competitions)} rows into fact_competitions")

        conn.commit()
        logger.info("Data loading completed successfully")

    except sqlite3.Error as e:
        logger.error(f"Database error during loading: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during loading: {str(e)}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")