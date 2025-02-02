import pandas as pd
import sqlite3
import os
import logging
from datetime import datetime

from etl.extract import drop_data, extract_data
from etl.transform import transform_data
from etl.load import create_tables, load_data

logger = logging.getLogger(__name__)

def setup_logging():
    """Configure logging with file and stream handlers"""
    # Create logs directory if it doesn't exist
    LOGS_DIR = "logs"
    os.makedirs(LOGS_DIR, exist_ok=True)

    # Configure logging with timestamp in filename 
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOGS_DIR, f'football_etl_{current_time}.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def export_summary():
    """
    Exports a summary of the number of teams in each competition to a CSV file.

    This function connects to a SQLite database, executes a SQL query to retrieve
    the number of teams in each competition, and writes the results to a CSV file
    located at 'output/summary.csv'. The CSV file will contain two columns:
    'Competition' and 'Number_of_Teams'.

    The function performs the following steps:
    1. Connects to the SQLite database located at 'db/football_data.sqlite'.
    2. Executes a SQL query to retrieve the number of teams in each competition.
    3. Writes the query results to 'output/summary.csv'.
    4. Closes the database connection.

    Raises:
        sqlite3.DatabaseError: If there is an error connecting to the database or executing the query.
        pandas.errors.EmptyDataError: If the query returns no data.
        IOError: If there is an error writing the CSV file.
    """
    logger.info("Starting summary export process")
    
    try:
        os.makedirs("output", exist_ok=True)
        logger.debug("Output directory checked/created")

        conn = sqlite3.connect("db/football_data.sqlite")
        logger.debug("Connected to database")

        query = """
        SELECT c.name AS Competition, COUNT(f.team_id) AS Number_of_Teams
        FROM dim_competitions c
        JOIN fact_competitions f ON c.id = f.competition_id
        GROUP BY c.name
        ORDER BY COUNT(f.team_id) DESC;
        """
        logger.debug("Executing summary query")
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            logger.warning("Query returned no data")
        else:
            logger.info(f"Query returned {len(df)} rows")

        df.to_csv("output/summary.csv", index=False)
        logger.info("Summary exported to output/summary.csv")

    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error during summary export: {str(e)}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")


def main():
    """
    Main function to execute the ETL process for football data.
    This function performs the following steps:
    1. Cleans the data folder by calling `drop_data()`.
    2. Extracts data by calling `extract_data()` and stores the results in `competitions` and `all_teams`.
    3. Transforms the extracted data by calling `transform_data()` and stores the results in `dim_competitions`, `dim_teams`, and `fact_competitions`.
    4. Creates tables in the database by calling `create_tables()`.
    5. Loads the transformed data into the database by calling `load_data()` with `dim_competitions`, `dim_teams`, and `fact_competitions` as arguments.
    6. Prints the first few rows of the transformed data for verification.
    7. Exports a summary by calling `export_summary()`.
    Returns:
        None
    """
    logger.info("Starting ETL process")
    
    try:
        logger.info("Cleaning data folder")
        drop_data()
        
        logger.info("Extracting data")
        competitions, all_teams = extract_data()
        
        logger.info("Transforming data")
        dim_competitions, dim_teams, fact_competitions = transform_data(competitions, all_teams)
        
        logger.info("Loading data to database")
        create_tables()
        load_data(dim_competitions, dim_teams, fact_competitions)
        
        logger.info("Exporting summary")
        export_summary()
        
        logger.info("ETL process completed successfully")

    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    setup_logging() 
    main()
