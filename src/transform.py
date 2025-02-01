import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_data(competitions, all_teams):
    """
    Transforms the extracted data into DataFrames.
    Args:
        competitions (list): A list of dictionaries containing competition data.
        all_teams (list): A list of dictionaries containing team data.
    Returns:
        tuple: A tuple containing three pandas DataFrames:
            - dim_competitions: DataFrame for competitions dimension with columns ["id", "name"].
            - dim_teams: DataFrame for teams dimension with columns ["id", "name"].
            - fact_competitions: DataFrame for relationships between teams and competitions with columns ["competition_id", "team_id"].
    """
    logger.info("Starting data transformation process")
    
    try:
        # Create DataFrame for competitions dimension
        logger.debug("Creating competitions dimension DataFrame")
        dim_competitions = pd.DataFrame(competitions, columns=["id", "name"])
        null_count = dim_competitions.isnull().sum().sum()
        if null_count > 0:
            logger.warning(f"Found {null_count} null values in competitions data")
        dim_competitions.dropna(inplace=True)
        logger.info(f"Created competitions dimension with shape: {dim_competitions.shape}")

        # Create DataFrame for teams dimension
        logger.debug("Creating teams dimension DataFrame")
        dim_teams = pd.DataFrame(all_teams, columns=["team_id", "team_name"]).drop_duplicates()
        dim_teams.rename(columns={"team_id": "id", "team_name": "name"}, inplace=True)
        logger.info(f"Created teams dimension with shape: {dim_teams.shape}")

        # Create DataFrame for fact_competitions
        logger.debug("Creating fact competitions DataFrame")
        fact_competitions = pd.DataFrame(all_teams, columns=["competition_id", "team_id"])
        logger.info(f"Created fact table with shape: {fact_competitions.shape}")

        logger.info("Data transformation completed successfully")
        return dim_competitions, dim_teams, fact_competitions

    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}", exc_info=True)
        raise