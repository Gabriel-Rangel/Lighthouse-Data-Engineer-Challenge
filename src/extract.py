import requests
import time
import os
import shutil
import logging

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

API_URL = "https://api.football-data.org/v4/competitions"
API_KEY = os.getenv("API_KEY")
HEADERS = {"X-Auth-Token": API_KEY}
DATA_FOLDER = "data/raw"


def fetch_data(url: str, file_name: str) -> dict:
    """
    Fetches data from the specified API URL and saves it to a local file.
    Args:
        url (str): The URL of the API endpoint to fetch data from.
        file_name (str): The name of the file to save the fetched data.
    Returns:
        dict: The JSON response from the API if the request is successful,
              or an empty dictionary if an error occurs.
    Raises:
        requests.RequestException: If there is an issue with the HTTP request.
    Notes:
        - If the API rate limit is reached (status code 429), the function waits for 60 seconds
          and retries the request.
        - The fetched data is saved to a file in the DATA_FOLDER directory with the specified file_name.
    """
    if not API_KEY:
        logger.error("API_KEY not found in environment variables")
        raise ValueError("API_KEY is required")

    os.makedirs(DATA_FOLDER, exist_ok=True)

    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            file_path = os.path.join(DATA_FOLDER, file_name)
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(response.text)
            logger.info(f"\nData saved to {file_path}")
            return response.json()

        elif response.status_code == 429:
            logger.warning("\nRate limit reached. Retrying in 60 seconds...")
            time.sleep(60)
            return fetch_data(url, file_name)

        else:
            logger.error(f"Unexpected error: {response.status_code}")
            return {}

    except requests.RequestException as e:
        logger.error(f"Request error: {e}")
        return {}


def drop_data():
    """
    Cleans the specified data folder by removing all files and subdirectories.

    This function checks if the DATA_FOLDER directory exists. If it does, it
    iterates through all entries in the directory. If an entry is a file, it
    removes the file. If an entry is a directory, it removes the directory and
    all its contents. After cleaning up, it recreates the DATA_FOLDER directory.
    """
    logger.info(f"Starting cleanup of {DATA_FOLDER}")
    
    if not os.path.exists(DATA_FOLDER):
        logger.warning(f"Directory {DATA_FOLDER} does not exist")
    else:
        for entry in os.scandir(DATA_FOLDER):
            if entry.is_file():
                os.remove(entry.path)
            elif entry.is_dir():
                shutil.rmtree(entry.path)
        logger.info(f"Cleaned up existing contents in {DATA_FOLDER}")
    
    os.makedirs(DATA_FOLDER, exist_ok=True)
    logger.info(f"Recreation of {DATA_FOLDER} completed")


def extract_data():
    """
    Extracts data from the API for football competitions and their respective teams.
    This function fetches data for all competitions and then iterates through each competition
    to fetch the teams associated with it. The data is then compiled into two lists: one for
    competitions and one for teams.
    Returns:
        tuple: A tuple containing two elements:
            - competitions (list): A list of dictionaries, each representing a competition with its details.
            - all_teams (list): A list of dictionaries, each representing a team with its details and the competition it belongs to.
    """
    competitions_data = fetch_data(API_URL, "competitions.json")
    competitions = competitions_data.get("competitions", [])

    # Extract teams for each competition
    all_teams = []
    for competition in competitions:
        competition_id = competition.get("id")
        competition_name = competition.get("name")
        competition_code = competition.get("code")
        if not competition_code:
            continue

        logger.info(f"Fetching teams for competition: {competition_name}")
        teams_url = f"{API_URL}/{competition_code}/teams"
        teams_data = fetch_data(teams_url, f"teams_{competition_code}.json")
        teams = teams_data.get("teams", [])

        for team in teams:
            all_teams.append(
                {
                    "competition_id": competition_id,
                    "competition_name": competition_name,
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                }
            )

    return competitions, all_teams
