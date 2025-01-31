import requests
import time
import os
import shutil

from dotenv import load_dotenv

load_dotenv()

API_URL = "https://api.football-data.org/v4/competitions"
API_KEY = os.getenv("API_KEY")
HEADERS = {"X-Auth-Token": API_KEY}
DATA_FOLDER = "data/raw"

os.makedirs(DATA_FOLDER, exist_ok=True)


def fetch_data(url: str, file_name: str) -> dict:
    """Fetches data from the API and persists it locally."""
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            file_path = os.path.join(DATA_FOLDER, file_name)
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(response.text)
            print(f"Data saved to {file_path}")
            return response.json()

        elif response.status_code == 429:
            print("Rate limit reached. Retrying in 60 seconds...")
            time.sleep(60)
            return fetch_data(url, file_name)

        else:
            print(f"Unexpected error: {response.status_code}")
            return {}

    except requests.RequestException as e:
        print(f"Request error: {e}")
        return {}


def drop_data():
    """Cleans the specified data folder."""
    if os.path.exists(DATA_FOLDER):
        for entry in os.scandir(DATA_FOLDER):
            if entry.is_file():
                os.remove(entry.path)
            elif entry.is_dir():
                shutil.rmtree(entry.path)
    os.makedirs(DATA_FOLDER, exist_ok=True)


def extract_data():
    competitions_data = fetch_data(API_URL, "competitions.json")
    competitions = competitions_data.get("competitions", [])

    # Extract teams for each competition
    all_teams = []
    for competition in competitions:
        competition_id = competition.get("id")
        competition_name = competition.get("name")
        if not competition_id or not competition_name:
            continue

        print(f"Fetching teams for competition: {competition_name}")
        teams_url = f"{API_URL}/{competition_id}/teams"
        teams_data = fetch_data(teams_url, f"teams_{competition_id}.json")
        teams = teams_data.get("teams", [])

        for team in teams:
            all_teams.append({"competition_id": competition_id, "competition_name": competition_name,
                               "team_id": team.get("id"), "team_name": team.get("name")})

    return competitions, all_teams



