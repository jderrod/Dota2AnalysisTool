#!/usr/bin/env python
import os
import json
import requests
import time
from pathlib import Path

# Configuration / Constants
TEAM_NAME = "Yakult Brothers"              # Set the team name you want to look up 
OPENDOTA_API_BASE = "https://api.opendota.com/api"
OUTPUT_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw", "team_data")))

def ensure_directory_exists(directory):
    """Create directory if it doesn't exist."""
    os.makedirs(directory, exist_ok=True)

def get_team_info(team_name):
    """
    Look up team info for a given team name using the OpenDota API.
    Returns a dictionary containing team details.
    """
    print(f"Looking up team info for {team_name}...")
    teams_url = f"{OPENDOTA_API_BASE}/teams"
    response = requests.get(teams_url)
    
    if response.status_code != 200:
        print(f"Error fetching teams: {response.status_code}")
        return None
    
    teams = response.json()
    for team in teams:
        if team.get("name") == team_name:
            print(f"Found team '{team_name}' with ID: {team['team_id']}")
            return team
    print(f"Team '{team_name}' not found.")
    return None

def get_team_matches(team_id, limit=20):
    """
    Fetch the past `limit` matches for a given team ID.
    Returns a list of match IDs.
    """
    print(f"Fetching past {limit} matches for team ID {team_id}...")
    matches_url = f"{OPENDOTA_API_BASE}/teams/{team_id}/matches"
    response = requests.get(matches_url)
    
    if response.status_code != 200:
        print(f"Error fetching matches: {response.status_code}")
        return []
    
    matches = response.json()
    # OpenDota returns matches in descending order (newest first)
    match_ids = [m["match_id"] for m in matches[:limit]]
    print(f"Found {len(match_ids)} matches.")
    return match_ids

def main():
    ensure_directory_exists(OUTPUT_DIR)
    
    # Get team information from the API
    team_info = get_team_info(TEAM_NAME)
    if not team_info:
        return

    team_id = team_info.get("team_id")
    # Get the list of match IDs for the past 20 games
    match_ids = get_team_matches(team_id, limit=20)
    
    # Prepare the output data with only the desired fields
    output_data = {
        "team_id": team_info.get("team_id"),
        "team_name": team_info.get("name"),
        "team_logo": team_info.get("logo_url"),
        "match_ids": match_ids
    }
    
    # Save the data to a JSON file in the output directory
    output_file = os.path.join(OUTPUT_DIR, f"{TEAM_NAME.replace(' ', '_')}_info.json")
    with open(output_file, "w") as f:
        json.dump(output_data, f, indent=2)
    
    print(f"Saved team info to {output_file}")

if __name__ == "__main__":
    main()
