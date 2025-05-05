#!/usr/bin/env python
"""
Update Teams with Real ELO Ratings

This script fetches and updates the ELO ratings for a list of known top teams
using the OpenDota API. It uses direct database updates to ensure proper persistence.
"""
import os
import sys
import time
import logging
import argparse
import requests
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("real_elos_update.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# OpenDota API base URL
OPENDOTA_API_URL = "https://api.opendota.com/api"

# List of top teams with their OpenDota team IDs, limited to the ones we know work well
TOP_TEAMS = [
    {"id": 7119388, "name": "Team Spirit"},
    {"id": 8599101, "name": "Gaimin Gladiators"},
    {"id": 8291895, "name": "Tundra Esports"},
    {"id": 2163, "name": "Team Liquid"},
    {"id": 8255888, "name": "BetBoom Team"},
    {"id": 36, "name": "Natus Vincere"},
    {"id": 39, "name": "Evil Geniuses / Shopify Rebellion"},
    {"id": 350190, "name": "Virtus.pro / Outsiders / BetBoom"},
    {"id": 15, "name": "PSG.LGD"},
    {"id": 1838315, "name": "Team Secret"},
    {"id": 8605863, "name": "9Pandas / Cloud9"},
    {"id": 8261648, "name": "TSM / Old G"},
    {"id": 2586976, "name": "OG"}
]


def direct_db_update(team_id, team_name, elo):
    """
    Directly update the database with team ELO rating to ensure proper persistence.
    
    Args:
        team_id (int): Team ID
        team_name (str): Team name
        elo (float): ELO rating
        
    Returns:
        bool: True if successful, False otherwise
    """
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    db_path = os.path.join(data_dir, 'dota_matches.db')
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if team exists
        cursor.execute("SELECT COUNT(*) FROM teams WHERE team_id = ?", (team_id,))
        if cursor.fetchone()[0] > 0:
            # Update existing team
            cursor.execute("UPDATE teams SET elo = ? WHERE team_id = ?", (elo, team_id))
            
            # Also update name if provided
            if team_name:
                cursor.execute("UPDATE teams SET name = ? WHERE team_id = ? AND (name IS NULL OR name = '')", 
                              (team_name, team_id))
            
            logger.info(f"Updated team {team_id} ({team_name}) with ELO {elo}")
        else:
            # Insert new team
            cursor.execute("INSERT INTO teams (team_id, name, elo) VALUES (?, ?, ?)", 
                          (team_id, team_name, elo))
            logger.info(f"Added new team {team_id} ({team_name}) with ELO {elo}")
        
        conn.commit()
        return True
    
    except Exception as e:
        logger.error(f"Database error for team {team_id}: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()


def fetch_team_rating(team_id, api_key=None):
    """
    Fetch ELO rating for a specific team with no frills.
    
    Args:
        team_id (int): Team ID
        api_key (str, optional): OpenDota API key
        
    Returns:
        tuple: (success, team_name, rating)
    """
    url = f"{OPENDOTA_API_URL}/teams/{team_id}"
    if api_key:
        url += f"?api_key={api_key}"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200 and response.text:
            data = response.json()
            team_name = data.get('name', 'Unknown')
            rating = data.get('rating')
            
            if rating:
                return (True, team_name, float(rating))
            else:
                logger.warning(f"No rating found for team {team_id} ({team_name})")
                return (False, team_name, None)
        
        elif response.status_code == 429:
            logger.warning(f"Rate limited for team {team_id}. Waiting for 5 seconds.")
            time.sleep(5)
            return (False, None, None)
        
        else:
            logger.error(f"Error fetching team {team_id}: HTTP {response.status_code}")
            return (False, None, None)
    
    except Exception as e:
        logger.error(f"Request error for team {team_id}: {e}")
        return (False, None, None)


def main():
    """Main function to fetch and update team ELO ratings."""
    parser = argparse.ArgumentParser(description="Update teams with real ELO ratings from OpenDota API")
    parser.add_argument("--api-key", type=str, help="OpenDota API key")
    args = parser.parse_args()
    
    print(f"Updating {len(TOP_TEAMS)} top Dota 2 teams with real ELO ratings...")
    
    success_count = 0
    failed_teams = []
    
    for i, team in enumerate(TOP_TEAMS, 1):
        team_id = team["id"]
        print(f"Processing {i}/{len(TOP_TEAMS)}: {team['name']} (ID: {team_id})")
        
        success, team_name, rating = fetch_team_rating(team_id, args.api_key)
        
        if success and rating:
            if direct_db_update(team_id, team_name, rating):
                success_count += 1
                print(f"  ✓ Updated {team_name} with ELO {rating:.2f}")
            else:
                failed_teams.append((team_id, team["name"]))
                print(f"  ✕ Database update failed")
        else:
            failed_teams.append((team_id, team["name"]))
            print(f"  ✕ Failed to fetch rating")
        
        # Sleep to avoid rate limiting
        time.sleep(1 if args.api_key else 2)
    
    # Print summary
    print(f"\nELO Update Summary:")
    print(f"  Successfully updated: {success_count}/{len(TOP_TEAMS)} teams")
    
    if failed_teams:
        print(f"  Failed teams:")
        for team_id, name in failed_teams:
            print(f"    - {name} (ID: {team_id})")
    
    print("\nTo see all teams ranked by ELO, run:")
    print("python list_teams_by_elo.py")


if __name__ == "__main__":
    main()
