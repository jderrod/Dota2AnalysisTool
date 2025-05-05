#!/usr/bin/env python
"""
Fetch Top Team ELO Ratings

This script fetches ELO ratings for the top professional Dota 2 teams
from the OpenDota API and stores them in the database.
"""
import os
import sys
import time
import logging
import argparse
import json
import requests
import sqlite3
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("top_teams_elo.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# OpenDota API base URL
OPENDOTA_API_URL = "https://api.opendota.com/api"

# List of top teams with their OpenDota team IDs
# These are the top teams in competitive Dota 2 (as of 2025)
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
    {"id": 8376426, "name": "Team Falcons"},
    {"id": 8605863, "name": "9Pandas"},
    {"id": 8261648, "name": "TSM / Quest"},
    {"id": 2586976, "name": "OG"},
    {"id": 8575175, "name": "IVY / Nagpur Globastomp"},
    {"id": 8343488, "name": "Aurora"},
    {"id": 8254400, "name": "beastcoast"},
    {"id": 7390454, "name": "Talon Esports"},
    {"id": 8204512, "name": "Entity"},
    {"id": 8131728, "name": "Team Liquid (NA) / Nouns"},
    {"id": 2108395, "name": "Tundra Esports"},
    {"id": 8597976, "name": "Xtreme Gaming"},
    {"id": 8687717, "name": "Talon Esports"},
    {"id": 8605863, "name": "9Pandas"},
    {"id": 8214850, "name": "Azure Ray"}
]


def fetch_team_rating(team_id, api_key=None):
    """
    Fetch ELO rating for a specific team.
    
    Args:
        team_id (int): Team ID
        api_key (str, optional): OpenDota API key
        
    Returns:
        tuple: (team_id, name, rating) or None if failed
    """
    url = f"{OPENDOTA_API_URL}/teams/{team_id}"
    if api_key:
        url += f"?api_key={api_key}"
    
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200 and response.text:
                try:
                    data = response.json()
                    team_name = data.get('name', 'Unknown')
                    rating = data.get('rating')
                    
                    if rating:
                        logger.info(f"Fetched team {team_id} ({team_name}): ELO {rating}")
                        return (team_id, team_name, float(rating))
                    else:
                        logger.warning(f"No rating found for team {team_id} ({team_name})")
                        return (team_id, team_name, None)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON for team {team_id}: {response.text[:100]}")
            
            elif response.status_code == 429:
                # Rate limited
                retry_after = int(response.headers.get('Retry-After', retry_delay))
                logger.warning(f"Rate limited. Waiting {retry_after}s before retry.")
                time.sleep(retry_after)
            
            elif response.status_code == 404:
                logger.warning(f"Team {team_id} not found")
                return (team_id, None, None)
            
            else:
                logger.error(f"Error fetching team {team_id}: HTTP {response.status_code}")
                time.sleep(retry_delay)
        
        except (RequestException, json.JSONDecodeError) as e:
            logger.error(f"Request error for team {team_id}: {e}")
            time.sleep(retry_delay)
        
        retry_delay *= 2  # Exponential backoff
    
    logger.error(f"Failed to fetch rating for team {team_id} after {max_retries} attempts")
    return None


def update_db_with_ratings(ratings):
    """
    Update the database with team ratings.
    
    Args:
        ratings (list): List of (team_id, team_name, rating) tuples
        
    Returns:
        int: Number of teams updated
    """
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    db_path = os.path.join(data_dir, 'dota_matches.db')
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Ensure elo column exists
        try:
            cursor.execute("SELECT elo FROM teams LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding elo column to teams table")
            cursor.execute("ALTER TABLE teams ADD COLUMN elo FLOAT")
        
        # Update ratings
        updated = 0
        for team_id, team_name, rating in ratings:
            if rating is not None:
                # Check if team exists
                cursor.execute("SELECT COUNT(*) FROM teams WHERE team_id = ?", (team_id,))
                if cursor.fetchone()[0] > 0:
                    cursor.execute("UPDATE teams SET elo = ? WHERE team_id = ?", (rating, team_id))
                    if team_name and team_name != "Unknown":
                        # Update team name if it's provided
                        cursor.execute("UPDATE teams SET name = ? WHERE team_id = ? AND (name IS NULL OR name = '')", 
                                      (team_name, team_id))
                    updated += 1
                else:
                    # Insert new team if it doesn't exist
                    logger.info(f"Adding new team {team_id} ({team_name}) to database")
                    cursor.execute(
                        "INSERT INTO teams (team_id, name, elo) VALUES (?, ?, ?)",
                        (team_id, team_name, rating)
                    )
                    updated += 1
        
        conn.commit()
        
        # Verify
        cursor.execute("SELECT COUNT(*) FROM teams WHERE elo IS NOT NULL")
        total_with_ratings = cursor.fetchone()[0]
        logger.info(f"Total teams with ratings in database: {total_with_ratings}")
        
        return updated
    
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        if 'conn' in locals():
            conn.rollback()
        return 0
    
    finally:
        if 'conn' in locals():
            conn.close()


def main():
    """Main function to fetch and update team ELO ratings."""
    parser = argparse.ArgumentParser(description="Fetch top team ELO ratings from OpenDota API")
    parser.add_argument("--api-key", type=str, help="OpenDota API key")
    args = parser.parse_args()
    
    print(f"Fetching ELO ratings for {len(TOP_TEAMS)} top Dota 2 teams...")
    
    # Fetch ratings
    ratings = []
    for i, team in enumerate(TOP_TEAMS, 1):
        team_id = team["id"]
        print(f"Fetching {i}/{len(TOP_TEAMS)}: {team['name']} (ID: {team_id})")
        
        rating_data = fetch_team_rating(team_id, args.api_key)
        if rating_data:
            ratings.append(rating_data)
        
        # Sleep to avoid rate limiting
        time.sleep(2 if not args.api_key else 1)
    
    # Filter out None ratings
    valid_ratings = [r for r in ratings if r[2] is not None]
    logger.info(f"Got valid ratings for {len(valid_ratings)} out of {len(ratings)} teams")
    
    # Update database
    updated = update_db_with_ratings(valid_ratings)
    
    # Display summary
    print(f"\nELO Rating Update Summary:")
    print(f"  Teams processed: {len(TOP_TEAMS)}")
    print(f"  Teams with valid ratings: {len(valid_ratings)}")
    print(f"  Teams updated in database: {updated}")
    
    # Display top teams
    print("\nTop Teams by ELO Rating:")
    print("-" * 60)
    
    sorted_ratings = sorted(valid_ratings, key=lambda x: x[2] if x[2] else 0, reverse=True)
    for i, (team_id, name, rating) in enumerate(sorted_ratings[:15], 1):
        name = name if name else "Unknown"
        print(f"{i}. {name:<35} ELO: {rating:.2f}")
    
    print("\nTo see all teams ranked by ELO, run:")
    print("python list_teams_by_elo.py")


if __name__ == "__main__":
    main()
