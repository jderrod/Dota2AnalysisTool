#!/usr/bin/env python
"""
Update Team ELO Ratings

This script fetches team ELO ratings from the OpenDota API and updates the database.
It adds an 'elo' column to the 'teams' table if it doesn't exist.
"""
import os
import sys
import time
import logging
import argparse
import requests
from sqlalchemy import create_engine, Column, Float, inspect, text
from sqlalchemy.orm import sessionmaker
from database import DotaDatabase, Team, Base

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("team_elos.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# OpenDota API endpoints
OPENDOTA_API_URL = "https://api.opendota.com/api"


def add_elo_column_if_not_exists():
    """
    Add an 'elo' column to the 'teams' table if it doesn't exist.
    
    Returns:
        bool: True if column was added, False if it already existed
    """
    db = DotaDatabase()
    engine = db.engine
    inspector = inspect(engine)
    
    # Check if 'elo' column exists in 'teams' table
    columns = [col['name'] for col in inspector.get_columns('teams')]
    if 'elo' in columns:
        logger.info("Column 'elo' already exists in 'teams' table")
        return False
    
    # Add 'elo' column to 'teams' table
    try:
        # Create a connection and begin a transaction
        conn = engine.connect()
        trans = conn.begin()
        
        # Add the column
        conn.execute(text('ALTER TABLE teams ADD COLUMN elo FLOAT'))
        
        # Commit the transaction
        trans.commit()
        conn.close()
        
        logger.info("Added 'elo' column to 'teams' table")
        return True
    except Exception as e:
        logger.error(f"Error adding 'elo' column: {e}")
        return False


def fetch_team_elos(api_key=None, max_retries=3, retry_delay=5):
    """
    Fetch team ELO ratings from the OpenDota API.
    
    Args:
        api_key (str, optional): OpenDota API key for rate limit increase
        max_retries (int): Maximum number of retries on failure
        retry_delay (int): Delay between retries in seconds
        
    Returns:
        dict: Dictionary mapping team_id to ELO rating
    """
    # Construct API URL with API key if provided
    teams_url = f"{OPENDOTA_API_URL}/teams"
    if api_key:
        teams_url += f"?api_key={api_key}"
    
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"Fetching teams from OpenDota API (attempt {attempt + 1}/{max_retries + 1})...")
            response = requests.get(teams_url)
            
            if response.status_code == 200:
                teams = response.json()
                logger.info(f"Successfully fetched {len(teams)} teams")
                
                # Extract team_id and rating
                team_elos = {}
                for team in teams:
                    team_id = team.get('team_id')
                    rating = team.get('rating')
                    
                    if team_id and rating:
                        team_elos[team_id] = float(rating)
                
                logger.info(f"Extracted ELO ratings for {len(team_elos)} teams")
                return team_elos
            
            elif response.status_code == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', retry_delay))
                logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds before retry.")
                
                if attempt < max_retries:
                    time.sleep(retry_after)
                else:
                    logger.error("Maximum retry attempts reached. Unable to fetch teams.")
                    return {}
            
            else:
                logger.error(f"Failed to fetch teams: HTTP {response.status_code}")
                
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Maximum retry attempts reached. Unable to fetch teams.")
                    return {}
        
        except Exception as e:
            logger.error(f"Error fetching team ELOs: {e}")
            
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Maximum retry attempts reached. Unable to fetch teams.")
                return {}
    
    return {}


def fetch_team_elo_individually(team_ids, api_key=None, max_retries=2, retry_delay=1):
    """
    Fetch ELO ratings for teams individually to avoid rate limits.
    
    Args:
        team_ids (list): List of team IDs to fetch ratings for
        api_key (str, optional): OpenDota API key
        max_retries (int): Maximum number of retries per team
        retry_delay (int): Delay between retries in seconds
        
    Returns:
        dict: Dictionary mapping team_id to ELO rating
    """
    team_elos = {}
    delay_between_requests = 0.5  # Delay between API requests to avoid rate limits
    
    logger.info(f"Fetching ELO ratings for {len(team_ids)} teams individually...")
    
    for i, team_id in enumerate(team_ids):
        # Construct API URL with API key if provided
        team_url = f"{OPENDOTA_API_URL}/teams/{team_id}"
        if api_key:
            team_url += f"?api_key={api_key}"
        
        for attempt in range(max_retries + 1):
            try:
                # Progress update every 10 teams
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(team_ids)} teams processed")
                
                response = requests.get(team_url)
                
                if response.status_code == 200:
                    team_data = response.json()
                    rating = team_data.get('rating')
                    
                    if rating:
                        team_elos[team_id] = float(rating)
                    
                    # Add delay to avoid rate limits
                    time.sleep(delay_between_requests)
                    break
                
                elif response.status_code == 429:  # Rate limit exceeded
                    retry_after = int(response.headers.get('Retry-After', retry_delay * 2))
                    logger.warning(f"Rate limit exceeded for team {team_id}. Waiting {retry_after} seconds.")
                    
                    if attempt < max_retries:
                        time.sleep(retry_after)
                    else:
                        logger.error(f"Maximum retry attempts reached for team {team_id}. Skipping.")
                        break
                
                else:
                    logger.warning(f"Failed to fetch team {team_id}: HTTP {response.status_code}")
                    
                    if attempt < max_retries:
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Maximum retry attempts reached for team {team_id}. Skipping.")
                        break
            
            except Exception as e:
                logger.error(f"Error fetching ELO for team {team_id}: {e}")
                
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Maximum retry attempts reached for team {team_id}. Skipping.")
                    break
    
    logger.info(f"Successfully fetched ELO ratings for {len(team_elos)} teams individually")
    return team_elos


def update_team_elos(api_key=None, use_individual_fetch=False):
    """
    Update team ELO ratings in the database.
    
    Args:
        api_key (str, optional): OpenDota API key
        use_individual_fetch (bool): Whether to fetch ratings for each team individually
        
    Returns:
        int: Number of teams updated
    """
    # Add 'elo' column if it doesn't exist
    add_elo_column_if_not_exists()
    
    # Get list of team IDs from database
    db = DotaDatabase()
    session = db.Session()
    db_teams = session.query(Team).all()
    team_ids = [team.team_id for team in db_teams]
    logger.info(f"Found {len(team_ids)} teams in database")
    
    session.close()
    
    # Fetch team ELOs
    team_elos = {}
    if use_individual_fetch:
        team_elos = fetch_team_elo_individually(team_ids, api_key=api_key)
    else:
        team_elos = fetch_team_elos(api_key=api_key)
    
    if not team_elos:
        logger.warning("No team ELOs to update")
        return 0
    
    # Update teams in the database
    session = db.Session()
    
    try:
        # Update ELO ratings
        updated = 0
        for team in session.query(Team).all():
            if team.team_id in team_elos:
                setattr(team, 'elo', team_elos[team.team_id])
                updated += 1
        
        session.commit()
        logger.info(f"Updated ELO ratings for {updated} teams")
        
        # Print team info with ELO ratings
        if updated > 0:
            print(f"\nTop 10 Teams by ELO Rating:")
            top_teams = session.query(Team).order_by(text("elo DESC")).limit(10).all()
            
            for i, team in enumerate(top_teams, 1):
                elo = getattr(team, 'elo', None)
                if elo is not None:
                    print(f"{i}. {team.name:<30} ELO: {elo:.2f}")
        
        return updated
    
    except Exception as e:
        logger.error(f"Error updating team ELOs in database: {e}")
        session.rollback()
        return 0
    
    finally:
        session.close()


def main():
    """Main function to update team ELO ratings."""
    parser = argparse.ArgumentParser(description="Update team ELO ratings in the database")
    parser.add_argument("--api-key", type=str, help="OpenDota API key")
    parser.add_argument("--individual", action="store_true", help="Fetch ratings for each team individually")
    args = parser.parse_args()
    
    # Check if database exists
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    db_file = os.path.join(data_dir, 'dota_matches.db')
    
    if not os.path.exists(db_file):
        logger.error(f"Database file not found: {db_file}")
        print(f"Error: Database file not found at {db_file}")
        return
    
    # Update team ELO ratings
    print(f"Updating team ELO ratings (individual mode: {'enabled' if args.individual else 'disabled'})...")
    updated = update_team_elos(api_key=args.api_key, use_individual_fetch=args.individual)
    
    if updated > 0:
        print(f"\nUpdated ELO ratings for {updated} teams.")
        print("You can now use ELO ratings in your analysis.")
        print("\nTo list all teams sorted by ELO rating, run:")
        print("  python list_teams_by_elo.py")
    else:
        print("No team ELO ratings were updated.")
        print("\nIf you're encountering rate limits, try again with your OpenDota API key:")
        print("  python update_team_elos.py --api-key YOUR_API_KEY")
        print("\nOr try the individual fetch mode which is slower but more reliable:")
        print("  python update_team_elos.py --individual")


if __name__ == "__main__":
    main()
