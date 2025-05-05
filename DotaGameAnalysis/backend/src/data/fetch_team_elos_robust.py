#!/usr/bin/env python
"""
Fetch Team ELO Ratings - Robust Version

A robust script to fetch team ELO ratings from the OpenDota API with proper
rate limiting, exponential backoff, and retry logic.
"""
import os
import sys
import time
import logging
import argparse
import random
import requests
import sqlite3
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("team_elos_fetch.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# OpenDota API base URL
OPENDOTA_API_URL = "https://api.opendota.com/api"
DEFAULT_RETRY_DELAY = 2  # Default seconds to wait between requests
MAX_RETRY_DELAY = 60  # Maximum seconds to wait for rate limit reset


def get_db_team_ids():
    """
    Get all team IDs from the database.
    
    Returns:
        list: List of team IDs
    """
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    db_path = os.path.join(data_dir, 'dota_matches.db')
    
    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        return []
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all team IDs
        cursor.execute("SELECT team_id FROM teams ORDER BY team_id")
        team_ids = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Found {len(team_ids)} teams in database")
        return team_ids
    
    except Exception as e:
        logger.error(f"Error getting team IDs from database: {e}")
        return []
    
    finally:
        if 'conn' in locals():
            conn.close()


def fetch_team_rating(team_id, api_key=None, initial_delay=DEFAULT_RETRY_DELAY):
    """
    Fetch ELO rating for a specific team with robust retry logic.
    
    Args:
        team_id (int): Team ID
        api_key (str, optional): OpenDota API key
        initial_delay (int): Initial delay in seconds between retries
        
    Returns:
        tuple: (team_id, name, rating) or None if failed
    """
    url = f"{OPENDOTA_API_URL}/teams/{team_id}"
    if api_key:
        url += f"?api_key={api_key}"
    
    delay = initial_delay
    max_retries = 5
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                team_name = data.get('name', 'Unknown')
                rating = data.get('rating')
                
                if rating:
                    logger.info(f"Fetched team {team_id} ({team_name}): ELO {rating}")
                    return (team_id, team_name, float(rating))
                else:
                    logger.warning(f"No rating found for team {team_id} ({team_name})")
                    return (team_id, team_name, None)
            
            elif response.status_code == 429:
                # Rate limited - exponential backoff
                retry_after = int(response.headers.get('Retry-After', delay))
                retry_after = min(retry_after * (attempt + 1), MAX_RETRY_DELAY)
                
                logger.warning(f"Rate limited. Waiting {retry_after}s before retry. Attempt {attempt + 1}/{max_retries}")
                time.sleep(retry_after)
                
                # Add some jitter to avoid all requests hitting simultaneously
                delay = min(delay * 2, MAX_RETRY_DELAY) + random.uniform(0.1, 1.0)
            
            elif response.status_code == 404:
                logger.warning(f"Team {team_id} not found")
                return (team_id, None, None)
            
            else:
                logger.error(f"Error fetching team {team_id}: HTTP {response.status_code}")
                time.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
        
        except RequestException as e:
            logger.error(f"Request error for team {team_id}: {e}")
            time.sleep(delay)
            delay = min(delay * 2, MAX_RETRY_DELAY)
    
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
                cursor.execute("UPDATE teams SET elo = ? WHERE team_id = ?", (rating, team_id))
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


def fetch_batch_ratings(team_ids, api_key=None, batch_size=10, delay=DEFAULT_RETRY_DELAY):
    """
    Fetch ratings for a batch of teams with controlled rate.
    
    Args:
        team_ids (list): List of team IDs
        api_key (str, optional): OpenDota API key
        batch_size (int): Number of teams to process before showing progress
        delay (int): Base delay between requests
        
    Returns:
        list: List of (team_id, team_name, rating) tuples
    """
    ratings = []
    total_teams = len(team_ids)
    
    logger.info(f"Fetching ratings for {total_teams} teams in batches of {batch_size}")
    print(f"Fetching team ELO ratings from OpenDota API ({total_teams} teams total)...")
    
    for i, team_id in enumerate(team_ids):
        # Progress update
        if i > 0 and i % batch_size == 0:
            progress = (i / total_teams) * 100
            logger.info(f"Progress: {i}/{total_teams} teams ({progress:.1f}%)")
            print(f"Progress: {i}/{total_teams} teams ({progress:.1f}%)")
        
        # Fetch rating
        rating_data = fetch_team_rating(team_id, api_key, delay)
        if rating_data:
            ratings.append(rating_data)
        
        # Sleep between requests to avoid rate limiting
        actual_delay = delay
        if not api_key:
            # Add more delay if no API key
            actual_delay += random.uniform(0.5, 1.5)
        else:
            actual_delay += random.uniform(0.1, 0.3)
        
        time.sleep(actual_delay)
    
    logger.info(f"Completed fetching ratings for {len(ratings)} teams")
    return ratings


def main():
    """Main function to fetch and update team ELO ratings."""
    parser = argparse.ArgumentParser(description="Fetch team ELO ratings from OpenDota API")
    parser.add_argument("--api-key", type=str, help="OpenDota API key")
    parser.add_argument("--delay", type=float, default=DEFAULT_RETRY_DELAY, 
                      help=f"Base delay between requests in seconds (default: {DEFAULT_RETRY_DELAY})")
    parser.add_argument("--batch-size", type=int, default=10,
                      help="Number of teams to process before showing progress (default: 10)")
    parser.add_argument("--limit", type=int, help="Limit to N most recent teams")
    args = parser.parse_args()
    
    # Get team IDs from database
    team_ids = get_db_team_ids()
    if not team_ids:
        print("No teams found in database.")
        return
    
    # Apply limit if specified
    if args.limit and args.limit > 0:
        logger.info(f"Limiting to {args.limit} teams")
        team_ids = team_ids[-args.limit:]
    
    # Fetch ratings
    ratings = fetch_batch_ratings(
        team_ids, 
        api_key=args.api_key, 
        batch_size=args.batch_size,
        delay=args.delay
    )
    
    # Filter out None ratings
    valid_ratings = [r for r in ratings if r[2] is not None]
    logger.info(f"Got valid ratings for {len(valid_ratings)} out of {len(ratings)} teams")
    
    # Update database
    updated = update_db_with_ratings(valid_ratings)
    
    # Display summary
    print(f"\nELO Rating Update Summary:")
    print(f"  Teams in database: {len(team_ids)}")
    print(f"  Teams with valid ratings: {len(valid_ratings)}")
    print(f"  Teams updated in database: {updated}")
    
    # Display top teams
    print("\nTop 10 Teams by ELO Rating:")
    print("-" * 60)
    
    sorted_ratings = sorted(valid_ratings, key=lambda x: x[2] if x[2] else 0, reverse=True)
    for i, (team_id, name, rating) in enumerate(sorted_ratings[:10], 1):
        name = name if name else "Unknown"
        print(f"{i}. {name:<35} ELO: {rating:.2f}")
    
    print("\nTo see all teams ranked by ELO, run:")
    print("python list_teams_by_elo.py")


if __name__ == "__main__":
    main()
