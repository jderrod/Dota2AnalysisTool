#!/usr/bin/env python3
"""
Script to process downloaded match files and add them to the database without wiping existing data.
This script finds matches in team-specific folders and adds them to the database.
"""

import os
import json
import logging
import sys
import glob
from datetime import datetime
from tqdm import tqdm
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import database modules
from database.database_pro_teams import DotaDatabase, populate_from_json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Path to team games directory
TEAM_GAMES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'teams_games'))

def verify_database():
    """Check and report on the database population."""
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Get counts from each table using SQLAlchemy
        tables = {
            'Matches': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_matches").scalar()),
            'Leagues': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_leagues").scalar()),
            'Teams': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_teams").scalar()),
            'Players': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_players").scalar()),
            'Heroes': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_heroes").scalar()),
            'Match Players': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_match_player_metrics").scalar()),
            'Draft Timings': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_draft_timings").scalar()),
            'Team Fights': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_teamfights").scalar()),
            'Team Fight Players': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_teamfight_players").scalar()),
            'Objectives': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_objectives").scalar()),
            'Chat Wheel': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_chatwheel").scalar()),
            'Time vs Stats': db.Session().query(db.engine.execute("SELECT COUNT(*) FROM pro_timevsstats").scalar())
        }
        
        print("\n" + "=" * 40)
        print("DATABASE POPULATION SUMMARY")
        print("=" * 40)
        for table_name, count in tables.items():
            print(f"{table_name}: {count}")
        print("=" * 40)
        
        return tables['Matches']
    except Exception as e:
        logger.error(f"Error verifying database: {str(e)}")
        return 0
    finally:
        session.close()

def get_existing_match_ids():
    """Get match IDs that are already in the database."""
    db = DotaDatabase()
    
    try:
        # Use raw SQL to get all match_ids (faster than ORM for large tables)
        result = db.engine.execute("SELECT match_id FROM pro_matches")
        return {row[0] for row in result}
    except Exception as e:
        logger.error(f"Error getting existing match IDs: {str(e)}")
        return set()

def find_match_files():
    """Find all match JSON files in team-specific folders."""
    if not os.path.exists(TEAM_GAMES_DIR):
        logger.error(f"Team games directory not found: {TEAM_GAMES_DIR}")
        return []
    
    # Use glob to find all match_*.json files in all subdirectories
    match_files = []
    for team_dir in os.listdir(TEAM_GAMES_DIR):
        team_path = os.path.join(TEAM_GAMES_DIR, team_dir)
        if os.path.isdir(team_path):
            # Find all match_*.json files in the team directory
            team_match_files = glob.glob(os.path.join(team_path, "match_*.json"))
            match_files.extend(team_match_files)
    
    return match_files

def import_match_files():
    """
    Import all downloaded match files that aren't already in the database.
    Uses the existing populate_from_json function to add match data.
    """
    # Get all match files
    match_files = find_match_files()
    logger.info(f"Found {len(match_files)} total match files")
    
    # Get existing match IDs
    existing_match_ids = get_existing_match_ids()
    logger.info(f"Found {len(existing_match_ids)} existing matches in database")
    
    # Filter out already processed matches by extracting match_id from filename
    new_match_files = []
    for file_path in match_files:
        try:
            # Extract match_id from filename (format: match_<match_id>.json)
            filename = os.path.basename(file_path)
            match_id = int(filename.split('_')[1].split('.')[0])
            
            if match_id not in existing_match_ids:
                new_match_files.append(file_path)
        except Exception as e:
            logger.error(f"Error parsing match ID from {file_path}: {str(e)}")
    
    if not new_match_files:
        logger.info("No new matches to process. Database is up to date.")
        return 0
    
    logger.info(f"Found {len(new_match_files)} new match files to process")
    
    # Process each match file
    matches_processed = 0
    for json_path in tqdm(new_match_files, desc="Importing matches"):
        try:
            # Use the existing function to populate the database
            populate_from_json(json_path)
            matches_processed += 1
        except Exception as e:
            logger.error(f"Error importing match data from {json_path}: {str(e)}")
    
    return matches_processed

def main():
    """Main function."""
    print("=" * 80)
    print("PROCESSING DOWNLOADED MATCHES (WITHOUT CLEARING EXISTING DATA)")
    print("=" * 80)
    
    # Verify current database state
    print("\nSTEP 1: Checking current database state")
    current_match_count = verify_database()
    
    # Import new matches
    print("\nSTEP 2: Importing new matches to database")
    matches_imported = import_match_files()
    
    # Final verification
    print("\nSTEP 3: Verifying final database state")
    final_match_count = verify_database()
    
    # Report results
    print(f"\nProcessed {matches_imported} new matches.")
    print(f"Previous match count: {current_match_count}")
    print(f"Current match count: {final_match_count}")
    print(f"Difference: {final_match_count - current_match_count} matches added")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
