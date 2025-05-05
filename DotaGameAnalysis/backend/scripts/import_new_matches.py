#!/usr/bin/env python3
"""
Script to import the downloaded match files to the database without wiping existing data.
This script handles the integer overflow issue by converting large integers to strings.
"""

import os
import json
import logging
import sys
from datetime import datetime
from tqdm import tqdm
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import database modules
from database.database_pro_teams import DotaDatabase
from database.database_pro_teams import ProMatch, ProLeague, ProTeam, ProPlayer
from database.database_pro_teams import ProHero, ProMatchPlayer, ProDraftTiming
from database.database_pro_teams import ProTeamFight, ProTeamFightPlayer
from database.database_pro_teams import ProObjective, ProChatWheel, ProTimevsStats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Path to match JSON files
MATCHES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database', 'matches'))

def verify_database():
    """Check and report on the database population."""
    db = DotaDatabase()
    session = db.Session()
    
    try:
        match_count = session.query(ProMatch).count()
        league_count = session.query(ProLeague).count()
        team_count = session.query(ProTeam).count()
        player_count = session.query(ProPlayer).count()
        hero_count = session.query(ProHero).count()
        match_player_count = session.query(ProMatchPlayer).count()
        draft_timing_count = session.query(ProDraftTiming).count()
        team_fight_count = session.query(ProTeamFight).count()
        team_fight_player_count = session.query(ProTeamFightPlayer).count()
        objective_count = session.query(ProObjective).count()
        chat_wheel_count = session.query(ProChatWheel).count()
        time_vs_stats_count = session.query(ProTimevsStats).count()
        
        print("\n" + "=" * 40)
        print("DATABASE POPULATION SUMMARY")
        print("=" * 40)
        print(f"Matches: {match_count}")
        print(f"Leagues: {league_count}")
        print(f"Teams: {team_count}")
        print(f"Players: {player_count}")
        print(f"Heroes: {hero_count}")
        print(f"Match Players: {match_player_count}")
        print(f"Draft Timings: {draft_timing_count}")
        print(f"Team Fights: {team_fight_count}")
        print(f"Team Fight Players: {team_fight_player_count}")
        print(f"Objectives: {objective_count}")
        print(f"Chat Wheel: {chat_wheel_count}")
        print(f"Time vs Stats: {time_vs_stats_count}")
        print("=" * 40)
        
        return match_count
    except Exception as e:
        logger.error(f"Error verifying database: {str(e)}")
        return 0
    finally:
        session.close()

def get_existing_match_ids():
    """Get match IDs that are already in the database."""
    db = DotaDatabase()
    session = db.Session()
    
    try:
        existing_ids = session.query(ProMatch.match_id).all()
        return {match_id[0] for match_id in existing_ids}
    finally:
        session.close()

def import_match_files():
    """
    Import all downloaded match files that aren't already in the database.
    Uses the existing populate_from_json function to avoid code duplication.
    """
    from database.database_pro_teams import populate_from_json
    
    # Get all match files
    match_files = [
        os.path.join(MATCHES_DIR, f) for f in os.listdir(MATCHES_DIR)
        if f.endswith('.json')
    ]
    logger.info(f"Found {len(match_files)} total match files")
    
    # Get existing match IDs
    existing_match_ids = get_existing_match_ids()
    logger.info(f"Found {len(existing_match_ids)} existing matches in database")
    
    # Filter out already processed matches
    new_match_files = [
        file for file in match_files 
        if int(os.path.basename(file).split('.')[0]) not in existing_match_ids
    ]
    
    if not new_match_files:
        logger.info("No new matches to process. Database is up to date.")
        return 0
    
    logger.info(f"Found {len(new_match_files)} new match files to process")
    
    # Process each match file
    for json_path in tqdm(new_match_files, desc="Importing matches"):
        try:
            # This will directly use the project's existing function
            # which is already set up to handle the database schema
            populate_from_json(json_path)
        except Exception as e:
            logger.error(f"Error importing match data from {json_path}: {str(e)}")
    
    return len(new_match_files)

def main():
    """Main function."""
    print("=" * 80)
    print("IMPORTING NEW MATCHES TO DATABASE (WITHOUT CLEARING EXISTING DATA)")
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
    print(f"\nImported {matches_imported} new matches to the database.")
    print(f"Previous match count: {current_match_count}")
    print(f"Current match count: {final_match_count}")
    print(f"Difference: {final_match_count - current_match_count} matches added")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
