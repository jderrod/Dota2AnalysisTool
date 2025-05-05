#!/usr/bin/env python3
"""
Script to populate the hero names in the pro_heroes table.
Uses the OpenDota API to get the correct hero names for each hero ID.
"""

import os
import sys
import logging
import requests
import time
from sqlalchemy import text

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import database modules
from database.database_pro_teams import DotaDatabase, ProHero

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("populate_hero_names.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
OPENDOTA_API_BASE = "https://api.opendota.com/api"
OPENDOTA_API_KEY = "6f146503-15ad-497e-9f97-35a6b7d74a31"  # Using your provided API key
DOTA_HEROES_ENDPOINT = "/heroes"

def get_heroes_from_api():
    """
    Get the list of heroes from the OpenDota API
    
    Returns:
        dict: Mapping of hero_id to hero_name
    """
    url = f"{OPENDOTA_API_BASE}{DOTA_HEROES_ENDPOINT}"
    params = {"api_key": OPENDOTA_API_KEY}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise error for bad status codes
        
        heroes = response.json()
        logger.info(f"Retrieved {len(heroes)} heroes from OpenDota API")
        
        # Create a mapping of hero_id to localized name
        hero_map = {hero["id"]: hero["localized_name"] for hero in heroes}
        return hero_map
        
    except Exception as e:
        logger.error(f"Error fetching heroes from OpenDota API: {e}")
        return {}

def populate_hero_names():
    """
    Update the pro_heroes table with proper hero names
    """
    # Get heroes from API
    hero_map = get_heroes_from_api()
    if not hero_map:
        logger.error("Failed to get heroes from API, aborting")
        return
    
    # Connect to database
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Get the distinct hero IDs from the database
        logger.info("Fetching heroes from database")
        result = session.execute(text("SELECT DISTINCT hero_id FROM pro_heroes"))
        db_heroes = [row[0] for row in result.fetchall()]
        logger.info(f"Found {len(db_heroes)} distinct heroes in database")
        
        # Count heroes with missing names
        missing_name_query = "SELECT COUNT(*) FROM pro_heroes WHERE name IS NULL OR name = '' OR name LIKE 'Hero %'"
        missing_count = session.execute(text(missing_name_query)).scalar()
        logger.info(f"Found {missing_count} heroes with missing names")
        
        # Update the hero names
        updated_count = 0
        for hero_id in db_heroes:
            if hero_id in hero_map:
                hero_name = hero_map[hero_id]
                
                # Update the hero name in the database
                update_query = text("""
                    UPDATE pro_heroes
                    SET name = :name
                    WHERE hero_id = :hero_id AND (name IS NULL OR name = '' OR name LIKE 'Hero %')
                """)
                
                result = session.execute(update_query, {"name": hero_name, "hero_id": hero_id})
                updated_count += result.rowcount
        
        # Commit the changes
        session.commit()
        logger.info(f"Updated {updated_count} hero entries with proper names")
        
        # Verify the updates
        after_count = session.execute(text(missing_name_query)).scalar()
        logger.info(f"Heroes still missing names after update: {after_count}")
        
        # Display some sample heroes to verify
        sample_query = "SELECT hero_id, name FROM pro_heroes GROUP BY hero_id, name LIMIT 20"
        samples = session.execute(text(sample_query)).fetchall()
        
        print("\nSample of Hero Names in Database:")
        print("---------------------------------")
        for hero_id, name in samples:
            print(f"Hero ID: {hero_id}, Name: {name}")
        
        print(f"\nUpdated {updated_count} hero entries.")
        print(f"Heroes still missing names: {after_count}")
        
    except Exception as e:
        logger.error(f"Error updating hero names: {e}")
        session.rollback()
    finally:
        session.close()

def main():
    print("\n========================================")
    print("HERO NAME POPULATION SCRIPT")
    print("========================================\n")
    
    populate_hero_names()
    
    print("\nDone!")

if __name__ == "__main__":
    main()
