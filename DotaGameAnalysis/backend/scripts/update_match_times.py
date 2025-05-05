"""
Update Match Times Script

This script updates all existing match records in the database to use the correct start_time
from their corresponding JSON files instead of the time they were added to the database.
"""
import os
import json
import logging
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add the parent directory to the Python path so we can import from backend
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.database_pro_teams import ProMatch, DotaDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("update_match_times.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def update_match_times_from_json():
    """
    Update all match times in the database from their corresponding JSON files.
    """
    # Connect to the database
    db = DotaDatabase()
    Session = db.Session
    session = Session()
    
    # Get the path to the match JSON files directory
    matches_dir = os.path.abspath(os.path.join(
        os.path.dirname(__file__), 
        '..', 
        'data',
        'raw',
        'matches'
    ))
    
    if not os.path.exists(matches_dir):
        logger.error(f"Matches directory not found: {matches_dir}")
        return
    
    logger.info(f"Scanning for match JSON files in: {matches_dir}")
    
    # Get all matches from the database
    all_matches = session.query(ProMatch).all()
    match_ids = {match.match_id: match for match in all_matches}
    
    logger.info(f"Found {len(match_ids)} matches in the database")
    
    # Keep track of updated matches
    updated_count = 0
    not_found_count = 0
    no_timestamp_count = 0
    
    # Scan through all JSON files in the matches directory
    for filename in os.listdir(matches_dir):
        if filename.startswith("match_") and filename.endswith(".json"):
            json_path = os.path.join(matches_dir, filename)
            
            try:
                # Load the JSON file
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                # Get the match ID and start time from the JSON
                match_id = data.get("match_id")
                start_time_unix = data.get("start_time")
                
                if not match_id:
                    logger.warning(f"No match_id found in JSON file: {filename}")
                    continue
                
                # Find the match in the database
                if match_id in match_ids:
                    match_record = match_ids[match_id]
                    
                    if start_time_unix:
                        # Convert Unix timestamp to datetime
                        start_time = datetime.fromtimestamp(start_time_unix)
                        
                        # Update the match start time
                        old_time = match_record.start_time
                        match_record.start_time = start_time
                        
                        logger.info(f"Updated match {match_id} start_time from {old_time} to {start_time}")
                        updated_count += 1
                    else:
                        logger.warning(f"No start_time found in JSON for match {match_id}")
                        no_timestamp_count += 1
                else:
                    logger.warning(f"Match {match_id} found in JSON but not in database")
                    not_found_count += 1
            
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
    
    # Commit the changes to the database
    try:
        session.commit()
        logger.info(f"Successfully updated {updated_count} match times in the database")
        if not_found_count > 0:
            logger.warning(f"{not_found_count} matches found in JSON files but not in database")
        if no_timestamp_count > 0:
            logger.warning(f"{no_timestamp_count} matches had no timestamp in their JSON files")
    except Exception as e:
        session.rollback()
        logger.error(f"Error committing changes to database: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    logger.info("Starting match times update process")
    update_match_times_from_json()
    logger.info("Match times update process completed")
