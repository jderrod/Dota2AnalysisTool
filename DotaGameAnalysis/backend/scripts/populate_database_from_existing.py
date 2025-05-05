"""
Populate Database From Existing Matches

This script takes the already downloaded match JSON files and populates the database tables,
skipping the download phase. It's used when you've already downloaded matches but hit API rate limits.
"""
import os
import sys
import logging
import time
from datetime import datetime

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the database population functions
from scripts.scrape_top_teams_matches import populate_database_from_jsons, clear_database, verify_database_population
from database.database_pro_teams import DotaDatabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("populate_database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def count_match_files():
    """Count how many match files we have downloaded."""
    data_dir = os.path.abspath(os.path.join(
        os.path.dirname(__file__), 
        '..', 
        'data',
        'raw',
        'matches'
    ))
    
    if not os.path.exists(data_dir):
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Matches directory not found: {data_dir}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Matches directory not found: {data_dir}")
        return 0
    
    match_files = [f for f in os.listdir(data_dir) if f.startswith("match_") and f.endswith(".json")]
    return len(match_files)

def main():
    """
    Main function to clear the database and populate it with the already downloaded match files.
    """
    start_time = time.time()
    print(f"\n{'='*80}\n[{datetime.now().strftime('%H:%M:%S')}] POPULATING DATABASE FROM EXISTING MATCH FILES\n{'='*80}")
    
    # Count how many match files we have
    match_count = count_match_files()
    if match_count == 0:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] No match files found. Exiting.")
        return
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {match_count} match files to process")
    
    # Clear the database
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Clearing database...")
    clear_database()
    
    # Populate the database
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Populating database from downloaded match data...")
    populate_database_from_jsons()
    
    # Report completion
    elapsed_time = time.time() - start_time
    elapsed_minutes = int(elapsed_time // 60)
    elapsed_seconds = int(elapsed_time % 60)
    
    print(f"\n{'='*80}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] DATABASE POPULATION COMPLETED IN {elapsed_minutes}m {elapsed_seconds}s")
    print(f"Processed {match_count} matches")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
