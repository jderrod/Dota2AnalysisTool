#!/usr/bin/env python
"""
Process Team Games Time vs Stats

This script processes all team game JSON files and adds their time series data
to the ProTimevsStats table.
"""

import os
import sys
import json
import logging
import argparse
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add the parent directory to sys.path to import the database modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the database module and needed functions
from database_pro_teams import DotaDatabase, populate_time_vs_stats, Base, ProTimevsStats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("time_vs_stats_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("time_vs_stats_processor")

def clear_existing_time_vs_stats(session):
    """
    Clear all existing entries in the ProTimevsStats table.
    This is useful if you want to rebuild the entire table from scratch.
    
    Args:
        session: SQLAlchemy session object
    """
    try:
        logger.info("Clearing existing time vs stats data...")
        count = session.query(ProTimevsStats).delete()
        session.commit()
        logger.info(f"Deleted {count} existing time vs stats entries.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error clearing time vs stats table: {e}")
        raise

def process_team_games_directory(directory_path, clear_existing=False, batch_size=100):
    """
    Process all team game JSON files in the specified directory and its subdirectories.
    
    Args:
        directory_path (str): Path to the directory containing team game JSON files
        clear_existing (bool): Whether to clear existing entries before processing
        batch_size (int): Number of files to process before committing to avoid memory issues
    
    Returns:
        tuple: (processed_count, error_count)
    """
    # Create a database instance
    db = DotaDatabase()
    session = db.Session()
    
    # Ensure the tables exist
    Base.metadata.create_all(db.engine)
    
    # Clear existing data if requested
    if clear_existing:
        clear_existing_time_vs_stats(session)
    
    processed_count = 0
    error_count = 0
    start_time = time.time()
    
    # Check if the directory exists
    if not os.path.exists(directory_path):
        logger.error(f"Directory not found: {directory_path}")
        return (0, 0)
    
    # Get list of all JSON files in the directory and its subdirectories
    json_files = []
    logger.info(f"Searching for JSON files in {directory_path}...")
    
    for dirpath, dirnames, filenames in os.walk(directory_path):
        for filename in filenames:
            if filename.endswith(".json"):
                json_files.append(os.path.join(dirpath, filename))
    
    total_files = len(json_files)
    logger.info(f"Found {total_files} JSON files to process")
    
    # Process each JSON file
    for i, json_file in enumerate(json_files):
        try:
            logger.info(f"Processing file {i+1}/{total_files}: {json_file}")
            populate_time_vs_stats(json_file)
            processed_count += 1
            
            # Log progress periodically
            if (i + 1) % batch_size == 0:
                elapsed_time = time.time() - start_time
                rate = (i + 1) / elapsed_time if elapsed_time > 0 else 0
                remaining = (total_files - (i + 1)) / rate if rate > 0 else "unknown"
                logger.info(f"Progress: {i+1}/{total_files} files processed ({(i+1)/total_files*100:.1f}%)")
                logger.info(f"Processing rate: {rate:.2f} files/sec, Estimated time remaining: {remaining:.1f} seconds")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Error processing {json_file}: {e}")
    
    # Log summary
    elapsed_time = time.time() - start_time
    logger.info(f"Processing complete. Time elapsed: {elapsed_time:.2f} seconds")
    logger.info(f"Successfully processed {processed_count} files with {error_count} errors")
    
    return (processed_count, error_count)

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Process team games and add time vs stats data to the database.')
    parser.add_argument('--directory', type=str, required=True, 
                        help='Directory containing team game JSON files to process')
    parser.add_argument('--clear-existing', action='store_true',
                        help='Clear existing time vs stats data before processing')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of files to process before logging progress')
    
    args = parser.parse_args()
    
    logger.info(f"Starting time vs stats processing from directory: {args.directory}")
    logger.info(f"Clear existing data: {args.clear_existing}")
    
    processed_count, error_count = process_team_games_directory(
        args.directory, 
        args.clear_existing,
        args.batch_size
    )
    
    logger.info(f"Processing complete. Processed {processed_count} files with {error_count} errors.")
    
    # Return non-zero exit code if there were errors
    return 1 if error_count > 0 else 0

if __name__ == "__main__":
    sys.exit(main())
