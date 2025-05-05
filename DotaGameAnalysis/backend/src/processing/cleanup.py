"""
Database and file cleanup utility for the Dota 2 Professional Match Analysis System.

This script removes the database file, checkpoints, and all cached match data
to ensure a clean state before running a fresh data scrape.
"""
import os
import shutil
import logging
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def cleanup():
    """Remove all database files, cached matches, and checkpoints."""
    # Define paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, 'data')
    raw_dir = os.path.join(data_dir, 'raw')
    matches_dir = os.path.join(raw_dir, 'matches')
    checkpoint_file = os.path.join(data_dir, 'checkpoint.json')
    database_file = os.path.join(base_dir, 'dota.db')
    
    # Delete database
    if os.path.exists(database_file):
        try:
            os.remove(database_file)
            logger.info(f"Removed database file: {database_file}")
        except Exception as e:
            logger.error(f"Failed to remove database file: {e}")
    else:
        logger.info("No database file found to remove")
    
    # Delete checkpoint file
    if os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
            logger.info(f"Removed checkpoint file: {checkpoint_file}")
        except Exception as e:
            logger.error(f"Failed to remove checkpoint file: {e}")
    else:
        logger.info("No checkpoint file found to remove")
    
    # Delete match files in data/raw
    if os.path.exists(raw_dir):
        json_files = [f for f in os.listdir(raw_dir) if f.endswith('.json')]
        for file in json_files:
            try:
                os.remove(os.path.join(raw_dir, file))
                logger.info(f"Removed data file: {file}")
            except Exception as e:
                logger.error(f"Failed to remove data file {file}: {e}")
    else:
        logger.info("No raw data directory found")
    
    # Delete individual match files
    if os.path.exists(matches_dir):
        try:
            shutil.rmtree(matches_dir)
            logger.info(f"Removed matches directory: {matches_dir}")
        except Exception as e:
            logger.error(f"Failed to remove matches directory: {e}")
    else:
        logger.info("No matches directory found to remove")
    
    # Recreate empty directories
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(matches_dir, exist_ok=True)
    
    logger.info("Cleanup complete! The system is ready for a fresh data scrape.")

if __name__ == "__main__":
    cleanup()
