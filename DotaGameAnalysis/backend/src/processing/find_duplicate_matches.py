#!/usr/bin/env python
"""
Script to identify duplicate match entries in the database.

This script checks for duplicate match_id values in the matches table
and prints out any match IDs that have multiple entries.
"""
import os
import sys
import logging
from datetime import datetime
from sqlalchemy import create_engine, func, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from database import DotaDatabase, Match

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("duplicate_matches.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def find_duplicate_matches():
    """
    Identify duplicate match entries in the database.
    
    Returns:
        dict: Dictionary mapping match_ids to their count of occurrences
    """
    # Initialize database connection
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Query for match_ids that appear more than once
        # Using SQLAlchemy's group_by and having clauses
        duplicate_matches_query = (
            session.query(
                Match.match_id,
                func.count(Match.match_id).label('count')
            )
            .group_by(Match.match_id)
            .having(func.count(Match.match_id) > 1)
        )
        
        # Execute query and convert to dictionary for easy access
        duplicate_matches = {row.match_id: row.count for row in duplicate_matches_query}
        
        return duplicate_matches
        
    except Exception as e:
        logger.error(f"Error finding duplicate matches: {e}")
        return {}
    finally:
        session.close()

def find_duplicate_records():
    """
    Find and print detailed information about duplicate match records.
    
    This function not only identifies which match_ids have duplicates,
    but also retrieves all the duplicate records to show their primary keys
    and creation timestamps.
    """
    # Initialize database connection
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # First identify which match_ids have duplicates
        duplicate_matches = find_duplicate_matches()
        
        if not duplicate_matches:
            logger.info("No duplicate matches found in the database.")
            print("No duplicate matches found in the database.")
            return
            
        logger.info(f"Found {len(duplicate_matches)} match IDs with duplicate records")
        print(f"Found {len(duplicate_matches)} match IDs with duplicate records:")
        
        # Now get details for each duplicate match
        total_duplicates = 0
        
        # Create a timestamped file to save the results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"duplicate_matches_{timestamp}.txt"
        
        with open(output_file, 'w') as f:
            for match_id, count in duplicate_matches.items():
                # Get all records for this match_id
                duplicate_records = session.query(Match).filter_by(match_id=match_id).all()
                
                # Write to file and console
                msg = f"Match ID {match_id} has {count} duplicate entries with database IDs: "
                msg += ", ".join([str(record.id) for record in duplicate_records])
                
                print(msg)
                f.write(msg + "\n")
                
                # For each record, show more details
                for i, record in enumerate(duplicate_records):
                    record_details = (
                        f"  Record {i+1}: DB ID={record.id}, "
                        f"Created={record.created_at}, "
                        f"Updated={record.updated_at}"
                    )
                    f.write(record_details + "\n")
                
                total_duplicates += count - 1  # Subtract 1 as one record is valid
        
        logger.info(f"Total of {total_duplicates} duplicate records found across {len(duplicate_matches)} match IDs")
        print(f"\nTotal duplicate records: {total_duplicates}")
        print(f"Results saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error processing duplicate matches: {e}")
        print(f"Error: {e}")
    finally:
        session.close()

def main():
    """Main function to run the duplicate match finder."""
    logger.info("Starting duplicate match search")
    
    try:
        # Check if database file exists
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        db_file = os.path.join(data_dir, 'dota_matches.db')
        
        if not os.path.exists(db_file):
            logger.error(f"Database file not found: {db_file}")
            print(f"Error: Database file not found at {db_file}")
            sys.exit(1)
            
        # Find and display duplicate matches
        find_duplicate_records()
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        
    logger.info("Duplicate match search completed")

if __name__ == "__main__":
    main()
