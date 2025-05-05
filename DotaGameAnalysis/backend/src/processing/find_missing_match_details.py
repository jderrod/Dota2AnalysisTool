#!/usr/bin/env python
"""
Script to find match IDs that do not have associated match details files,
and optionally fetch those missing match details using the OpenDota API.
"""
import os
import json
import glob
import logging
import argparse
from datetime import datetime
from scraper import DotaMatchScraper
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("missing_matches.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_match_ids_from_summary_files():
    """
    Extract all match IDs from the summary JSON files.
    
    Returns:
        set: Set of match IDs from summary files
    """
    match_ids = set()
    summary_files = glob.glob(os.path.join('data', 'raw', 'recent_pro_matches_*.json'))
    summary_files.extend(glob.glob(os.path.join('data', 'raw', 'pro_matches_*.json')))
    
    logger.info(f"Found {len(summary_files)} summary files to check")
    
    for file_path in summary_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                matches = json.load(f)
                
                if isinstance(matches, list):
                    for match in matches:
                        if 'match_id' in match:
                            match_ids.add(match['match_id'])
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    logger.info(f"Extracted {len(match_ids)} unique match IDs from summary files")
    return match_ids

def get_existing_detail_files():
    """
    Get the set of match IDs that already have detail files.
    
    Returns:
        set: Set of match IDs that have detail files
    """
    detail_files = set()
    matches_dir = os.path.join('data', 'raw', 'matches')
    
    if not os.path.exists(matches_dir):
        logger.warning(f"Matches directory {matches_dir} does not exist")
        return detail_files
    
    for file_name in os.listdir(matches_dir):
        if file_name.startswith('match_') and file_name.endswith('.json'):
            try:
                match_id = int(file_name.replace('match_', '').replace('.json', ''))
                detail_files.add(match_id)
            except ValueError:
                logger.warning(f"Could not parse match ID from {file_name}")
    
    logger.info(f"Found {len(detail_files)} existing match detail files")
    return detail_files

def find_missing_match_details():
    """
    Find match IDs that don't have corresponding detail files.
    
    Returns:
        list: List of missing match IDs
    """
    # Get all match IDs from summary files
    all_match_ids = load_match_ids_from_summary_files()
    
    # Get match IDs with existing detail files
    existing_detail_ids = get_existing_detail_files()
    
    # Find missing match details
    missing_match_ids = all_match_ids - existing_detail_ids
    
    return sorted(list(missing_match_ids))

def fetch_missing_match_details(missing_match_ids, api_key=None):
    """
    Fetch details for missing matches using the OpenDota API.
    
    Args:
        missing_match_ids (list): List of match IDs to fetch
        api_key (str, optional): OpenDota API key. Recommended for premium access.
        
    Returns:
        int: Number of successfully fetched matches
    """
    if not missing_match_ids:
        logger.info("No missing matches to fetch")
        return 0
        
    logger.info(f"Fetching details for {len(missing_match_ids)} missing matches")
    
    # Initialize scraper with API key (if provided)
    scraper = DotaMatchScraper(api_key=api_key)
    
    # Define target directory for match details
    matches_dir = os.path.join('data', 'raw', 'matches')
    os.makedirs(matches_dir, exist_ok=True)
    
    # Keep track of success count
    success_count = 0
    
    # Fetch each match detail individually
    for match_id in tqdm(missing_match_ids, desc="Fetching missing match details"):
        # Skip if we already have this match (shouldn't happen, but just in case)
        if os.path.exists(os.path.join(matches_dir, f"match_{match_id}.json")):
            logger.debug(f"Match {match_id} already exists, skipping")
            continue
            
        # Get match details
        match_details = scraper.get_match_details(match_id)
        
        if match_details:
            # Save the match details
            file_path = os.path.join(matches_dir, f"match_{match_id}.json")
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(match_details, f, indent=2)
                
            success_count += 1
        else:
            logger.warning(f"Failed to fetch details for match {match_id}")
    
    return success_count

def main():
    """Main function to find and optionally fetch missing match details."""
    parser = argparse.ArgumentParser(description="Find and fetch missing match details")
    parser.add_argument("--fetch", action="store_true", help="Fetch missing match details")
    parser.add_argument("--api-key", type=str, help="OpenDota API key (recommended for premium access)")
    args = parser.parse_args()
    
    logger.info("Starting missing match details search")
    
    # Find missing match details
    missing_match_ids = find_missing_match_details()
    
    if missing_match_ids:
        logger.info(f"Found {len(missing_match_ids)} matches without detail files")
        
        # Save missing match IDs to a file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"missing_match_ids_{timestamp}.txt"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            for match_id in missing_match_ids:
                f.write(f"{match_id}\n")
        
        logger.info(f"Saved missing match IDs to {output_file}")
        
        # Print the missing match IDs
        print(f"Missing match details for {len(missing_match_ids)} matches:")
        for match_id in missing_match_ids[:10]:  # Just show the first 10 for brevity
            print(match_id)
            
        if len(missing_match_ids) > 10:
            print(f"... and {len(missing_match_ids) - 10} more (see {output_file} for the full list)")
        
        # Fetch missing match details if requested
        if args.fetch:
            logger.info("Fetching missing match details")
            success_count = fetch_missing_match_details(missing_match_ids, api_key=args.api_key)
            logger.info(f"Successfully fetched {success_count} of {len(missing_match_ids)} missing match details")
            
            # Check if any matches are still missing
            if success_count < len(missing_match_ids):
                logger.warning(f"Failed to fetch {len(missing_match_ids) - success_count} match details")
                print(f"Failed to fetch {len(missing_match_ids) - success_count} match details. Check the log for details.")
            else:
                logger.info("All missing match details have been fetched successfully")
                print("All missing match details have been fetched successfully!")
    else:
        logger.info("No missing match details found")
        print("No missing match details found. All matches have detail files.")

if __name__ == "__main__":
    main()
