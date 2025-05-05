"""
Continue Scraping Pro Matches

This script continues scraping pro matches from where we left off, adding to the existing database.
It implements proper rate limiting and will scrape up to a target number of additional matches.
"""
import os
import sys
import json
import logging
import time
from datetime import datetime
import sqlite3
from tqdm import tqdm
import requests

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import backend modules
from database.database_pro_teams import DotaDatabase
from database.database_pro_teams import ProMatch, ProLeague, ProTeam
from src.data.scraper import DotaMatchScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("continue_scraping.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Settings
TARGET_ADDITIONAL_MATCHES = 400  # Target number of additional matches to scrape
MAX_API_BATCHES = 200  # Maximum number of API batch calls
RATE_LIMIT_WAIT = 10  # Seconds to wait between API calls if approaching rate limit
MAX_RETRIES = 3  # Maximum number of retries for API calls

# OpenDota API constants
OPENDOTA_API_BASE_URL = "https://api.opendota.com/api"

def get_last_match_id():
    """
    Get the ID of the last match in the database to use as a starting point.
    
    Returns:
        int: The match_id of the last match in the database, or None if no matches
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Get the minimum match_id from the database (matches were added in descending order)
        min_match = session.query(ProMatch.match_id).order_by(ProMatch.match_id.asc()).first()
        if min_match:
            return min_match[0]
        return None
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error getting last match ID: {str(e)}")
        return None
    finally:
        session.close()

def get_top_teams():
    """
    Get the IDs of top teams from the database to filter matches.
    
    Returns:
        list: List of team_ids for top teams
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Get all team IDs from the database
        teams = session.query(ProTeam.team_id, ProTeam.name).all()
        return [(team_id, name) for team_id, name in teams]
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error getting team IDs: {str(e)}")
        return []
    finally:
        session.close()

def continue_downloading_matches(starting_match_id, top_teams, target_count=TARGET_ADDITIONAL_MATCHES):
    """
    Continue downloading matches from OpenDota API, starting from a specific match ID.
    
    Args:
        starting_match_id (int): Match ID to start from
        top_teams (list): List of tuples (team_id, name) for filtering
        target_count (int): Target number of matches to download
        
    Returns:
        list: List of match IDs that were downloaded
    """
    scraper = DotaMatchScraper()
    data_dir = os.path.abspath(os.path.join(
        os.path.dirname(__file__), 
        '..', 
        'data',
        'raw',
        'matches'
    ))
    os.makedirs(data_dir, exist_ok=True)
    
    # Extract team IDs for efficient lookup
    team_ids = [team[0] for team in top_teams]
    team_names = {team[0]: team[1] for team in top_teams}
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Starting match scraping from match_id < {starting_match_id}")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting match scraping from match_id < {starting_match_id}")
    print(f"Target: {target_count} additional matches for top teams")
    
    # Tracking variables
    matches_to_download = []
    last_match_id = starting_match_id
    batch_num = 1
    team_match_counts = {team_id: 0 for team_id in team_ids}
    consecutive_empty_batches = 0
    rate_limit_hits = 0
    
    # Create a progress bar for match fetching
    pbar = tqdm(total=target_count, desc=f"Fetching additional matches", ncols=100)
    
    # Loop until we have enough matches or reach the batch limit
    while len(matches_to_download) < target_count and batch_num <= MAX_API_BATCHES:
        # Respect rate limits
        if rate_limit_hits > 0:
            wait_time = min(RATE_LIMIT_WAIT * rate_limit_hits, 60)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Rate limit hit, waiting {wait_time}s before retrying...")
            time.sleep(wait_time)
        
        # Prepare API parameters
        params = {'less_than_match_id': last_match_id, 'limit': 100}
        
        # Make the API call with retry logic
        success = False
        retry_count = 0
        pro_matches = []
        
        while not success and retry_count < MAX_RETRIES:
            try:
                # Fetch pro matches
                logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] API Call: Requesting batch #{batch_num} (matches < {last_match_id})")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] API Call: Requesting batch #{batch_num} (matches < {last_match_id})...")
                
                batch_start_time = time.time()
                pro_matches = scraper.get_pro_matches(**params)
                batch_duration = time.time() - batch_start_time
                
                if pro_matches is not None:
                    success = True
                    rate_limit_hits = 0  # Reset rate limit counter on success
                else:
                    # Handle possible rate limit
                    retry_count += 1
                    rate_limit_hits += 1
                    logger.warning(f"[{datetime.now().strftime('%H:%M:%S')}] API returned None, possible rate limit. Retry {retry_count}/{MAX_RETRIES}")
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] API returned None, possible rate limit. Retry {retry_count}/{MAX_RETRIES}")
                    time.sleep(RATE_LIMIT_WAIT * retry_count)  # Exponential backoff
            except Exception as e:
                retry_count += 1
                rate_limit_hits += 1
                logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error requesting batch #{batch_num}: {str(e)}. Retry {retry_count}/{MAX_RETRIES}")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Error requesting batch #{batch_num}: {str(e)}. Retry {retry_count}/{MAX_RETRIES}")
                time.sleep(RATE_LIMIT_WAIT * retry_count)  # Exponential backoff
        
        if not success:
            logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to get batch #{batch_num} after {MAX_RETRIES} retries.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to get batch #{batch_num} after {MAX_RETRIES} retries.")
            break
            
        if not pro_matches or len(pro_matches) == 0:
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] No matches returned from API. Ending search.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] No matches returned from API. Ending search.")
            consecutive_empty_batches += 1
            
            if consecutive_empty_batches >= 3:
                logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Received {consecutive_empty_batches} empty batches in a row. Ending search.")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Received {consecutive_empty_batches} empty batches in a row. Ending search.")
                break
                
            # Try to continue with a lower match_id
            if last_match_id > 10000:
                last_match_id -= 10000
                batch_num += 1
                continue
            else:
                break
        else:
            consecutive_empty_batches = 0  # Reset counter
        
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Received {len(pro_matches)} matches in {batch_duration:.2f}s")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Received {len(pro_matches)} matches from API in {batch_duration:.2f}s")
        
        # Filter matches involving any of our top teams
        batch_matches_for_top_teams = []
        for match in pro_matches:
            radiant_team_id = match.get('radiant_team_id')
            dire_team_id = match.get('dire_team_id')
            
            # Check if either team is in our top teams list
            is_top_team_match = False
            match_teams = []
            
            if radiant_team_id in team_ids:
                is_top_team_match = True
                team_match_counts[radiant_team_id] += 1
                match_teams.append(team_names.get(radiant_team_id, f"Team {radiant_team_id}"))
                
            if dire_team_id in team_ids:
                is_top_team_match = True
                team_match_counts[dire_team_id] += 1
                match_teams.append(team_names.get(dire_team_id, f"Team {dire_team_id}"))
            
            if is_top_team_match:
                # Add team names to the match for logging purposes
                match['_teams_involved'] = match_teams
                batch_matches_for_top_teams.append(match)
        
        # Report matches found in this batch
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(batch_matches_for_top_teams)} matches for top teams in batch #{batch_num}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(batch_matches_for_top_teams)} matches for top teams in batch #{batch_num}")
        
        # Add matches to our download list        
        new_matches_to_add = batch_matches_for_top_teams[:target_count - len(matches_to_download)]
        matches_to_download.extend(new_matches_to_add)
        
        # Update progress bar with new matches found
        pbar.update(len(new_matches_to_add))
        
        # Update last_match_id for pagination (whether or not we found matches for our teams)
        if pro_matches:
            last_match_id = pro_matches[-1]['match_id']
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] New pagination ID: {last_match_id}")
        
        # Check if we have enough matches
        if len(matches_to_download) >= target_count:
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Reached target of {target_count} matches. Stopping search.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Reached target of {target_count} matches. Stopping search.")
            break
            
        # Check if we've reached the maximum number of API batches
        if batch_num >= MAX_API_BATCHES:
            logger.warning(f"[{datetime.now().strftime('%H:%M:%S')}] Reached maximum number of API batches ({MAX_API_BATCHES}). Stopping search.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Reached maximum number of API batches ({MAX_API_BATCHES}). Stopping search.")
            break
            
        batch_num += 1
        # Small delay between batches to avoid hitting rate limits
        time.sleep(1)
    
    pbar.close()
    
    # Print match counts per team
    print("\nMatches found per team:")
    for team_id, count in team_match_counts.items():
        if count > 0:  # Only show teams with matches
            print(f"  {team_names.get(team_id, f'Team {team_id}')}: {count} matches")
    
    total_matches = len(matches_to_download)
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Found a total of {total_matches} additional matches for top teams")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Found a total of {total_matches} additional matches for top teams")
    
    if not matches_to_download:
        logger.warning(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: No additional matches found.")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: No additional matches found.")
        return []
    
    # Download full match data
    downloaded_match_ids = []
    
    # Create a progress bar for downloading match details
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting download of {total_matches} match details")
    pbar = tqdm(total=total_matches, desc=f"Downloading match details", ncols=100)
    
    for i, match in enumerate(matches_to_download):
        match_id = match['match_id']
        match_file = os.path.join(data_dir, f"match_{match_id}.json")
        team_info = " vs ".join(match.get('_teams_involved', []))
        
        # Skip if the match file already exists
        if os.path.exists(match_file):
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Match {match_id} already exists ({i+1}/{total_matches})")
            downloaded_match_ids.append(match_id)
            pbar.update(1)
            continue
        
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] API Call: Requesting details for match {match_id} ({i+1}/{total_matches}) {team_info}")
        if i % 5 == 0:  # Print every 5th match to avoid console spam
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Downloading match {match_id} ({i+1}/{total_matches})")
        
        # Get full match details with retry logic
        success = False
        retry_count = 0
        
        while not success and retry_count < MAX_RETRIES:
            try:
                call_start_time = time.time()
                match_details = scraper._make_request(f"matches/{match_id}")
                call_duration = time.time() - call_start_time
                
                if match_details:
                    # Add some OpenDota fields that might be missing in the match details
                    match_details['match_id'] = match_id
                    match_details['radiant_team_id'] = match.get('radiant_team_id')
                    match_details['dire_team_id'] = match.get('dire_team_id')
                    match_details['league_id'] = match.get('league_id')
                    match_details['series_id'] = match.get('series_id')
                    match_details['series_type'] = match.get('series_type')
                    match_details['start_time'] = match.get('start_time')
                    
                    # Save match details to file
                    with open(match_file, 'w', encoding='utf-8') as f:
                        json.dump(match_details, f, indent=2)
                        
                    downloaded_match_ids.append(match_id)
                    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Successfully downloaded match {match_id} in {call_duration:.2f}s")
                    success = True
                    rate_limit_hits = 0  # Reset rate limit counter on success
                else:
                    # Handle possible rate limit
                    retry_count += 1
                    rate_limit_hits += 1
                    logger.warning(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to get details for match {match_id}. Retry {retry_count}/{MAX_RETRIES}")
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to get details for match {match_id}. Retry {retry_count}/{MAX_RETRIES}")
                    time.sleep(RATE_LIMIT_WAIT * retry_count)  # Exponential backoff
            except Exception as e:
                retry_count += 1
                rate_limit_hits += 1
                logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error downloading match {match_id}: {str(e)}. Retry {retry_count}/{MAX_RETRIES}")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Error downloading match {match_id}: {str(e)}. Retry {retry_count}/{MAX_RETRIES}")
                time.sleep(RATE_LIMIT_WAIT * retry_count)  # Exponential backoff
                
        if not success:
            logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to download match {match_id} after {MAX_RETRIES} retries.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to download match {match_id} after {MAX_RETRIES} retries.")
            
        pbar.update(1)
        
        # Check if we're hitting rate limits
        if rate_limit_hits > 0:
            wait_time = min(RATE_LIMIT_WAIT * rate_limit_hits, 60)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Possible rate limit, waiting {wait_time}s before next request...")
            time.sleep(wait_time)
        else:
            # Normal delay between requests
            time.sleep(1)
        
    pbar.close()
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Downloaded {len(downloaded_match_ids)} additional matches")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Downloaded {len(downloaded_match_ids)} additional matches")
    return downloaded_match_ids

def populate_new_matches():
    """
    Populate the database with newly downloaded match data.
    Only processes JSON files that haven't been added to the database yet.
    """
    db = DotaDatabase()
    session = db.Session()
    
    # Get the path to the match JSON files directory
    matches_dir = os.path.abspath(os.path.join(
        os.path.dirname(__file__), 
        '..', 
        'data',
        'raw',
        'matches'
    ))
    
    if not os.path.exists(matches_dir):
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Matches directory not found: {matches_dir}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Matches directory not found: {matches_dir}")
        return
    
    # Get all match JSON files
    match_files = [f for f in os.listdir(matches_dir) if f.startswith("match_") and f.endswith(".json")]
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(match_files)} match JSON files to process")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(match_files)} match JSON files to process")
    
    # Get existing match IDs from database
    existing_match_ids = set()
    try:
        for match_id, in session.query(ProMatch.match_id).all():
            existing_match_ids.add(str(match_id))
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(existing_match_ids)} existing matches in the database")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(existing_match_ids)} existing matches in the database")
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error getting existing match IDs: {str(e)}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Error getting existing match IDs: {str(e)}")
    
    # Filter out match files that are already in the database
    new_match_files = []
    for match_file in match_files:
        match_id = match_file.replace("match_", "").replace(".json", "")
        if match_id not in existing_match_ids:
            new_match_files.append(match_file)
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(new_match_files)} new match files to add to database")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(new_match_files)} new match files to add to database")
    
    if not new_match_files:
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] No new matches to add to database")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] No new matches to add to database")
        return
    
    # Step 1: First pass - Extract and populate teams and leagues
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] First pass: Extracting teams and leagues information")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] First pass: Extracting teams and leagues information")
    team_info = {}
    league_info = {}
    
    # Create a progress bar for the first pass
    pbar_first_pass = tqdm(total=len(new_match_files), desc="Extracting teams and leagues", ncols=100)
    
    for i, match_file in enumerate(new_match_files):
        json_path = os.path.join(matches_dir, match_file)
        match_id = match_file.replace("match_", "").replace(".json", "")
        
        try:
            # Load JSON
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Extract team information
            radiant_team_id = data.get("radiant_team_id")
            dire_team_id = data.get("dire_team_id")
            
            if radiant_team_id and radiant_team_id not in team_info:
                existing_team = session.query(ProTeam).filter_by(team_id=radiant_team_id).one_or_none()
                if not existing_team:
                    team_name = data.get("radiant_name", f"Team {radiant_team_id}")
                    team_info[radiant_team_id] = {
                        "team_id": radiant_team_id,
                        "name": team_name,
                        "tag": data.get("radiant_tag", f"T{radiant_team_id}"),
                        "logo_url": data.get("radiant_logo", None)
                    }
                    logger.debug(f"[{datetime.now().strftime('%H:%M:%S')}] Extracted radiant team: {team_name} (ID: {radiant_team_id})")
            
            if dire_team_id and dire_team_id not in team_info:
                existing_team = session.query(ProTeam).filter_by(team_id=dire_team_id).one_or_none()
                if not existing_team:
                    team_name = data.get("dire_name", f"Team {dire_team_id}")
                    team_info[dire_team_id] = {
                        "team_id": dire_team_id,
                        "name": team_name,
                        "tag": data.get("dire_tag", f"T{dire_team_id}"),
                        "logo_url": data.get("dire_logo", None)
                    }
                    logger.debug(f"[{datetime.now().strftime('%H:%M:%S')}] Extracted dire team: {team_name} (ID: {dire_team_id})")
            
            # Extract league information
            league_id = data.get("league_id")
            if league_id and league_id not in league_info:
                existing_league = session.query(ProLeague).filter_by(league_id=league_id).one_or_none()
                if not existing_league:
                    league_name = data.get("league", {}).get("name", data.get("league_name", f"League {league_id}"))
                    league_info[league_id] = {
                        "league_id": league_id,
                        "name": league_name,
                        "tier": data.get("league", {}).get("tier", None),
                        "region": data.get("league", {}).get("region", None),
                    }
                    logger.debug(f"[{datetime.now().strftime('%H:%M:%S')}] Extracted league: {league_name} (ID: {league_id})")
            
            # Log progress periodically
            if i % 20 == 0 and i > 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {i+1}/{len(new_match_files)} match files for metadata")
            
        except Exception as e:
            logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error extracting metadata from {match_file}: {str(e)}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error extracting metadata from {match_file}: {str(e)}")
        
        pbar_first_pass.update(1)
    
    pbar_first_pass.close()
    
    # Step 2: Insert teams and leagues into database
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Inserting {len(team_info)} new teams into database")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Inserting {len(team_info)} new teams into database")
    
    for team_id, team_data in team_info.items():
        existing_team = session.query(ProTeam).filter_by(team_id=team_id).one_or_none()
        
        if not existing_team:
            team = ProTeam(
                team_id=team_id,
                name=team_data["name"],
                tag=team_data["tag"],
                logo_url=team_data["logo_url"],
                rank=None  # We don't have rank information from the match data
            )
            session.add(team)
            logger.debug(f"[{datetime.now().strftime('%H:%M:%S')}] Added team: {team_data['name']} (ID: {team_id})")
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Inserting {len(league_info)} new leagues into database")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Inserting {len(league_info)} new leagues into database")
    
    for league_id, league_data in league_info.items():
        existing_league = session.query(ProLeague).filter_by(league_id=league_id).one_or_none()
        
        if not existing_league:
            league = ProLeague(
                league_id=league_id,
                name=league_data["name"],
                tier=league_data["tier"],
                region=league_data["region"],
                start_date=None,  # We don't have exact league dates from match data
                end_date=None,
                prize_pool=None
            )
            session.add(league)
            logger.debug(f"[{datetime.now().strftime('%H:%M:%S')}] Added league: {league_data['name']} (ID: {league_id})")
    
    # Commit teams and leagues
    try:
        session.commit()
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Teams and leagues inserted successfully")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Teams and leagues inserted successfully")
    except Exception as e:
        session.rollback()
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error inserting teams and leagues: {str(e)}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Error inserting teams and leagues: {str(e)}")
    
    # Step 3: Populate the database with match data
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Second pass: Populating match data for {len(new_match_files)} new matches")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Second pass: Populating match data for {len(new_match_files)} new matches")
    pbar = tqdm(total=len(new_match_files), desc="Populating new matches", ncols=100)
    
    # Process each match JSON file
    successful_matches = 0
    failed_matches = 0
    
    for i, match_file in enumerate(new_match_files):
        json_path = os.path.join(matches_dir, match_file)
        match_id = match_file.replace("match_", "").replace(".json", "")
        
        # Log progress periodically
        if i % 10 == 0 and i > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Populating database with match {i+1}/{len(new_match_files)}: {match_id}")
        
        try:
            # Import here to avoid circular imports
            from database.database_pro_teams import populate_from_json
            
            # Populate the database from the JSON file
            populate_time_start = time.time()
            populate_from_json(json_path)
            
            # Also populate time-series stats if needed
            from database.database_pro_teams import populate_time_vs_stats
            populate_time_vs_stats(json_path)
            
            populate_time_end = time.time()
            process_duration = populate_time_end - populate_time_start
            
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Processed match {match_id} in {process_duration:.2f}s")
            successful_matches += 1
            
        except Exception as e:
            logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error processing match {match_id}: {str(e)}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error processing match {match_id}: {str(e)}")
            failed_matches += 1
        
        pbar.update(1)
    
    pbar.close()
    
    # Close the session
    session.close()
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Database populated with {successful_matches} new matches successfully, {failed_matches} failed")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Database populated with {successful_matches} new matches successfully, {failed_matches} failed")
    
    # Return how many matches were added successfully
    return successful_matches

def verify_current_database():
    """
    Verify the current state of the database and print statistics.
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Get counts for each table
        from database.database_pro_teams import (
            ProMatch, ProLeague, ProTeam, ProPlayer, ProHero, ProMatchPlayer,
            ProDraftTiming, ProTeamFight, ProTeamFightPlayer, ProObjective,
            ProChatWheel, ProTimevsStats
        )
        
        match_count = session.query(ProMatch).count()
        league_count = session.query(ProLeague).count()
        team_count = session.query(ProTeam).count()
        player_count = session.query(ProPlayer).count()
        hero_count = session.query(ProHero).count()
        match_player_count = session.query(ProMatchPlayer).count()
        draft_timing_count = session.query(ProDraftTiming).count()
        teamfight_count = session.query(ProTeamFight).count()
        teamfight_player_count = session.query(ProTeamFightPlayer).count()
        objective_count = session.query(ProObjective).count()
        chatwheel_count = session.query(ProChatWheel).count()
        time_vs_stats_count = session.query(ProTimevsStats).count()
        
        # Print table counts
        logger.info("=== Database Population Verification ===")
        print("\n"+"="*40)
        print("DATABASE POPULATION SUMMARY")
        print("="*40)
        print(f"Matches: {match_count}")
        print(f"Leagues: {league_count}")
        print(f"Teams: {team_count}")
        print(f"Players: {player_count}")
        print(f"Heroes: {hero_count}")
        print(f"Match Players: {match_player_count}")
        print(f"Draft Timings: {draft_timing_count}")
        print(f"Team Fights: {teamfight_count}")
        print(f"Team Fight Players: {teamfight_player_count}")
        print(f"Objectives: {objective_count}")
        print(f"Chat Wheel: {chatwheel_count}")
        print(f"Time vs Stats: {time_vs_stats_count}")
        print("="*40+"\n")
            
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error verifying database: {str(e)}")
        print(f"ERROR: Failed to verify database: {str(e)}")
    finally:
        session.close()

def main():
    """
    Main function to continue scraping matches from where we left off.
    """
    start_time = time.time()
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Starting continuation of Dota match scraping")
    print(f"\n{'='*80}\n[{datetime.now().strftime('%H:%M:%S')}] CONTINUING DOTA MATCH SCRAPING\n{'='*80}")
    
    # Step 1: Check the current database state
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 1: Checking current database state")
    verify_current_database()
    
    # Step 2: Get the last match ID we fetched
    last_match_id = get_last_match_id()
    if not last_match_id:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Unable to determine last match ID. Exiting.")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Unable to determine last match ID. Exiting.")
        return
        
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Last match ID in database: {last_match_id}")
    
    # Step 3: Get the team IDs from the database
    top_teams = get_top_teams()
    if not top_teams:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] No teams found in database. Exiting.")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: No teams found in database. Exiting.")
        return
        
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(top_teams)} teams in database")
    
    # Step 4: Continue downloading matches
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 4: Downloading additional matches")
    downloaded_match_ids = continue_downloading_matches(last_match_id, top_teams, target_count=TARGET_ADDITIONAL_MATCHES)
    
    if not downloaded_match_ids:
        logger.warning(f"[{datetime.now().strftime('%H:%M:%S')}] No additional matches were downloaded. Exiting.")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: No additional matches were downloaded. Exiting.")
        return
    
    # Step 5: Populate the database with new matches
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 5: Populating database with new matches")
    added_matches = populate_new_matches()
    
    # Step 6: Verify the updated database
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 6: Verifying updated database")
    verify_current_database()
    
    # Report completion
    elapsed_time = time.time() - start_time
    elapsed_minutes = int(elapsed_time // 60)
    elapsed_seconds = int(elapsed_time % 60)
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Continuation completed in {elapsed_minutes}m {elapsed_seconds}s")
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Added {added_matches} new matches to the database")
    
    # Get final database counts
    db = DotaDatabase()
    session = db.Session()
    final_count = 0
    try:
        final_count = session.query(ProMatch).count()
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error getting final match count: {str(e)}")
    finally:
        session.close()
    
    print(f"\n{'='*80}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] SCRAPING CONTINUATION COMPLETED IN {elapsed_minutes}m {elapsed_seconds}s")
    print(f"Added {added_matches} new matches to the database")
    print(f"Total matches in database: {final_count}")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
