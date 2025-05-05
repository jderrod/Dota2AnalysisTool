"""
Scrape Top Teams Matches

This script scrapes the most recent 100 matches for the top 10 teams by ELO rating,
wipes all existing data from the pro tables, and repopulates them with the scraped match data.
It includes progress bars for both scraping and database population.
"""
import os
import sys
import json
import logging
import time
from datetime import datetime, timedelta
from tqdm import tqdm
import requests

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import backend modules
from database.database_pro_teams import DotaDatabase, Base
from database.database_pro_teams import (
    ProMatch, ProLeague, ProTeam, ProPlayer, ProHero, ProMatchPlayer,
    ProDraftTiming, ProTeamFight, ProTeamFightPlayer, ProObjective,
    ProChatWheel, ProTimevsStats
)
from src.data.scraper import DotaMatchScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scrape_top_teams.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Settings
NUM_TOP_TEAMS = 10  # Number of top teams to fetch
MATCHES_PER_TEAM = 100  # Number of matches to fetch per team
MAX_API_BATCHES = 50  # Maximum number of API batch calls per team (to avoid infinite loops)

# OpenDota API constants
OPENDOTA_API_BASE_URL = "https://api.opendota.com/api"

def get_top_teams_by_elo():
    """
    Fetch the top teams by ELO rating from OpenDota API.
    
    Returns:
        list: List of dictionaries containing team_id and name for top teams
    """
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching top {NUM_TOP_TEAMS} teams by ELO rating")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching top {NUM_TOP_TEAMS} teams by ELO rating")
    
    try:
        # Make API request to get team rankings
        url = f"{OPENDOTA_API_BASE_URL}/teams"
        print(f"[{datetime.now().strftime('%H:%M:%S')}] API Call: {url}")
        response = requests.get(url)
        response.raise_for_status()
        
        # Sort teams by rating (descending)
        teams = response.json()
        teams.sort(key=lambda x: x.get('rating', 0), reverse=True)
        
        # Get top N teams
        top_teams = []
        for team in teams[:NUM_TOP_TEAMS]:
            team_id = team.get('team_id')
            name = team.get('name', f"Team {team_id}")
            
            if team_id:
                top_teams.append({
                    "team_id": team_id,
                    "name": name,
                    "rating": team.get('rating', 0)
                })
                logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Team: {name} (ID: {team_id}, Rating: {team.get('rating', 0)})")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Team: {name} (ID: {team_id}, Rating: {team.get('rating', 0)})")
        
        if not top_teams:
            logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] No teams found from the API")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: No teams found from the API")
            return []
            
        return top_teams
    
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error fetching top teams: {str(e)}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR: Failed to fetch top teams: {str(e)}")
        return []

def clear_database():
    """
    Drop and recreate all tables in the database.
    """
    db = DotaDatabase()
    engine = db.engine
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Dropping all tables...")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Dropping all tables...")
    Base.metadata.drop_all(engine)
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Recreating all tables...")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Recreating all tables...")
    Base.metadata.create_all(engine)
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Database cleared successfully.")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Database cleared successfully.")

def download_pro_matches_comprehensive(top_teams, target_match_count=1000):
    """
    Download recent professional matches that involve any of the top teams.
    Instead of fetching matches one team at a time, this function fetches batches of pro matches
    and checks if any match contains any of the top teams.
    
    Args:
        top_teams (list): List of dictionaries with team info, each containing 'team_id' and 'name'
        target_match_count (int): Target number of total matches to download
        
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
    team_ids = [team["team_id"] for team in top_teams]
    team_names = {team["team_id"]: team["name"] for team in top_teams}
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Starting comprehensive match search for {len(top_teams)} top teams")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Searching for matches involving any of {len(top_teams)} top teams")
    print(f"Target: {target_match_count} total matches")
    
    # Tracking variables
    matches_to_download = []
    last_match_id = None
    batch_num = 1
    team_match_counts = {team_id: 0 for team_id in team_ids}
    
    # Create a progress bar for match fetching
    pbar = tqdm(total=target_match_count, desc=f"Fetching matches for top teams", ncols=100)
    
    # Loop until we have enough matches or reach the batch limit
    while len(matches_to_download) < target_match_count and batch_num <= MAX_API_BATCHES:
        params = {'limit': 100}
        if last_match_id:
            params['less_than_match_id'] = last_match_id
            
        # Fetch pro matches
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] API Call: Requesting batch #{batch_num} of pro matches")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] API Call: Requesting batch #{batch_num} of pro matches...")
        batch_start_time = time.time()
        pro_matches = scraper.get_pro_matches(**params)
        batch_duration = time.time() - batch_start_time
        
        if not pro_matches or len(pro_matches) == 0:
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] No matches returned from API. Ending search.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] No matches returned from API. Ending search.")
            break
        
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
        new_matches_to_add = batch_matches_for_top_teams[:target_match_count - len(matches_to_download)]
        matches_to_download.extend(new_matches_to_add)
        
        # Update progress bar with new matches found
        pbar.update(len(new_matches_to_add))
        
        # Update last_match_id for pagination (whether or not we found matches for our teams)
        if pro_matches:
            last_match_id = pro_matches[-1]['match_id']
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] New pagination ID: {last_match_id}")
        
        # Check if we have enough matches
        if len(matches_to_download) >= target_match_count:
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Reached target of {target_match_count} matches. Stopping search.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Reached target of {target_match_count} matches. Stopping search.")
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
        print(f"  {team_names.get(team_id, f'Team {team_id}')}: {count} matches")
    
    total_matches = len(matches_to_download)
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Found a total of {total_matches} matches for top teams")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Found a total of {total_matches} matches for top teams")
    
    if not matches_to_download:
        logger.warning(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: No matches found for any of the top teams. Check team IDs.")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: No matches found for any of the top teams. Check team IDs.")
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
            
        # Get full match details
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
            else:
                logger.warning(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to get details for match {match_id}")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to get details for match {match_id}")
        except Exception as e:
            logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error downloading match {match_id}: {str(e)}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error downloading match {match_id}: {str(e)}")
            
        pbar.update(1)
        
        # Sleep to respect API rate limits
        time.sleep(1)
        
    pbar.close()
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Downloaded {len(downloaded_match_ids)} matches for top teams")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Downloaded {len(downloaded_match_ids)} matches for top teams")
    return downloaded_match_ids

def populate_database_from_jsons():
    """
    Populate the database with match data from all downloaded JSON files.
    Ensures all tables and columns are properly populated.
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
    
    # Step 1: First pass - Extract and populate teams and leagues
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] First pass: Extracting teams and leagues information")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] First pass: Extracting teams and leagues information")
    team_info = {}
    league_info = {}
    
    # Create a progress bar for the first pass
    pbar_first_pass = tqdm(total=len(match_files), desc="Extracting teams and leagues", ncols=100)
    
    for i, match_file in enumerate(match_files):
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
                team_name = data.get("radiant_name", f"Team {radiant_team_id}")
                team_info[radiant_team_id] = {
                    "team_id": radiant_team_id,
                    "name": team_name,
                    "tag": data.get("radiant_tag", f"T{radiant_team_id}"),
                    "logo_url": data.get("radiant_logo", None)
                }
                logger.debug(f"[{datetime.now().strftime('%H:%M:%S')}] Extracted radiant team: {team_name} (ID: {radiant_team_id})")
            
            if dire_team_id and dire_team_id not in team_info:
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
                league_name = data.get("league", {}).get("name", data.get("league_name", f"League {league_id}"))
                league_info[league_id] = {
                    "league_id": league_id,
                    "name": league_name,
                    "tier": data.get("league", {}).get("tier", None),
                    "region": data.get("league", {}).get("region", None),
                }
                logger.debug(f"[{datetime.now().strftime('%H:%M:%S')}] Extracted league: {league_name} (ID: {league_id})")
            
            # Log progress periodically
            if i % 20 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {i+1}/{len(match_files)} match files for metadata")
            
        except Exception as e:
            logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error extracting metadata from {match_file}: {str(e)}")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error extracting metadata from {match_file}: {str(e)}")
        
        pbar_first_pass.update(1)
    
    pbar_first_pass.close()
    
    # Step 2: Insert teams and leagues into database
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Inserting {len(team_info)} teams into database")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Inserting {len(team_info)} teams into database")
    
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
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Inserting {len(league_info)} leagues into database")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Inserting {len(league_info)} leagues into database")
    
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
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Second pass: Populating all match data")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Second pass: Populating all match data")
    pbar = tqdm(total=len(match_files), desc="Populating matches", ncols=100)
    
    # Process each match JSON file
    successful_matches = 0
    failed_matches = 0
    
    for i, match_file in enumerate(match_files):
        json_path = os.path.join(matches_dir, match_file)
        match_id = match_file.replace("match_", "").replace(".json", "")
        
        # Log progress periodically
        if i % 10 == 0 or i == len(match_files) - 1:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Populating database with match {i+1}/{len(match_files)}: {match_id}")
        
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
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Database populated with {successful_matches} matches successfully, {failed_matches} failed")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Database populated with {successful_matches} matches successfully, {failed_matches} failed")
    
    # Step 4: Verify data population
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Running database verification checks...")
    verify_database_population(db)

def verify_database_population(db):
    """
    Verify that all tables in the database have been populated properly.
    Reports counts for each table and checks for any critical issues.
    
    Args:
        db: DotaDatabase instance
    """
    session = db.Session()
    
    try:
        # Get counts for each table
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
        
        # Verify match data integrity
        matches_without_teams = session.query(ProMatch).filter(
            (ProMatch.radiant_team_id == None) & (ProMatch.dire_team_id == None)
        ).count()
        
        if matches_without_teams > 0:
            logger.warning(f"{matches_without_teams} matches have no team information")
            print(f"WARNING: {matches_without_teams} matches have no team information")
        
        matches_without_leagues = session.query(ProMatch).filter(ProMatch.league_id == None).count()
        if matches_without_leagues > 0:
            logger.warning(f"{matches_without_leagues} matches have no league information")
            print(f"WARNING: {matches_without_leagues} matches have no league information")
            
        # Output overall assessment
        if match_count > 0 and player_count > 0 and match_player_count > 0:
            logger.info("Database appears to be properly populated with essential data")
            print("Database appears to be properly populated with essential data")
        else:
            logger.error("Database may be missing critical data in essential tables")
            print("ERROR: Database may be missing critical data in essential tables")
            
    except Exception as e:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Error verifying database population: {str(e)}")
        print(f"ERROR: Failed to verify database population: {str(e)}")
    finally:
        session.close()

def main():
    """
    Main function to scrape matches for top teams, wipe the database, and repopulate it.
    Uses a comprehensive approach to download matches that involve any of the top teams.
    """
    start_time = time.time()
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Starting top teams match scraping process")
    print(f"\n{'='*80}\n[{datetime.now().strftime('%H:%M:%S')}] STARTING DOTA MATCH SCRAPING PROCESS\n{'='*80}")
    
    # Step 1: Get top teams by ELO rating
    top_teams = get_top_teams_by_elo()
    if not top_teams:
        logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] Failed to get top teams. Exiting.")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] FATAL ERROR: Failed to get top teams. Exiting.")
        return
    
    # Print team list for reference
    print("\nTop teams by ELO rating:")
    for i, team in enumerate(top_teams):
        print(f"  {i+1}. {team['name']} (ID: {team['team_id']}, Rating: {team['rating']})")
    print("\n")
    
    # Step 2: Clear the database
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Clearing database...")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 2: Clearing database...")
    clear_database()
    
    # Step 3: Download matches comprehensively for all top teams
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 3: Downloading matches for all top teams...")
    target_matches = 1000  # Target around 1000 total matches involving top teams
    
    # Use the comprehensive approach to download matches for all teams at once
    print(f"\n{'='*60}")
    print(f"Downloading matches involving any of the top {len(top_teams)} teams")
    print(f"Target: {target_matches} unique matches")
    print(f"{'='*60}\n")
    
    downloaded_match_ids = download_pro_matches_comprehensive(top_teams, target_match_count=target_matches)
    
    unique_matches = len(set(downloaded_match_ids))
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Downloaded a total of {unique_matches} unique matches")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Downloaded a total of {unique_matches} unique matches")
    
    # Step 4: Populate the database from all downloaded JSON files
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Populating database from downloaded match data...")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 4: Populating database from downloaded match data...")
    populate_database_from_jsons()
    
    # Report completion
    elapsed_time = time.time() - start_time
    elapsed_minutes = int(elapsed_time // 60)
    elapsed_seconds = int(elapsed_time % 60)
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Top teams match scraping completed in {elapsed_minutes}m {elapsed_seconds}s")
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {unique_matches} unique matches for {len(top_teams)} teams")
    
    print(f"\n{'='*80}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] SCRAPING PROCESS COMPLETED IN {elapsed_minutes}m {elapsed_seconds}s")
    print(f"Processed {unique_matches} unique matches for top {len(top_teams)} teams")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
