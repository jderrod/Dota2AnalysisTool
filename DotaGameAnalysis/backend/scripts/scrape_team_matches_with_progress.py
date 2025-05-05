"""
Scrape Team Matches with Progress

This script scrapes the most recent 100 matches for specified teams,
wipes all existing data from the database, and repopulates all tables with the scraped match data.
It includes progress bars for both scraping and database population.
"""
import os
import sys
import json
import logging
import time
from datetime import datetime, timedelta
from tqdm import tqdm

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
        logging.FileHandler("scrape_team_matches.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define the teams to scrape - focusing on teams we know have recent matches
TEAMS = [
    {"name": "Team Liquid", "team_id": 2163},
    {"name": "Team Spirit", "team_id": 7119388},
    {"name": "Tundra Esports", "team_id": 8291895},
    {"name": "Gaimin Gladiators", "team_id": 8599101},
    {"name": "BetBoom Team", "team_id": 8255888},
    {"name": "Team Falcons", "team_id": 8606828}
    # Removing teams with less activity or potential ID issues:
    # {"name": "Parivision", "team_id": 9451878},
    # {"name": "Aurora Gaming", "team_id": 8204070},
    # {"name": "Chimera Esports", "team_id": 9489246},
]

# Number of matches to fetch per team (reduced to 20 for faster demonstration)
MATCHES_PER_TEAM = 20

def clear_database():
    """
    Drop and recreate all tables in the database.
    """
    db = DotaDatabase()
    engine = db.engine
    
    logger.info("Dropping all tables...")
    Base.metadata.drop_all(engine)
    
    logger.info("Recreating all tables...")
    Base.metadata.create_all(engine)
    
    logger.info("Database cleared successfully.")

def download_team_matches(team_id, team_name, matches_per_team=MATCHES_PER_TEAM):
    """
    Download recent matches for a team.
    
    Args:
        team_id (int): Team ID to fetch matches for
        team_name (str): Name of the team (for logging)
        matches_per_team (int): Number of matches to fetch
        
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
    
    # Get recent matches for the team
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Starting match search for {team_name} (ID: {team_id})")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Searching for matches for {team_name} (Team ID: {team_id})")
    
    matches = []
    last_match_id = None
    batch_num = 1
    
    # Create a progress bar for match fetching
    pbar = tqdm(total=matches_per_team, desc=f"Fetching {team_name} matches", ncols=100)
    
    while len(matches) < matches_per_team:
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
        
        # Filter matches for the team
        filtered_matches = [
            match for match in pro_matches 
            if match.get('radiant_team_id') == team_id or match.get('dire_team_id') == team_id
        ]
        
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(filtered_matches)} matches for {team_name} in this batch")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Found {len(filtered_matches)} matches for {team_name} in this batch")
        
        if filtered_matches:
            # Update matches list
            matches.extend(filtered_matches[:matches_per_team - len(matches)])
            # Update progress bar
            pbar.update(min(len(filtered_matches), matches_per_team - len(matches) + len(filtered_matches)))
            
            # Update last_match_id for pagination
            last_match_id = pro_matches[-1]['match_id']
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] New pagination ID: {last_match_id}")
        else:
            # If we found no matches for this team in the current batch, move to the next batch
            last_match_id = pro_matches[-1]['match_id']
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] No matches for this team in batch. New pagination ID: {last_match_id}")
        
        # Check if we have enough matches
        if len(matches) >= matches_per_team:
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Reached target of {matches_per_team} matches. Stopping search.")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Reached target of {matches_per_team} matches. Stopping search.")
            break
        
        batch_num += 1
        # Sleep briefly to avoid hitting rate limits too hard
        time.sleep(1)  # Small delay between batches for clearer logging
            
    pbar.close()
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Found a total of {len(matches)} matches for {team_name}")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Found a total of {len(matches)} matches for {team_name}")
    
    if not matches:
        logger.warning(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: No matches found for {team_name}. Check the team ID.")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: No matches found for {team_name}. Check the team ID.")
        return []
    
    # Download full match data
    downloaded_match_ids = []
    
    # Create a progress bar for downloading match details
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting download of {len(matches)} match details for {team_name}")
    pbar = tqdm(total=len(matches), desc=f"Downloading {team_name} match details", ncols=100)
    
    for i, match in enumerate(matches):
        match_id = match['match_id']
        match_file = os.path.join(data_dir, f"match_{match_id}.json")
        
        # Skip if the match file already exists
        if os.path.exists(match_file):
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Match {match_id} already exists ({i+1}/{len(matches)})")
            downloaded_match_ids.append(match_id)
            pbar.update(1)
            continue
        
        logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] API Call: Requesting details for match {match_id} ({i+1}/{len(matches)})")
        if i % 5 == 0:  # Print every 5th match to avoid console spam
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Downloading match {match_id} ({i+1}/{len(matches)})")
            
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
        
        # Sleep briefly to respect API rate limits
        time.sleep(1)
        
    pbar.close()
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Downloaded {len(downloaded_match_ids)} matches for {team_name}")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Downloaded {len(downloaded_match_ids)} matches for {team_name}")
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
        logger.info(f"Matches: {match_count}")
        logger.info(f"Leagues: {league_count}")
        logger.info(f"Teams: {team_count}")
        logger.info(f"Players: {player_count}")
        logger.info(f"Heroes: {hero_count}")
        logger.info(f"Match Players: {match_player_count}")
        logger.info(f"Draft Timings: {draft_timing_count}")
        logger.info(f"Team Fights: {teamfight_count}")
        logger.info(f"Team Fight Players: {teamfight_player_count}")
        logger.info(f"Objectives: {objective_count}")
        logger.info(f"Chat Wheel: {chatwheel_count}")
        logger.info(f"Time vs Stats: {time_vs_stats_count}")
        
        # Verify match data integrity
        matches_without_teams = session.query(ProMatch).filter(
            (ProMatch.radiant_team_id == None) & (ProMatch.dire_team_id == None)
        ).count()
        
        if matches_without_teams > 0:
            logger.warning(f"{matches_without_teams} matches have no team information")
        
        matches_without_leagues = session.query(ProMatch).filter(ProMatch.league_id == None).count()
        if matches_without_leagues > 0:
            logger.warning(f"{matches_without_leagues} matches have no league information")
            
        # Output overall assessment
        if match_count > 0 and player_count > 0 and match_player_count > 0:
            logger.info("Database appears to be properly populated with essential data")
        else:
            logger.error("Database may be missing critical data in essential tables")
            
    except Exception as e:
        logger.error(f"Error verifying database population: {str(e)}")
    finally:
        session.close()

def main():
    """
    Main function to scrape matches, wipe the database, and repopulate it.
    """
    start_time = time.time()
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Starting team match scraping process")
    print(f"\n{'='*80}\n[{datetime.now().strftime('%H:%M:%S')}] STARTING DOTA MATCH SCRAPING PROCESS\n{'='*80}")
    
    # Step 1: Clear the database
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Clearing database...")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 1: Clearing database...")
    clear_database()
    
    # Step 2: Download matches for each team
    all_downloaded_match_ids = []
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 2: Downloading matches for {len(TEAMS)} teams...")
    
    # For better visibility, print all team names upfront
    print("\nTeams to process:")
    for i, team in enumerate(TEAMS):
        print(f"  {i+1}. {team['name']} (ID: {team['team_id']})")
    print("\n")
    
    for i, team in enumerate(TEAMS):
        team_id = team["team_id"]
        team_name = team["name"]
        
        print(f"\n{'='*60}")
        print(f"Processing team {i+1}/{len(TEAMS)}: {team_name} (ID: {team_id})")
        print(f"{'='*60}\n")
        
        match_ids = download_team_matches(team_id, team_name)
        all_downloaded_match_ids.extend(match_ids)
        
        # Report progress after each team
        unique_matches = len(set(all_downloaded_match_ids))
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Progress: Downloaded {unique_matches} unique matches so far ({i+1}/{len(TEAMS)} teams processed)")
    
    unique_matches = len(set(all_downloaded_match_ids))
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Downloaded a total of {unique_matches} unique matches")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Downloaded a total of {unique_matches} unique matches")
    
    # Step 3: Populate the database from all downloaded JSON files
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Populating database from downloaded match data...")
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] STEP 3: Populating database from downloaded match data...")
    populate_database_from_jsons()
    
    # Report completion
    elapsed_time = time.time() - start_time
    elapsed_minutes = int(elapsed_time // 60)
    elapsed_seconds = int(elapsed_time % 60)
    
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Team match scraping completed in {elapsed_minutes}m {elapsed_seconds}s")
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Processed {unique_matches} unique matches for {len(TEAMS)} teams")
    
    print(f"\n{'='*80}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] SCRAPING PROCESS COMPLETED IN {elapsed_minutes}m {elapsed_seconds}s")
    print(f"Processed {unique_matches} unique matches for {len(TEAMS)} teams")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
