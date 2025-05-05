#!/usr/bin/env python3
"""
Script to scrape 2000 matches total for the top 20 Dota 2 teams.
Uses the OpenDota API key to avoid rate limiting.
"""

import os
import sys
import json
import logging
import time
import requests
from datetime import datetime
from tqdm import tqdm
from pathlib import Path
import traceback
from sqlalchemy import text

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import database modules
from database.database_pro_teams import DotaDatabase, Base
from database.database_pro_teams import ProMatch, ProLeague, ProTeam, ProPlayer
from database.database_pro_teams import ProHero, ProMatchPlayer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scrape_2000_matches.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
OPENDOTA_API_KEY = "6f146503-15ad-497e-9f97-35a6b7d74a31"
OPENDOTA_API_BASE = "https://api.opendota.com/api"
TARGET_MATCH_COUNT = 2000
MATCHES_PER_TEAM = 100  # Number of matches to scrape per team
RATE_LIMIT_WAIT = 1  # Seconds to wait between API calls
MAX_RETRIES = 5  # Maximum number of retries for API calls
DOWNLOAD_BATCH_SIZE = 10  # Number of matches to download in parallel

# Directories
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
TEAM_DATA_DIR = os.path.join(RAW_DATA_DIR, 'team_data')
TEAM_GAMES_DIR = os.path.join(RAW_DATA_DIR, 'teams_games')

# Ensure directories exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(TEAM_DATA_DIR, exist_ok=True)
os.makedirs(TEAM_GAMES_DIR, exist_ok=True)

def api_request(endpoint, params=None, max_retries=MAX_RETRIES):
    """
    Make a request to the OpenDota API with retry logic and rate limiting.
    
    Args:
        endpoint (str): API endpoint to call (without base URL)
        params (dict, optional): Query parameters
        max_retries (int): Maximum number of retries
        
    Returns:
        dict: JSON response from the API
    """
    if params is None:
        params = {}
    
    # Add API key to parameters
    params['api_key'] = OPENDOTA_API_KEY
    
    url = f"{OPENDOTA_API_BASE}/{endpoint}"
    retries = 0
    
    while retries < max_retries:
        try:
            response = requests.get(url, params=params)
            
            # Check for rate limiting
            if response.status_code == 429:
                wait_time = int(response.headers.get('Retry-After', RATE_LIMIT_WAIT * 10))
                logger.warning(f"Rate limited. Waiting {wait_time} seconds.")
                time.sleep(wait_time)
                retries += 1
                continue
            
            # Check for other status codes
            if response.status_code != 200:
                logger.error(f"API error: {response.status_code} - {response.text}")
                retries += 1
                time.sleep(RATE_LIMIT_WAIT)
                continue
            
            # Success - return JSON response
            time.sleep(RATE_LIMIT_WAIT)  # Always wait to avoid rate limiting
            return response.json()
            
        except Exception as e:
            logger.error(f"API request error: {str(e)}")
            retries += 1
            time.sleep(RATE_LIMIT_WAIT * 2)
    
    logger.error(f"Failed to get response from {url} after {max_retries} retries.")
    return None

def verify_database_population():
    """Verify the current database population and print counts of all tables."""
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Use direct SQL queries
        match_count = session.execute(text("SELECT COUNT(*) FROM pro_matches")).scalar()
        league_count = session.execute(text("SELECT COUNT(*) FROM pro_leagues")).scalar()
        team_count = session.execute(text("SELECT COUNT(*) FROM pro_teams")).scalar()
        player_count = session.execute(text("SELECT COUNT(*) FROM pro_players")).scalar()
        hero_count = session.execute(text("SELECT COUNT(*) FROM pro_heroes")).scalar()
        match_player_count = session.execute(text("SELECT COUNT(*) FROM pro_match_player_metrics")).scalar()
        draft_timing_count = session.execute(text("SELECT COUNT(*) FROM pro_draft_timings")).scalar()
        
        print("\n" + "=" * 40)
        print("DATABASE POPULATION SUMMARY")
        print("=" * 40)
        print(f"Matches: {match_count}")
        print(f"Leagues: {league_count}")
        print(f"Teams: {team_count}")
        print(f"Players: {player_count}")
        print(f"Heroes: {hero_count}")
        print(f"Match Players: {match_player_count}")
        print(f"Draft Timings: {draft_timing_count}")
        print("=" * 40)
        
        return match_count
    except Exception as e:
        logger.error(f"Error verifying database: {str(e)}")
        return 0
    finally:
        session.close()

def get_existing_match_ids():
    """Get the set of match IDs already in the database."""
    db = DotaDatabase()
    session = db.Session()
    
    try:
        result = session.execute(text("SELECT match_id FROM pro_matches"))
        match_ids = {row[0] for row in result}
        logger.info(f"Found {len(match_ids)} existing matches in database")
        return match_ids
    except Exception as e:
        logger.error(f"Error getting existing match IDs: {str(e)}")
        return set()
    finally:
        session.close()

def get_existing_team_ids():
    """Get the set of team IDs already in the database."""
    db = DotaDatabase()
    session = db.Session()
    
    try:
        result = session.execute(text("SELECT team_id FROM pro_teams"))
        team_ids = {row[0] for row in result}
        logger.info(f"Found {len(team_ids)} existing teams in database")
        return team_ids
    except Exception as e:
        logger.error(f"Error getting existing team IDs: {str(e)}")
        return set()
    finally:
        session.close()

def get_top_teams(count=20):
    """
    Get the top professional Dota 2 teams.
    
    Args:
        count (int): Number of top teams to retrieve
        
    Returns:
        list: List of team data dictionaries
    """
    logger.info(f"Fetching top {count} teams from OpenDota API")
    
    # Get teams from the API
    teams = api_request('teams')
    
    if not teams:
        logger.error("Failed to get teams from the API")
        return []
    
    # Sort by rating and get top N teams
    sorted_teams = sorted(teams, key=lambda x: x.get('rating', 0), reverse=True)
    top_teams = sorted_teams[:count]
    
    logger.info(f"Retrieved {len(top_teams)} top teams")
    
    # Save team data to files
    for team in top_teams:
        team_name = team.get('name', 'Unknown').replace(' ', '_')
        file_path = os.path.join(TEAM_DATA_DIR, f"{team_name}_info.json")
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(team, f, indent=2)
        
        logger.info(f"Saved team info for {team_name}")
    
    return top_teams

def get_team_matches(team_id, limit=MATCHES_PER_TEAM):
    """
    Get matches for a specific team.
    
    Args:
        team_id (int): Team ID
        limit (int): Maximum number of matches to retrieve
        
    Returns:
        list: List of match IDs
    """
    logger.info(f"Fetching up to {limit} matches for team {team_id}")
    
    # Get team matches from the API
    matches = api_request(f'teams/{team_id}/matches')
    
    if not matches:
        logger.error(f"Failed to get matches for team {team_id}")
        return []
    
    # Limit the number of matches
    matches = matches[:limit]
    
    # Extract match IDs
    match_ids = [match.get('match_id') for match in matches if match.get('match_id')]
    
    logger.info(f"Retrieved {len(match_ids)} matches for team {team_id}")
    
    return match_ids

def download_match(match_id):
    """
    Download detailed match data for a specific match.
    
    Args:
        match_id (int): Match ID
        
    Returns:
        dict: Match data
    """
    logger.info(f"Downloading match {match_id}")
    
    # Get match data from the API
    match_data = api_request(f'matches/{match_id}')
    
    if not match_data:
        logger.error(f"Failed to get data for match {match_id}")
        return None
    
    return match_data

def save_match_data(match_data, team_name):
    """
    Save match data to a file in the team's directory.
    
    Args:
        match_data (dict): Match data
        team_name (str): Team name
        
    Returns:
        str: Path to the saved file
    """
    if not match_data or 'match_id' not in match_data:
        logger.error("Invalid match data")
        return None
    
    # Create team directory if it doesn't exist
    team_dir = os.path.join(TEAM_GAMES_DIR, team_name)
    os.makedirs(team_dir, exist_ok=True)
    
    # Save match data to file
    match_id = match_data['match_id']
    file_path = os.path.join(team_dir, f"match_{match_id}.json")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(match_data, f, indent=2)
    
    logger.info(f"Saved match data for match {match_id} to {file_path}")
    
    return file_path

def add_to_database(match_data, session):
    """
    Add match data to the database.
    
    Args:
        match_data (dict): Match data
        session (Session): Database session
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        match_id = match_data.get('match_id')
        
        # Check if match already exists
        existing_match = session.execute(
            text("SELECT match_id FROM pro_matches WHERE match_id = :match_id"),
            {"match_id": match_id}
        ).first()
        
        if existing_match:
            logger.info(f"Match {match_id} already exists in database, skipping")
            return False
        
        # Add league if it doesn't exist
        league_id = None
        if 'league' in match_data and match_data['league']:
            league_id = match_data['league'].get('leagueid')
            if league_id:
                # Check if league exists
                existing_league = session.execute(
                    text("SELECT league_id FROM pro_leagues WHERE league_id = :league_id"),
                    {"league_id": league_id}
                ).first()
                
                if not existing_league:
                    # Insert league
                    session.execute(
                        text("""
                            INSERT INTO pro_leagues 
                            (league_id, name, tier) 
                            VALUES (:league_id, :name, :tier)
                        """),
                        {
                            "league_id": league_id,
                            "name": match_data['league'].get('name'),
                            "tier": match_data['league'].get('tier')
                        }
                    )
                    session.commit()
        
        # Process team information
        radiant_team_id = None
        dire_team_id = None
        
        # Handle radiant team
        if 'radiant_team' in match_data and match_data['radiant_team']:
            team_data = match_data['radiant_team']
            if 'team_id' in team_data:
                radiant_team_id = team_data['team_id']
                
                # Check if team exists
                existing_team = session.execute(
                    text("SELECT team_id FROM pro_teams WHERE team_id = :team_id"),
                    {"team_id": radiant_team_id}
                ).first()
                
                if not existing_team:
                    # Insert team
                    session.execute(
                        text("""
                            INSERT INTO pro_teams 
                            (team_id, name, tag, logo_url) 
                            VALUES (:team_id, :name, :tag, :logo_url)
                        """),
                        {
                            "team_id": radiant_team_id,
                            "name": team_data.get('name'),
                            "tag": team_data.get('tag'),
                            "logo_url": team_data.get('logo_url')
                        }
                    )
                    session.commit()
        
        # Handle dire team
        if 'dire_team' in match_data and match_data['dire_team']:
            team_data = match_data['dire_team']
            if 'team_id' in team_data:
                dire_team_id = team_data['team_id']
                
                # Check if team exists
                existing_team = session.execute(
                    text("SELECT team_id FROM pro_teams WHERE team_id = :team_id"),
                    {"team_id": dire_team_id}
                ).first()
                
                if not existing_team:
                    # Insert team
                    session.execute(
                        text("""
                            INSERT INTO pro_teams 
                            (team_id, name, tag, logo_url) 
                            VALUES (:team_id, :name, :tag, :logo_url)
                        """),
                        {
                            "team_id": dire_team_id,
                            "name": team_data.get('name'),
                            "tag": team_data.get('tag'),
                            "logo_url": team_data.get('logo_url')
                        }
                    )
                    session.commit()
        
        # Insert match
        start_time = match_data.get('start_time')
        duration = match_data.get('duration')
        series_id = match_data.get('series_id')
        series_type = match_data.get('series_type')
        
        # Handle potentially large arrays by converting to strings
        radiant_gold_adv = json.dumps(match_data.get('radiant_gold_adv')) if 'radiant_gold_adv' in match_data else None
        dire_gold_adv = json.dumps(match_data.get('dire_gold_adv')) if 'dire_gold_adv' in match_data else None
        
        # Insert match
        session.execute(
            text("""
                INSERT INTO pro_matches 
                (match_id, version, start_time, duration, league_id, 
                 series_id, series_type, radiant_team_id, dire_team_id,
                 radiant_score, dire_score, radiant_win, radiant_gold_adv, dire_gold_adv) 
                VALUES 
                (:match_id, :version, :start_time, :duration, :league_id,
                 :series_id, :series_type, :radiant_team_id, :dire_team_id,
                 :radiant_score, :dire_score, :radiant_win, :radiant_gold_adv, :dire_gold_adv)
            """),
            {
                "match_id": match_id,
                "version": match_data.get('version'),
                "start_time": datetime.fromtimestamp(start_time) if start_time else None,
                "duration": duration,
                "league_id": league_id,
                "series_id": series_id,
                "series_type": series_type,
                "radiant_team_id": radiant_team_id,
                "dire_team_id": dire_team_id,
                "radiant_score": match_data.get('radiant_score'),
                "dire_score": match_data.get('dire_score'),
                "radiant_win": match_data.get('radiant_win'),
                "radiant_gold_adv": radiant_gold_adv,
                "dire_gold_adv": dire_gold_adv
            }
        )
        session.commit()
        
        # Process players
        if 'players' in match_data:
            for player_data in match_data['players']:
                account_id = player_data.get('account_id')
                
                if account_id:
                    # Check if player exists
                    existing_player = session.execute(
                        text("SELECT account_id FROM pro_players WHERE account_id = :account_id"),
                        {"account_id": account_id}
                    ).first()
                    
                    if not existing_player:
                        # Insert player
                        player_name = player_data.get('name') or player_data.get('personaname')
                        session.execute(
                            text("""
                                INSERT INTO pro_players 
                                (account_id, name, match_id) 
                                VALUES (:account_id, :name, :match_id)
                            """),
                            {
                                "account_id": account_id,
                                "name": player_name,
                                "match_id": match_id
                            }
                        )
                
                # Process hero
                hero_id = player_data.get('hero_id')
                if hero_id:
                    # Check if hero exists
                    existing_hero = session.execute(
                        text("SELECT hero_id FROM pro_heroes WHERE hero_id = :hero_id"),
                        {"hero_id": hero_id}
                    ).first()
                    
                    if not existing_hero:
                        # Insert hero
                        session.execute(
                            text("""
                                INSERT INTO pro_heroes 
                                (hero_id, name, match_id) 
                                VALUES (:hero_id, :name, :match_id)
                            """),
                            {
                                "hero_id": hero_id,
                                "name": player_data.get('hero', f"Hero {hero_id}"),
                                "match_id": match_id
                            }
                        )
                
                # Add match player metrics
                if account_id and hero_id:
                    player_slot = player_data.get('player_slot')
                    
                    # Check if this player-match combination already exists
                    existing_player_match = session.execute(
                        text("""
                            SELECT id FROM pro_match_player_metrics 
                            WHERE match_id = :match_id AND account_id = :account_id AND hero_id = :hero_id
                        """),
                        {
                            "match_id": match_id,
                            "account_id": account_id,
                            "hero_id": hero_id
                        }
                    ).first()
                    
                    if not existing_player_match:
                        # Insert match player
                        session.execute(
                            text("""
                                INSERT INTO pro_match_player_metrics 
                                (match_id, account_id, hero_id, player_slot, 
                                 kills, deaths, assists, last_hits, denies, 
                                 gold_per_min, xp_per_min, hero_damage, tower_damage, 
                                 hero_healing, level, item_0, item_1, item_2, 
                                 item_3, item_4, item_5)
                                VALUES 
                                (:match_id, :account_id, :hero_id, :player_slot, 
                                 :kills, :deaths, :assists, :last_hits, :denies, 
                                 :gold_per_min, :xp_per_min, :hero_damage, :tower_damage, 
                                 :hero_healing, :level, :item_0, :item_1, :item_2, 
                                 :item_3, :item_4, :item_5)
                            """),
                            {
                                "match_id": match_id,
                                "account_id": account_id,
                                "hero_id": hero_id,
                                "player_slot": player_slot,
                                "kills": player_data.get('kills'),
                                "deaths": player_data.get('deaths'),
                                "assists": player_data.get('assists'),
                                "last_hits": player_data.get('last_hits'),
                                "denies": player_data.get('denies'),
                                "gold_per_min": player_data.get('gold_per_min'),
                                "xp_per_min": player_data.get('xp_per_min'),
                                "hero_damage": player_data.get('hero_damage'),
                                "tower_damage": player_data.get('tower_damage'),
                                "hero_healing": player_data.get('hero_healing'),
                                "level": player_data.get('level'),
                                "item_0": player_data.get('item_0'),
                                "item_1": player_data.get('item_1'),
                                "item_2": player_data.get('item_2'),
                                "item_3": player_data.get('item_3'),
                                "item_4": player_data.get('item_4'),
                                "item_5": player_data.get('item_5')
                            }
                        )
            
            session.commit()
        
        # Process draft timings
        if 'draft_timings' in match_data and match_data['draft_timings']:
            for draft_data in match_data['draft_timings']:
                session.execute(
                    text("""
                        INSERT INTO pro_draft_timings 
                        (match_id, "order", pick, active_team, hero_id, 
                         player_slot, extra_time, total_time_taken)
                        VALUES 
                        (:match_id, :order, :pick, :active_team, :hero_id, 
                         :player_slot, :extra_time, :total_time_taken)
                    """),
                    {
                        "match_id": match_id,
                        "order": draft_data.get('order'),
                        "pick": draft_data.get('pick'),
                        "active_team": draft_data.get('active_team'),
                        "hero_id": draft_data.get('hero_id'),
                        "player_slot": draft_data.get('player_slot'),
                        "extra_time": draft_data.get('extra_time'),
                        "total_time_taken": draft_data.get('total_time_taken')
                    }
                )
            
            session.commit()
        
        return True
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding match to database: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def process_team_matches(team, existing_match_ids):
    """
    Process matches for a specific team.
    
    Args:
        team (dict): Team data
        existing_match_ids (set): Set of existing match IDs
        
    Returns:
        int: Number of new matches added
    """
    team_id = team.get('team_id')
    team_name = team.get('name', 'Unknown').replace(' ', '_')
    
    logger.info(f"Processing matches for team {team_name} (ID: {team_id})")
    
    # Create team directory
    team_dir = os.path.join(TEAM_GAMES_DIR, f"{team_name}_{team_id}")
    os.makedirs(team_dir, exist_ok=True)
    
    # Save team info
    team_info_path = os.path.join(team_dir, f"{team_name}_info.json")
    with open(team_info_path, 'w', encoding='utf-8') as f:
        json.dump(team, f, indent=2)
    
    # Get team matches
    match_ids = get_team_matches(team_id, MATCHES_PER_TEAM)
    
    # Filter out existing matches
    new_match_ids = [match_id for match_id in match_ids if match_id not in existing_match_ids]
    
    if not new_match_ids:
        logger.info(f"No new matches for team {team_name}")
        return 0
    
    logger.info(f"Found {len(new_match_ids)} new matches for team {team_name}")
    
    # Initialize database session
    db = DotaDatabase()
    db.create_tables()  # Ensure tables exist
    session = db.Session()
    
    # Download and process matches
    matches_added = 0
    
    for i in range(0, len(new_match_ids), DOWNLOAD_BATCH_SIZE):
        batch = new_match_ids[i:i+DOWNLOAD_BATCH_SIZE]
        
        for match_id in tqdm(batch, desc=f"Processing batch {i//DOWNLOAD_BATCH_SIZE + 1}/{(len(new_match_ids)-1)//DOWNLOAD_BATCH_SIZE + 1}"):
            try:
                # Download match data
                match_data = download_match(match_id)
                
                if not match_data:
                    logger.error(f"Failed to download match {match_id}")
                    continue
                
                # Save match data
                save_match_data(match_data, f"{team_name}_{team_id}")
                
                # Add to database
                success = add_to_database(match_data, session)
                
                if success:
                    existing_match_ids.add(match_id)
                    matches_added += 1
                    logger.info(f"Added match {match_id} to database (Total: {matches_added})")
                
            except Exception as e:
                logger.error(f"Error processing match {match_id}: {str(e)}")
                logger.error(traceback.format_exc())
    
    session.close()
    
    logger.info(f"Added {matches_added} new matches for team {team_name}")
    
    return matches_added

def main():
    """Main function to scrape matches."""
    print("\n" + "=" * 80)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] SCRAPING MATCHES FOR TOP 20 TEAMS (TARGET: 2000 MATCHES)")
    print("=" * 80)
    
    # Create database if it doesn't exist
    db = DotaDatabase()
    db.create_tables()
    
    # Verify current database state
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking current database state")
    current_match_count = verify_database_population()
    
    # Get existing match IDs
    existing_match_ids = get_existing_match_ids()
    
    # Get top teams
    top_teams = get_top_teams(20)
    
    if not top_teams:
        logger.error("Failed to get top teams")
        return
    
    # Process team matches
    total_matches_added = 0
    target_remaining = TARGET_MATCH_COUNT - current_match_count
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting to scrape matches")
    print(f"Target: {target_remaining} additional matches (Total: {TARGET_MATCH_COUNT})")
    
    for team in tqdm(top_teams, desc="Processing teams"):
        # Check if we've reached the target
        if total_matches_added >= target_remaining:
            logger.info(f"Reached target of {TARGET_MATCH_COUNT} matches. Stopping.")
            break
        
        # Process team matches
        matches_added = process_team_matches(team, existing_match_ids)
        total_matches_added += matches_added
        
        logger.info(f"Progress: {total_matches_added}/{target_remaining} matches added")
    
    # Final verification
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Verifying final database state")
    final_match_count = verify_database_population()
    
    # Report results
    print(f"\nAdded {total_matches_added} new matches to the database.")
    print(f"Previous match count: {current_match_count}")
    print(f"Current match count: {final_match_count}")
    print(f"Difference: {final_match_count - current_match_count} matches added")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
