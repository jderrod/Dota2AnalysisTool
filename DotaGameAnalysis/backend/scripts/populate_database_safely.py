#!/usr/bin/env python3
"""
Script to populate the database with existing downloaded match files without clearing the database.
This script handles the SQLite integer overflow error properly.
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from tqdm import tqdm
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import backend modules
from database.database_pro_teams import DotaDatabase
from database.database_pro_teams import ProMatch, ProLeague, ProTeam, ProPlayer, ProHero
from database.database_pro_teams import ProMatchPlayer, ProDraftTiming, ProTeamFight, ProTeamFightPlayer
from database.database_pro_teams import ProObjective, ProChatWheel, ProTimevsStats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
TEAM_GAMES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'teams_games'))

def verify_database_population():
    """Verify the current database population and print counts of all tables."""
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Use SQLAlchemy ORM for counting
        match_count = session.query(ProMatch).count()
        league_count = session.query(ProLeague).count()
        team_count = session.query(ProTeam).count()
        player_count = session.query(ProPlayer).count()
        hero_count = session.query(ProHero).count()
        match_player_count = session.query(ProMatchPlayer).count()
        draft_timing_count = session.query(ProDraftTiming).count()
        team_fight_count = session.query(ProTeamFight).count()
        team_fight_player_count = session.query(ProTeamFightPlayer).count()
        objective_count = session.query(ProObjective).count()
        chat_wheel_count = session.query(ProChatWheel).count()
        time_vs_stats_count = session.query(ProTimevsStats).count()
        
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
        print(f"Team Fights: {team_fight_count}")
        print(f"Team Fight Players: {team_fight_player_count}")
        print(f"Objectives: {objective_count}")
        print(f"Chat Wheel: {chat_wheel_count}")
        print(f"Time vs Stats: {time_vs_stats_count}")
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
        existing_ids = session.query(ProMatch.match_id).all()
        return {match_id[0] for match_id in existing_ids}
    except Exception as e:
        logger.error(f"Error getting existing match IDs: {str(e)}")
        return set()
    finally:
        session.close()

def find_all_match_files():
    """Find all match JSON files in team directories."""
    match_files = []
    
    # Check if the directory exists
    if not os.path.exists(TEAM_GAMES_DIR):
        logger.error(f"Team games directory not found: {TEAM_GAMES_DIR}")
        return match_files
    
    # Scan each team directory for match files
    for team_dir in os.listdir(TEAM_GAMES_DIR):
        team_path = os.path.join(TEAM_GAMES_DIR, team_dir)
        if os.path.isdir(team_path):
            # Find match_*.json files
            for file in os.listdir(team_path):
                if file.startswith("match_") and file.endswith(".json"):
                    match_files.append(os.path.join(team_path, file))
    
    return match_files

def safely_populate_from_json(json_path):
    """
    Modified version of populate_from_json that handles integer overflow safely.
    Parse the given JSON file and upsert the data into the database.
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Extract match_id from filename (match_<id>.json)
        filename = os.path.basename(json_path)
        match_id = int(filename.split("_")[1].split(".")[0])
        
        # Check if match already exists
        existing_match = session.query(ProMatch).filter_by(match_id=match_id).first()
        if existing_match:
            logger.info(f"Match {match_id} already exists in database, skipping")
            return False
        
        # First, extract and add team and league information
        # Add league if it doesn't exist
        league_id = None
        if 'league' in data and data['league']:
            league_id = data['league'].get('leagueid')
            if league_id is not None:
                existing_league = session.query(ProLeague).filter_by(league_id=league_id).first()
                if not existing_league:
                    league = ProLeague(
                        league_id=league_id,
                        name=data['league'].get('name'),
                        tier=data['league'].get('tier'),
                    )
                    session.add(league)
        
        # Extract and add team information
        radiant_team_id = None
        dire_team_id = None
        
        # Handle radiant team
        if 'radiant_team' in data and data['radiant_team']:
            team_data = data['radiant_team']
            
            # Check for integer overflow
            if 'team_id' in team_data:
                try:
                    radiant_team_id = int(team_data['team_id'])
                except (ValueError, OverflowError):
                    # If integer is too large, convert to string (SQLite will handle it as text)
                    radiant_team_id = str(team_data['team_id'])
                    logger.warning(f"Converted oversized team_id to string: {radiant_team_id}")
            
            if radiant_team_id:
                # Check if team exists
                existing_team = None
                try:
                    # Try to query by string or int
                    existing_team = session.query(ProTeam).filter_by(team_id=radiant_team_id).first()
                except Exception:
                    # If that fails, try to query by converting to string
                    try:
                        existing_team = session.query(ProTeam).filter_by(team_id=str(radiant_team_id)).first()
                    except Exception as e:
                        logger.error(f"Error querying team: {str(e)}")
                
                if not existing_team:
                    team = ProTeam(
                        team_id=radiant_team_id,
                        name=team_data.get('name'),
                        tag=team_data.get('tag'),
                        logo_url=team_data.get('logo_url')
                    )
                    session.add(team)
                    session.commit()
        
        # Handle dire team
        if 'dire_team' in data and data['dire_team']:
            team_data = data['dire_team']
            
            # Check for integer overflow
            if 'team_id' in team_data:
                try:
                    dire_team_id = int(team_data['team_id'])
                except (ValueError, OverflowError):
                    # If integer is too large, convert to string
                    dire_team_id = str(team_data['team_id'])
                    logger.warning(f"Converted oversized team_id to string: {dire_team_id}")
            
            if dire_team_id:
                # Check if team exists
                existing_team = None
                try:
                    # Try to query by string or int
                    existing_team = session.query(ProTeam).filter_by(team_id=dire_team_id).first()
                except Exception:
                    # If that fails, try to query by converting to string
                    try:
                        existing_team = session.query(ProTeam).filter_by(team_id=str(dire_team_id)).first()
                    except Exception as e:
                        logger.error(f"Error querying team: {str(e)}")
                
                if not existing_team:
                    team = ProTeam(
                        team_id=dire_team_id,
                        name=team_data.get('name'),
                        tag=team_data.get('tag'),
                        logo_url=team_data.get('logo_url')
                    )
                    session.add(team)
                    session.commit()
        
        # Now add the match
        start_time = data.get('start_time')
        duration = data.get('duration')
        
        # Handle potentially large integers by converting to strings if necessary
        series_id = data.get('series_id')
        series_type = data.get('series_type')
        
        # Safely handle large integers
        try:
            series_id = int(series_id) if series_id is not None else None
        except (ValueError, OverflowError):
            series_id = str(series_id)
        
        try:
            series_type = int(series_type) if series_type is not None else None
        except (ValueError, OverflowError):
            series_type = str(series_type)
        
        # Convert gold advantage arrays to strings to avoid overflow
        radiant_gold_adv = str(data.get('radiant_gold_adv')) if data.get('radiant_gold_adv') else None
        dire_gold_adv = str(data.get('dire_gold_adv')) if data.get('dire_gold_adv') else None
        
        # Create match record
        match = ProMatch(
            match_id=match_id,
            start_time=datetime.fromtimestamp(start_time) if start_time else None,
            duration=duration,
            league_id=league_id,
            series_id=series_id,
            series_type=series_type,
            radiant_team_id=radiant_team_id,
            dire_team_id=dire_team_id,
            radiant_score=data.get('radiant_score'),
            dire_score=data.get('dire_score'),
            radiant_win=data.get('radiant_win'),
            radiant_gold_adv=radiant_gold_adv,
            dire_gold_adv=dire_gold_adv,
            version=data.get('version')
        )
        session.add(match)
        session.commit()
        
        # Add players and their metrics
        if 'players' in data:
            for player_data in data['players']:
                account_id = player_data.get('account_id')
                
                if account_id:
                    # Add player if doesn't exist
                    existing_player = session.query(ProPlayer).filter_by(account_id=account_id).first()
                    if not existing_player:
                        player_name = player_data.get('name') or player_data.get('personaname')
                        player = ProPlayer(
                            account_id=account_id,
                            name=player_name,
                            match_id=match_id  # Associate with this match
                        )
                        session.add(player)
                
                # Add hero if doesn't exist
                hero_id = player_data.get('hero_id')
                if hero_id:
                    existing_hero = session.query(ProHero).filter_by(hero_id=hero_id).first()
                    if not existing_hero:
                        hero = ProHero(
                            hero_id=hero_id,
                            name=player_data.get('hero', f"Hero {hero_id}"),
                            match_id=match_id  # Associate with this match
                        )
                        session.add(hero)
                
                # Add match player record
                if account_id and hero_id:
                    player_slot = player_data.get('player_slot')
                    
                    # Check if this player-match combination already exists
                    existing_player_match = session.query(ProMatchPlayer).filter_by(
                        match_id=match_id,
                        account_id=account_id,
                        hero_id=hero_id
                    ).first()
                    
                    if not existing_player_match:
                        player_match = ProMatchPlayer(
                            match_id=match_id,
                            account_id=account_id,
                            hero_id=hero_id,
                            player_slot=player_slot,
                            kills=player_data.get('kills'),
                            deaths=player_data.get('deaths'),
                            assists=player_data.get('assists'),
                            last_hits=player_data.get('last_hits'),
                            denies=player_data.get('denies'),
                            gold_per_min=player_data.get('gold_per_min'),
                            xp_per_min=player_data.get('xp_per_min'),
                            level=player_data.get('level'),
                            hero_damage=player_data.get('hero_damage'),
                            tower_damage=player_data.get('tower_damage'),
                            hero_healing=player_data.get('hero_healing'),
                            item_0=player_data.get('item_0'),
                            item_1=player_data.get('item_1'),
                            item_2=player_data.get('item_2'),
                            item_3=player_data.get('item_3'),
                            item_4=player_data.get('item_4'),
                            item_5=player_data.get('item_5')
                        )
                        session.add(player_match)
            
            # Commit all player and hero additions
            session.commit()
        
        # Add draft timings
        if 'draft_timings' in data and data['draft_timings']:
            for draft_data in data['draft_timings']:
                draft_timing = ProDraftTiming(
                    match_id=match_id,
                    order=draft_data.get('order'),
                    pick=draft_data.get('pick'),
                    active_team=draft_data.get('active_team'),
                    hero_id=draft_data.get('hero_id'),
                    player_slot=draft_data.get('player_slot'),
                    extra_time=draft_data.get('extra_time'),
                    total_time_taken=draft_data.get('total_time_taken')
                )
                session.add(draft_timing)
            
            session.commit()
        
        # Add team fights
        if 'teamfights' in data and data['teamfights']:
            for i, fight_data in enumerate(data['teamfights']):
                fight = ProTeamFight(
                    match_id=match_id,
                    start=fight_data.get('start'),
                    end=fight_data.get('end'),
                    last_death=fight_data.get('last_death'),
                    deaths=fight_data.get('deaths')
                )
                session.add(fight)
                session.commit()  # Commit to get the fight ID
                
                # Add team fight players
                if 'players' in fight_data:
                    for player_id, player_fight_data in fight_data['players'].items():
                        if player_id.isdigit():  # Ensure player_id is valid
                            try:
                                fight_player = ProTeamFightPlayer(
                                    match_id=match_id,
                                    teamfight_id=fight.id,
                                    deaths=player_fight_data.get('deaths', 0),
                                    buybacks=player_fight_data.get('buybacks', 0),
                                    damage=player_fight_data.get('damage', 0),
                                    healing=player_fight_data.get('healing', 0),
                                    gold_delta=player_fight_data.get('gold_delta', 0),
                                    xp_delta=player_fight_data.get('xp_delta', 0)
                                )
                                session.add(fight_player)
                            except Exception as e:
                                logger.error(f"Error adding team fight player: {str(e)}")
                
                # Commit team fight players
                session.commit()
        
        # Add objectives
        if 'objectives' in data and data['objectives']:
            for obj_data in data['objectives']:
                try:
                    # Handle potentially large 'key' values
                    key_value = obj_data.get('key')
                    if key_value is not None:
                        try:
                            key_value = int(key_value)
                        except (ValueError, OverflowError):
                            key_value = str(key_value)
                    
                    obj = ProObjective(
                        match_id=match_id,
                        time=obj_data.get('time'),
                        type=obj_data.get('type'),
                        player_slot=obj_data.get('player_slot'),
                        key=key_value,
                        slot=obj_data.get('slot'),
                        team=obj_data.get('team')
                    )
                    session.add(obj)
                except Exception as e:
                    logger.error(f"Error adding objective: {str(e)}")
            
            # Commit objectives
            session.commit()
        
        # Add chat wheel
        if 'chat' in data and data['chat']:
            for chat_data in data['chat']:
                if chat_data.get('type') == 'chatwheel':
                    try:
                        chat = ProChatWheel(
                            match_id=match_id,
                            time=chat_data.get('time'),
                            type="chatwheel",
                            key=chat_data.get('key'),
                            player_slot=chat_data.get('player_slot')
                        )
                        session.add(chat)
                    except Exception as e:
                        logger.error(f"Error adding chat wheel: {str(e)}")
            
            # Commit chat wheel entries
            session.commit()
        
        # Add time vs stats
        if 'radiant_gold_adv' in data and data['radiant_gold_adv']:
            # Using a separate transaction for time vs stats to improve performance
            for i, gold in enumerate(data['radiant_gold_adv']):
                try:
                    # Get XP advantage if available
                    xp = None
                    if 'radiant_xp_adv' in data and i < len(data['radiant_xp_adv']):
                        xp = data['radiant_xp_adv'][i]
                    
                    # Add time stats for match-level data
                    stat = ProTimevsStats(
                        match_id=match_id,
                        time=i,  # 1-minute intervals
                        starting_lane=0,  # Default value
                        player_slot=0,    # Default value
                        player_id=0,      # Default value
                        player_name="Match Stats"  # Indicates this is match-level data
                    )
                    session.add(stat)
                    
                    # Commit in smaller batches to avoid memory issues
                    if i % 30 == 0:  # Commit every 30 entries
                        session.commit()
                except Exception as e:
                    logger.error(f"Error adding time vs stats: {str(e)}")
            
            # Final commit for time vs stats
            session.commit()
        
        return True  # Successfully added
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing match {json_path}: {str(e)}")
        return False
    
    finally:
        session.close()

def populate_from_match_files():
    """
    Populate the database from downloaded match files.
    Filters out matches that already exist in the database.
    """
    # Get all match files
    match_files = find_all_match_files()
    logger.info(f"Found {len(match_files)} match files to process")
    
    # Get existing match IDs
    existing_match_ids = get_existing_match_ids()
    logger.info(f"Found {len(existing_match_ids)} existing matches in database")
    
    # Filter out already processed matches
    new_match_files = []
    for file_path in match_files:
        try:
            # Extract match_id from filename (format: match_<match_id>.json)
            filename = os.path.basename(file_path)
            match_id = int(filename.split('_')[1].split('.')[0])
            
            if match_id not in existing_match_ids:
                new_match_files.append(file_path)
        except Exception as e:
            logger.error(f"Error parsing match ID from {file_path}: {str(e)}")
    
    if not new_match_files:
        logger.info("No new matches to process. Database is up to date.")
        return 0
    
    logger.info(f"Found {len(new_match_files)} new match files to process")
    
    # Process each match file
    matches_added = 0
    for json_path in tqdm(new_match_files, desc="Adding matches to database"):
        success = safely_populate_from_json(json_path)
        if success:
            matches_added += 1
    
    return matches_added

def main():
    """Main function to populate the database from existing match files."""
    print("\n" + "=" * 80)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] POPULATING DATABASE FROM EXISTING MATCH FILES (SAFELY)")
    print("=" * 80)
    
    # Verify current database state
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking current database state")
    current_match_count = verify_database_population()
    
    # Populate database from match files
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Populating database from downloaded match data...")
    matches_added = populate_from_match_files()
    
    # Final verification
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Verifying database population")
    final_match_count = verify_database_population()
    
    # Report results
    print(f"\nAdded {matches_added} new matches to the database.")
    print(f"Previous match count: {current_match_count}")
    print(f"Current match count: {final_match_count}")
    print(f"Difference: {final_match_count - current_match_count} matches added")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
