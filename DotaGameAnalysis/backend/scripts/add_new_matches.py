#!/usr/bin/env python3
"""
Script to add downloaded match files to database without wiping existing data.
This script specifically handles SQLite integer overflow issues.
"""

import os
import sys
import json
import logging
from datetime import datetime
import sqlite3
from tqdm import tqdm
import requests

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import backend modules
from database.database_pro_teams import DotaDatabase
from database.database_pro_teams import ProMatch, ProLeague, ProTeam, ProPlayer, ProHero
from database.database_pro_teams import ProMatchPlayerMetric, ProDraftTiming, ProTeamFight
from database.database_pro_teams import ProTeamFightPlayer, ProObjective, ProChatWheel, ProTimeVsStats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database and matches directory paths
DB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database', 'data'))
MATCHES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'database', 'matches'))

def verify_database_population():
    """
    Verify the population of all tables in the database and print a summary.
    """
    logger.info("=== Database Population Verification ===")
    
    db = DotaDatabase()
    session = db.Session()
    
    try:
        match_count = session.query(ProMatch).count()
        league_count = session.query(ProLeague).count()
        team_count = session.query(ProTeam).count()
        player_count = session.query(ProPlayer).count()
        hero_count = session.query(ProHero).count()
        match_player_count = session.query(ProMatchPlayerMetric).count()
        draft_timing_count = session.query(ProDraftTiming).count()
        team_fight_count = session.query(ProTeamFight).count()
        team_fight_player_count = session.query(ProTeamFightPlayer).count()
        objective_count = session.query(ProObjective).count()
        chat_wheel_count = session.query(ProChatWheel).count()
        time_vs_stats_count = session.query(ProTimeVsStats).count()
        
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
    """
    Get the IDs of matches that already exist in the database.
    
    Returns:
        set: Set of match IDs already in the database
    """
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

def handle_large_integers(data, key):
    """
    Safely handle potentially large integers to prevent SQLite overflow.
    
    Args:
        data (dict): Data dictionary
        key (str): Key to handle
        
    Returns:
        value: Safely handled value that won't cause integer overflow
    """
    if key not in data or data[key] is None:
        return None
    
    value = data[key]
    
    # If it's already a string, return it
    if isinstance(value, str):
        return value
    
    # Try to convert to int, but if it's too large, convert to string
    try:
        int_value = int(value)
        # SQLite INTEGER max is 2^63-1, but be more conservative
        if abs(int_value) > 2**60:
            return str(value)
        return int_value
    except (ValueError, OverflowError):
        # If it can't be converted to int or is too large, return as string
        return str(value)

def add_new_matches_to_database():
    """
    Add new match files to the database without wiping existing data.
    Only processes JSON files for matches that don't already exist in the database.
    """
    print("\nScanning match files and database...")
    
    # Get all match files
    match_files = [
        os.path.join(MATCHES_DIR, f) for f in os.listdir(MATCHES_DIR)
        if f.endswith('.json')
    ]
    logger.info(f"Found {len(match_files)} match files")
    
    # Get existing match IDs
    existing_match_ids = get_existing_match_ids()
    logger.info(f"Found {len(existing_match_ids)} existing matches in database")
    
    # Filter out already processed matches
    new_match_files = [
        file for file in match_files 
        if int(os.path.basename(file).split('.')[0]) not in existing_match_ids
    ]
    
    if not new_match_files:
        logger.info("No new matches to process. Database is up to date.")
        return 0
    
    logger.info(f"Found {len(new_match_files)} new match files to process")
    
    # Initialize database connection
    db = DotaDatabase()
    
    # First pass: Extract and add team and league information
    print("\nProcessing team and league information...")
    teams_added = 0
    leagues_added = 0
    
    for json_path in tqdm(new_match_files, desc="First pass"):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            session = db.Session()
            try:
                # Add league if it doesn't exist
                if 'league' in data and data['league'] and 'leagueid' in data['league']:
                    league_id = data['league'].get('leagueid')
                    if league_id is not None:
                        # Check if league exists
                        existing_league = session.query(ProLeague).filter_by(league_id=league_id).first()
                        if not existing_league:
                            league = ProLeague(
                                league_id=league_id,
                                name=data['league'].get('name'),
                                tier=data['league'].get('tier'),
                                ticket=data['league'].get('ticket')
                            )
                            session.add(league)
                            leagues_added += 1
                
                # Add teams if they don't exist
                for side in ['radiant', 'dire']:
                    team_key = f'{side}_team'
                    if team_key in data and data[team_key] and 'team_id' in data[team_key]:
                        team_data = data[team_key]
                        team_id = handle_large_integers(team_data, 'team_id')
                        if team_id is not None:
                            # Check if team exists
                            existing_team = session.query(ProTeam).filter_by(team_id=team_id).first()
                            if not existing_team:
                                team = ProTeam(
                                    team_id=team_id,
                                    name=team_data.get('name'),
                                    tag=team_data.get('tag'),
                                    logo_url=team_data.get('logo_url')
                                )
                                session.add(team)
                                teams_added += 1
                
                session.commit()
            except Exception as e:
                logger.error(f"Error processing league/team data for {json_path}: {str(e)}")
                session.rollback()
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"Error loading match data from {json_path}: {str(e)}")
    
    logger.info(f"Added {leagues_added} new leagues and {teams_added} new teams")
    
    # Second pass: Process all match data
    print("\nAdding match data to database...")
    matches_added = 0
    
    for json_path in tqdm(new_match_files, desc="Second pass"):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            match_id = int(os.path.basename(json_path).split('.')[0])
            
            # Skip if this match is already in the database
            if match_id in existing_match_ids:
                continue
            
            session = db.Session()
            try:
                # Get relevant data for ProMatch
                start_time = data.get('start_time')
                duration = data.get('duration')
                league_id = data.get('leagueid')
                radiant_score = data.get('radiant_score')
                dire_score = data.get('dire_score')
                radiant_win = data.get('radiant_win')
                
                # Handle potentially problematic integer values
                series_id = handle_large_integers(data, 'series_id')
                series_type = handle_large_integers(data, 'series_type')
                
                # Get team IDs if available
                radiant_team_id = None
                dire_team_id = None
                
                if 'radiant_team' in data and data['radiant_team'] and 'team_id' in data['radiant_team']:
                    radiant_team_id = handle_large_integers(data['radiant_team'], 'team_id')
                
                if 'dire_team' in data and data['dire_team'] and 'team_id' in data['dire_team']:
                    dire_team_id = handle_large_integers(data['dire_team'], 'team_id')
                
                # Safely convert radiant_gold_adv and dire_gold_adv to strings
                radiant_gold_adv = None
                dire_gold_adv = None
                
                if 'radiant_gold_adv' in data and data['radiant_gold_adv']:
                    radiant_gold_adv = str(data['radiant_gold_adv'])
                
                if 'dire_gold_adv' in data and data['dire_gold_adv']:
                    dire_gold_adv = str(data['dire_gold_adv'])
                
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
                    radiant_score=radiant_score,
                    dire_score=dire_score,
                    radiant_win=radiant_win,
                    radiant_gold_adv=radiant_gold_adv,
                    dire_gold_adv=dire_gold_adv,
                    version=data.get('version')
                )
                session.add(match)
                
                # Add players
                if 'players' in data:
                    for player_data in data['players']:
                        account_id = player_data.get('account_id')
                        if account_id:
                            # Add player if doesn't exist
                            player = session.query(ProPlayer).filter_by(account_id=account_id).first()
                            if not player:
                                player = ProPlayer(
                                    account_id=account_id,
                                    name=player_data.get('name') or player_data.get('personaname'),
                                    steamid=player_data.get('steamid')
                                )
                                session.add(player)
                        
                        # Add hero if doesn't exist
                        hero_id = player_data.get('hero_id')
                        if hero_id:
                            hero = session.query(ProHero).filter_by(hero_id=hero_id).first()
                            if not hero:
                                hero = ProHero(
                                    hero_id=hero_id,
                                    name=player_data.get('hero', f"Hero {hero_id}")
                                )
                                session.add(hero)
                        
                        # Add match player metrics
                        if account_id and hero_id:
                            player_slot = player_data.get('player_slot')
                            metrics = ProMatchPlayerMetric(
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
                                gold=player_data.get('gold'),
                                gold_spent=player_data.get('gold_spent'),
                                item_0=player_data.get('item_0'),
                                item_1=player_data.get('item_1'),
                                item_2=player_data.get('item_2'),
                                item_3=player_data.get('item_3'),
                                item_4=player_data.get('item_4'),
                                item_5=player_data.get('item_5')
                            )
                            session.add(metrics)
                
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
                
                # Add team fights
                if 'teamfights' in data and data['teamfights']:
                    for i, fight_data in enumerate(data['teamfights']):
                        fight = ProTeamFight(
                            match_id=match_id,
                            fight_number=i,
                            start_time=fight_data.get('start'),
                            end_time=fight_data.get('end'),
                            last_death=fight_data.get('last_death'),
                            deaths=fight_data.get('deaths'),
                            radiant_gold_advantage_delta=fight_data.get('radiant_gold_adv_delta', 0)
                        )
                        session.add(fight)
                        
                        # Add team fight players
                        if 'players' in fight_data:
                            for player_id, player_fight_data in fight_data['players'].items():
                                if player_id.isdigit():  # Ensure player_id is valid
                                    fight_player = ProTeamFightPlayer(
                                        match_id=match_id,
                                        fight_number=i,
                                        account_id=int(player_id),
                                        deaths=player_fight_data.get('deaths', 0),
                                        buybacks=player_fight_data.get('buybacks', 0),
                                        damage=player_fight_data.get('damage', 0),
                                        healing=player_fight_data.get('healing', 0),
                                        gold_delta=player_fight_data.get('gold_delta', 0),
                                        xp_delta=player_fight_data.get('xp_delta', 0)
                                    )
                                    session.add(fight_player)
                
                # Add objectives
                if 'objectives' in data and data['objectives']:
                    for obj_data in data['objectives']:
                        # Handle potentially large 'key' values
                        key_value = handle_large_integers(obj_data, 'key')
                        
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
                
                # Add chat wheel
                if 'chat' in data and data['chat']:
                    for chat_data in data['chat']:
                        if chat_data.get('type') == 'chatwheel':
                            chat = ProChatWheel(
                                match_id=match_id,
                                time=chat_data.get('time'),
                                player_slot=chat_data.get('player_slot'),
                                message_id=chat_data.get('key')
                            )
                            session.add(chat)
                
                # Add time vs stats
                if 'radiant_gold_adv' in data and data['radiant_gold_adv']:
                    for i, gold in enumerate(data['radiant_gold_adv']):
                        xp = data.get('radiant_xp_adv', [])[i] if i < len(data.get('radiant_xp_adv', [])) else None
                        stat = ProTimeVsStats(
                            match_id=match_id,
                            time=i,  # 1-minute intervals
                            radiant_gold_advantage=gold,
                            radiant_xp_advantage=xp
                        )
                        session.add(stat)
                
                # Commit after each match to avoid large transactions
                session.commit()
                matches_added += 1
                
            except Exception as e:
                logger.error(f"Error processing match data for {json_path}: {str(e)}")
                session.rollback()
            finally:
                session.close()
        
        except Exception as e:
            logger.error(f"Error loading match data from {json_path}: {str(e)}")
    
    return matches_added

def main():
    """Main function."""
    print("=" * 80)
    print("ADDING NEW MATCHES TO DATABASE (WITHOUT CLEARING EXISTING DATA)")
    print("=" * 80)
    
    # Verify current database state
    print("\nSTEP 1: Checking current database state")
    current_match_count = verify_database_population()
    
    # Add new matches to database
    print("\nSTEP 2: Adding new matches to database")
    matches_added = add_new_matches_to_database()
    
    # Final verification
    print("\nSTEP 3: Verifying final database state")
    final_match_count = verify_database_population()
    
    # Report results
    print(f"\nSuccessfully added {matches_added} new matches to the database.")
    print(f"Total matches in database: {final_match_count}")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
