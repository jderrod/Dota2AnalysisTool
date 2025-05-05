#!/usr/bin/env python3
"""
Script to safely populate the database with downloaded match files.
This script handles integer overflow issues and doesn't clear existing data.
"""

import json
import os
import sys
import logging
from datetime import datetime
from tqdm import tqdm
import sqlalchemy
from sqlalchemy import create_engine, func, Column, String, Integer, Boolean, Float, ForeignKey, DateTime, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, aliased
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.sql import text
from pathlib import Path

# Get the absolute path to the project root
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

# Import models directly from their location
try:
    from models.pro_models import (
        Base, ProMatch, ProLeague, ProTeam, ProPlayer, ProHero,
        ProMatchPlayerMetric, ProDraftTiming, ProTeamFight,
        ProTeamFightPlayer, ProObjective, ProChatWheel, ProTimeVsStats
    )
except ImportError:
    # Alternative import path if the first one fails
    from database.models.pro_models import (
        Base, ProMatch, ProLeague, ProTeam, ProPlayer, ProHero,
        ProMatchPlayerMetric, ProDraftTiming, ProTeamFight,
        ProTeamFightPlayer, ProObjective, ProChatWheel, ProTimeVsStats
    )

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database path
DB_PATH = str(Path(__file__).parent.parent / "database" / "data" / "dota_matches.db")
MATCHES_DIR = str(Path(__file__).parent.parent / "database" / "matches")

def get_db_engine():
    """Create and return a database engine."""
    engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
    return engine

def verify_database_population(engine):
    """Verify the population of all tables in the database."""
    logger.info("=== Database Population Verification ===")
    
    with engine.connect() as conn:
        match_count = conn.execute(text("SELECT COUNT(*) FROM pro_matches")).scalar()
        league_count = conn.execute(text("SELECT COUNT(*) FROM pro_leagues")).scalar()
        team_count = conn.execute(text("SELECT COUNT(*) FROM pro_teams")).scalar()
        player_count = conn.execute(text("SELECT COUNT(*) FROM pro_players")).scalar()
        hero_count = conn.execute(text("SELECT COUNT(*) FROM pro_heroes")).scalar()
        match_player_count = conn.execute(text("SELECT COUNT(*) FROM pro_match_player_metrics")).scalar()
        draft_timing_count = conn.execute(text("SELECT COUNT(*) FROM pro_draft_timings")).scalar()
        team_fight_count = conn.execute(text("SELECT COUNT(*) FROM pro_teamfights")).scalar()
        team_fight_player_count = conn.execute(text("SELECT COUNT(*) FROM pro_teamfight_players")).scalar()
        objective_count = conn.execute(text("SELECT COUNT(*) FROM pro_objectives")).scalar()
        chat_wheel_count = conn.execute(text("SELECT COUNT(*) FROM pro_chatwheel")).scalar()
        time_vs_stats_count = conn.execute(text("SELECT COUNT(*) FROM pro_timevsstats")).scalar()
    
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

def safely_convert_integer(value):
    """Safely convert potentially large integers to fit SQLite limits."""
    if value is None:
        return None
        
    try:
        # Try to convert to int first
        return int(value)
    except (OverflowError, ValueError):
        # If it's too large or not a valid int, convert to string
        return str(value)

def populate_database_from_jsons(engine, match_files):
    """
    Populate the database with match data from JSON files.
    """
    print("Populating database from downloaded match files...")
    
    # Create database session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Get existing match IDs to avoid duplicates
    existing_match_ids = {match_id[0] for match_id in session.query(ProMatch.match_id).all()}
    logger.info(f"Found {len(existing_match_ids)} existing matches in database")
    
    # Filter out already processed matches
    new_match_files = [
        file for file in match_files 
        if int(os.path.basename(file).split('.')[0]) not in existing_match_ids
    ]
    
    logger.info(f"Found {len(new_match_files)} new match files to process")
    
    if not new_match_files:
        logger.info("No new matches to process. Database is up to date.")
        return
    
    # First pass: Extract team and league information
    for json_path in tqdm(new_match_files, desc="First pass - extracting teams & leagues"):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Add league if doesn't exist
            if 'league' in data and data['league']:
                league_id = data['league'].get('leagueid')
                if league_id is not None:
                    # Check if league exists
                    existing_league = session.query(ProLeague).filter_by(league_id=league_id).one_or_none()
                    if not existing_league:
                        league = ProLeague(
                            league_id=league_id,
                            name=data['league'].get('name'),
                            tier=data['league'].get('tier'),
                            ticket=data['league'].get('ticket')
                        )
                        session.add(league)
            
            # Add teams if they don't exist
            for side in ['radiant', 'dire']:
                team_key = f'{side}_team'
                if team_key in data and data[team_key]:
                    team_data = data[team_key]
                    team_id = team_data.get('team_id')
                    if team_id is not None:
                        # Convert team_id if needed
                        team_id = safely_convert_integer(team_id)
                        
                        # Check if team exists
                        existing_team = session.query(ProTeam).filter_by(team_id=team_id).one_or_none()
                        if not existing_team:
                            team = ProTeam(
                                team_id=team_id,
                                name=team_data.get('name'),
                                tag=team_data.get('tag'),
                                logo_url=team_data.get('logo_url')
                            )
                            session.add(team)
            
            # Commit after each batch to avoid memory issues
            session.commit()
                
        except Exception as e:
            logger.error(f"Error processing league/team data for {json_path}: {str(e)}")
            session.rollback()
    
    # Second pass: Process all match data
    for json_path in tqdm(new_match_files, desc="Second pass - populating matches & details"):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            match_id = int(os.path.basename(json_path).split('.')[0])
            
            # Skip if this match is already in the database
            if match_id in existing_match_ids:
                continue
            
            # Get relevant data for ProMatch
            start_time = data.get('start_time')
            duration = data.get('duration')
            league_id = data.get('leagueid')
            radiant_score = data.get('radiant_score')
            dire_score = data.get('dire_score')
            radiant_win = data.get('radiant_win')
            
            # Handle potentially problematic integer values
            series_id = safely_convert_integer(data.get('series_id'))
            series_type = safely_convert_integer(data.get('series_type'))
            
            # Get team IDs if available
            radiant_team_id = None
            dire_team_id = None
            
            if 'radiant_team' in data and data['radiant_team']:
                radiant_team_id = safely_convert_integer(data['radiant_team'].get('team_id'))
            
            if 'dire_team' in data and data['dire_team']:
                dire_team_id = safely_convert_integer(data['dire_team'].get('team_id'))
            
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
                radiant_gold_adv=str(data.get('radiant_gold_adv')) if data.get('radiant_gold_adv') else None,
                dire_gold_adv=str(data.get('dire_gold_adv')) if data.get('dire_gold_adv') else None,
                version=data.get('version')
            )
            session.add(match)
            
            # Add players
            if 'players' in data:
                for player_data in data['players']:
                    account_id = player_data.get('account_id')
                    if account_id:
                        # Add player if doesn't exist
                        player = session.query(ProPlayer).filter_by(account_id=account_id).one_or_none()
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
                        hero = session.query(ProHero).filter_by(hero_id=hero_id).one_or_none()
                        if not hero:
                            hero = ProHero(
                                hero_id=hero_id,
                                name=player_data.get('hero', f"Hero {hero_id}")
                            )
                            session.add(hero)
                    
                    # Add match player metrics
                    player_slot = player_data.get('player_slot')
                    metrics = ProMatchPlayerMetric(
                        match_id=match_id,
                        account_id=account_id,
                        hero_id=hero_id,
                        player_slot=player_slot,
                        # Only store string values to avoid integer overflow
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
                        radiant_gold_advantage_delta=fight_data.get('radiant_gold_adv_delta') 
                            if 'radiant_gold_adv_delta' in fight_data else 0
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
                    obj = ProObjective(
                        match_id=match_id,
                        time=obj_data.get('time'),
                        type=obj_data.get('type'),
                        player_slot=obj_data.get('player_slot'),
                        key=str(obj_data.get('key')),  # Convert to string to avoid overflow
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
                
        except Exception as e:
            logger.error(f"Error processing match data for {json_path}: {str(e)}")
            session.rollback()
    
    session.close()
    logger.info(f"Database population complete. Added {len(new_match_files)} new matches.")

def main():
    """Main function."""
    print("=" * 80)
    print(f"POPULATING DATABASE FROM DOWNLOADED MATCH FILES (SAFELY)")
    print("=" * 80)
    
    # Create engine and initialize database
    engine = get_db_engine()
    
    # Verify current database state
    print("\nSTEP 1: Checking current database state")
    current_match_count = verify_database_population(engine)
    
    # Get all match files
    match_files = [
        os.path.join(MATCHES_DIR, f) for f in os.listdir(MATCHES_DIR)
        if f.endswith('.json')
    ]
    print(f"\nSTEP 2: Found {len(match_files)} match files")
    
    # Populate database
    print("\nSTEP 3: Populating database with new matches")
    populate_database_from_jsons(engine, match_files)
    
    # Final verification
    print("\nSTEP 4: Verifying database population")
    final_match_count = verify_database_population(engine)
    
    # Report results
    print(f"\nAdded {final_match_count - current_match_count} new matches to the database.")
    print(f"Total matches in database: {final_match_count}")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
