#!/usr/bin/env python3
"""
Import all match files, even if they might already exist in the database.
This script will check the actual match ID inside the JSON, not just the filename.
"""

import os
import sys
import json
import logging
from datetime import datetime
from tqdm import tqdm
from pathlib import Path
import traceback
from sqlalchemy import text, exc
import glob

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import database modules
from database.database_pro_teams import DotaDatabase, Base
from database.database_pro_teams import ProMatch, ProLeague, ProTeam, ProPlayer, ProHero
from database.database_pro_teams import ProMatchPlayer, ProDraftTiming, ProTeamFight
from database.database_pro_teams import ProTeamFightPlayer, ProObjective, ProChatWheel, ProTimevsStats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("import_all_matches.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Path to team games directory
TEAM_GAMES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'teams_games'))

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
        team_fight_count = session.execute(text("SELECT COUNT(*) FROM pro_teamfights")).scalar()
        team_fight_player_count = session.execute(text("SELECT COUNT(*) FROM pro_teamfight_players")).scalar()
        objective_count = session.execute(text("SELECT COUNT(*) FROM pro_objectives")).scalar()
        chat_wheel_count = session.execute(text("SELECT COUNT(*) FROM pro_chatwheel")).scalar()
        time_vs_stats_count = session.execute(text("SELECT COUNT(*) FROM pro_timevsstats")).scalar()
        
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
        result = session.execute(text("SELECT match_id FROM pro_matches"))
        existing_ids = {row[0] for row in result}
        print(f"Found {len(existing_ids)} existing match IDs in database")
        return existing_ids
    except Exception as e:
        logger.error(f"Error getting existing match IDs: {str(e)}")
        return set()
    finally:
        session.close()

def find_all_match_files():
    """Find all match JSON files in team directories."""
    match_files = []
    
    # Use glob to find all match_*.json files recursively
    pattern = os.path.join(TEAM_GAMES_DIR, "**", "match_*.json")
    match_files = glob.glob(pattern, recursive=True)
    
    if not match_files:
        logger.error(f"No match files found in: {TEAM_GAMES_DIR}")
    
    return match_files

def safely_convert_int(value):
    """Safely convert a value to int, or return as string if it's too large."""
    if value is None:
        return None
    
    try:
        return int(value)
    except (ValueError, OverflowError):
        # We'll convert large integers to strings
        return str(value)

def add_match_to_database(json_path, existing_match_ids):
    """Add a match from JSON file to database, handling large integers."""
    # First, check if the match is already in the database by examining the JSON content
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        # Get the actual match ID from the JSON data, not just the filename
        match_id = data.get('match_id')
        if not match_id:
            logger.error(f"No match_id found in {json_path}")
            return False
            
        if match_id in existing_match_ids:
            logger.info(f"Match {match_id} already exists in database, skipping")
            return False
    except Exception as e:
        logger.error(f"Error reading match data from {json_path}: {str(e)}")
        return False
        
    # Now proceed with adding the match
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Process leagues
        league_id = None
        if 'league' in data and data['league']:
            league_id = data['league'].get('leagueid')
            if league_id:
                # Check if league exists
                existing_league = session.execute(
                    text("SELECT league_id FROM pro_leagues WHERE league_id = :league_id"),
                    {"league_id": league_id}
                ).first()
                
                if not existing_league:
                    # Insert league
                    try:
                        session.execute(
                            text("""
                                INSERT INTO pro_leagues 
                                (league_id, name, tier) 
                                VALUES (:league_id, :name, :tier)
                            """),
                            {
                                "league_id": league_id,
                                "name": data['league'].get('name'),
                                "tier": data['league'].get('tier')
                            }
                        )
                        session.commit()
                        logger.info(f"Added league {league_id}")
                    except exc.SQLAlchemyError as e:
                        session.rollback()
                        logger.warning(f"Error adding league {league_id}: {str(e)}")
        
        # Process teams
        radiant_team_id = None
        dire_team_id = None
        
        # Handle radiant team
        if 'radiant_team' in data and data['radiant_team']:
            team_data = data['radiant_team']
            if 'team_id' in team_data:
                radiant_team_id = safely_convert_int(team_data['team_id'])
                
                # Check if team exists
                existing_team = session.execute(
                    text("SELECT team_id FROM pro_teams WHERE team_id = :team_id"),
                    {"team_id": radiant_team_id}
                ).first()
                
                if not existing_team:
                    # Insert team
                    try:
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
                        logger.info(f"Added radiant team {radiant_team_id}")
                    except exc.SQLAlchemyError as e:
                        session.rollback()
                        logger.warning(f"Error adding radiant team {radiant_team_id}: {str(e)}")
        
        # Handle dire team
        if 'dire_team' in data and data['dire_team']:
            team_data = data['dire_team']
            if 'team_id' in team_data:
                dire_team_id = safely_convert_int(team_data['team_id'])
                
                # Check if team exists
                existing_team = session.execute(
                    text("SELECT team_id FROM pro_teams WHERE team_id = :team_id"),
                    {"team_id": dire_team_id}
                ).first()
                
                if not existing_team:
                    # Insert team
                    try:
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
                        logger.info(f"Added dire team {dire_team_id}")
                    except exc.SQLAlchemyError as e:
                        session.rollback()
                        logger.warning(f"Error adding dire team {dire_team_id}: {str(e)}")
        
        # Process match
        start_time = data.get('start_time')
        duration = data.get('duration')
        
        # Handle large integers
        series_id = safely_convert_int(data.get('series_id'))
        series_type = safely_convert_int(data.get('series_type'))
        
        # Convert gold advantage arrays to strings to avoid sqlite integer limits
        radiant_gold_adv = json.dumps(data.get('radiant_gold_adv')) if 'radiant_gold_adv' in data else None
        dire_gold_adv = json.dumps(data.get('dire_gold_adv')) if 'dire_gold_adv' in data else None
        
        # Insert match
        try:
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
                    "version": data.get('version'),
                    "start_time": datetime.fromtimestamp(start_time) if start_time else None,
                    "duration": duration,
                    "league_id": league_id,
                    "series_id": series_id,
                    "series_type": series_type,
                    "radiant_team_id": radiant_team_id,
                    "dire_team_id": dire_team_id,
                    "radiant_score": data.get('radiant_score'),
                    "dire_score": data.get('dire_score'),
                    "radiant_win": data.get('radiant_win'),
                    "radiant_gold_adv": radiant_gold_adv,
                    "dire_gold_adv": dire_gold_adv
                }
            )
            session.commit()
            logger.info(f"Added match {match_id}")
        except exc.SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error adding match {match_id}: {str(e)}")
            return False
        
        # Process players
        if 'players' in data:
            for player_data in data['players']:
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
                        try:
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
                            session.commit()
                        except exc.SQLAlchemyError as e:
                            session.rollback()
                            logger.warning(f"Error adding player {account_id}: {str(e)}")
                
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
                        try:
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
                            session.commit()
                        except exc.SQLAlchemyError as e:
                            session.rollback()
                            logger.warning(f"Error adding hero {hero_id}: {str(e)}")
                
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
                        try:
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
                        except exc.SQLAlchemyError as e:
                            session.rollback()
                            logger.warning(f"Error adding match player metrics: {str(e)}")
        
        # Process draft timings
        if 'draft_timings' in data and data['draft_timings']:
            for draft_data in data['draft_timings']:
                try:
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
                except exc.SQLAlchemyError as e:
                    session.rollback()
                    logger.warning(f"Error adding draft timing: {str(e)}")
        
        # Process teamfights
        if 'teamfights' in data and data['teamfights']:
            for i, fight_data in enumerate(data['teamfights']):
                try:
                    result = session.execute(
                        text("""
                            INSERT INTO pro_teamfights 
                            (match_id, start, end, last_death, deaths)
                            VALUES 
                            (:match_id, :start, :end, :last_death, :deaths)
                            RETURNING id
                        """),
                        {
                            "match_id": match_id,
                            "start": fight_data.get('start'),
                            "end": fight_data.get('end'),
                            "last_death": fight_data.get('last_death'),
                            "deaths": fight_data.get('deaths')
                        }
                    )
                    session.commit()
                    
                    teamfight_id = result.fetchone()[0]
                    
                    # Add teamfight players
                    if 'players' in fight_data:
                        for player_id, player_fight_data in fight_data['players'].items():
                            if player_id.isdigit():
                                try:
                                    session.execute(
                                        text("""
                                            INSERT INTO pro_teamfight_players 
                                            (match_id, teamfight_id, deaths, buybacks, 
                                             damage, healing, gold_delta, xp_delta, 
                                             xp_start, xp_end)
                                            VALUES 
                                            (:match_id, :teamfight_id, :deaths, :buybacks, 
                                             :damage, :healing, :gold_delta, :xp_delta, 
                                             :xp_start, :xp_end)
                                        """),
                                        {
                                            "match_id": match_id,
                                            "teamfight_id": teamfight_id,
                                            "deaths": player_fight_data.get('deaths', 0),
                                            "buybacks": player_fight_data.get('buybacks', 0),
                                            "damage": player_fight_data.get('damage', 0),
                                            "healing": player_fight_data.get('healing', 0),
                                            "gold_delta": player_fight_data.get('gold_delta', 0),
                                            "xp_delta": player_fight_data.get('xp_delta', 0),
                                            "xp_start": player_fight_data.get('xp_start', 0),
                                            "xp_end": player_fight_data.get('xp_end', 0)
                                        }
                                    )
                                    session.commit()
                                except exc.SQLAlchemyError as e:
                                    session.rollback()
                                    logger.warning(f"Error adding teamfight player: {str(e)}")
                except exc.SQLAlchemyError as e:
                    session.rollback()
                    logger.warning(f"Error adding teamfight: {str(e)}")
        
        # Process objectives
        if 'objectives' in data and data['objectives']:
            for obj_data in data['objectives']:
                key_value = safely_convert_int(obj_data.get('key'))
                
                try:
                    session.execute(
                        text("""
                            INSERT INTO pro_objectives 
                            (match_id, time, type, player_slot, key, slot, team)
                            VALUES 
                            (:match_id, :time, :type, :player_slot, :key, :slot, :team)
                        """),
                        {
                            "match_id": match_id,
                            "time": obj_data.get('time'),
                            "type": obj_data.get('type'),
                            "player_slot": obj_data.get('player_slot'),
                            "key": str(key_value) if key_value is not None else None,
                            "slot": obj_data.get('slot'),
                            "team": obj_data.get('team')
                        }
                    )
                    session.commit()
                except exc.SQLAlchemyError as e:
                    session.rollback()
                    logger.warning(f"Error adding objective: {str(e)}")
        
        # Process chat
        if 'chat' in data and data['chat']:
            for chat_data in data['chat']:
                if chat_data.get('type') == 'chatwheel':
                    try:
                        session.execute(
                            text("""
                                INSERT INTO pro_chatwheel 
                                (match_id, time, type, key, player_slot)
                                VALUES 
                                (:match_id, :time, :type, :key, :player_slot)
                            """),
                            {
                                "match_id": match_id,
                                "time": chat_data.get('time'),
                                "type": "chatwheel",
                                "key": chat_data.get('key'),
                                "player_slot": chat_data.get('player_slot')
                            }
                        )
                        session.commit()
                    except exc.SQLAlchemyError as e:
                        session.rollback()
                        logger.warning(f"Error adding chat wheel: {str(e)}")
        
        # Process time vs stats (in batches)
        if 'radiant_gold_adv' in data and data['radiant_gold_adv']:
            gold_adv_data = data['radiant_gold_adv']
            xp_adv_data = data.get('radiant_xp_adv', [])
            
            for i in range(0, len(gold_adv_data), 50):  # Process in batches of 50
                end_idx = min(i + 50, len(gold_adv_data))
                
                for j in range(i, end_idx):
                    xp = xp_adv_data[j] if j < len(xp_adv_data) else None
                    
                    try:
                        session.execute(
                            text("""
                                INSERT INTO pro_timevsstats 
                                (match_id, time, player_id, player_name, player_slot, 
                                 starting_lane, gold, xp)
                                VALUES 
                                (:match_id, :time, :player_id, :player_name, :player_slot, 
                                 :starting_lane, :gold, :xp)
                            """),
                            {
                                "match_id": match_id,
                                "time": j,
                                "player_id": 0,
                                "player_name": "Match Stats",
                                "player_slot": 0,
                                "starting_lane": 0,
                                "gold": gold_adv_data[j],
                                "xp": xp
                            }
                        )
                    except exc.SQLAlchemyError as e:
                        session.rollback()
                        logger.warning(f"Error adding time vs stats: {str(e)}")
                        continue
                
                session.commit()
        
        return True
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing match {json_path}: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    
    finally:
        session.close()

def import_match_files():
    """Import match files that aren't already in the database."""
    # Get all match files
    match_files = find_all_match_files()
    print(f"Found {len(match_files)} match files")
    
    # Get existing match IDs
    existing_match_ids = get_existing_match_ids()
    
    # Process each match file
    matches_added = 0
    for json_path in tqdm(match_files, desc="Importing matches"):
        success = add_match_to_database(json_path, existing_match_ids)
        if success:
            existing_match_ids.add(match_id)
            matches_added += 1
    
    return matches_added

def main():
    """Main function to import all missing matches to the database."""
    print("\n" + "=" * 80)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] IMPORTING ALL MISSING MATCHES TO DATABASE")
    print("=" * 80)
    
    # Ensure database schema exists
    db = DotaDatabase()
    db.create_tables()
    
    # Verify current database state
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking current database state")
    current_match_count = verify_database_population()
    
    # Import match files
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Importing match files")
    matches_added = import_match_files()
    
    # Final verification
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Verifying final database state")
    final_match_count = verify_database_population()
    
    # Report results
    print(f"\nImported {matches_added} new matches to the database.")
    print(f"Previous match count: {current_match_count}")
    print(f"Current match count: {final_match_count}")
    print(f"Difference: {final_match_count - current_match_count} matches added")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
