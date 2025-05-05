#!/usr/bin/env python3
"""
Add missing matches to the database without clearing existing data.
This script specifically handles the SQLite integer overflow error by
converting large integers to strings.
"""

import os
import sys
import json
import logging
from datetime import datetime
from tqdm import tqdm
from pathlib import Path
import traceback
from sqlalchemy import text

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
        logging.FileHandler("add_missing_matches.log", mode="w"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Directory with team match data
TEAM_GAMES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'teams_games'))

def verify_database_population():
    """Verify the current database population and print counts of all tables."""
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Use direct SQL for more reliable counts
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
        return {row[0] for row in result}
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

def safely_convert_int(value):
    """Safely convert a value to int, or return as string if it's too large."""
    if value is None:
        return None
    
    try:
        return int(value)
    except (ValueError, OverflowError):
        logger.warning(f"Converting large integer to string: {value}")
        return str(value)

def add_match_to_database(json_path):
    """
    Add a match from a JSON file to the database.
    Handle integer overflow by converting large integers to strings.
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Load JSON data
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Extract match_id from filename (match_<id>.json)
        filename = os.path.basename(json_path)
        match_id = int(filename.split("_")[1].split(".")[0])
        
        # Check if match already exists
        existing_match = session.execute(
            text("SELECT match_id FROM pro_matches WHERE match_id = :match_id"),
            {"match_id": match_id}
        ).first()
        
        if existing_match:
            logger.info(f"Match {match_id} already exists in database, skipping")
            return False
        
        # Add league if needed
        league_id = None
        if 'league' in data and data['league']:
            league_id = data['league'].get('leagueid')
            if league_id is not None:
                # Check if league exists
                existing_league = session.execute(
                    text("SELECT league_id FROM pro_leagues WHERE league_id = :league_id"),
                    {"league_id": league_id}
                ).first()
                
                if not existing_league:
                    # Insert new league
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
        
        # Process team information
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
                    # Insert new team
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
                    # Insert new team
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
        
        # Insert match data
        start_time = data.get('start_time')
        duration = data.get('duration')
        series_id = safely_convert_int(data.get('series_id'))
        series_type = safely_convert_int(data.get('series_type'))
        
        # Convert gold advantage arrays to strings
        radiant_gold_adv = json.dumps(data.get('radiant_gold_adv')) if 'radiant_gold_adv' in data else None
        dire_gold_adv = json.dumps(data.get('dire_gold_adv')) if 'dire_gold_adv' in data else None
        
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
                
                # Check if hero exists
                hero_id = player_data.get('hero_id')
                if hero_id:
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
                
                # Insert match player metrics
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
                        # Insert player metrics
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
        if 'draft_timings' in data and data['draft_timings']:
            for draft_data in data['draft_timings']:
                # Insert draft timing
                session.execute(
                    text("""
                        INSERT INTO pro_draft_timings 
                        (match_id, order, pick, active_team, hero_id, 
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
        
        # Process team fights
        if 'teamfights' in data and data['teamfights']:
            for i, fight_data in enumerate(data['teamfights']):
                # Insert team fight
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
                
                teamfight_id = result.fetchone()[0]
                session.commit()
                
                # Process team fight players
                if 'players' in fight_data:
                    for player_id, player_fight_data in fight_data['players'].items():
                        if player_id.isdigit():
                            try:
                                # Insert team fight player
                                session.execute(
                                    text("""
                                        INSERT INTO pro_teamfight_players 
                                        (match_id, teamfight_id, deaths, buybacks,
                                         damage, healing, gold_delta, xp_delta)
                                        VALUES 
                                        (:match_id, :teamfight_id, :deaths, :buybacks,
                                         :damage, :healing, :gold_delta, :xp_delta)
                                    """),
                                    {
                                        "match_id": match_id,
                                        "teamfight_id": teamfight_id,
                                        "deaths": player_fight_data.get('deaths', 0),
                                        "buybacks": player_fight_data.get('buybacks', 0),
                                        "damage": player_fight_data.get('damage', 0),
                                        "healing": player_fight_data.get('healing', 0),
                                        "gold_delta": player_fight_data.get('gold_delta', 0),
                                        "xp_delta": player_fight_data.get('xp_delta', 0)
                                    }
                                )
                            except Exception as e:
                                logger.error(f"Error adding team fight player: {e}")
                                continue
                
                session.commit()
        
        # Process objectives
        if 'objectives' in data and data['objectives']:
            for obj_data in data['objectives']:
                try:
                    # Safely handle potentially large 'key' values
                    key_value = obj_data.get('key')
                    if key_value is not None:
                        key_value = safely_convert_int(key_value)
                    
                    # Insert objective
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
                except Exception as e:
                    logger.error(f"Error adding objective: {e}")
                    continue
            
            session.commit()
        
        # Process chat wheel
        if 'chat' in data and data['chat']:
            for chat_data in data['chat']:
                if chat_data.get('type') == 'chatwheel':
                    try:
                        # Insert chat wheel
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
                    except Exception as e:
                        logger.error(f"Error adding chat wheel: {e}")
                        continue
            
            session.commit()
        
        # Process time vs stats
        if 'radiant_gold_adv' in data and data['radiant_gold_adv']:
            gold_adv_data = data['radiant_gold_adv']
            xp_adv_data = data.get('radiant_xp_adv', [])
            
            # Insert in batches to avoid too many transactions
            batch_size = 50
            for i in range(0, len(gold_adv_data), batch_size):
                batch = []
                end_idx = min(i + batch_size, len(gold_adv_data))
                
                for j in range(i, end_idx):
                    xp = xp_adv_data[j] if j < len(xp_adv_data) else None
                    
                    # Add to batch
                    batch.append({
                        "match_id": match_id,
                        "time": j,
                        "player_id": 0,  # Default value
                        "player_name": "Match Stats",
                        "player_slot": 0,
                        "starting_lane": 0,
                        "gold": gold_adv_data[j],
                        "xp": xp
                    })
                
                # Execute batch insert
                session.execute(
                    text("""
                        INSERT INTO pro_timevsstats 
                        (match_id, time, player_id, player_name, player_slot, starting_lane, gold, xp)
                        VALUES 
                        (:match_id, :time, :player_id, :player_name, :player_slot, :starting_lane, :gold, :xp)
                    """),
                    batch
                )
                
                session.commit()
        
        return True
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing match {json_path}: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    
    finally:
        session.close()

def process_match_files():
    """
    Process match files and add them to the database.
    Only process files for matches that don't already exist in the database.
    """
    # Get all match files
    match_files = find_all_match_files()
    logger.info(f"Found {len(match_files)} match files")
    
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
    for json_path in tqdm(new_match_files, desc="Adding matches"):
        success = add_match_to_database(json_path)
        if success:
            matches_added += 1
    
    return matches_added

def main():
    """Main function to add missing matches to the database."""
    print("\n" + "=" * 80)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] ADDING MISSING MATCHES TO DATABASE (PRESERVING EXISTING DATA)")
    print("=" * 80)
    
    # Ensure database schema exists
    db = DotaDatabase()
    db.create_tables()
    
    # Verify current database state
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking current database state")
    current_match_count = verify_database_population()
    
    # Process match files
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Processing match files")
    matches_added = process_match_files()
    
    # Final verification
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Verifying final database state")
    final_match_count = verify_database_population()
    
    # Report results
    print(f"\nAdded {matches_added} new matches to the database.")
    print(f"Previous match count: {current_match_count}")
    print(f"Current match count: {final_match_count}")
    print(f"Difference: {final_match_count - current_match_count} matches added")
    
    print("\nDone!")

if __name__ == "__main__":
    main()
