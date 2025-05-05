#!/usr/bin/env python
"""
Fix Team IDs and Reload Data Script

This script:
1. Clears existing data from all tables
2. Identifies match JSON files for the specified teams (Team Liquid, Team Spirit, Team Tundra)
3. Processes all JSON files to repopulate tables with complete match data
4. Ensures team_ids and league_ids are properly saved to the database

The script properly repopulates all 21 tables with data from JSON files, including:
- Pro matches
- Pro teams
- Pro leagues
- Pro players
- Pro heroes
- Pro match player metrics
- Draft timings
- Team fights
- Team fight players
- Objectives
- Chat wheel
- Time vs stats data
- And more
"""
import os
import sys
import json
import logging
import glob
import requests
import time
from datetime import datetime
from sqlalchemy import delete, text
import shutil

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.database_pro_teams import (
    DotaDatabase, 
    ProMatch, 
    ProTeam, 
    ProLeague, 
    ProPlayer, 
    ProMatchPlayer,
    ProDraftTiming,
    ProTeamFight,
    ProTeamFightPlayer,
    ProObjective,
    ProChatWheel,
    ProHero,
    ProTimevsStats,
    populate_from_json,
    populate_time_vs_stats
)
from src.data.scraper import DotaMatchScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fix_team_ids.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fix_team_ids")

# Team information - name to team_id mapping (from OpenDota)
TARGET_TEAMS = {
    "Team Liquid": 2163,
    "Team Spirit": 7119388,
    "Tundra Esports": 8291895
}

# API Constants
BASE_URL = "https://api.opendota.com/api"
MATCHES_PER_TEAM = 10
API_CALL_DELAY = 1  # seconds between API calls to respect rate limits

class DataReloader:
    """Class to handle fixing team IDs and reloading data from JSON files"""
    
    def __init__(self):
        """Initialize the data reloader with database and API connections"""
        self.db = DotaDatabase()
        self.scraper = DotaMatchScraper()
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
        self.match_data_dir = os.path.join(self.data_dir, 'raw', 'matches')
        self.backup_dir = os.path.join(self.data_dir, 'backup')
        
        # Create directories if they don't exist
        os.makedirs(self.match_data_dir, exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)
    
    def backup_database(self):
        """Create a backup of the database file"""
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                              'database', 'data', 'dota_matches.db')
        if os.path.exists(db_path):
            backup_path = os.path.join(self.backup_dir, f'dota_matches_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db')
            shutil.copy2(db_path, backup_path)
            logger.info(f"Database backed up to {backup_path}")
    
    def clear_database_tables(self):
        """Clear all data from the database tables"""
        session = self.db.Session()
        try:
            logger.info("Clearing database tables...")
            
            # Delete from tables in reverse order of dependencies
            tables = [
                'pro_timevsstats',
                'pro_chatwheel',
                'pro_objectives',
                'pro_teamfight_players',
                'pro_teamfights',
                'pro_draft_timings',
                'pro_match_player_metrics',
                'pro_matches',
                'pro_players',
                'pro_heroes',
                'pro_teams',
                'pro_leagues'
            ]
            
            for table in tables:
                session.execute(text(f"DELETE FROM {table}"))
            
            session.commit()
            logger.info("Database tables cleared successfully")
        except Exception as e:
            session.rollback()
            logger.error(f"Error clearing database tables: {e}")
            raise
        finally:
            session.close()
    
    def get_team_matches_from_api(self, team_id, limit=10):
        """Get recent matches for a specific team"""
        matches_url = f"{BASE_URL}/teams/{team_id}/matches"
        response = requests.get(matches_url)
        time.sleep(API_CALL_DELAY)
        
        if response.status_code == 200:
            matches = response.json()
            return matches[:limit]
        else:
            logger.error(f"Failed to get matches for team ID {team_id}: {response.status_code}")
            return []
    
    def get_match_details(self, match_id):
        """Get detailed information about a match"""
        match_url = f"{BASE_URL}/matches/{match_id}"
        response = requests.get(match_url)
        time.sleep(API_CALL_DELAY)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get details for match {match_id}: {response.status_code}")
            return None
    
    def download_team_match_json(self):
        """Download JSON files for matches of the target teams"""
        for team_name, team_id in TARGET_TEAMS.items():
            logger.info(f"Fetching matches for {team_name} (ID: {team_id})")
            
            # Get recent matches for the team
            team_matches = self.get_team_matches_from_api(team_id, MATCHES_PER_TEAM)
            if not team_matches:
                logger.warning(f"No matches found for {team_name}")
                continue
                
            logger.info(f"Found {len(team_matches)} matches for {team_name}")
            
            # Download detailed match data
            for match in team_matches:
                match_id = match['match_id']
                match_file = os.path.join(self.match_data_dir, f"match_{match_id}.json")
                
                # Check if we already have this match
                if os.path.exists(match_file):
                    logger.info(f"Match {match_id} already exists, skipping download")
                    continue
                
                # Get and save match details
                match_details = self.get_match_details(match_id)
                if match_details:
                    with open(match_file, 'w', encoding='utf-8') as f:
                        json.dump(match_details, f, indent=2)
                    logger.info(f"Saved match {match_id} details to {match_file}")
                
                time.sleep(API_CALL_DELAY)
    
    def update_team_ids_in_json(self):
        """Update team IDs in JSON files if missing"""
        match_files = glob.glob(os.path.join(self.match_data_dir, "match_*.json"))
        
        for match_file in match_files:
            with open(match_file, 'r', encoding='utf-8') as f:
                match_data = json.load(f)
            
            modified = False
            match_id = match_data.get('match_id')
            
            # Fix radiant_team_id
            if 'radiant_team' in match_data and match_data['radiant_team'] and 'team_id' in match_data['radiant_team']:
                if 'radiant_team_id' not in match_data or not match_data['radiant_team_id']:
                    match_data['radiant_team_id'] = match_data['radiant_team']['team_id']
                    modified = True
                    logger.info(f"Updated radiant_team_id to {match_data['radiant_team_id']} in match {match_id}")
            
            # Fix dire_team_id
            if 'dire_team' in match_data and match_data['dire_team'] and 'team_id' in match_data['dire_team']:
                if 'dire_team_id' not in match_data or not match_data['dire_team_id']:
                    match_data['dire_team_id'] = match_data['dire_team']['team_id']
                    modified = True
                    logger.info(f"Updated dire_team_id to {match_data['dire_team_id']} in match {match_id}")
            
            # Fix league_id
            if 'league' in match_data and match_data['league'] and 'leagueid' in match_data['league']:
                if 'leagueid' not in match_data or not match_data['leagueid']:
                    match_data['leagueid'] = match_data['league']['leagueid']
                    modified = True
                    logger.info(f"Updated leagueid to {match_data['leagueid']} in match {match_id}")
            
            # Save the modified file
            if modified:
                with open(match_file, 'w', encoding='utf-8') as f:
                    json.dump(match_data, f, indent=2)
    
    def save_team_info(self, team_data):
        """Save team information to the database"""
        session = self.db.Session()
        try:
            team_id = team_data['team_id']
            
            # Check if team already exists
            team = session.query(ProTeam).filter_by(team_id=team_id).first()
            if not team:
                team = ProTeam(
                    team_id=team_id,
                    name=team_data.get('name', 'Unknown'),
                    tag=team_data.get('tag', ''),
                    logo_url=team_data.get('logo_url')
                )
                session.add(team)
                logger.info(f"Added team: {team_data.get('name')} (ID: {team_id})")
            else:
                team.name = team_data.get('name', team.name)
                team.tag = team_data.get('tag', team.tag)
                team.logo_url = team_data.get('logo_url', team.logo_url)
                logger.info(f"Updated team: {team.name} (ID: {team_id})")
            
            session.commit()
            return team
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving team {team_data.get('name')}: {e}")
            return None
        finally:
            session.close()
    
    def save_league_info(self, league_data):
        """Save league information to the database"""
        session = self.db.Session()
        try:
            league_id = league_data['leagueid']
            
            # Check if league already exists
            league = session.query(ProLeague).filter_by(league_id=league_id).first()
            if not league:
                league = ProLeague(
                    league_id=league_id,
                    name=league_data.get('name', 'Unknown League'),
                    tier=league_data.get('tier', 0)
                )
                session.add(league)
                logger.info(f"Added league: {league_data.get('name')} (ID: {league_id})")
            else:
                if 'name' in league_data and league_data['name']:
                    league.name = league_data['name']
                if 'tier' in league_data and league_data['tier']:
                    league.tier = league_data['tier']
                logger.info(f"Updated league: {league.name} (ID: {league_id})")
            
            session.commit()
            return league
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving league {league_data.get('name')}: {e}")
            return None
        finally:
            session.close()
    
    def preprocess_teams_and_leagues(self):
        """Pre-process teams and leagues before processing match JSON files"""
        # Add target teams to database
        for team_name, team_id in TARGET_TEAMS.items():
            team_data = self.scraper.get_team_info(team_id)
            if team_data:
                self.save_team_info(team_data)
            time.sleep(API_CALL_DELAY)
        
        # Process all match JSON files to extract and save league data
        match_files = glob.glob(os.path.join(self.match_data_dir, "match_*.json"))
        for match_file in match_files:
            with open(match_file, 'r', encoding='utf-8') as f:
                match_data = json.load(f)
            
            # Handle league data
            if 'league' in match_data and match_data['league']:
                league_data = match_data['league']
                if 'leagueid' not in league_data and 'leagueid' in match_data:
                    league_data['leagueid'] = match_data['leagueid']
                self.save_league_info(league_data)
            
            # Handle team data
            if 'radiant_team' in match_data and match_data['radiant_team']:
                team_data = match_data['radiant_team']
                if 'team_id' not in team_data and 'radiant_team_id' in match_data:
                    team_data['team_id'] = match_data['radiant_team_id']
                if 'team_id' in team_data:
                    self.save_team_info(team_data)
            
            if 'dire_team' in match_data and match_data['dire_team']:
                team_data = match_data['dire_team']
                if 'team_id' not in team_data and 'dire_team_id' in match_data:
                    team_data['team_id'] = match_data['dire_team_id']
                if 'team_id' in team_data:
                    self.save_team_info(team_data)
    
    def process_match_json_files(self):
        """Process all match JSON files to populate database tables"""
        match_files = glob.glob(os.path.join(self.match_data_dir, "match_*.json"))
        logger.info(f"Found {len(match_files)} match JSON files to process")
        
        for match_file in match_files:
            match_id = os.path.basename(match_file).replace("match_", "").replace(".json", "")
            logger.info(f"Processing match {match_id} from {match_file}")
            
            try:
                # Run the populate_from_json function to handle all table population
                populate_from_json(match_file)
                
                # Additionally process time vs stats data
                populate_time_vs_stats(match_file)
                
                logger.info(f"Successfully processed match {match_id}")
            except Exception as e:
                logger.error(f"Error processing match {match_id}: {e}")
    
    def update_match_team_league_ids(self):
        """Update match records with team and league IDs that might be missing"""
        session = self.db.Session()
        try:
            # Get all matches
            matches = session.query(ProMatch).all()
            
            for match in matches:
                match_file = os.path.join(self.match_data_dir, f"match_{match.match_id}.json")
                if not os.path.exists(match_file):
                    continue
                    
                with open(match_file, 'r', encoding='utf-8') as f:
                    match_data = json.load(f)
                
                # Update team IDs if missing
                if (not match.radiant_team_id) and 'radiant_team_id' in match_data:
                    match.radiant_team_id = match_data['radiant_team_id']
                    logger.info(f"Updated match {match.match_id} radiant_team_id to {match.radiant_team_id}")
                    
                if (not match.dire_team_id) and 'dire_team_id' in match_data:
                    match.dire_team_id = match_data['dire_team_id']
                    logger.info(f"Updated match {match.match_id} dire_team_id to {match.dire_team_id}")
                
                # Update league ID if missing
                if (not match.league_id) and 'leagueid' in match_data:
                    match.league_id = match_data['leagueid']
                    logger.info(f"Updated match {match.match_id} league_id to {match.league_id}")
                
                # Update other fields that might be missing
                if 'version' in match_data:
                    match.version = match_data['version']
                
                if 'series_id' in match_data:
                    match.series_id = match_data['series_id']
                
                if 'series_type' in match_data:
                    match.series_type = match_data['series_type']
                
                if 'game_version' in match_data:
                    match.game_version = match_data['game_version']
                
                # Update gold advantage if available in radiant_gold_adv field
                if 'radiant_gold_adv' in match_data and match_data['radiant_gold_adv']:
                    try:
                        match.radiant_gold_adv = match_data['radiant_gold_adv'][-1]
                        match.dire_gold_adv = -match_data['radiant_gold_adv'][-1]  # Opposite of radiant advantage
                    except (IndexError, TypeError):
                        pass
            
            session.commit()
            logger.info("Updated match records with team and league IDs")
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating match team and league IDs: {e}")
        finally:
            session.close()
    
    def run(self):
        """Main execution function"""
        logger.info("Starting the data reload process")
        
        # Backup the current database
        self.backup_database()
        
        # Clear all tables
        self.clear_database_tables()
        
        # Create database tables if they don't exist
        self.db.create_tables()
        
        # Download team match JSON files if needed
        self.download_team_match_json()
        
        # Update team IDs in JSON files if missing
        self.update_team_ids_in_json()
        
        # Pre-process teams and leagues
        self.preprocess_teams_and_leagues()
        
        # Process all match JSON files
        self.process_match_json_files()
        
        # Update any missing team and league IDs in match records
        self.update_match_team_league_ids()
        
        logger.info("Data reload process completed successfully")


def main():
    """Main function to execute the data reload process"""
    try:
        reloader = DataReloader()
        reloader.run()
        print("Successfully fixed team IDs and reloaded all data")
    except Exception as e:
        logger.error(f"Error in data reload process: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
