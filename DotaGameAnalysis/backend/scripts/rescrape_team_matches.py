#!/usr/bin/env python
"""
Rescrape Team Matches Script

This script:
1. Clears existing data from pro_matches and related tables
2. Scrapes the last 10 matches for specified teams (Team Liquid, Team Spirit, Team Tundra)
3. Ensures team_ids and league_ids are properly saved to the database
"""
import os
import sys
import logging
import requests
import time
from datetime import datetime
from sqlalchemy import delete

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
    ProTimevsStats
)
from src.data.scraper import DotaMatchScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("rescrape.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("rescrape_teams")

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

class TeamMatchRescraper:
    """Class to handle rescraping matches for specific teams"""
    
    def __init__(self):
        """Initialize the rescraper with database and API connections"""
        self.db = DotaDatabase()
        self.scraper = DotaMatchScraper()
        
    def clear_database_tables(self):
        """Clear all data from the pro_matches and related tables"""
        session = self.db.Session()
        try:
            logger.info("Clearing database tables...")
            
            # Delete from tables in reverse order of dependencies
            session.execute(delete(ProTimevsStats))
            session.execute(delete(ProChatWheel))
            session.execute(delete(ProObjective))
            session.execute(delete(ProTeamFightPlayer))
            session.execute(delete(ProTeamFight))
            session.execute(delete(ProDraftTiming))
            session.execute(delete(ProMatchPlayer))
            session.execute(delete(ProMatch))
            
            # We'll keep the team and league data, just update it
            session.commit()
            logger.info("Database tables cleared successfully")
        except Exception as e:
            session.rollback()
            logger.error(f"Error clearing database tables: {e}")
            raise
        finally:
            session.close()
    
    def get_team_info(self, team_id):
        """Get detailed information about a team"""
        team_url = f"{BASE_URL}/teams/{team_id}"
        response = requests.get(team_url)
        time.sleep(API_CALL_DELAY)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Failed to get team info for ID {team_id}: {response.status_code}")
            return None
    
    def get_team_matches(self, team_id, limit=10):
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
    
    def save_team_to_db(self, team_data):
        """Save or update team information in the database"""
        session = self.db.Session()
        try:
            team_id = team_data['team_id']
            team = session.query(ProTeam).filter_by(team_id=team_id).first()
            
            if not team:
                team = ProTeam(
                    team_id=team_id,
                    name=team_data.get('name', 'Unknown'),
                    tag=team_data.get('tag', ''),
                    logo_url=team_data.get('logo_url')
                )
                session.add(team)
                logger.info(f"Added new team: {team_data.get('name')}")
            else:
                team.name = team_data.get('name', team.name)
                team.tag = team_data.get('tag', team.tag)
                team.logo_url = team_data.get('logo_url', team.logo_url)
                logger.info(f"Updated existing team: {team.name}")
            
            session.commit()
            return team
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving team to database: {e}")
            return None
        finally:
            session.close()
    
    def save_league_to_db(self, league_data):
        """Save or update league information in the database"""
        if not league_data or 'leagueid' not in league_data:
            return None
            
        session = self.db.Session()
        try:
            league_id = league_data['leagueid']
            league = session.query(ProLeague).filter_by(league_id=league_id).first()
            
            if not league:
                league = ProLeague(
                    league_id=league_id,
                    name=league_data.get('league_name', 'Unknown League'),
                    tier=league_data.get('tier', 0)
                )
                session.add(league)
                logger.info(f"Added new league: {league_data.get('league_name')}")
            else:
                if 'league_name' in league_data and league_data['league_name']:
                    league.name = league_data['league_name']
                if 'tier' in league_data and league_data['tier']:
                    league.tier = league_data['tier']
                logger.info(f"Updated existing league: {league.name}")
            
            session.commit()
            return league
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving league to database: {e}")
            return None
        finally:
            session.close()
    
    def save_match_to_db(self, match_data):
        """Save match information to the database with proper team and league IDs"""
        session = self.db.Session()
        try:
            match_id = match_data['match_id']
            
            # Check if match already exists
            existing_match = session.query(ProMatch).filter_by(match_id=match_id).first()
            if existing_match:
                logger.info(f"Match {match_id} already exists, skipping")
                return existing_match
            
            # Ensure we have a valid league record
            if 'leagueid' in match_data and match_data['leagueid']:
                # Make sure to save league first to get foreign key relationship
                self.save_league_to_db(match_data)
            
            # Create the match record with all required fields
            match = ProMatch(
                match_id=match_id,
                league_id=match_data.get('leagueid'),
                start_time=datetime.fromtimestamp(match_data['start_time']),
                duration=match_data['duration'],
                radiant_team_id=match_data.get('radiant_team_id'),
                dire_team_id=match_data.get('dire_team_id'),
                radiant_score=match_data.get('radiant_score', 0),
                dire_score=match_data.get('dire_score', 0),
                radiant_win=match_data['radiant_win'],
                series_id=match_data.get('series_id'),
                series_type=match_data.get('series_type'),
                game_version=match_data.get('version')
            )
            
            session.add(match)
            session.commit()
            logger.info(f"Saved match {match_id} to database with teams {match.radiant_team_id} vs {match.dire_team_id}")
            return match
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving match to database: {e}")
            return None
        finally:
            session.close()
    
    def process_match_details(self, match_id):
        """Get and save detailed match information"""
        match_details = self.scraper.get_match_details(match_id)
        if not match_details:
            logger.error(f"Failed to get details for match {match_id}")
            return None
            
        # Save match to ensure we have the basic info
        session = self.db.Session()
        try:
            # Handle league data
            if 'league' in match_details:
                league_data = {
                    'leagueid': match_details.get('leagueid'),
                    'league_name': match_details.get('league', {}).get('name'),
                    'tier': match_details.get('league', {}).get('tier')
                }
                self.save_league_to_db(league_data)
            
            # Handle team data
            if 'radiant_team' in match_details and match_details['radiant_team']:
                team_data = match_details['radiant_team']
                if 'team_id' not in team_data and 'radiant_team_id' in match_details:
                    team_data['team_id'] = match_details['radiant_team_id']
                self.save_team_to_db(team_data)
            
            if 'dire_team' in match_details and match_details['dire_team']:
                team_data = match_details['dire_team']
                if 'team_id' not in team_data and 'dire_team_id' in match_details:
                    team_data['team_id'] = match_details['dire_team_id']
                self.save_team_to_db(team_data)
            
            # Update match with any new information
            match = session.query(ProMatch).filter_by(match_id=match_id).first()
            if match:
                if not match.radiant_team_id and 'radiant_team_id' in match_details:
                    match.radiant_team_id = match_details['radiant_team_id']
                if not match.dire_team_id and 'dire_team_id' in match_details:
                    match.dire_team_id = match_details['dire_team_id']
                if not match.league_id and 'leagueid' in match_details:
                    match.league_id = match_details['leagueid']
                session.commit()
                logger.info(f"Updated match {match_id} with additional details")
                
            return match_details
        except Exception as e:
            session.rollback()
            logger.error(f"Error processing match details: {e}")
            return None
        finally:
            session.close()
    
    def rescrape_team_matches(self):
        """Main function to rescrape matches for all target teams"""
        logger.info("Starting team match rescraping process")
        
        # First, clear the database tables
        self.clear_database_tables()
        
        # Process each team
        for team_name, team_id in TARGET_TEAMS.items():
            logger.info(f"Processing matches for {team_name} (ID: {team_id})")
            
            # Get team info and save to database
            team_info = self.get_team_info(team_id)
            if team_info:
                self.save_team_to_db(team_info)
            
            # Get recent matches for the team
            team_matches = self.get_team_matches(team_id, MATCHES_PER_TEAM)
            logger.info(f"Found {len(team_matches)} matches for {team_name}")
            
            # Process each match
            for match in team_matches:
                # First save the basic match info
                self.save_match_to_db(match)
                
                # Then get and save detailed match info
                match_id = match['match_id']
                self.process_match_details(match_id)
                
                # Respect API rate limits
                time.sleep(API_CALL_DELAY)
            
            logger.info(f"Completed processing for {team_name}")
        
        logger.info("Team match rescraping process completed successfully")


def main():
    """Main function to execute the rescraping process"""
    try:
        rescraper = TeamMatchRescraper()
        rescraper.rescrape_team_matches()
        print("Successfully rescrapped matches for Team Liquid, Team Spirit, and Tundra Esports")
    except Exception as e:
        logger.error(f"Error in rescraping process: {e}")
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
