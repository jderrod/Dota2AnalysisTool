#!/usr/bin/env python
"""
Fetch Top Team ELO Ratings

A simplified script to fetch and update ELO ratings for popular Dota 2 teams.
Retrieves ELO ratings directly from the OpenDota API for a list of hardcoded top team IDs.
"""
import sys
import time
import logging
import requests
from sqlalchemy import text
from database import DotaDatabase, Team

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# OpenDota API endpoint
OPENDOTA_API_URL = "https://api.opendota.com/api"

# List of popular team IDs to fetch
TOP_TEAM_IDS = [
    2163,       # Team Liquid
    7119388,    # Team Spirit
    8291895,    # Tundra Esports
    8599101,    # Gaimin Gladiators
    8255888,    # BetBoom Team
    6209804,    # Team Secret
    8740097,    # Entity
    8687717,    # Talon Esports
    8260983,    # TSM
    8605863,    # 9Pandas
    36,         # Natus Vincere
    39,         # Evil Geniuses
    15,         # PSG.LGD
    726228,     # Vici Gaming
    543897,     # Fnatic
    111471,     # Alliance
    350190,     # Virtus.pro
    67,         # Invictus Gaming
    6209166,    # OG
    8261648     # Shopify Rebellion
]

def fetch_team_elo(team_id, api_key=None):
    """
    Fetch ELO rating for a specific team from the OpenDota API.
    
    Args:
        team_id (int): Team ID to fetch rating for
        api_key (str, optional): OpenDota API key
    
    Returns:
        float or None: ELO rating or None if not available
    """
    # Construct API URL with API key if provided
    team_url = f"{OPENDOTA_API_URL}/teams/{team_id}"
    if api_key:
        team_url += f"?api_key={api_key}"
    
    try:
        logger.info(f"Fetching ELO for team {team_id}...")
        response = requests.get(team_url)
        
        if response.status_code == 200:
            team_data = response.json()
            name = team_data.get('name', 'Unknown')
            rating = team_data.get('rating')
            
            if rating:
                rating = float(rating)
                logger.info(f"Team {team_id} ({name}) has ELO: {rating}")
                return rating
            else:
                logger.warning(f"No ELO rating found for team {team_id} ({name})")
                return None
        
        elif response.status_code == 429:  # Rate limit exceeded
            logger.error(f"Rate limit exceeded for team {team_id}. Please use an API key.")
            return None
        
        else:
            logger.error(f"Failed to fetch team {team_id}: HTTP {response.status_code}")
            return None
    
    except Exception as e:
        logger.error(f"Error fetching ELO for team {team_id}: {e}")
        return None


def update_database_elo(team_id, elo):
    """
    Update the ELO rating for a team in the database.
    
    Args:
        team_id (int): Team ID to update
        elo (float): ELO rating to set
    
    Returns:
        bool: True if successful, False otherwise
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Get the team from the database
        team = session.query(Team).filter(Team.team_id == team_id).first()
        
        if team:
            # Ensure 'elo' attribute exists on the Team object
            if not hasattr(team, 'elo'):
                logger.error(f"Team object does not have 'elo' attribute. Check database schema.")
                # Try to add the column if it doesn't exist
                conn = db.engine.connect()
                trans = conn.begin()
                conn.execute(text('ALTER TABLE teams ADD COLUMN elo FLOAT'))
                trans.commit()
                conn.close()
                logger.info("Added 'elo' column to 'teams' table")
            
            # Update the ELO rating
            setattr(team, 'elo', elo)
            session.commit()
            
            # Verify the update
            refreshed_team = session.query(Team).filter(Team.team_id == team_id).first()
            refreshed_elo = getattr(refreshed_team, 'elo', None)
            
            if refreshed_elo == elo:
                logger.info(f"Successfully updated ELO for team {team_id} to {elo}")
                return True
            else:
                logger.error(f"Failed to verify ELO update for team {team_id}. Expected: {elo}, Got: {refreshed_elo}")
                return False
        
        else:
            logger.warning(f"Team {team_id} not found in database")
            return False
    
    except Exception as e:
        logger.error(f"Error updating ELO in database: {e}")
        session.rollback()
        return False
    
    finally:
        session.close()


def main():
    """Main function to fetch and update ELO ratings for top teams."""
    print("Fetching ELO ratings for top Dota 2 teams...\n")
    
    # Get API key from command line
    api_key = None
    if len(sys.argv) > 1 and '--api-key' in sys.argv:
        api_key_index = sys.argv.index('--api-key')
        if api_key_index < len(sys.argv) - 1:
            api_key = sys.argv[api_key_index + 1]
            logger.info(f"Using provided API key")
    
    # Fetch and update ELO ratings for each team
    successful_updates = 0
    failed_updates = 0
    
    for team_id in TOP_TEAM_IDS:
        # Fetch ELO rating
        elo = fetch_team_elo(team_id, api_key)
        
        if elo:
            # Update database
            if update_database_elo(team_id, elo):
                successful_updates += 1
            else:
                failed_updates += 1
        else:
            failed_updates += 1
        
        # Add delay to avoid rate limits
        time.sleep(1)
    
    # Print summary
    print(f"\nELO Update Summary:")
    print(f"  Teams processed: {len(TOP_TEAM_IDS)}")
    print(f"  Successful updates: {successful_updates}")
    print(f"  Failed updates: {failed_updates}")
    
    if failed_updates > 0 and not api_key:
        print(f"\nYou had {failed_updates} failed updates. Try using an API key:")
        print(f"  python fetch_top_team_elos.py --api-key YOUR_API_KEY")
    
    # Show how to list teams by ELO
    if successful_updates > 0:
        print(f"\nTo see teams ranked by ELO, run:")
        print(f"  python list_teams_by_elo.py")


if __name__ == "__main__":
    main()
