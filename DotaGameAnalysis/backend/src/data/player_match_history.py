"""
Player-specific Dota 2 Match History

This module retrieves match history and details for a specific Steam ID.
It integrates with the OpenDota API to get detailed match information.
"""
import os
import json
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from scraper import DotaMatchScraper
from database import DotaDatabase

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("player_history.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
STEAM_API_KEY = os.getenv("STEAM_API_KEY", "38CF12F61C4D249FCA6C4B5722D9DD19")
DEFAULT_STEAM_ID = "76561198168848863"
STEAM_API_BASE_URL = "https://api.steampowered.com/IDOTA2Match_570"
OPENDOTA_API_BASE_URL = "https://api.opendota.com/api"


class PlayerMatchHistory:
    """
    A class to retrieve and analyze match history for a specific player.
    """
    
    def __init__(self, steam_id=DEFAULT_STEAM_ID, api_key=STEAM_API_KEY):
        """
        Initialize with a Steam ID and API key.
        
        Args:
            steam_id (str): Steam ID to retrieve match history for.
            api_key (str): Steam API key.
        """
        self.steam_id = steam_id
        self.api_key = api_key
        self.scraper = DotaMatchScraper()
        self.db = DotaDatabase()
        self.db.create_tables()
        
    def get_match_history(self, matches_requested=10):
        """
        Get recent match history for the player from Steam API.
        
        Args:
            matches_requested (int): Number of matches to retrieve.
            
        Returns:
            list: List of match data.
        """
        logger.info(f"Fetching {matches_requested} recent matches for Steam ID {self.steam_id}")
        
        url = f"{STEAM_API_BASE_URL}/GetMatchHistory/v1/"
        params = {
            "key": self.api_key,
            "account_id": self.steam_id,
            "matches_requested": matches_requested
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get("result", {}).get("status") == 1:
                matches = data["result"]["matches"]
                logger.info(f"Retrieved {len(matches)} matches from Steam API")
                return matches
            else:
                logger.error(f"API error: {data.get('result', {}).get('statusDetail', 'Unknown error')}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return []
    
    def get_match_details_from_steam(self, match_id):
        """
        Get match details from Steam API.
        
        Args:
            match_id (str): Match ID to retrieve details for.
            
        Returns:
            dict: Match details.
        """
        url = f"{STEAM_API_BASE_URL}/GetMatchDetails/v1/"
        params = {
            "key": self.api_key,
            "match_id": match_id
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "result" in data:
                return data["result"]
            else:
                logger.error(f"No result in response for match {match_id}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for match {match_id}: {e}")
            return None
    
    def get_match_details_from_opendota(self, match_id):
        """
        Get detailed match information from OpenDota API.
        
        Args:
            match_id (str): Match ID to retrieve details for.
            
        Returns:
            dict: Match details.
        """
        return self.scraper.get_match_details(match_id)
    
    def get_player_profile(self):
        """
        Get player profile information from OpenDota API.
        
        Returns:
            dict: Player profile data.
        """
        url = f"{OPENDOTA_API_BASE_URL}/players/{self.steam_id}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
    
    def process_player_matches(self, matches_requested=10):
        """
        Process player matches, retrieving detailed information and storing in the database.
        
        Args:
            matches_requested (int): Number of matches to retrieve.
            
        Returns:
            list: List of processed match details.
        """
        # Get basic match history
        match_history = self.get_match_history(matches_requested)
        if not match_history:
            return []
            
        # Save match history
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'player')
        os.makedirs(data_dir, exist_ok=True)
        
        with open(os.path.join(data_dir, f"match_history_{self.steam_id}.json"), 'w', encoding='utf-8') as f:
            json.dump(match_history, f, indent=2)
            
        # Get and process detailed match information
        processed_matches = []
        
        for match in match_history:
            match_id = match.get("match_id")
            if not match_id:
                continue
                
            # Get details from OpenDota
            match_details = self.get_match_details_from_opendota(match_id)
            
            if match_details:
                # Store in database
                self.db.insert_match_details(match_details)
                processed_matches.append(match_details)
                
                # Save individual match details
                with open(os.path.join(data_dir, f"match_{match_id}.json"), 'w', encoding='utf-8') as f:
                    json.dump(match_details, f, indent=2)
                    
        logger.info(f"Processed {len(processed_matches)} matches for player {self.steam_id}")
        return processed_matches
    
    def generate_player_statistics(self):
        """
        Generate player statistics from processed matches.
        
        Returns:
            dict: Player statistics.
        """
        stats = self.db.get_player_statistics(int(self.steam_id) % (2**32))  # Convert to 32-bit ID
        
        # If no stats found, try alternative ID format
        if 'error' in stats:
            logger.warning(f"No stats found for {self.steam_id}, trying alternative ID format")
            alt_id = int(self.steam_id) - 76561197960265728  # Convert to 32-bit ID
            stats = self.db.get_player_statistics(alt_id)
            
        # Get player profile for additional information
        profile = self.get_player_profile()
        if profile:
            stats['profile'] = profile
            
        return stats


if __name__ == "__main__":
    # Example usage
    player_id = DEFAULT_STEAM_ID
    history = PlayerMatchHistory(player_id)
    
    # Process recent matches
    matches = history.process_player_matches(10)
    
    if matches:
        # Generate player statistics
        stats = history.generate_player_statistics()
        
        # Print summary
        print(f"\nPlayer Summary for Steam ID {player_id}:")
        print(f"Total matches processed: {len(matches)}")
        
        if 'profile' in stats:
            print(f"Player name: {stats['profile'].get('profile', {}).get('personaname', 'Unknown')}")
            print(f"MMR Estimate: {stats['profile'].get('mmr_estimate', {}).get('estimate', 'Unknown')}")
        
        print(f"\nPerformance Stats:")
        if 'error' not in stats:
            print(f"Win rate: {stats.get('win_rate', 0) * 100:.2f}%")
            print(f"Average KDA: {stats.get('avg_kills', 0):.1f}/{stats.get('avg_deaths', 0):.1f}/{stats.get('avg_assists', 0):.1f}")
            print(f"Average GPM: {stats.get('avg_gpm', 0):.1f}")
        else:
            print("No performance stats available yet.")
