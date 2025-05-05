"""
Dota 2 Professional Match Scraper

This module handles fetching professional Dota 2 match data from the OpenDota API.
"""
import os
import time
import json
import logging
from datetime import datetime, timedelta
import requests
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API Constants
BASE_URL = "https://api.opendota.com/api"
API_RATE_LIMIT = 60  # requests per minute (OpenDota free tier)
API_DAILY_LIMIT = 2000  # requests per day (OpenDota free tier)


class DotaMatchScraper:
    """
    A class to scrape professional Dota 2 match data from the OpenDota API.
    """

    def __init__(self, api_key=None):
        """
        Initialize the scraper with an API key.
        
        Args:
            api_key (str, optional): OpenDota API key. Defaults to None.
        """
        self.api_key = api_key
        self.session = requests.Session()
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 60 / API_RATE_LIMIT  # seconds between requests
        self.current_day_count = 0
        self.day_start = datetime.now().date()
        self.checkpoint_file = os.path.join("data", "checkpoint.json")
        
    def _handle_rate_limiting(self):
        """
        Ensure the scraper respects API rate limits.
        """
        # Check if we've moved to a new day
        current_date = datetime.now().date()
        if current_date > self.day_start:
            self.day_start = current_date
            self.current_day_count = 0
            
        # Check if we're approaching the daily limit
        if self.current_day_count >= API_DAILY_LIMIT:
            logger.warning("Daily API limit reached. Waiting until tomorrow.")
            tomorrow = datetime.combine(current_date + timedelta(days=1), datetime.min.time())
            sleep_time = (tomorrow - datetime.now()).total_seconds()
            time.sleep(sleep_time)
            self.current_day_count = 0
            
        # Handle rate limiting within a minute
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        
        self.last_request_time = time.time()
        self.current_day_count += 1
        
    def _make_request(self, endpoint, params=None):
        """
        Make a request to the OpenDota API.
        
        Args:
            endpoint (str): API endpoint to request.
            params (dict, optional): Query parameters. Defaults to None.
            
        Returns:
            dict: JSON response from the API.
        """
        url = f"{BASE_URL}/{endpoint}"
        query_params = params or {}
        
        # Add API key if available
        if self.api_key:
            query_params['api_key'] = self.api_key
            
        self._handle_rate_limiting()
        
        try:
            response = self.session.get(url, params=query_params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(response, 'status_code') and response.status_code == 429:
                logger.warning("Rate limit exceeded. Waiting before retrying...")
                time.sleep(60)  # Wait a minute before retrying
                return self._make_request(endpoint, params)
            return None
    
    def get_pro_matches(self, limit=100, less_than_match_id=None):
        """
        Get a list of professional matches.
        
        Args:
            limit (int, optional): Number of matches to retrieve. Defaults to 100.
            less_than_match_id (int, optional): Get matches with ID less than this value. Used for pagination.
            
        Returns:
            list: List of professional match data.
        """
        params = {'limit': limit}
        if less_than_match_id:
            params['less_than_match_id'] = less_than_match_id
            
        return self._make_request('proMatches', params)
    
    def get_pro_matches_by_date_range(self, start_date, end_date, min_league_tier=1):
        """
        Get all professional matches within a date range.
        
        Args:
            start_date (str): Start date in ISO format (YYYY-MM-DD).
            end_date (str): End date in ISO format (YYYY-MM-DD).
            min_league_tier (int, optional): Minimum league tier to include (1-3). Defaults to 1.
            
        Returns:
            list: List of professional match data.
        """
        all_matches = []
        
        # Convert dates to timestamps
        start_ts = int(datetime.fromisoformat(start_date).timestamp())
        end_ts = int(datetime.fromisoformat(end_date).timestamp())
        
        last_match_id = None
        total_matches = 0
        
        logger.info(f"Fetching pro matches from {start_date} to {end_date}")
        
        while True:
            params = {'limit': 100}
            if last_match_id:
                params['less_than_match_id'] = last_match_id
                
            matches = self.get_pro_matches(**params)
            
            if not matches or len(matches) == 0:
                logger.info("No more matches to fetch.")
                break
                
            # Filter matches based on date range
            filtered_matches = []
            for match in matches:
                # Match timestamp is in seconds since epoch
                match_time = match.get('start_time', 0)
                league_tier = match.get('league_tier', 0)
                
                if start_ts <= match_time <= end_ts and league_tier >= min_league_tier:
                    filtered_matches.append(match)
            
            all_matches.extend(filtered_matches)
            total_matches += len(filtered_matches)
            
            # Update the last match ID for pagination
            if matches:
                last_match_id = matches[-1]['match_id']
                logger.debug(f"Last match ID: {last_match_id}")
            
            logger.info(f"Fetched {len(filtered_matches)} matches in this batch, {total_matches} total matches so far")
            
            # If we didn't get any matches in our date range, we might be past our date range
            if len(filtered_matches) == 0 and matches[0]['start_time'] < start_ts:
                logger.info("Reached matches before our start date. Stopping.")
                break
                
        logger.info(f"Retrieved a total of {len(all_matches)} professional matches")
        return all_matches
    
    def get_recent_pro_matches(self, limit=5, last_match_id=None):
        """
        Get the most recent professional matches, regardless of tier.
        
        Args:
            limit (int, optional): Maximum number of matches to retrieve. Defaults to 5.
            last_match_id (int, optional): If provided, get matches older than this ID.
            
        Returns:
            list: List of professional match data.
            int: The ID of the last match processed, can be used for pagination.
        """
        all_matches = []
        current_match_id = last_match_id
        batch_count = 0
        max_batches = 5  # Limit the number of batches to avoid infinite loops
        
        logger.info(f"Fetching {limit} recent professional matches")
        
        if last_match_id:
            logger.info(f"Starting from match ID less than {last_match_id}")
        
        while len(all_matches) < limit and batch_count < max_batches:
            batch_count += 1
            params = {'limit': 100}
            if current_match_id:
                params['less_than_match_id'] = current_match_id
                
            matches = self.get_pro_matches(**params)
            
            if not matches or len(matches) == 0:
                logger.info("No more matches available from the API.")
                break
            
            # Take only what we need to reach the limit
            remaining_needed = limit - len(all_matches)
            matches_to_add = matches[:remaining_needed]
            
            all_matches.extend(matches_to_add)
            
            # Update the match ID for the next iteration
            if matches:
                current_match_id = matches[-1]['match_id']
                
            logger.info(f"Found {len(matches_to_add)} matches in this batch, {len(all_matches)} total matches so far")
            
            # If we got fewer matches than we requested, we've reached the end
            if len(matches) < 100:
                logger.info("Reached the end of available matches.")
                break
                
        logger.info(f"Retrieved a total of {len(all_matches)} professional matches")
        
        # Return the matches and the last match ID for continuation
        last_processed_id = current_match_id if matches else None
        return all_matches, last_processed_id
    
    def save_checkpoint(self, last_match_id):
        """
        Save the last processed match ID to a checkpoint file.
        
        Args:
            last_match_id (int): The ID of the last match processed.
        """
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        
        checkpoint_data = {
            'last_match_id': last_match_id,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2)
            
        logger.info(f"Saved checkpoint with last match ID: {last_match_id}")
    
    def load_checkpoint(self):
        """
        Load the last processed match ID from a checkpoint file.
        
        Returns:
            int: The ID of the last match processed, None if not available.
        """
        if not os.path.exists(self.checkpoint_file):
            logger.info("No checkpoint file found. Starting from the most recent matches.")
            return None
            
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
                
            last_match_id = checkpoint_data.get('last_match_id')
            timestamp = checkpoint_data.get('timestamp')
            
            logger.info(f"Loaded checkpoint from {timestamp} with last match ID: {last_match_id}")
            return last_match_id
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            return None
    
    def get_match_details(self, match_id):
        """
        Get detailed information about a specific match.
        
        Args:
            match_id (int): Match ID to retrieve details for.
            
        Returns:
            dict: Match details.
        """
        return self._make_request(f"matches/{match_id}")
    
    def get_match_details_batch(self, match_ids, save_individual_files=False, directory=None):
        """
        Get detailed information for a batch of matches.
        
        Args:
            match_ids (list): List of match IDs to retrieve details for.
            save_individual_files (bool, optional): Whether to save each match detail in its own file.
            directory (str, optional): Directory to save individual match files to.
            
        Returns:
            list: List of match details.
        """
        match_details = []
        
        logger.info(f"Fetching details for {len(match_ids)} matches")
        
        # Create directory if needed
        if save_individual_files and directory:
            os.makedirs(directory, exist_ok=True)
        
        for match_id in tqdm(match_ids, desc="Fetching match details"):
            details = self.get_match_details(match_id)
            if details:
                match_details.append(details)
                
                # Save individual file if requested
                if save_individual_files and directory:
                    self.save_match_to_json(details, os.path.join(directory, f"match_{match_id}.json"))
                
        return match_details
    
    def save_match_to_json(self, match_data, filename):
        """
        Save a single match to a JSON file.
        
        Args:
            match_data (dict): Match data to save.
            filename (str): Filename to save to.
        """
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(match_data, f, indent=2)
            
        logger.debug(f"Saved match data to {filename}")
    
    def get_pro_players(self):
        """
        Get a list of professional players.
        
        Returns:
            list: List of professional players.
        """
        return self._make_request('proPlayers')
    
    def get_team_info(self, team_id):
        """
        Get information about a team.
        
        Args:
            team_id (int): Team ID to retrieve info for.
            
        Returns:
            dict: Team information.
        """
        return self._make_request(f"teams/{team_id}")
    
    def get_leagues(self):
        """
        Get a list of leagues.
        
        Returns:
            list: List of leagues.
        """
        return self._make_request('leagues')
    
    def save_to_json(self, data, filename):
        """
        Save data to a JSON file.
        
        Args:
            data: Data to save.
            filename (str): Filename to save to.
        """
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
            
        logger.info(f"Saved data to {filename}")


if __name__ == "__main__":
    # Example usage
    scraper = DotaMatchScraper()
    
    # Try to load the last match ID from checkpoint
    last_match_id = scraper.load_checkpoint()
    
    # Fetch recent professional matches (without tier filtering)
    matches, last_processed_id = scraper.get_recent_pro_matches(limit=5, last_match_id=last_match_id)
    
    # Save the new checkpoint
    if last_processed_id:
        scraper.save_checkpoint(last_processed_id)
    
    # Save matches to disk
    if matches:
        scraper.save_to_json(matches, "data/raw/recent_pro_matches.json")
        
        # Get detailed information for the matches
        match_ids = [match['match_id'] for match in matches]
        match_details = scraper.get_match_details_batch(
            match_ids, 
            save_individual_files=True,
            directory="data/raw/matches"
        )
        
        if match_details:
            scraper.save_to_json(match_details, "data/raw/recent_match_details.json")
