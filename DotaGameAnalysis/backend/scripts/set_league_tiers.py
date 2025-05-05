#!/usr/bin/env python
"""
Script to set or update league tiers in the database.

This script checks all leagues in the database and allows setting
proper tier values for them, enabling tier-specific analysis.
"""
import os
import sys
import logging
import requests
import json
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from database import DotaDatabase, League, Match

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("league_tiers.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# OpenDota API endpoints
OPENDOTA_API_URL = "https://api.opendota.com/api"


def get_known_tier1_leagues():
    """
    Return a set of known tier 1 league IDs.
    These are major tournaments and premium leagues in the Dota 2 professional scene.
    
    Returns:
        set: Set of league IDs that are considered tier 1
    """
    # This is a manually curated list of tier 1 leagues/tournaments
    # You may want to update this list based on your specific criteria
    tier1_leagues = {
        # The International tournaments
        13256,  # The International 2022
        14268,  # The International 2023
        15535,  # The International 2024 (if it exists in your data)
        
        # Dota Pro Circuit (DPC) Majors
        14417,  # Lima Major 2023
        14891,  # Berlin Major 2023
        14435,  # Bali Major 2023
        15080,  # Riyadh Masters 2023
        15438,  # DreamLeague Season 21
        
        # ESL One events
        14390,  # ESL One Berlin 2023
        15233,  # ESL One Kuala Lumpur 2023
        15612,  # ESL One Birmingham 2024
        
        # Other premium tournaments
        15504,  # DreamLeague Season 22
        13661,  # ESL One Malaysia 2022
        15169,  # Riyadh Masters 2023
        13379,  # PGL Arlington Major 2022
    }
    
    return tier1_leagues


def update_league_tiers(api_key=None):
    """
    Update league tiers in the database.
    
    Args:
        api_key (str, optional): OpenDota API key
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Get all leagues from database
        leagues = session.query(League).all()
        logger.info(f"Found {len(leagues)} leagues in database")
        
        if not leagues:
            logger.warning("No leagues found in database")
            return
            
        # Get known tier 1 leagues
        tier1_leagues = get_known_tier1_leagues()
        
        # Count leagues by tier before update
        tier_counts_before = {}
        for league in leagues:
            tier_counts_before[league.tier] = tier_counts_before.get(league.tier, 0) + 1
            
        logger.info(f"League tiers before update: {tier_counts_before}")
        
        # Update league tiers
        updated_count = 0
        
        for league in leagues:
            old_tier = league.tier
            
            # Set tier based on known tier 1 leagues
            if league.league_id in tier1_leagues:
                league.tier = 1
            elif league.tier == 0:
                # For unknown leagues, make an educated guess
                # This is a simple heuristic - if the league name contains certain keywords,
                # it's likely a higher tier league
                name_lower = league.name.lower() if league.name else ""
                if any(term in name_lower for term in ['major', 'international', 'ti', 'esl one', 'dreamleague']):
                    league.tier = 1
                elif any(term in name_lower for term in ['premier', 'championship', 'final']):
                    league.tier = 2
                else:
                    league.tier = 3
            
            if league.tier != old_tier:
                updated_count += 1
                logger.info(f"Updated league {league.name} (ID: {league.league_id}) from tier {old_tier} to tier {league.tier}")
        
        if updated_count > 0:
            session.commit()
            logger.info(f"Updated {updated_count} league tiers")
        else:
            logger.info("No league tiers needed updating")
            
        # Count leagues by tier after update
        tier_counts_after = {}
        leagues = session.query(League).all()
        for league in leagues:
            tier_counts_after[league.tier] = tier_counts_after.get(league.tier, 0) + 1
            
        logger.info(f"League tiers after update: {tier_counts_after}")
        
        # Print summary of tier 1 leagues
        tier1_leagues_data = session.query(League).filter_by(tier=1).all()
        print(f"\nTier 1 Leagues ({len(tier1_leagues_data)}):")
        for league in tier1_leagues_data:
            print(f"  - {league.name} (ID: {league.league_id})")
            
        # Print summary of matches by league tier
        matches_by_tier = {}
        for tier in range(1, 4):
            # Get league IDs for this tier
            tier_league_ids = [l.league_id for l in session.query(League).filter_by(tier=tier).all()]
            
            # Count matches in these leagues
            match_count = session.query(Match).filter(Match.league_id.in_(tier_league_ids)).count()
            matches_by_tier[tier] = match_count
            
        print("\nMatches by League Tier:")
        for tier, count in matches_by_tier.items():
            print(f"  - Tier {tier}: {count} matches")
            
    except Exception as e:
        logger.error(f"Error updating league tiers: {e}")
        session.rollback()
    finally:
        session.close()


def main():
    """Main function to update league tiers."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Update league tiers in the database")
    parser.add_argument("--api-key", type=str, help="OpenDota API key")
    args = parser.parse_args()
    
    # Check if database exists
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    db_file = os.path.join(data_dir, 'dota_matches.db')
    
    if not os.path.exists(db_file):
        logger.error(f"Database file not found: {db_file}")
        print(f"Error: Database file not found at {db_file}")
        return
        
    # Update league tiers
    update_league_tiers(api_key=args.api_key)
    
    print("\nLeague tiers have been updated. You can now run tier-specific analysis.")
    print("Example: python tier1_analysis.py --report")


if __name__ == "__main__":
    main()
