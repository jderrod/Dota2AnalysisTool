import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import func, desc, and_, or_
import seaborn as sns
from datetime import datetime
import requests
import numpy as np

# Add the parent directory to sys.path to import database modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from database.database_pro_teams import DotaDatabase, TimevsStats, MatchPlayer, Match, TeamFight, TeamFightPlayer 


def get_all_team_fights(match_id):
    """
    Retrieve all team fights for a specific match
    
    Args:
        match_id: The ID of the match to analyze
        
    Returns:
        List of TeamFight objects for the match
    """
    db = DotaDatabase()
    session = db.Session()
    
    team_fights = session.query(TeamFight).filter(
        TeamFight.match_id == match_id
    ).all()

    session.close()
    
    return team_fights

def get_team_fight_player_data(match_id):
    """
    Retrieve team fight data for a specific match
    
    Args:
        match_id: The ID of the match to analyze
        
    Returns:
        List of dictionaries containing team fight data
    """ 
    db = DotaDatabase()
    session = db.Session()
    
    # Query team fight data
    team_fights = session.query(TeamFightPlayer).filter(
        TeamFightPlayer.match_id == match_id
    ).all()    
    session.close()
        
    return team_fights

def get_specific_team_fight(match_id, teamfight_id):
    db = DotaDatabase()
    session = db.Session()
    
    # Query team fight data
    team_fights = session.query(TeamFightPlayer).filter(
        TeamFightPlayer.match_id == match_id,
        TeamFightPlayer.teamfight_id == teamfight_id
    ).all()
    session.close()
        
    return team_fights

def get_deaths(match_id, teamfight_id):
    """
    Get death information from a specific team fight
    
    Args:
        match_id: The ID of the match
        teamfight_id: The ID of the team fight
        
    Returns:
        List of tuples containing (deaths, death_positions) for each player who died
    """
    db = DotaDatabase()
    session = db.Session()
    
    # Query team fight data for players who died
    deaths = session.query(TeamFightPlayer).filter(
        TeamFightPlayer.match_id == match_id,
        TeamFightPlayer.teamfight_id == teamfight_id,
        TeamFightPlayer.deaths > 0
    ).all()
    
    # Extract death information
    death_info = []
    for player in deaths:
        death_info.append({
            'deaths': player.deaths,
            'death_position': player.deaths_pos,
            'damage_taken': player.damage,
            'gold_lost': -player.gold_delta if player.gold_delta < 0 else 0
        })
    
    session.close()
    return death_info

def main():
    """
    Main function to analyze team fight data
    
    Returns:
        None
    """
    match_id = 8190432750
    print("All team fights:")
    print(get_all_team_fights(match_id))
    print("Team fight player data:")
    print(get_team_fight_player_data(match_id))
    print("Specific team fight:")
    print(get_specific_team_fight(match_id, 1))
    print("Deaths:")
    print(get_deaths(match_id, 1))
if __name__ == "__main__":
    main()