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
from database.database_pro_teams import DotaDatabase, TimevsStats, MatchPlayer, Match

def get_curr_gold_radiant(match_id, time):
    """Get the total gold for all Radiant players at a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query gold for all radiant players (player_slot < 128)
    result = session.query(func.sum(TimevsStats.gold)).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_slot < 128,
        TimevsStats.time <= time,
        TimevsStats.event_type == None  # Regular time series entry
    ).order_by(TimevsStats.time.desc()).first()
    
    session.close()
    return result[0] if result and result[0] else 0

def get_curr_gold_dire(match_id, time):
    """Get the total gold for all Dire players at a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query gold for all dire players (player_slot >= 128)
    result = session.query(func.sum(TimevsStats.gold)).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_slot >= 128,
        TimevsStats.time <= time,
        TimevsStats.event_type == None  # Regular time series entry
    ).order_by(TimevsStats.time.desc()).first()
    
    session.close()
    return result[0] if result and result[0] else 0

def get_curr_gold_player(match_id, player_name, time):
    """Get the gold for a specific player at a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query the latest gold value for the player before or at the given time
    result = session.query(TimevsStats.gold).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_name == player_name,
        TimevsStats.time <= time,
        TimevsStats.event_type == None  # Regular time series entry
    ).order_by(TimevsStats.time.desc()).first()
    
    session.close()
    return result[0] if result else 0

def get_curr_xp_player(match_id, player_name, time):
    """Get the XP for a specific player at a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query the latest XP value for the player before or at the given time
    result = session.query(TimevsStats.xp).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_name == player_name,
        TimevsStats.time <= time,
        TimevsStats.event_type == None  # Regular time series entry
    ).order_by(TimevsStats.time.desc()).first()
    
    session.close()
    return result[0] if result else 0

def get_curr_last_hits_player(match_id, player_name, time):
    """Get the last hits for a specific player at a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query the latest last_hits value for the player before or at the given time
    result = session.query(TimevsStats.last_hits).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_name == player_name,
        TimevsStats.time <= time,
        TimevsStats.event_type == None  # Regular time series entry
    ).order_by(TimevsStats.time.desc()).first()
    
    session.close()
    return result[0] if result else 0

def get_curr_denies_player(match_id, player_name, time):
    """Get the denies for a specific player at a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query the latest denies value for the player before or at the given time
    result = session.query(TimevsStats.denies).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_name == player_name,
        TimevsStats.time <= time,
        TimevsStats.event_type == None  # Regular time series entry
    ).order_by(TimevsStats.time.desc()).first()
    
    session.close()
    return result[0] if result else 0

def get_curr_kill_log(match_id, player_name, time):
    """Get all kills by a specific player up to a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query all kill events for the player up to the given time
    results = session.query(
        TimevsStats.time, 
        TimevsStats.killed_hero
    ).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_name == player_name,
        TimevsStats.time <= time,
        TimevsStats.event_type == "kill"
    ).order_by(TimevsStats.time).all()
    
    session.close()
    return [(r.time, r.killed_hero) for r in results]

def get_curr_purchase_log(match_id, player_name, time):
    """Get all purchases by a specific player up to a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query all purchase events for the player up to the given time
    results = session.query(
        TimevsStats.time, 
        TimevsStats.purchased_item
    ).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_name == player_name,
        TimevsStats.time <= time,
        TimevsStats.event_type == "purchase"
    ).order_by(TimevsStats.time).all()
    
    session.close()
    return [(r.time, r.purchased_item) for r in results]

def get_curr_assists_log(match_id, player_name, time):
    """Get all assists by a specific player up to a specific time"""
    # Note: This would require additional data in the TimevsStats table
    # For now, we'll return an empty list as assists aren't tracked in the current schema
    return []

def get_runes_log(match_id, player_name, time):
    """Get all runes picked up by a specific player up to a specific time"""
    db = DotaDatabase()
    session = db.Session()
    
    # Query all rune events for the player up to the given time
    results = session.query(
        TimevsStats.time, 
        TimevsStats.rune_type
    ).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_name == player_name,
        TimevsStats.time <= time,
        TimevsStats.event_type == "rune"
    ).order_by(TimevsStats.time).all()
    
    session.close()
    return [(r.time, r.rune_type) for r in results]

def get_player_slot(match_id, player_name):
    db = DotaDatabase()
    session = db.Session()
    
    # Query the current lane for the player
    result = session.query(TimevsStats.player_slot).filter(
        TimevsStats.match_id == match_id,
        TimevsStats.player_name == player_name
    ).first()
    
    session.close()
    return result.player_slot

def get_all_player_slots(match_id):
    """
    Get all player slots for a specific match
    
    Returns:
        list: List of player slots in the match
    """
    db = DotaDatabase()
    session = db.Session()
    
    # Query all player slots for the match
    results = session.query(
        TimevsStats.player_slot
    ).filter(
        TimevsStats.match_id == match_id
    ).distinct().all()
    
    session.close()
    # Return a list of player slots
    return [r.player_slot for r in results]

def get_all_player_lanes(match_id):
    """
    Get all player slots and their starting lanes for a specific match
    
    Returns:
        list: List of tuples (player_slot, lane, player_name)
    """
    db = DotaDatabase()
    session = db.Session()
    
    # Query all player slots and their starting lanes for the match
    results = session.query(
        TimevsStats.player_slot,
        TimevsStats.starting_lane,
        TimevsStats.player_name
    ).filter(
        TimevsStats.match_id == match_id
    ).distinct().all()
    
    session.close()
    # Return a list of tuples (player_slot, lane, player_name)
    return [(r.player_slot, r.starting_lane, r.player_name) for r in results]

def match_player_lanes(match_id):
    """
    Get players organized by team and lane
    
    Returns:
        dict: Dictionary with teams and lanes, containing player information
    """
    # Get all player data
    player_data = get_all_player_lanes(match_id)
    
    # Initialize result structure
    result = {
        'radiant': {
            'safe': [],  # Lane 1
            'mid': [],   # Lane 2
            'off': []    # Lane 3
        },
        'dire': {
            'safe': [],  # Lane 1
            'mid': [],   # Lane 2
            'off': []    # Lane 3
        }
    }
    
    # Organize players by team and lane
    for player_slot, lane, player_name in player_data:
        # Determine team (Radiant: 0-127, Dire: 128-255)
        team = 'radiant' if player_slot < 128 else 'dire'
        
        # Determine lane name
        lane_name = None
        if lane == 1:
            lane_name = 'safe'
        elif lane == 2:
            lane_name = 'mid'
        elif lane == 3:
            lane_name = 'off'
        
        # Add player to appropriate team and lane
        if lane_name:
            result[team][lane_name].append({
                'player_slot': player_slot,
                'player_name': player_name
            })
    
    return result

def get_player_stats_at_time(match_id, player_slot, time):
    """Get comprehensive stats for a player at a specific time"""
    return {
        'gold': get_curr_gold_player(match_id, player_slot, time),
        'xp': get_curr_xp_player(match_id, player_slot, time),
        'last_hits': get_curr_last_hits_player(match_id, player_slot, time),
        'denies': get_curr_denies_player(match_id, player_slot, time),
        'kills': len(get_curr_kill_log(match_id, player_slot, time)),
        'purchases': get_curr_purchase_log(match_id, player_slot, time),
        'runes': get_runes_log(match_id, player_slot, time)
    }

def off_vs_safe_radiant(match_id, time):
    """
    Compare offlane vs safelane matchup at a specific time
    
    From Radiant perspective:
    - Radiant offlane (lane 3) vs Dire safelane (lane 1)
    
    Returns:
        dict: Comprehensive stats for both sides of the matchup
    """
    # Get players organized by team and lane
    lane_data = match_player_lanes(match_id)
    
    # Get stats for each player at the specified time
    matchup = {
        'radiant_offlane': [],
        'dire_safelane': [],
        'radiant_advantage': {}
    }
    
    # Process Radiant offlane
    for player in lane_data['radiant']['off']:
        player_slot = player['player_slot']
        player_name = player['player_name']
        stats = get_player_stats_at_time(match_id, player_slot, time)
        matchup['radiant_offlane'].append({
            'player_slot': player_slot,
            'player_name': player_name,
            'stats': stats
        })
    
    # Process Dire safelane
    for player in lane_data['dire']['safe']:
        player_slot = player['player_slot']
        player_name = player['player_name']
        stats = get_player_stats_at_time(match_id, player_slot, time)
        matchup['dire_safelane'].append({
            'player_slot': player_slot,
            'player_name': player_name,
            'stats': stats
        })
    
    # Calculate lane advantage metrics
    radiant_lane_gold = sum(p['stats']['gold'] for p in matchup['radiant_offlane'])
    dire_lane_gold = sum(p['stats']['gold'] for p in matchup['dire_safelane'])
    
    radiant_lane_xp = sum(p['stats']['xp'] for p in matchup['radiant_offlane'])
    dire_lane_xp = sum(p['stats']['xp'] for p in matchup['dire_safelane'])
    
    radiant_lane_cs = sum(p['stats']['last_hits'] for p in matchup['radiant_offlane'])
    dire_lane_cs = sum(p['stats']['last_hits'] for p in matchup['dire_safelane'])
    
    matchup['radiant_advantage'] = {
        'gold': radiant_lane_gold - dire_lane_gold,
        'xp': radiant_lane_xp - dire_lane_xp,
        'cs': radiant_lane_cs - dire_lane_cs
    }
    
    return matchup

def mid_vs_mid(match_id, time):
    """
    Compare midlane matchup at a specific time
    
    Returns:
        dict: Comprehensive stats for both midlaners
    """
    # Get players organized by team and lane
    lane_data = match_player_lanes(match_id)
    
    # Get stats for each player at the specified time
    matchup = {
        'radiant_mid': [],
        'dire_mid': [],
        'radiant_advantage': {}
    }
    
    # Process Radiant mid
    for player in lane_data['radiant']['mid']:
        player_slot = player['player_slot']
        player_name = player['player_name']
        stats = get_player_stats_at_time(match_id, player_slot, time)
        matchup['radiant_mid'].append({
            'player_slot': player_slot,
            'player_name': player_name,
            'stats': stats
        })
    
    # Process Dire mid
    for player in lane_data['dire']['mid']:
        player_slot = player['player_slot']
        player_name = player['player_name']
        stats = get_player_stats_at_time(match_id, player_slot, time)
        matchup['dire_mid'].append({
            'player_slot': player_slot,
            'player_name': player_name,
            'stats': stats
        })
    
    # Calculate lane advantage metrics
    radiant_lane_gold = sum(p['stats']['gold'] for p in matchup['radiant_mid'])
    dire_lane_gold = sum(p['stats']['gold'] for p in matchup['dire_mid'])
    
    radiant_lane_xp = sum(p['stats']['xp'] for p in matchup['radiant_mid'])
    dire_lane_xp = sum(p['stats']['xp'] for p in matchup['dire_mid'])
    
    radiant_lane_cs = sum(p['stats']['last_hits'] for p in matchup['radiant_mid'])
    dire_lane_cs = sum(p['stats']['last_hits'] for p in matchup['dire_mid'])
    
    matchup['radiant_advantage'] = {
        'gold': radiant_lane_gold - dire_lane_gold,
        'xp': radiant_lane_xp - dire_lane_xp,
        'cs': radiant_lane_cs - dire_lane_cs
    }
    
    return matchup

def safe_vs_off_radiant(match_id, time):
    """
    Compare safelane vs offlane matchup at a specific time
    
    From Radiant perspective:
    - Radiant safelane (lane 1) vs Dire offlane (lane 3)
    
    Returns:
        dict: Comprehensive stats for both sides of the matchup
    """
    # Get players organized by team and lane
    lane_data = match_player_lanes(match_id)
    
    # Get stats for each player at the specified time
    matchup = {
        'radiant_safelane': [],
        'dire_offlane': [],
        'radiant_advantage': {}
    }
    
    # Process Radiant safelane
    for player in lane_data['radiant']['safe']:
        player_slot = player['player_slot']
        player_name = player['player_name']
        stats = get_player_stats_at_time(match_id, player_slot, time)
        matchup['radiant_safelane'].append({
            'player_slot': player_slot,
            'player_name': player_name,
            'stats': stats
        })
    
    # Process Dire offlane
    for player in lane_data['dire']['off']:
        player_slot = player['player_slot']
        player_name = player['player_name']
        stats = get_player_stats_at_time(match_id, player_slot, time)
        matchup['dire_offlane'].append({
            'player_slot': player_slot,
            'player_name': player_name,
            'stats': stats
        })
    
    # Calculate lane advantage metrics
    radiant_lane_gold = sum(p['stats']['gold'] for p in matchup['radiant_safelane'])
    dire_lane_gold = sum(p['stats']['gold'] for p in matchup['dire_offlane'])
    
    radiant_lane_xp = sum(p['stats']['xp'] for p in matchup['radiant_safelane'])
    dire_lane_xp = sum(p['stats']['xp'] for p in matchup['dire_offlane'])
    
    radiant_lane_cs = sum(p['stats']['last_hits'] for p in matchup['radiant_safelane'])
    dire_lane_cs = sum(p['stats']['last_hits'] for p in matchup['dire_offlane'])
    
    matchup['radiant_advantage'] = {
        'gold': radiant_lane_gold - dire_lane_gold,
        'xp': radiant_lane_xp - dire_lane_xp,
        'cs': radiant_lane_cs - dire_lane_cs
    }
    
    return matchup

def track_lane_stats_over_time(match_id, lane_type, start_time=0, end_time=None, interval=60):
    """
    Track lane statistics over time intervals
    
    Args:
        match_id (int): The match ID
        lane_type (str): Type of lane to track ('off_vs_safe', 'mid_vs_mid', or 'safe_vs_off')
        start_time (int): Starting time in seconds (default: 0)
        end_time (int): Ending time in seconds (default: None - will use match duration)
        interval (int): Time interval in seconds (default: 60 - every minute)
    
    Returns:
        dict: Time series data for the lane matchup
    """
    # If end_time is not provided, get the match duration from the database
    if end_time is None:
        db = DotaDatabase()
        session = db.Session()
        match_duration = session.query(Match.duration).filter(Match.match_id == match_id).first()
        session.close()
        
        if match_duration:
            end_time = match_duration[0]
        else:
            # Default to 20 minutes if match duration not found
            end_time = 1200
    
    time_points = range(start_time, end_time + interval, interval)
    time_series = {
        'times': list(time_points),
        'data': []
    }
    
    # Select the appropriate lane function
    if lane_type == 'off_vs_safe':
        lane_func = off_vs_safe_radiant
    elif lane_type == 'mid_vs_mid':
        lane_func = mid_vs_mid
    elif lane_type == 'safe_vs_off':
        lane_func = safe_vs_off_radiant
    else:
        raise ValueError(f"Unknown lane type: {lane_type}")
    
    # Collect data for each time point
    for time_point in time_points:
        matchup = lane_func(match_id, time_point)
        
        # Format the data for this time point
        time_data = {
            'time': time_point,
            'time_min': time_point / 60,  # Convert to minutes for readability
            'matchup': matchup
        }
        
        # Add player-specific stats for easier access
        if lane_type == 'off_vs_safe':
            # Radiant offlane players
            for i, player in enumerate(matchup['radiant_offlane']):
                player_key = f"radiant_offlane_{i+1}"
                time_data[player_key] = {
                    'name': player['player_name'],
                    'gold': player['stats']['gold'],
                    'xp': player['stats']['xp'],
                    'last_hits': player['stats']['last_hits'],
                    'denies': player['stats']['denies']
                }
            
            # Dire safelane players
            for i, player in enumerate(matchup['dire_safelane']):
                player_key = f"dire_safelane_{i+1}"
                time_data[player_key] = {
                    'name': player['player_name'],
                    'gold': player['stats']['gold'],
                    'xp': player['stats']['xp'],
                    'last_hits': player['stats']['last_hits'],
                    'denies': player['stats']['denies']
                }
            
            # Add advantage metrics
            time_data['radiant_advantage'] = matchup['radiant_advantage']
            
        elif lane_type == 'mid_vs_mid':
            # Radiant mid
            if matchup['radiant_mid']:
                time_data['radiant_mid'] = {
                    'name': matchup['radiant_mid'][0]['player_name'],
                    'gold': matchup['radiant_mid'][0]['stats']['gold'],
                    'xp': matchup['radiant_mid'][0]['stats']['xp'],
                    'last_hits': matchup['radiant_mid'][0]['stats']['last_hits'],
                    'denies': matchup['radiant_mid'][0]['stats']['denies']
                }
            
            # Dire mid
            if matchup['dire_mid']:
                time_data['dire_mid'] = {
                    'name': matchup['dire_mid'][0]['player_name'],
                    'gold': matchup['dire_mid'][0]['stats']['gold'],
                    'xp': matchup['dire_mid'][0]['stats']['xp'],
                    'last_hits': matchup['dire_mid'][0]['stats']['last_hits'],
                    'denies': matchup['dire_mid'][0]['stats']['denies']
                }
            
            # Add advantage metrics
            if 'radiant_advantage' in matchup:
                time_data['radiant_advantage'] = matchup['radiant_advantage']
        
        # Add data for this time point to the time series
        time_series['data'].append(time_data)
    
    return time_series
def get_all_player_names(match_id):
    """
    Get all player names organized by team
    
    Args:
        match_id (int): The match ID
        
    Returns:
        dict: Player names organized by team
            {
                'radiant': [player1, player2, player3, player4, player5],
                'dire': [player6, player7, player8, player9, player10]
            }
    """
    try:
        # Get player slots for the match
        player_slots = get_all_player_slots(match_id)
        
        # Get player lanes
        player_lanes = get_all_player_lanes(match_id)
        
        # Initialize team arrays
        radiant_players = []
        dire_players = []
        
        # Process each player
        for slot, lane, player_name in player_lanes:
            if slot in player_slots:
                # Determine team based on slot
                if slot < 128:  # Radiant team (slots 0-127)
                    radiant_players.append(player_name)
                else:  # Dire team (slots 128-255)
                    dire_players.append(player_name)
        
        # Sort players by lane order (1-5 for Radiant, 6-10 for Dire)
        radiant_players.sort(key=lambda name: next((i for i, x in enumerate(player_lanes) if x[2] == name), 0))
        dire_players.sort(key=lambda name: next((i for i, x in enumerate(player_lanes) if x[2] == name), 0))
        
        return {
            'radiant': radiant_players,
            'dire': dire_players
        }
        
    except Exception as e:
        print(f"Error getting player names for match {match_id}: {str(e)}")
        raise
def main():   
    # Use a real match ID from the database
    match_id = 8190432750

    # Display the organized players by team and lane
    print("Players organized by team and lane:")
    print(match_player_lanes(match_id))
    
    # Display lane matchups at the 2-minute mark
    print("\nLane matchups at 2 minutes:")
    print("Offlane vs Safelane:", off_vs_safe_radiant(match_id, 120))
    print("Mid vs Mid:", mid_vs_mid(match_id, 120))
    print("Safelane vs Offlane:", safe_vs_off_radiant(match_id, 120))
    
    # Track lane statistics over time (first 10 minutes only for brevity)
    print("\nTracking lane statistics over time (first 10 minutes):")
    print("Offlane vs Safelane stats:", track_off_vs_safe_over_time(match_id, 0, 600, 120))
    print("Mid vs Mid stats:", track_mid_vs_mid_over_time(match_id, 0, 600, 120))
    print("Safelane vs Offlane stats:", track_safe_vs_off_over_time(match_id, 0, 600, 120))
    
if __name__ == '__main__':
    main()