#!/usr/bin/env python
import os
import json
import requests
from pathlib import Path

def get_hero_mapping():
    """Fetch hero ID to name mapping from OpenDota API."""
    try:
        print("Fetching hero data from OpenDota API...")
        response = requests.get("https://api.opendota.com/api/heroes")
        heroes = response.json()
        hero_mapping = {hero['id']: hero['localized_name'] for hero in heroes}
        return hero_mapping
    except Exception as e:
        print(f"Error fetching hero data: {e}")
        return {}

def find_tundra_directories():
    """Find all Tundra Esports directories in the teams folder."""
    base_path = Path("c:/Users/jtder/Dota2ProGames/DotaProMatchAnalysis/data/raw/teams")
    tundra_dirs = [d for d in base_path.iterdir() if d.is_dir() and "Tundra" in d.name]
    return tundra_dirs

def extract_players_info(match_file, hero_mapping):
    """Extract player names, heroes, and team info from a match file."""
    with open(match_file, 'r') as f:
        match_data = json.load(f)
    
    match_id = match_data.get('match_id', 'Unknown')
    players_info = []
    for player in match_data.get('players', []):
        hero_id = player.get('hero_id')
        # Determine the player's side and corresponding team info
        player_side = 'radiant' if player.get('isRadiant', False) else 'dire'
        team_data = match_data.get(f'{player_side}_team', {})
        team_name = team_data.get('name', 'Unknown')
        
        player_info = {
            'name': player.get('name', 'Unknown'),
            'persona_name': player.get('personaname', 'Unknown'),
            'account_id': player.get('account_id'),
            'hero_id': hero_id,
            'hero_name': hero_mapping.get(hero_id, f"Hero_{hero_id}"),
            'team_side': player_side,
            'team_name': team_name,
            'kills': player.get('kills', 0),
            'deaths': player.get('deaths', 0),
            'assists': player.get('assists', 0)
        }
        players_info.append(player_info)
    
    return match_id, players_info

def print_match_hero_details():
    """For each match file, print which hero each player played."""
    tundra_dirs = find_tundra_directories()
    if not tundra_dirs:
        print("No Tundra Esports directories found")
        return
    
    hero_mapping = get_hero_mapping()
    print("\nMatch Hero Details:\n")
    
    for team_dir in tundra_dirs:
        match_files = list(team_dir.glob("match_*.json"))
        if not match_files:
            print(f"No match files found in {team_dir}")
            continue
        
        for match_file in match_files:
            match_id, players = extract_players_info(match_file, hero_mapping)
            print(f"Match {match_id}:")
            for player in players:
                # Choose a display name if available
                name = player.get('name') if player.get('name') != 'Unknown' else player.get('persona_name', 'Unknown')
                hero_name = player.get('hero_name', 'Unknown')
                print(f"  Player {name} played {hero_name}")
                print(f"  Kills: {player.get('kills', 0)}, Deaths: {player.get('deaths', 0)}, Assists: {player.get('assists', 0)}")
            print()  # Blank line between matches

if __name__ == "__main__":
    print_match_hero_details()
