import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import func, desc
import seaborn as sns

# Add the parent directory to the path so we can import from database
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from database.database_pro_teams import DotaDatabase, Player, Hero, MatchPlayer, Match

def get_match_player_data(match_id):
    """
    Retrieve all player data for a specific match
    
    Args:
        match_id: The ID of the match to analyze
        
    Returns:
        List of dictionaries containing player data
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Query the database for all players in this match
        match_players = session.query(
            MatchPlayer, Player, Hero
        ).join(
            Player, MatchPlayer.account_id == Player.account_id
        ).join(
            Hero, MatchPlayer.hero_id == Hero.hero_id
        ).filter(
            MatchPlayer.match_id == match_id
        ).all()
        
        if not match_players:
            print(f"No player data found for match ID {match_id}")
            return []
        
        # Process the results into a list of dictionaries
        players_data = []
        for mp, player, hero in match_players:
            player_data = {
                'player_name': player.name or player.personaname or f"Player {player.account_id}",
                'account_id': player.account_id,
                'hero_name': hero.localized_name,
                'player_slot': mp.player_slot,
                'kills': mp.kills,
                'deaths': mp.deaths,
                'assists': mp.assists,
                'gold_per_min': mp.gold_per_min,
                'xp_per_min': mp.xp_per_min,
                'last_hits': mp.last_hits,
                'denies': mp.denies,
                'hero_damage': mp.hero_damage,
                'tower_damage': mp.tower_damage,
                'hero_healing': mp.hero_healing,
                'level': mp.level,
                'team': 'Radiant' if mp.player_slot < 128 else 'Dire'
            }
            players_data.append(player_data)
        
        return players_data
    
    finally:
        session.close()

def plot_player_metrics(players_data, metric, title=None, figsize=(12, 8)):
    """
    Create a bar chart comparing players on a specific metric
    
    Args:
        players_data: List of player dictionaries
        metric: The metric to compare (e.g., 'kills', 'gold_per_min')
        title: Chart title (optional)
        figsize: Figure size as tuple (width, height)
    """
    if not players_data:
        print("No player data to plot")
        return
    
    # Create a DataFrame from the player data
    df = pd.DataFrame(players_data)
    
    # Sort by team and then by the metric
    df = df.sort_values(['team', metric], ascending=[True, False])
    
    # Create a color palette based on team
    colors = ['#66BB6A' if team == 'Radiant' else '#EF5350' for team in df['team']]
    
    # Create the plot
    plt.figure(figsize=figsize)
    ax = sns.barplot(x='player_name', y=metric, hue='team', data=df, palette={'Radiant': '#66BB6A', 'Dire': '#EF5350'}, legend=False)
    
    # Add hero names as annotations
    for i, player in enumerate(df.itertuples()):
        ax.text(i, 2, player.hero_name, ha='center', rotation=90, color='white', fontweight='bold')
    
    # Customize the plot
    if title:
        plt.title(title, fontsize=16)
    else:
        plt.title(f"Player Comparison - {metric.replace('_', ' ').title()}", fontsize=16)
    
    plt.xlabel('Player', fontsize=14)
    plt.ylabel(metric.replace('_', ' ').title(), fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    # Add a legend for teams
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor='#66BB6A', label='Radiant'),
        Patch(facecolor='#EF5350', label='Dire')
    ]
    plt.legend(handles=legend_elements, loc='upper right')
    
    plt.show()

def analyze_match(match_id):
    """
    Main function to analyze and visualize player metrics for a match
    
    Args:
        match_id: The ID of the match to analyze
    """
    # Get match data from database
    players_data = get_match_player_data(match_id)
    
    if not players_data:
        return
    
    # Get match details for the title
    db = DotaDatabase()
    session = db.Session()
    match = session.query(Match).filter(Match.match_id == match_id).first()
    session.close()
    
    if match:
        match_title = f"Match {match_id} - Duration: {match.duration//60}m {match.duration%60}s"
        winner = "Radiant" if match.radiant_win else "Dire"
        match_title += f" - {winner} Victory"
    else:
        match_title = f"Match {match_id}"
    
    # Plot various metrics
    metrics_to_plot = [
        'kills', 'deaths', 'assists', 
        'gold_per_min', 'xp_per_min', 
        'last_hits', 'hero_damage', 
        'tower_damage', 'hero_healing'
    ]
    
    for metric in metrics_to_plot:
        plot_player_metrics(players_data, metric, f"{match_title} - {metric.replace('_', ' ').title()}")

if __name__ == "__main__":
    # Example usage: analyze a specific match
    match_id = 8215476026  # Replace with the match ID you want to analyze
    analyze_match(match_id)