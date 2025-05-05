import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import func, desc, and_, or_
import seaborn as sns
from datetime import datetime
import requests

# Add the parent directory to the path so we can import from database
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from database.database_pro_teams import DotaDatabase, Player, Hero, MatchPlayer, Match, Team

def hero_id_to_name(hero_id):
    """
    Given a hero_id, return the localized name of the hero by querying the OpenDota API.
    
    Args:
        hero_id (int): The hero ID.
    
    Returns:
        str: The localized name of the hero, or None if not found.
    """
    # OpenDota API endpoint for heroes
    heroes_url = "https://api.opendota.com/api/heroes"
    
    try:
        response = requests.get(heroes_url)
        response.raise_for_status()  # raise an exception for bad responses
        heroes = response.json()
    except Exception as e:
        print(f"Error fetching heroes: {e}")
        return None

    for hero in heroes:
        if hero.get("id") == hero_id:
            # Some API responses use "localized_name" as the hero name.
            return hero.get("localized_name")
    
    return f"Unknown Hero ({hero_id})"

def get_team_id_by_name(team_name):
    """
    Get the team ID from the team name
    
    Args:
        team_name: Partial or full name of the team
        
    Returns:
        team_id or None if not found
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Try exact match first
        team = session.query(Team).filter(
            Team.name.ilike(f"{team_name}")
        ).first()
        
        # If not found, try partial match
        if not team:
            team = session.query(Team).filter(
                Team.name.ilike(f"%{team_name}%")
            ).first()
        
        return team.team_id if team else None
    
    finally:
        session.close()

def get_players_from_team(team_id):
    """
    Get all players from a specific team
    
    Args:
        team_id: The ID of the team
        
    Returns:
        List of player account IDs and names
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        players = session.query(Player).filter(
            Player.team_id == team_id
        ).all()
        
        return [(player.account_id, player.name or player.personaname or f"Player {player.account_id}") 
                for player in players]
    
    finally:
        session.close()

def get_player_recent_matches(account_id, limit=20):
    """
    Get statistics for a player's recent matches
    
    Args:
        account_id: The player's account ID
        limit: Maximum number of matches to analyze
        
    Returns:
        List of match data dictionaries
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Get the player's recent matches
        match_players = session.query(
            MatchPlayer, Match, Hero
        ).join(
            Match, MatchPlayer.match_id == Match.match_id
        ).join(
            Hero, MatchPlayer.hero_id == Hero.hero_id
        ).filter(
            MatchPlayer.account_id == account_id
        ).order_by(
            Match.start_time.desc()
        ).limit(limit).all()
        
        if not match_players:
            return []
        
        # Process match data
        matches = []
        for mp, match, hero in match_players:
            player_team = "Radiant" if mp.player_slot < 128 else "Dire"
            player_won = (player_team == "Radiant" and match.radiant_win) or \
                        (player_team == "Dire" and not match.radiant_win)
            
            # If hero name is missing, try to get it from API
            hero_name = hero.localized_name
            if not hero_name:
                hero_name = hero_id_to_name(mp.hero_id) or f"Hero {mp.hero_id}"
            
            match_data = {
                'match_id': match.match_id,
                'hero_id': mp.hero_id,
                'hero_name': hero_name,
                'kills': mp.kills,
                'deaths': mp.deaths,
                'assists': mp.assists,
                'gold_per_min': mp.gold_per_min,
                'xp_per_min': mp.xp_per_min,
                'hero_damage': mp.hero_damage,
                'tower_damage': mp.tower_damage,
                'hero_healing': mp.hero_healing,
                'last_hits': mp.last_hits,
                'denies': mp.denies,
                'level': mp.level,
                'win': player_won,
                'duration': match.duration,
                'start_time': match.start_time,
                'date': match.start_time
            }
            matches.append(match_data)
        
        return matches
    
    finally:
        session.close()


def analyze_player_recent_matches(player_name, limit=20):
    """
    Analyze a player's recent matches grouped by hero
    
    Args:
        player_name: Name of the player
        limit: Maximum number of matches to analyze
    """
    db = DotaDatabase()
    session = db.Session()
    
    # Find the player by name
    player = session.query(Player).filter(
        or_(
            Player.name.ilike(f"%{player_name}%"),
            Player.personaname.ilike(f"%{player_name}%")
        )
    ).first()
    session.close()
    
    if not player:
        print(f"Player '{player_name}' not found")
        return
    
    # Get their account ID and display name
    account_id = player.account_id
    display_name = player.name or player.personaname or f"Player {player.account_id}"
    
    print(f"\n{'='*60}")
    print(f"Recent Match Analysis for {display_name}")
    print(f"{'='*60}")
    
    # Get recent matches
    matches = get_player_recent_matches(account_id, limit)
    
    if not matches:
        print(f"No recent matches found for {display_name}")
        return
    
    # Group matches by hero
    matches_df = pd.DataFrame(matches)
    hero_groups = matches_df.groupby('hero_name')
    
    # Print overall stats
    total_matches = len(matches)
    total_wins = matches_df['win'].sum()
    win_rate = (total_wins / total_matches) * 100
    
    print(f"Total Matches: {total_matches}")
    print(f"Overall Win Rate: {win_rate:.2f}% ({total_wins} wins, {total_matches - total_wins} losses)\n")
    
    print("Heroes Played:")
    # Create a dictionary to store hero stats for visualization
    hero_stats = {}
    
    for hero_name, group in hero_groups:
        hero_matches = len(group)
        hero_wins = group['win'].sum()
        hero_winrate = (hero_wins / hero_matches) * 100
        avg_kda = f"{group['kills'].mean():.1f}/{group['deaths'].mean():.1f}/{group['assists'].mean():.1f}"
        
        print(f"  {hero_name}: {hero_matches} matches, {hero_winrate:.2f}% win rate, Avg KDA: {avg_kda}")
        
        # Store hero stats for visualization
        hero_stats[hero_name] = {
            'matches': hero_matches,
            'wins': hero_wins,
            'win_rate': hero_winrate / 100,  # Store as decimal
            'avg_kills': group['kills'].mean(),
            'avg_deaths': group['deaths'].mean(),
            'avg_assists': group['assists'].mean()
        }
    
    print("\nDetailed Hero Statistics:")
    
    # Analyze each hero's performance
    for hero_name, group in hero_groups:
        hero_matches = len(group)
        hero_wins = group['win'].sum()
        hero_winrate = (hero_wins / hero_matches) * 100
        
        print(f"\n{'-'*60}")
        print(f"{hero_name} - {hero_matches} matches, {hero_winrate:.2f}% win rate")
        print(f"{'-'*60}")
        
        # Calculate averages
        avg_stats = {
            'kills': group['kills'].mean(),
            'deaths': group['deaths'].mean(),
            'assists': group['assists'].mean(),
            'gold_per_min': group['gold_per_min'].mean(),
            'xp_per_min': group['xp_per_min'].mean(),
            'hero_damage': group['hero_damage'].mean(),
            'tower_damage': group['tower_damage'].mean(),
            'last_hits': group['last_hits'].mean(),
            'level': group['level'].mean()
        }
        
        print(f"Average Stats:")
        print(f"  KDA: {avg_stats['kills']:.2f}/{avg_stats['deaths']:.2f}/{avg_stats['assists']:.2f}")
        print(f"  GPM: {avg_stats['gold_per_min']:.2f}")
        print(f"  XPM: {avg_stats['xp_per_min']:.2f}")
        print(f"  Last Hits: {avg_stats['last_hits']:.2f}")
        print(f"  Hero Damage: {avg_stats['hero_damage']:.2f}")
        print(f"  Tower Damage: {avg_stats['tower_damage']:.2f}")
        print(f"  Level: {avg_stats['level']:.2f}")
        
        print(f"\nMatches:")
        for i, match in group.iterrows():
            win_status = "WIN" if match['win'] else "LOSS"
            minutes = match['duration'] // 60
            seconds = match['duration'] % 60
            print(f"  {win_status} - Match {match['match_id']} - KDA: {match['kills']}/{match['deaths']}/{match['assists']} - {minutes}m {seconds}s")
        
        # Plot stats for this hero if there are enough matches
        # Removed individual hero performance chart generation
    
    # Plot the hero summary chart
    plot_heroes_summary(display_name, hero_stats)
    
def plot_hero_performance(player_name, hero_name, matches_df):
    """
    Create visualizations for a player's performance with a hero
    
    Args:
        player_name: Name of the player
        hero_name: Name of the hero
        matches_df: DataFrame with match data
    """
    # Sort by start_time for chronological order
    matches_df = matches_df.sort_values('start_time')
    
    # Plot KDA as bar charts instead of line plots
    plt.figure(figsize=(14, 8))
    plt.title(f"{player_name}'s KDA with {hero_name}", fontsize=16)
    
    match_indices = range(len(matches_df))
    
    # Create a bar chart with KDA
    bar_width = 0.25
    index = np.arange(len(matches_df))
    
    plt.bar(index - bar_width, matches_df['kills'], bar_width, color='green', label='Kills')
    plt.bar(index, matches_df['deaths'], bar_width, color='red', label='Deaths')
    plt.bar(index + bar_width, matches_df['assists'], bar_width, color='blue', label='Assists')
    
    # Add win/loss indicators at the bottom
    for i, win in enumerate(matches_df['win']):
        color = 'green' if win else 'red'
        plt.axvspan(i-0.5, i+0.5, 0, 0.03, color=color)
    
    plt.xlabel('Match Index (Oldest to Most Recent)', fontsize=14)
    plt.ylabel('Count', fontsize=14)
    plt.xticks(index, [str(i) for i in range(len(matches_df))])
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()
    
    # Add a pie chart for win/loss ratio
    plt.figure(figsize=(10, 8))
    win_count = matches_df['win'].sum()
    loss_count = len(matches_df) - win_count
    
    labels = [f'Wins ({win_count})', f'Losses ({loss_count})']
    sizes = [win_count, loss_count]
    colors = ['green', 'red']
    explode = (0.1, 0)  # Explode the first slice (Wins)
    
    plt.pie(sizes, explode=explode, labels=labels, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=90)
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
    plt.title(f"{player_name}'s Win/Loss Ratio with {hero_name}", fontsize=16)
    plt.tight_layout()
    plt.show()
    
    # Plot various metrics in a bar chart
    metrics = ['kills', 'deaths', 'assists', 'gold_per_min', 'xp_per_min', 'last_hits']
    metrics_df = matches_df[metrics].copy()
    
    # Normalize GPM and XPM for better visualization
    metrics_df['gold_per_min'] = metrics_df['gold_per_min'] / 100
    metrics_df['xp_per_min'] = metrics_df['xp_per_min'] / 100
    
    plt.figure(figsize=(14, 8))
    ax = metrics_df.plot(kind='bar', figsize=(14, 8))
    plt.title(f"{player_name}'s Performance with {hero_name}", fontsize=16)
    plt.xlabel('Match Index', fontsize=14)
    plt.ylabel('Value', fontsize=14)
    plt.grid(True, alpha=0.3)
    
    # Add win/loss color to the bottom of the bars
    for i, win in enumerate(matches_df['win']):
        color = 'green' if win else 'red'
        plt.axvspan(i-0.4, i+0.4, 0, 0.02, color=color)
    
    plt.tight_layout()
    plt.legend(title='Metrics')
    plt.show()
    
    # Add a pie chart for average KDA distribution
    plt.figure(figsize=(10, 8))
    
    avg_kills = matches_df['kills'].mean()
    avg_deaths = matches_df['deaths'].mean()
    avg_assists = matches_df['assists'].mean()
    
    labels = [f'Avg Kills ({avg_kills:.1f})', f'Avg Deaths ({avg_deaths:.1f})', f'Avg Assists ({avg_assists:.1f})']
    sizes = [avg_kills, avg_deaths, avg_assists]
    colors = ['green', 'red', 'blue']
    explode = (0.1, 0, 0)  # Explode the first slice
    
    plt.pie(sizes, explode=explode, labels=labels, colors=colors,
            autopct='%1.1f%%', shadow=True, startangle=90)
    plt.axis('equal')
    plt.title(f"{player_name}'s Average KDA Distribution with {hero_name}", fontsize=16)
    plt.tight_layout()
    plt.show()

def plot_heroes_summary(player_name, hero_stats):
    """
    Create a comprehensive visualization of all heroes played by the player
    
    Args:
        player_name: Name of the player
        hero_stats: Dictionary with hero statistics
    """
    # Sort heroes by number of matches played (descending)
    sorted_heroes = sorted(hero_stats.keys(), key=lambda x: hero_stats[x]['matches'], reverse=True)
    
    # Prepare data for visualization
    heroes = []
    matches = []
    win_rates = []
    avg_kills = []
    avg_deaths = []
    avg_assists = []
    
    for hero in sorted_heroes:
        stats = hero_stats[hero]
        heroes.append(hero)
        matches.append(stats['matches'])
        win_rates.append(stats['win_rate'] * 100)  # Convert to percentage
        avg_kills.append(stats['avg_kills'])
        avg_deaths.append(stats['avg_deaths'])
        avg_assists.append(stats['avg_assists'])
    
    # Create a figure with appropriate size
    plt.figure(figsize=(16, 10))
    
    # Set up bar chart positions
    x = np.arange(len(heroes))
    width = 0.2
    
    # Create the bar chart with matches played
    ax1 = plt.subplot(111)
    matches_bars = ax1.bar(x - width*1.5, matches, width, label='Matches Played', color='blue', alpha=0.7)
    
    # Create bars for KDA
    kills_bars = ax1.bar(x - width/2, avg_kills, width, label='Avg. Kills', color='green', alpha=0.7)
    deaths_bars = ax1.bar(x + width/2, avg_deaths, width, label='Avg. Deaths', color='red', alpha=0.7)
    assists_bars = ax1.bar(x + width*1.5, avg_assists, width, label='Avg. Assists', color='purple', alpha=0.7)
    
    # Add a second y-axis for win rate
    ax2 = ax1.twinx()
    win_rate_line = ax2.plot(x, win_rates, 'o-', color='gold', linewidth=3, markersize=10, label='Win Rate %')
    
    # Add labels, title and legends
    ax1.set_xlabel('Hero', fontsize=14)
    ax1.set_ylabel('Count', fontsize=14)
    ax2.set_ylabel('Win Rate %', fontsize=14)
    plt.title(f"{player_name}'s Hero Performance Summary", fontsize=18)
    
    # Add text labels for win rate percentages
    for i, wr in enumerate(win_rates):
        plt.text(x[i], wr + 2, f"{wr:.1f}%", ha='center', va='bottom', fontweight='bold')
    
    # Add hero names as x-tick labels with rotation
    plt.xticks(x, heroes, rotation=45, ha='right')
    
    # Add grid lines for better readability
    ax1.grid(axis='y', linestyle='--', alpha=0.6)
    
    # Add a legend that combines both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=12)
    
    # Add annotations for matches with 100% win rate
    for i, (hero, wr) in enumerate(zip(heroes, win_rates)):
        if wr == 100:
            plt.annotate('100% Win!', xy=(x[i], max(avg_kills[i], avg_assists[i]) + 1), 
                         xytext=(x[i], max(avg_kills[i], avg_assists[i]) + 3),
                         ha='center', va='bottom',
                         bbox=dict(boxstyle='round,pad=0.5', fc='gold', alpha=0.7),
                         fontsize=10)
    
    # Add pie chart as an inset to show overall win rate
    total_matches = sum(matches)
    total_wins = sum(hero_stats[hero]['wins'] for hero in sorted_heroes)
    win_rate = total_wins / total_matches * 100
    
    # Create inset pie chart for overall win rate
    ax_inset = plt.axes([0.70, 0.70, 0.2, 0.2])  # Adjust position as needed
    labels = [f'Wins ({total_wins})', f'Losses ({total_matches - total_wins})']
    sizes = [win_rate, 100 - win_rate]
    colors = ['green', 'red']
    ax_inset.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', 
                 shadow=True, startangle=90, textprops={'fontsize': 8})
    ax_inset.set_title(f'Overall: {win_rate:.1f}% Win Rate', fontsize=10)
    
    plt.tight_layout()
    plt.show()

def main():
    """Interactive command-line interface for player analysis"""
    print("\n===== Dota 2 Pro Player Analysis =====\n")
    
    player_name = input("Enter pro player name: ")
    
    # Get match limit
    try:
        limit_input = input("Enter maximum number of matches to analyze (default: 20): ")
        limit = int(limit_input) if limit_input.strip() else 20
    except ValueError:
        print("Invalid number, using default limit of 20 matches")
        limit = 20
    
    # Analyze player's recent matches
    analyze_player_recent_matches(player_name, limit)

if __name__ == "__main__":
    main()