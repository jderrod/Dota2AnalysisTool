"""
Dota 2 Professional Match Analysis

This module provides analysis functions for Dota 2 professional match data.
"""
import os
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from database import DotaDatabase, Match, Team, Player, Hero, MatchPlayer, League

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set plot style
plt.style.use('ggplot')
sns.set(style="darkgrid")


class DotaAnalyzer:
    """
    A class to analyze Dota 2 professional match data.
    """
    
    def __init__(self, db_instance=None):
        """
        Initialize the analyzer with a database connection.
        
        Args:
            db_instance (DotaDatabase, optional): Database instance. If None, a new one will be created.
        """
        self.db = db_instance or DotaDatabase()
        self.output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
        os.makedirs(self.output_dir, exist_ok=True)
        
    def _get_session(self):
        """Get a database session."""
        return self.db.Session()
    
    def analyze_team_performance(self, team_id=None, top_n=10):
        """
        Analyze team performance metrics.
        
        Args:
            team_id (int, optional): Team ID to analyze. If None, analyze all teams.
            top_n (int, optional): Number of top teams to include. Defaults to 10.
            
        Returns:
            pandas.DataFrame: Team performance statistics.
        """
        session = self._get_session()
        
        try:
            if team_id:
                # Get statistics for a specific team
                stats = self.db.get_team_statistics(team_id)
                df = pd.DataFrame([stats])
                
                # Get the team name
                team = session.query(Team).filter_by(team_id=team_id).first()
                if team:
                    title = f"Performance Analysis for {team.name}"
                else:
                    title = f"Performance Analysis for Team ID {team_id}"
            else:
                # Get statistics for all teams
                all_teams = session.query(Team).all()
                all_stats = []
                
                for team in all_teams:
                    stats = self.db.get_team_statistics(team.team_id)
                    if 'error' not in stats:
                        stats['name'] = team.name
                        all_stats.append(stats)
                
                df = pd.DataFrame(all_stats)
                
                # Filter to teams with a minimum number of matches
                df = df[df['total_matches'] >= 5]
                
                # Sort by win rate
                df = df.sort_values('win_rate', ascending=False).head(top_n)
                
                title = f"Top {top_n} Teams by Win Rate (min. 5 matches)"
            
            # Visualize win rates
            plt.figure(figsize=(12, 6))
            ax = sns.barplot(x='name' if 'name' in df.columns else 'team_id', 
                            y='win_rate', data=df)
            plt.title(title)
            plt.xlabel('Team')
            plt.ylabel('Win Rate')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            # Add win-loss record as text
            for i, row in enumerate(df.itertuples()):
                if hasattr(row, 'total_wins') and hasattr(row, 'total_losses'):
                    ax.text(i, row.win_rate / 2, f"{int(row.total_wins)}-{int(row.total_losses)}", 
                            ha='center', color='white', fontweight='bold')
            
            # Save the figure
            output_file = os.path.join(self.output_dir, f"team_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Team performance analysis saved to {output_file}")
            
            return df
        finally:
            session.close()
    
    def analyze_hero_popularity(self, limit=20):
        """
        Analyze hero pick/ban popularity.
        
        Args:
            limit (int, optional): Number of heroes to include. Defaults to 20.
            
        Returns:
            pandas.DataFrame: Hero popularity statistics.
        """
        session = self._get_session()
        
        try:
            # Count hero picks
            hero_counts = session.query(
                MatchPlayer.hero_id,
                func.count(MatchPlayer.id).label('pick_count')
            ).group_by(MatchPlayer.hero_id).all()
            
            # Convert to DataFrame
            df = pd.DataFrame(hero_counts, columns=['hero_id', 'pick_count'])
            
            # Get hero names
            hero_data = {}
            heroes = session.query(Hero).all()
            for hero in heroes:
                hero_data[hero.hero_id] = hero.localized_name
            
            df['hero_name'] = df['hero_id'].map(hero_data)
            
            # Sort and limit
            df = df.sort_values('pick_count', ascending=False).head(limit)
            
            # Visualize hero popularity
            plt.figure(figsize=(14, 8))
            ax = sns.barplot(x='hero_name', y='pick_count', data=df)
            plt.title(f"Top {limit} Most Picked Heroes")
            plt.xlabel('Hero')
            plt.ylabel('Number of Picks')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            # Save the figure
            output_file = os.path.join(self.output_dir, f"hero_popularity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Hero popularity analysis saved to {output_file}")
            
            return df
        finally:
            session.close()
    
    def analyze_hero_win_rates(self, min_picks=5):
        """
        Analyze hero win rates.
        
        Args:
            min_picks (int, optional): Minimum number of picks to include a hero. Defaults to 5.
            
        Returns:
            pandas.DataFrame: Hero win rate statistics.
        """
        session = self._get_session()
        
        try:
            # Get hero statistics
            hero_stats = self.db.get_hero_statistics()
            
            # Convert to DataFrame
            df = pd.DataFrame(hero_stats)
            
            # Filter heroes with minimum number of matches
            df = df[df['total_matches'] >= min_picks]
            
            # Sort by win rate
            df = df.sort_values('win_rate', ascending=False)
            
            # Visualize hero win rates
            plt.figure(figsize=(14, 8))
            ax = sns.barplot(x='name', y='win_rate', data=df.head(20))
            plt.title(f"Top 20 Heroes by Win Rate (min. {min_picks} matches)")
            plt.xlabel('Hero')
            plt.ylabel('Win Rate')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            # Add pick count as text
            for i, row in enumerate(df.head(20).itertuples()):
                ax.text(i, row.win_rate / 2, f"{int(row.total_matches)} picks", 
                        ha='center', color='white', fontweight='bold')
            
            # Save the figure
            output_file = os.path.join(self.output_dir, f"hero_win_rates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Hero win rate analysis saved to {output_file}")
            
            return df
        finally:
            session.close()
    
    def analyze_match_durations(self):
        """
        Analyze match duration distribution.
        
        Returns:
            pandas.DataFrame: Match duration statistics.
        """
        session = self._get_session()
        
        try:
            # Get all match durations
            matches = session.query(Match.duration).all()
            
            # Convert to DataFrame
            df = pd.DataFrame(matches, columns=['duration'])
            
            # Convert seconds to minutes
            df['duration_minutes'] = df['duration'] / 60
            
            # Visualize duration distribution
            plt.figure(figsize=(10, 6))
            sns.histplot(df['duration_minutes'], bins=30, kde=True)
            plt.title("Distribution of Match Durations")
            plt.xlabel("Duration (minutes)")
            plt.ylabel("Frequency")
            
            # Add mean and median lines
            mean_duration = df['duration_minutes'].mean()
            median_duration = df['duration_minutes'].median()
            
            plt.axvline(mean_duration, color='red', linestyle='--', label=f'Mean: {mean_duration:.2f} min')
            plt.axvline(median_duration, color='green', linestyle='--', label=f'Median: {median_duration:.2f} min')
            plt.legend()
            
            # Save the figure
            output_file = os.path.join(self.output_dir, f"match_durations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Match duration analysis saved to {output_file}")
            
            # Calculate statistics
            stats_df = pd.DataFrame({
                'Statistic': ['Mean', 'Median', 'Min', 'Max', 'Std Dev'],
                'Value (minutes)': [
                    mean_duration,
                    median_duration,
                    df['duration_minutes'].min(),
                    df['duration_minutes'].max(),
                    df['duration_minutes'].std()
                ]
            })
            
            return stats_df
        finally:
            session.close()
    
    def analyze_player_performance(self, account_id=None, top_n=10):
        """
        Analyze player performance metrics.
        
        Args:
            account_id (int, optional): Player account ID to analyze. If None, analyze top players.
            top_n (int, optional): Number of top players to include. Defaults to 10.
            
        Returns:
            pandas.DataFrame: Player performance statistics.
        """
        session = self._get_session()
        
        try:
            if account_id:
                # Get statistics for a specific player
                stats = self.db.get_player_statistics(account_id)
                df = pd.DataFrame([stats])
                
                # Get the player name
                player = session.query(Player).filter_by(account_id=account_id).first()
                if player:
                    title = f"Performance Analysis for {player.name}"
                else:
                    title = f"Performance Analysis for Player ID {account_id}"
            else:
                # Get statistics for all players
                all_players = session.query(Player).all()
                all_stats = []
                
                for player in all_players:
                    stats = self.db.get_player_statistics(player.account_id)
                    if 'error' not in stats:
                        stats['name'] = player.name or player.personaname
                        all_stats.append(stats)
                
                df = pd.DataFrame(all_stats)
                
                # Filter to players with a minimum number of matches
                df = df[df['total_matches'] >= 10]
                
                # Sort by KDA ratio
                df['kda_ratio'] = (df['avg_kills'] + df['avg_assists']) / df['avg_deaths'].replace(0, 1)  # Avoid division by zero
                df = df.sort_values('kda_ratio', ascending=False).head(top_n)
                
                title = f"Top {top_n} Players by KDA Ratio (min. 10 matches)"
            
            # Visualize KDA
            plt.figure(figsize=(14, 8))
            
            # Plot as a grouped bar chart
            data_to_plot = df.melt(
                id_vars=['name'] if 'name' in df.columns else ['account_id'],
                value_vars=['avg_kills', 'avg_deaths', 'avg_assists'],
                var_name='Metric',
                value_name='Value'
            )
            
            ax = sns.barplot(x='name' if 'name' in df.columns else 'account_id', 
                           y='Value', hue='Metric', data=data_to_plot)
            
            plt.title(title)
            plt.xlabel('Player')
            plt.ylabel('Average Value')
            plt.xticks(rotation=45, ha='right')
            plt.legend(title='Metric')
            plt.tight_layout()
            
            # Save the figure
            output_file = os.path.join(self.output_dir, f"player_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Player performance analysis saved to {output_file}")
            
            return df
        finally:
            session.close()
    
    def analyze_draft_patterns(self):
        """
        Analyze common hero drafting patterns and combinations.
        
        Returns:
            dict: Draft pattern statistics.
        """
        session = self._get_session()
        
        try:
            # Get all matches
            matches = session.query(Match.match_id).all()
            match_ids = [m[0] for m in matches]
            
            # Dictionary to store hero pair frequencies
            hero_pairs = {}
            
            for match_id in match_ids:
                # Get all heroes in a match, grouped by team
                radiant_heroes = session.query(MatchPlayer.hero_id).filter(
                    MatchPlayer.match_id == match_id,
                    MatchPlayer.player_slot < 128  # Radiant players
                ).all()
                
                dire_heroes = session.query(MatchPlayer.hero_id).filter(
                    MatchPlayer.match_id == match_id,
                    MatchPlayer.player_slot >= 128  # Dire players
                ).all()
                
                # Count co-occurrences within each team
                for team_heroes in [radiant_heroes, dire_heroes]:
                    team_heroes = [h[0] for h in team_heroes]
                    
                    for i in range(len(team_heroes)):
                        for j in range(i + 1, len(team_heroes)):
                            # Sort to ensure consistent ordering
                            pair = tuple(sorted([team_heroes[i], team_heroes[j]]))
                            hero_pairs[pair] = hero_pairs.get(pair, 0) + 1
            
            # Convert to DataFrame
            pairs_list = []
            for (hero1, hero2), count in hero_pairs.items():
                pairs_list.append({
                    'hero1_id': hero1,
                    'hero2_id': hero2,
                    'count': count
                })
            
            df = pd.DataFrame(pairs_list)
            
            # Get hero names
            hero_data = {}
            heroes = session.query(Hero).all()
            for hero in heroes:
                hero_data[hero.hero_id] = hero.localized_name
            
            df['hero1_name'] = df['hero1_id'].map(hero_data)
            df['hero2_name'] = df['hero2_id'].map(hero_data)
            
            # Sort by frequency
            df = df.sort_values('count', ascending=False)
            
            # Visualize top hero pairs
            plt.figure(figsize=(14, 8))
            top_pairs = df.head(15)
            
            # Create pair names
            top_pairs['pair_name'] = top_pairs.apply(lambda x: f"{x['hero1_name']} + {x['hero2_name']}", axis=1)
            
            ax = sns.barplot(x='pair_name', y='count', data=top_pairs)
            plt.title("Top 15 Most Common Hero Pairs")
            plt.xlabel('Hero Pair')
            plt.ylabel('Number of Matches')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            # Save the figure
            output_file = os.path.join(self.output_dir, f"hero_pairs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Hero pair analysis saved to {output_file}")
            
            # Return top pairs
            return df.head(30).to_dict('records')
        finally:
            session.close()
    
    def analyze_match_trends(self, window=7):
        """
        Analyze trends in match statistics over time.
        
        Args:
            window (int, optional): Rolling window size in days. Defaults to 7.
            
        Returns:
            pandas.DataFrame: Match trend statistics.
        """
        session = self._get_session()
        
        try:
            # Get all matches with start time and duration
            matches = session.query(Match.match_id, Match.start_time, Match.duration, 
                                    Match.radiant_score, Match.dire_score).all()
            
            # Convert to DataFrame
            df = pd.DataFrame(matches, columns=['match_id', 'start_time', 'duration', 
                                               'radiant_score', 'dire_score'])
            
            # Add derived columns
            df['duration_minutes'] = df['duration'] / 60
            df['total_score'] = df['radiant_score'] + df['dire_score']
            df['date'] = df['start_time'].dt.date
            
            # Group by date
            daily_stats = df.groupby('date').agg({
                'match_id': 'count',
                'duration_minutes': 'mean',
                'total_score': 'mean'
            }).reset_index()
            
            daily_stats.columns = ['date', 'match_count', 'avg_duration', 'avg_score']
            
            # Apply rolling average
            daily_stats['rolling_avg_duration'] = daily_stats['avg_duration'].rolling(window=window, min_periods=1).mean()
            daily_stats['rolling_avg_score'] = daily_stats['avg_score'].rolling(window=window, min_periods=1).mean()
            
            # Visualize trends
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 15), sharex=True)
            
            # Match count over time
            ax1.plot(daily_stats['date'], daily_stats['match_count'], marker='o')
            ax1.set_title('Number of Matches per Day')
            ax1.set_ylabel('Match Count')
            ax1.grid(True)
            
            # Average duration over time
            ax2.plot(daily_stats['date'], daily_stats['avg_duration'], marker='o', alpha=0.5, label='Daily')
            ax2.plot(daily_stats['date'], daily_stats['rolling_avg_duration'], 'r-', label=f'{window}-day Rolling Avg')
            ax2.set_title('Average Match Duration Over Time')
            ax2.set_ylabel('Duration (minutes)')
            ax2.legend()
            ax2.grid(True)
            
            # Average score over time
            ax3.plot(daily_stats['date'], daily_stats['avg_score'], marker='o', alpha=0.5, label='Daily')
            ax3.plot(daily_stats['date'], daily_stats['rolling_avg_score'], 'r-', label=f'{window}-day Rolling Avg')
            ax3.set_title('Average Total Score (Kills) Over Time')
            ax3.set_xlabel('Date')
            ax3.set_ylabel('Average Score')
            ax3.legend()
            ax3.grid(True)
            
            plt.tight_layout()
            
            # Save the figure
            output_file = os.path.join(self.output_dir, f"match_trends_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Match trends analysis saved to {output_file}")
            
            return daily_stats
        finally:
            session.close()
    
    def generate_comprehensive_report(self, output_file=None):
        """
        Generate a comprehensive analysis report.
        
        Args:
            output_file (str, optional): Output file path. If None, a default path will be used.
            
        Returns:
            str: Path to the generated report.
        """
        if not output_file:
            output_file = os.path.join(self.output_dir, f"dota_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
        
        # Get analysis results
        team_stats = self.analyze_team_performance(top_n=5)
        hero_popularity = self.analyze_hero_popularity(limit=10)
        hero_win_rates = self.analyze_hero_win_rates(min_picks=10)
        match_durations = self.analyze_match_durations()
        player_stats = self.analyze_player_performance(top_n=5)
        
        # Generate HTML report
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dota 2 Professional Match Analysis Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2, h3 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f2f2f2; }}
                tr:hover {{ background-color: #f5f5f5; }}
                .section {{ margin-bottom: 30px; }}
                .date {{ color: #666; font-style: italic; }}
            </style>
        </head>
        <body>
            <h1>Dota 2 Professional Match Analysis Report</h1>
            <p class="date">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="section">
                <h2>Team Performance</h2>
                <table>
                    <tr>
                        <th>Team</th>
                        <th>Matches</th>
                        <th>Wins</th>
                        <th>Losses</th>
                        <th>Win Rate</th>
                        <th>Avg. Match Duration (min)</th>
                    </tr>
                    {"".join(f"<tr><td>{row['name'] if 'name' in row else row['team_id']}</td><td>{row['total_matches']}</td><td>{row['total_wins']}</td><td>{row['total_losses']}</td><td>{row['win_rate']:.2%}</td><td>{row['avg_duration']/60:.2f}</td></tr>" for _, row in team_stats.iterrows())}
                </table>
            </div>
            
            <div class="section">
                <h2>Most Popular Heroes</h2>
                <table>
                    <tr>
                        <th>Hero</th>
                        <th>Pick Count</th>
                    </tr>
                    {"".join(f"<tr><td>{row['hero_name']}</td><td>{row['pick_count']}</td></tr>" for _, row in hero_popularity.iterrows())}
                </table>
            </div>
            
            <div class="section">
                <h2>Hero Win Rates</h2>
                <table>
                    <tr>
                        <th>Hero</th>
                        <th>Matches</th>
                        <th>Wins</th>
                        <th>Losses</th>
                        <th>Win Rate</th>
                    </tr>
                    {"".join(f"<tr><td>{row['name']}</td><td>{row['total_matches']}</td><td>{row['wins']}</td><td>{row['losses']}</td><td>{row['win_rate']:.2%}</td></tr>" for _, row in hero_win_rates.head(10).iterrows())}
                </table>
            </div>
            
            <div class="section">
                <h2>Match Duration Statistics</h2>
                <table>
                    <tr>
                        <th>Statistic</th>
                        <th>Value (minutes)</th>
                    </tr>
                    {"".join(f"<tr><td>{row['Statistic']}</td><td>{row['Value (minutes)']:.2f}</td></tr>" for _, row in match_durations.iterrows())}
                </table>
            </div>
            
            <div class="section">
                <h2>Top Player Performances</h2>
                <table>
                    <tr>
                        <th>Player</th>
                        <th>Matches</th>
                        <th>Win Rate</th>
                        <th>Avg. Kills</th>
                        <th>Avg. Deaths</th>
                        <th>Avg. Assists</th>
                        <th>Avg. GPM</th>
                    </tr>
                    {"".join(f"<tr><td>{row['name'] if 'name' in row else row['account_id']}</td><td>{row['total_matches']}</td><td>{row['win_rate']:.2%}</td><td>{row['avg_kills']:.1f}</td><td>{row['avg_deaths']:.1f}</td><td>{row['avg_assists']:.1f}</td><td>{row['avg_gpm']:.0f}</td></tr>" for _, row in player_stats.iterrows())}
                </table>
            </div>
            
            <div class="section">
                <h2>Summary of Findings</h2>
                <p>
                    This report provides an analysis of professional Dota 2 matches. Key insights include:
                </p>
                <ul>
                    <li>The average match duration is {match_durations.iloc[0]['Value (minutes)']:.2f} minutes.</li>
                    <li>The most picked hero is {hero_popularity.iloc[0]['hero_name']} with {hero_popularity.iloc[0]['pick_count']} picks.</li>
                    <li>The hero with the highest win rate (minimum 10 matches) is {hero_win_rates.iloc[0]['name']} at {hero_win_rates.iloc[0]['win_rate']:.2%}.</li>
                    <li>The team with the highest win rate is {team_stats.iloc[0]['name'] if 'name' in team_stats.columns else f"Team ID {team_stats.iloc[0]['team_id']}"} at {team_stats.iloc[0]['win_rate']:.2%}.</li>
                </ul>
            </div>
        </body>
        </html>
        """
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Comprehensive report generated and saved to {output_file}")
        return output_file


if __name__ == "__main__":
    # Initialize analyzer
    analyzer = DotaAnalyzer()
    
    # Generate a comprehensive report
    report_path = analyzer.generate_comprehensive_report()
    print(f"Report generated: {report_path}")
