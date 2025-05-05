#!/usr/bin/env python
"""
Tier 1 Dota 2 Professional Match Analysis

This script runs analysis specifically on tier 1 professional Dota 2 matches.
"""
import os
import sys
import logging
import argparse
from datetime import datetime
from sqlalchemy import create_engine, func, and_
from sqlalchemy.orm import sessionmaker
from database import DotaDatabase, Match, League, Team, Player, Hero, MatchPlayer
from analysis import DotaAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("tier1_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Tier1DotaAnalyzer(DotaAnalyzer):
    """
    Analyzer specifically for tier 1 professional Dota 2 matches.
    Inherits from the main DotaAnalyzer class but filters queries to only include tier 1 matches.
    """
    
    def __init__(self, db_instance=None):
        """Initialize with parent constructor"""
        super().__init__(db_instance)
        self.output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output', 'tier1')
        os.makedirs(self.output_dir, exist_ok=True)
        
    def _get_tier1_match_ids(self):
        """
        Get a list of match IDs that belong to tier 1 leagues.
        
        Returns:
            list: List of match IDs from tier 1 leagues
        """
        session = self._get_session()
        try:
            # Join Match and League tables and filter for tier 1
            tier1_matches = session.query(Match.match_id).join(
                League, Match.league_id == League.league_id
            ).filter(
                League.tier == 1
            ).all()
            
            # Extract match IDs from result tuples
            match_ids = [match[0] for match in tier1_matches]
            logger.info(f"Found {len(match_ids)} tier 1 matches")
            return match_ids
        finally:
            session.close()
    
    def calculate_team_statistics(self, team_id, match_ids):
        """
        Calculate statistics for a specific team in the given matches.
        
        Args:
            team_id (int): The team ID to calculate statistics for
            match_ids (list): List of match IDs to consider
            
        Returns:
            dict: Team statistics
        """
        session = self._get_session()
        
        try:
            # Get matches where this team participated and that are in our match_ids list
            matches = session.query(Match).filter(
                Match.match_id.in_(match_ids),
                (Match.radiant_team_id == team_id) | (Match.dire_team_id == team_id)
            ).all()
            
            if not matches:
                return {
                    "team_id": team_id,
                    "total_matches": 0,
                    "total_wins": 0,
                    "total_losses": 0,
                    "win_rate": 0.0,
                    "avg_duration": 0.0
                }
            
            wins = 0
            total_duration = 0
            
            for match in matches:
                # Check if this team won
                if ((match.radiant_team_id == team_id and match.radiant_win) or
                    (match.dire_team_id == team_id and not match.radiant_win)):
                    wins += 1
                
                total_duration += match.duration
            
            total_matches = len(matches)
            losses = total_matches - wins
            win_rate = wins / total_matches if total_matches > 0 else 0
            avg_duration = total_duration / total_matches if total_matches > 0 else 0
            
            return {
                "team_id": team_id,
                "total_matches": total_matches,
                "total_wins": wins,
                "total_losses": losses,
                "win_rate": win_rate,
                "avg_duration": avg_duration
            }
        except Exception as e:
            logger.error(f"Error calculating team statistics: {e}")
            return {
                "team_id": team_id,
                "error": str(e)
            }
        finally:
            session.close()
    
    def analyze_team_performance(self, team_id=None, top_n=10):
        """
        Analyze team performance metrics for tier 1 matches only.
        
        Args:
            team_id (int, optional): Team ID to analyze. If None, analyze all teams.
            top_n (int, optional): Number of top teams to include. Defaults to 10.
            
        Returns:
            pandas.DataFrame: Team performance statistics.
        """
        session = self._get_session()
        
        try:
            # Get tier 1 match IDs
            tier1_match_ids = self._get_tier1_match_ids()
            
            if not tier1_match_ids:
                logger.warning("No tier 1 matches found in the database")
                return None
            
            if team_id:
                # Get statistics for a specific team in tier 1 matches
                stats = self.calculate_team_statistics(team_id, tier1_match_ids)
                
                # Create DataFrame for visualization
                import pandas as pd
                df = pd.DataFrame([stats])
                
                # Get the team name
                team = session.query(Team).filter_by(team_id=team_id).first()
                if team:
                    title = f"Performance Analysis for {team.name} in Tier 1 Tournaments"
                else:
                    title = f"Performance Analysis for Team ID {team_id} in Tier 1 Tournaments"
            else:
                # For all teams, calculate stats for each
                all_teams = session.query(Team).all()
                all_stats = []
                
                for team in all_teams:
                    stats = self.calculate_team_statistics(team.team_id, tier1_match_ids)
                    if 'error' not in stats and stats['total_matches'] > 0:
                        stats['name'] = team.name
                        all_stats.append(stats)
                
                # Create DataFrame and generate visualizations
                import pandas as pd
                df = pd.DataFrame(all_stats)
                
                # Filter to teams with a minimum number of matches
                df = df[df['total_matches'] >= 3]
                
                # Sort by win rate
                df = df.sort_values('win_rate', ascending=False).head(top_n)
                
                title = f"Top {len(df)} Teams by Win Rate in Tier 1 Tournaments (min. 3 matches)"
            
            # Create visualization
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            if df.empty:
                logger.warning("No teams with enough matches for visualization")
                return df
                
            plt.figure(figsize=(12, 6))
            ax = sns.barplot(x='name', y='win_rate', data=df)
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
            output_file = os.path.join(self.output_dir, f"tier1_team_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Tier 1 team performance analysis saved to {output_file}")
            
            return df
        finally:
            session.close()
    
    def analyze_hero_popularity(self, limit=20):
        """
        Analyze hero pick/ban popularity in tier 1 matches only.
        
        Args:
            limit (int, optional): Number of heroes to include. Defaults to 20.
            
        Returns:
            pandas.DataFrame: Hero popularity statistics.
        """
        session = self._get_session()
        
        try:
            # Get tier 1 match IDs
            tier1_match_ids = self._get_tier1_match_ids()
            
            if not tier1_match_ids:
                logger.warning("No tier 1 matches found in the database")
                return None
            
            # Count hero picks in tier 1 matches only
            hero_counts = session.query(
                MatchPlayer.hero_id,
                func.count(MatchPlayer.id).label('pick_count')
            ).filter(
                MatchPlayer.match_id.in_(tier1_match_ids)
            ).group_by(MatchPlayer.hero_id).all()
            
            # Convert to DataFrame
            import pandas as pd
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
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            plt.figure(figsize=(14, 8))
            ax = sns.barplot(x='hero_name', y='pick_count', data=df)
            plt.title(f"Top {limit} Most Picked Heroes in Tier 1 Tournaments")
            plt.xlabel('Hero')
            plt.ylabel('Number of Picks')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            # Save the figure
            output_file = os.path.join(self.output_dir, f"tier1_hero_popularity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(output_file)
            logger.info(f"Tier 1 hero popularity analysis saved to {output_file}")
            
            return df
        finally:
            session.close()
    
    def generate_tier1_report(self, output_file=None):
        """
        Generate a comprehensive analysis report for tier 1 matches only.
        
        Args:
            output_file (str, optional): Output file path. If None, a default path will be used.
            
        Returns:
            str: Path to the generated report.
        """
        if not output_file:
            output_file = os.path.join(self.output_dir, f"tier1_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
        
        # Get analysis results
        team_stats = self.analyze_team_performance(top_n=10)
        hero_popularity = self.analyze_hero_popularity(limit=15)
        
        # Handle case where no tier 1 matches are found
        if team_stats is None or hero_popularity is None:
            logger.warning("Cannot generate report - no tier 1 matches found")
            return None
        
        # Generate HTML report
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Tier 1 Dota 2 Professional Match Analysis Report</title>
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
            <h1>Tier 1 Dota 2 Professional Match Analysis Report</h1>
            <p class="date">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="section">
                <h2>Team Performance (Tier 1 Only)</h2>
                <table>
                    <tr>
                        <th>Team</th>
                        <th>Matches</th>
                        <th>Wins</th>
                        <th>Losses</th>
                        <th>Win Rate</th>
                        <th>Avg. Match Duration (min)</th>
                    </tr>
                    {"".join(f"<tr><td>{row['name']}</td><td>{row['total_matches']}</td><td>{row['total_wins']}</td><td>{row['total_losses']}</td><td>{row['win_rate']:.2%}</td><td>{row['avg_duration']/60:.2f}</td></tr>" for _, row in team_stats.iterrows())}
                </table>
            </div>
            
            <div class="section">
                <h2>Most Popular Heroes (Tier 1 Only)</h2>
                <table>
                    <tr>
                        <th>Hero</th>
                        <th>Pick Count</th>
                    </tr>
                    {"".join(f"<tr><td>{row['hero_name']}</td><td>{row['pick_count']}</td></tr>" for _, row in hero_popularity.iterrows())}
                </table>
            </div>
            
            <p>This report contains analysis of only tier 1 professional Dota 2 matches.</p>
        </body>
        </html>
        """
        
        # Write HTML content to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Tier 1 analysis report generated at {output_file}")
        return output_file


def main():
    """Main entry point for the tier 1 analysis script."""
    parser = argparse.ArgumentParser(description="Analyze tier 1 Dota 2 professional matches")
    parser.add_argument("--team", type=int, help="Team ID to analyze")
    parser.add_argument("--report", action="store_true", help="Generate a comprehensive report")
    args = parser.parse_args()
    
    # Initialize the tier 1 analyzer
    analyzer = Tier1DotaAnalyzer()
    
    if args.team:
        # Analyze a specific team's performance in tier 1 matches
        analyzer.analyze_team_performance(team_id=args.team)
    elif args.report:
        # Generate comprehensive report
        report_path = analyzer.generate_tier1_report()
        if report_path:
            print(f"Report generated: {report_path}")
        else:
            print("Could not generate report - no tier 1 matches found")
    else:
        # Default: run all analyses
        team_stats = analyzer.analyze_team_performance()
        hero_stats = analyzer.analyze_hero_popularity()
        
        if team_stats is not None and hero_stats is not None:
            print("Analysis complete. Check the 'output/tier1' directory for results.")
        else:
            print("Could not complete analysis - no tier 1 matches found")


if __name__ == "__main__":
    main()
