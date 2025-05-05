#!/usr/bin/env python
"""
List Teams by ELO Rating

This script lists all teams in the database, sorted by their ELO rating.
It can output the result to the console or to a CSV file.
"""
import os
import sys
import logging
import argparse
import csv
from sqlalchemy import text
from database import DotaDatabase, Team

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("team_listing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_teams_by_elo(min_matches=0, include_no_elo=False):
    """
    Get all teams from the database, sorted by ELO rating.
    
    Args:
        min_matches (int): Minimum number of matches played by a team
        include_no_elo (bool): Whether to include teams without an ELO rating
        
    Returns:
        list: List of team data dictionaries
    """
    db = DotaDatabase()
    session = db.Session()
    
    try:
        # Debug: Print schema info
        for row in session.execute(text("PRAGMA table_info(teams)")):
            logger.info(f"Column in teams table: {row}")
        
        # Debug: Check for teams with non-NULL elo values
        result = session.execute(text("SELECT COUNT(*) FROM teams WHERE elo IS NOT NULL"))
        for row in result:
            logger.info(f"Teams with non-NULL elo: {row[0]}")
        
        # Build the query based on parameters
        if include_no_elo:
            # Include all teams, but prioritize those with ELO ratings
            stmt = """
                SELECT t.team_id, t.name, t.tag, t.elo,
                       COUNT(DISTINCT m.match_id) as match_count
                FROM teams t
                LEFT JOIN matches m ON t.team_id = m.radiant_team_id OR t.team_id = m.dire_team_id
                GROUP BY t.team_id, t.name, t.tag, t.elo
                HAVING match_count >= :min_matches
                ORDER BY t.elo DESC NULLS LAST, match_count DESC
            """
        else:
            # Only include teams with ELO ratings
            stmt = """
                SELECT t.team_id, t.name, t.tag, t.elo,
                       COUNT(DISTINCT m.match_id) as match_count
                FROM teams t
                LEFT JOIN matches m ON t.team_id = m.radiant_team_id OR t.team_id = m.dire_team_id
                WHERE t.elo IS NOT NULL
                GROUP BY t.team_id, t.name, t.tag, t.elo
                HAVING match_count >= :min_matches
                ORDER BY t.elo DESC, match_count DESC
            """
            
        # Try simplified query if the above fails
        if not include_no_elo:
            try:
                result = session.execute(text(stmt), {"min_matches": min_matches})
                teams = []
                for row in result:
                    teams.append({
                        "team_id": row[0],
                        "name": row[1],
                        "tag": row[2],
                        "elo": row[3],
                        "match_count": row[4]
                    })
                
                if not teams:
                    logger.info("No teams found with first query, trying simplified query")
                    # Use simpler query without match filtering
                    stmt = """
                        SELECT team_id, name, tag, elo, 0 as match_count
                        FROM teams
                        WHERE elo IS NOT NULL
                        ORDER BY elo DESC
                    """
                    result = session.execute(text(stmt))
                    for row in result:
                        teams.append({
                            "team_id": row[0],
                            "name": row[1],
                            "tag": row[2],
                            "elo": row[3],
                            "match_count": row[4]
                        })
            except Exception as e:
                logger.error(f"Error with first query: {e}")
                # Use simpler query without match filtering
                stmt = """
                    SELECT team_id, name, tag, elo, 0 as match_count
                    FROM teams
                    WHERE elo IS NOT NULL
                    ORDER BY elo DESC
                """
                result = session.execute(text(stmt))
                teams = []
                for row in result:
                    teams.append({
                        "team_id": row[0],
                        "name": row[1],
                        "tag": row[2],
                        "elo": row[3],
                        "match_count": row[4]
                    })
        else:
            result = session.execute(text(stmt), {"min_matches": min_matches})
            teams = []
            for row in result:
                teams.append({
                    "team_id": row[0],
                    "name": row[1],
                    "tag": row[2],
                    "elo": row[3],
                    "match_count": row[4]
                })
        
        logger.info(f"Retrieved {len(teams)} teams sorted by ELO rating")
        return teams
    
    except Exception as e:
        logger.error(f"Error retrieving teams: {e}")
        return []
    
    finally:
        session.close()


def display_teams(teams, output_file=None, include_no_elo=False):
    """
    Display teams in a formatted table or write to a CSV file.
    
    Args:
        teams (list): List of team data dictionaries
        output_file (str, optional): Path to output CSV file
        include_no_elo (bool): Whether to display teams without an ELO rating
    """
    if not teams:
        print("No teams found with the specified criteria.")
        return
    
    # Write to CSV file if requested
    if output_file:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Rank", "Team ID", "Name", "Tag", "ELO Rating", "Matches"])
            
            for i, team in enumerate(teams, 1):
                elo = team['elo'] if team['elo'] is not None else "N/A"
                writer.writerow([
                    i, 
                    team['team_id'], 
                    team['name'], 
                    team['tag'], 
                    elo, 
                    team['match_count']
                ])
        
        print(f"Team ELO rankings written to {output_file}")
        return
    
    # Print to console in a formatted table
    print("\nTeam Rankings by ELO Rating")
    print("-" * 80)
    print(f"{'Rank':<5} {'Team ID':<10} {'Name':<35} {'Tag':<8} {'ELO':<10} {'Matches':<8}")
    print("-" * 80)
    
    for i, team in enumerate(teams, 1):
        elo = f"{team['elo']:.2f}" if team['elo'] is not None else "N/A"
        name = team['name'] if team['name'] else "Unknown"
        tag = team['tag'] if team['tag'] else ""
        print(f"{i:<5} {team['team_id']:<10} {name[:33]:<35} {tag[:6]:<8} {elo:<10} {team['match_count']:<8}")


def main():
    """Main function to list teams by ELO rating."""
    parser = argparse.ArgumentParser(description="List teams sorted by ELO rating")
    parser.add_argument("--min-matches", type=int, default=0, help="Minimum number of matches played")
    parser.add_argument("--output", type=str, help="Output CSV file path")
    parser.add_argument("--include-no-elo", action="store_true", help="Include teams without ELO ratings")
    args = parser.parse_args()
    
    # Check if database exists
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    db_file = os.path.join(data_dir, 'dota_matches.db')
    
    if not os.path.exists(db_file):
        logger.error(f"Database file not found: {db_file}")
        print(f"Error: Database file not found at {db_file}")
        return
    
    # Get teams sorted by ELO rating
    teams = get_teams_by_elo(min_matches=args.min_matches, include_no_elo=args.include_no_elo)
    
    # Display teams
    display_teams(teams, output_file=args.output, include_no_elo=args.include_no_elo)


if __name__ == "__main__":
    main()
