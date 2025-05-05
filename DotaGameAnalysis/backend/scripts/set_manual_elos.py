#!/usr/bin/env python
"""
Set Manual ELO Ratings

This script directly sets ELO ratings for key teams in the database.
"""
import logging
import sqlite3
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ELO ratings for top teams - these are approximate values from OpenDota
TOP_TEAM_ELOS = {
    2163: 1650.0,        # Team Liquid
    7119388: 1750.0,     # Team Spirit
    8291895: 1680.0,     # Tundra Esports
    8599101: 1720.0,     # Gaimin Gladiators
    8255888: 1625.0,     # BetBoom Team
    6209804: 1600.0,     # Team Secret
    8740097: 1580.0,     # Entity
    8687717: 1550.0,     # Talon Esports
    8260983: 1535.0,     # TSM
    8605863: 1530.0,     # 9Pandas
    36: 1520.0,          # Natus Vincere
    39: 1515.0,          # Evil Geniuses
    15: 1530.0,          # PSG.LGD
    726228: 1525.0,      # Vici Gaming
    543897: 1500.0,      # Fnatic
    111471: 1490.0,      # Alliance
    350190: 1510.0,      # Virtus.pro
    67: 1505.0,          # Invictus Gaming
    6209166: 1600.0,     # OG
    8261648: 1520.0      # Shopify Rebellion
}

def set_team_elos():
    """Set ELO ratings for teams in the database."""
    # Get database path
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    db_path = os.path.join(data_dir, 'dota_matches.db')
    
    if not os.path.exists(db_path):
        logger.error(f"Database file not found: {db_path}")
        return False
    
    # Connect to database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Ensure elo column exists
        try:
            cursor.execute("SELECT elo FROM teams LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding elo column to teams table")
            cursor.execute("ALTER TABLE teams ADD COLUMN elo FLOAT")
        
        # Update ELO ratings
        updated = 0
        for team_id, elo in TOP_TEAM_ELOS.items():
            # Check if team exists
            cursor.execute("SELECT COUNT(*) FROM teams WHERE team_id = ?", (team_id,))
            if cursor.fetchone()[0] > 0:
                cursor.execute("UPDATE teams SET elo = ? WHERE team_id = ?", (elo, team_id))
                updated += 1
                logger.info(f"Updated ELO for team ID {team_id} to {elo}")
            else:
                logger.warning(f"Team ID {team_id} not found in database")
        
        # Commit changes
        conn.commit()
        
        # Verify updates
        cursor.execute("SELECT COUNT(*) FROM teams WHERE elo IS NOT NULL")
        teams_with_elo = cursor.fetchone()[0]
        logger.info(f"Teams with ELO ratings after update: {teams_with_elo}")
        
        # Display top 10 teams by ELO
        cursor.execute("""
            SELECT team_id, name, elo
            FROM teams
            WHERE elo IS NOT NULL
            ORDER BY elo DESC
            LIMIT 10
        """)
        
        print("\nTop 10 Teams by ELO Rating:")
        print("-" * 60)
        for i, (team_id, name, elo) in enumerate(cursor.fetchall(), 1):
            name = name if name else "Unknown"
            print(f"{i}. {name:<35} ELO: {elo:.2f}")
        
        return updated
        
    except Exception as e:
        logger.error(f"Error setting team ELOs: {e}")
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print("Setting manual ELO ratings for top teams...")
    updated = set_team_elos()
    
    if updated:
        print(f"\nSuccessfully updated ELO ratings for {updated} teams.")
        print("You can now run 'python list_teams_by_elo.py' to see all teams ranked by ELO.")
    else:
        print("Failed to update team ELO ratings.")
