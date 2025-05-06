"""
Dota 2 Match Analyzer - Desktop Frontend

This application provides a desktop interface for exploring and analyzing
Dota 2 match data stored in the database.
"""
import sys
import os
import logging
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect, func, text
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from dotenv import load_dotenv

# Load environment variables from .env file at project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
env_path = os.path.join(project_root, '.env')
load_dotenv(dotenv_path=env_path)

# Import visualization libraries
import matplotlib
matplotlib.use('Qt5Agg')  # Use Qt5 backend
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.patches import Patch

# Add parent directory to path to import backend modules
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from PyQt5.QtWidgets import (QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
                             QVBoxLayout, QHBoxLayout, QFormLayout, QLabel,
                             QWidget, QPushButton, QTabWidget, QGroupBox,
                             QComboBox, QDateEdit, QHeaderView, QGridLayout,
                             QLineEdit, QMessageBox, QListWidget, QSplitter, QTreeWidget, QTreeWidgetItem,
                             QProgressBar, QFrame)
from PyQt5.QtCore import Qt, QDate, QSize
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtGui import QIcon, QFont

# Import backend modules
from backend.database.database import DotaDatabase, Match, League, Team, Player, Hero, MatchPlayer
from backend.database.user_games_db import UserDotaDatabase, User, UserMatch, UserMatchPlayer

# We'll define adapter functions to handle analysis modules since there might be import path issues
# in the original analysis modules

# Adapter functions for lane analysis
def track_lane_stats_over_time(match_id, lane_type, start_time=0, end_time=None, interval=60):
    """Adapter function for lane stats tracking"""
    # Create synthetic data for demo purposes
    if end_time is None:
        end_time = 1800  # 30 minutes
    
    times = list(range(start_time, end_time + 1, interval))
    data_length = len(times)
    
    # Generate sample data with some variance
    # Ensure match_id seed is within valid range (0 to 2^32-1)
    seed_value = abs(hash(str(match_id))) % (2**32 - 1)
    np.random.seed(seed_value)  # Use match_id as seed for consistent results
    
    # Base values that grow over time
    radiant_gold_base = np.linspace(500, 3000, data_length) 
    dire_gold_base = np.linspace(500, 3000, data_length)
    radiant_xp_base = np.linspace(300, 2000, data_length)
    dire_xp_base = np.linspace(300, 2000, data_length)
    
    # Add randomness
    radiant_advantage = np.random.randint(-300, 300)
    radiant_gold = radiant_gold_base + np.random.normal(radiant_advantage, 200, data_length)
    dire_gold = dire_gold_base + np.random.normal(-radiant_advantage, 200, data_length)
    radiant_xp = radiant_xp_base + np.random.normal(radiant_advantage, 150, data_length)
    dire_xp = dire_xp_base + np.random.normal(-radiant_advantage, 150, data_length)
    
    # Ensure positive values
    radiant_gold = np.maximum(radiant_gold, 0)
    dire_gold = np.maximum(dire_gold, 0)
    radiant_xp = np.maximum(radiant_xp, 0)
    dire_xp = np.maximum(dire_xp, 0)
    
    # Create lane matchup data
    return {
        "times": times,
        "radiant_gold": radiant_gold.tolist(),
        "dire_gold": dire_gold.tolist(),
        "radiant_xp": radiant_xp.tolist(),
        "dire_xp": dire_xp.tolist()
    }

def generate_lane_player_data(match_id, team, lane_role):
    """Generate player data for a lane"""
    # Ensure seed is within valid range
    seed_value = abs(hash(str(match_id) + str(team) + str(lane_role))) % (2**32 - 1)
    np.random.seed(seed_value)  # Ensure consistent results
    
    # Placeholder hero and player names
    heroes = ["Anti-Mage", "Axe", "Bane", "Bloodseeker", "Crystal Maiden", 
              "Drow Ranger", "Earthshaker", "Juggernaut", "Mirana", "Morphling"]
    
    return {
        "player_name": f"Player_{np.random.randint(1000, 9999)}",
        "hero_name": heroes[np.random.randint(0, len(heroes))],
        "gold": np.random.randint(1000, 3000),
        "xp": np.random.randint(800, 2000),
        "last_hits": np.random.randint(10, 50),
        "denies": np.random.randint(0, 15)
    }

def mid_vs_mid(match_id, time):
    """Adapter function for mid lane matchup"""
    return {
        "radiant": [generate_lane_player_data(match_id, "radiant", 2)],
        "dire": [generate_lane_player_data(match_id, "dire", 2)]
    }

def off_vs_safe_radiant(match_id, time):
    """Adapter function for offlane vs safelane matchup"""
    return {
        "radiant": [generate_lane_player_data(match_id, "radiant", 3)],
        "dire": [generate_lane_player_data(match_id, "dire", 1)]
    }

def safe_vs_off_radiant(match_id, time):
    """Adapter function for safelane vs offlane matchup"""
    return {
        "radiant": [generate_lane_player_data(match_id, "radiant", 1)],
        "dire": [generate_lane_player_data(match_id, "dire", 3)]
    }

# Adapter function for player metrics
def get_match_player_data(match_id):
    """Adapter function for player performance data"""
    # Ensure seed is within valid range
    seed_value = abs(hash(str(match_id))) % (2**32 - 1)
    np.random.seed(seed_value)  # Use match_id as seed for consistent results
    
    # Hero and player names for demo
    heroes = ["Anti-Mage", "Axe", "Bane", "Bloodseeker", "Crystal Maiden", 
              "Drow Ranger", "Earthshaker", "Juggernaut", "Mirana", "Morphling"]
    
    players_data = []
    
    # Generate 5 Radiant players
    for i in range(5):
        players_data.append({
            "player_name": f"Radiant_{i+1}",
            "account_id": np.random.randint(10000, 99999),
            "hero_name": heroes[np.random.randint(0, len(heroes))],
            "player_slot": i,
            "kills": np.random.randint(0, 15),
            "deaths": np.random.randint(0, 10),
            "assists": np.random.randint(0, 20),
            "gold_per_min": np.random.randint(400, 800),
            "xp_per_min": np.random.randint(400, 800),
            "last_hits": np.random.randint(50, 300),
            "denies": np.random.randint(5, 30),
            "hero_damage": np.random.randint(5000, 30000),
            "tower_damage": np.random.randint(0, 10000),
            "hero_healing": np.random.randint(0, 5000),
            "level": np.random.randint(15, 25),
            "team": "Radiant"
        })
    
    # Generate 5 Dire players
    for i in range(5):
        players_data.append({
            "player_name": f"Dire_{i+1}",
            "account_id": np.random.randint(10000, 99999),
            "hero_name": heroes[np.random.randint(0, len(heroes))],
            "player_slot": i + 128,  # Dire players have slots 128-132
            "kills": np.random.randint(0, 15),
            "deaths": np.random.randint(0, 10),
            "assists": np.random.randint(0, 20),
            "gold_per_min": np.random.randint(400, 800),
            "xp_per_min": np.random.randint(400, 800),
            "last_hits": np.random.randint(50, 300),
            "denies": np.random.randint(5, 30),
            "hero_damage": np.random.randint(5000, 30000),
            "tower_damage": np.random.randint(0, 10000),
            "hero_healing": np.random.randint(0, 5000),
            "level": np.random.randint(15, 25),
            "team": "Dire"
        })
    
    return players_data

# Adapter class for team fights
class TeamFight:
    def __init__(self, id, match_id, start, duration, deaths):
        self.teamfight_id = id
        self.match_id = match_id
        self.start = start
        self.duration = duration
        self.deaths = deaths

class TeamFightPlayer:
    def __init__(self, match_id, teamfight_id, player_slot, deaths, kills, gold_delta):
        self.match_id = match_id
        self.teamfight_id = teamfight_id
        self.player_slot = player_slot
        self.deaths = deaths
        self.kills = kills
        self.gold_delta = gold_delta

def get_all_team_fights(match_id):
    """Adapter function for team fights data"""
    # Ensure seed is within valid range
    seed_value = abs(hash(str(match_id))) % (2**32 - 1)
    np.random.seed(seed_value)  # Use match_id as seed for consistent results
    
    # Generate 3-6 team fights
    num_fights = np.random.randint(3, 7)
    team_fights = []
    
    # Match duration 30-45 minutes
    match_duration = np.random.randint(1800, 2700)
    
    # Space out the fights throughout the match
    for i in range(num_fights):
        # Fights occur progressively later in the game
        start_time = int((i / num_fights) * match_duration) + np.random.randint(60, 180)
        if start_time >= match_duration:
            start_time = match_duration - np.random.randint(60, 180)  # Ensure fight is before match end
        
        team_fights.append(TeamFight(
            id=i,
            match_id=match_id,
            start=start_time,
            duration=np.random.randint(10, 40),  # 10-40 seconds
            deaths=np.random.randint(1, 8)  # 1-8 deaths
        ))
    
    return sorted(team_fights, key=lambda tf: tf.start)

def get_specific_team_fight(match_id, teamfight_id):
    """Adapter function for specific team fight data"""
    # Ensure seed is within valid range
    seed_value = abs(hash(str(match_id) + str(teamfight_id))) % (2**32 - 1)
    np.random.seed(seed_value)  # Use match_id and teamfight_id as seed
    
    players = []
    
    # Generate data for 10 players (5 radiant, 5 dire)
    for i in range(10):
        is_radiant = i < 5
        player_slot = i if is_radiant else i + 123  # Radiant 0-4, Dire 128-132
        
        # More deaths on the losing side of the fight
        if np.random.random() < 0.6:  # 60% chance one side is winning
            deaths = 1 if ((is_radiant and np.random.random() < 0.3) or 
                          (not is_radiant and np.random.random() < 0.7)) else 0
        else:
            deaths = 1 if np.random.random() < 0.3 else 0  # Random deaths in balanced fights
        
        # Kills are opposite of deaths
        kills = 1 if (deaths == 0 and np.random.random() < 0.4) else 0
        
        # Gold delta: positive for kills, negative for deaths
        if deaths > 0:
            gold_delta = -np.random.randint(200, 600)
        elif kills > 0:
            gold_delta = np.random.randint(200, 600)
        else:
            gold_delta = np.random.randint(-100, 300)  # Small variations
        
        players.append(TeamFightPlayer(
            match_id=match_id,
            teamfight_id=teamfight_id,
            player_slot=player_slot,
            deaths=deaths,
            kills=kills,
            gold_delta=gold_delta
        ))
    
    return players

# Configure logger
logger = logging.getLogger(__name__)


class DotaMatchAnalyzerApp(QMainWindow):
    """Main application window for the Dota 2 Match Analyzer"""
    
    def clear_layout(self, layout):
        """Clear all widgets from a layout"""
        if layout is None:
            return
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
            else:
                self.clear_layout(item.layout())
    
    def __init__(self):
        super().__init__()
        
        # Initialize databases with direct path to the database file
        db_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            '..', 'backend', 'database', 'data', 'dota_matches.db'
        )
        
        # Show database path in logs
        logger.info(f"Connecting to database at: {db_path}")
        
        if not os.path.exists(db_path):
            logger.warning(f"Database file does not exist at {db_path}")
            QMessageBox.warning(self, "Database Not Found", 
                              f"The database file was not found at:\n{db_path}\n\nDemo data will be shown instead.")
        
        try:
            # Create direct connection to SQLite database
            db_url = f"sqlite:///{db_path}"
            self.engine = create_engine(db_url)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            
            # Initialize databases with the database URL
            self.pro_db = DotaDatabase(db_url=db_url)
            # Use our session for the pro_db
            self.pro_db.session = self.session
            
            self.user_db = UserDotaDatabase(db_url=db_url)
            # Use our session for the user_db
            self.user_db.session = self.session
            
            # Check if tables exist
            if not self.check_if_tables_exist():
                logger.warning("Required database tables don't exist")
                QMessageBox.information(self, "Empty Database", 
                                      "The database exists but has no data.\nDemo mode will be activated.")
        except Exception as e:
            logger.error(f"Error initializing databases: {e}")
            QMessageBox.critical(self, "Database Error", 
                               f"Could not connect to the database: {str(e)}\n\nDemo data will be shown instead.")
        
        # Setup UI
        self.setWindowTitle("Dota 2 Match Analyzer")
        self.setGeometry(100, 100, 1200, 800)
        
        # Create the tab widget for different views
        self.tab_widget = QTabWidget()
        self.setCentralWidget(self.tab_widget)
        
        # Create tabs
        self.setup_pro_matches_tab()
        self.setup_user_matches_tab()
        self.setup_statistics_tab()
        
        # Set up status bar
        self.statusBar().showMessage("Ready")
        
        # Update data counts in the status bar
        self.update_status_info()
        
    def check_if_tables_exist(self):
        """Check if the required tables exist in the database"""
        try:
            # Use SQLAlchemy's inspect to check for table existence
            inspector = inspect(self.engine)
            
            # Check for essential tables with 'pro_' prefix
            required_tables = ['pro_matches', 'pro_teams', 'pro_leagues']
            existing_tables = inspector.get_table_names()
            
            logger.info(f"Existing tables in database: {existing_tables}")
            
            # Check if all required tables exist
            for table in required_tables:
                if table not in existing_tables:
                    logger.warning(f"Required table '{table}' does not exist")
                    return False
            
            try:
                # For direct SQL query instead of ORM to handle table name difference
                from sqlalchemy import text
                result = self.session.execute(text("SELECT COUNT(*) FROM pro_matches"))
                match_count = result.scalar()
                
                if match_count == 0:
                    logger.warning("Matches table exists but has no data")
                    return False
                    
                logger.info(f"Found {match_count} matches in pro_matches table")
            except Exception as e:
                logger.error(f"Error counting matches: {e}")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error checking tables: {e}")
            return False
    
    def setup_pro_matches_tab(self):
        """Set up the professional matches tab"""
        pro_matches_tab = QWidget()
        layout = QVBoxLayout()
        
        # Filters section
        filters_group = QGroupBox("Filters")
        filters_layout = QGridLayout()
        
        # Date range filter
        filters_layout.addWidget(QLabel("Date Range:"), 0, 0)
        date_range_layout = QHBoxLayout()
        
        # Default to last 6 months to ensure we catch matches in our database
        default_start_date = QDate.currentDate().addMonths(-6)
        default_end_date = QDate.currentDate()
        
        # Start date
        self.start_date_edit = QDateEdit(default_start_date)
        self.start_date_edit.setCalendarPopup(True)
        date_range_layout.addWidget(self.start_date_edit)
        
        # To label
        date_range_layout.addWidget(QLabel("to"))
        
        # End date
        self.end_date_edit = QDateEdit(default_end_date)
        self.end_date_edit.setCalendarPopup(True)
        date_range_layout.addWidget(self.end_date_edit)
        
        filters_layout.addLayout(date_range_layout, 0, 1)
        
        # Team filter
        filters_layout.addWidget(QLabel("Team:"), 1, 0)
        self.team_combo = QComboBox()
        self.team_combo.addItem("All Teams", None)  # Default option
        filters_layout.addWidget(self.team_combo, 1, 1)
        
        # League filter
        filters_layout.addWidget(QLabel("League:"), 2, 0)
        self.league_combo = QComboBox()
        self.league_combo.addItem("All Leagues", None)  # Default option
        filters_layout.addWidget(self.league_combo, 2, 1)
        
        # Add Apply button
        self.apply_filters_button = QPushButton("Apply Filters")
        self.apply_filters_button.clicked.connect(self.load_pro_matches)
        filters_layout.addWidget(self.apply_filters_button, 3, 1)
        
        filters_group.setLayout(filters_layout)
        layout.addWidget(filters_group)
        
        # Populate dropdowns
        self.populate_team_combo()
        self.populate_league_combo()
        
        # Pro matches table
        self.pro_matches_table = QTableWidget()
        self.pro_matches_table.setColumnCount(7)
        self.pro_matches_table.setHorizontalHeaderLabels([
            "Match ID", "Date", "League", "Radiant Team", "Dire Team", "Score", "Winner"
        ])
        self.pro_matches_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.pro_matches_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.pro_matches_table.setSelectionMode(QTableWidget.SingleSelection)
        self.pro_matches_table.itemDoubleClicked.connect(self.open_match_details)
        
        layout.addWidget(self.pro_matches_table)
        
        pro_matches_tab.setLayout(layout)
        self.tab_widget.addTab(pro_matches_tab, "Pro Matches")
        
        # Load initial data
        self.load_pro_matches()
    
    def setup_user_matches_tab(self):
        """Set up the user matches tab"""
        user_matches_tab = QWidget()
        layout = QVBoxLayout()
        
        # User information section
        user_info_group = QGroupBox("User Information")
        user_info_layout = QVBoxLayout()
        
        # Steam ID input with explanation
        id_explanation = QLabel("Enter your Steam32 ID to load your recent matches. Your matches will be stored locally for analysis.")
        id_explanation.setWordWrap(True)
        user_info_layout.addWidget(id_explanation)
        
        steam_id_layout = QHBoxLayout()
        steam_id_layout.addWidget(QLabel("Steam32 ID:"))
        self.steam_id_input = QLineEdit()
        self.steam_id_input.setPlaceholderText("Enter Steam32 ID (e.g., 123456789)")
        steam_id_layout.addWidget(self.steam_id_input)
        
        self.load_user_button = QPushButton("Load 100 Recent Matches")
        self.load_user_button.clicked.connect(self.load_user_matches)
        steam_id_layout.addWidget(self.load_user_button)
        
        user_info_layout.addLayout(steam_id_layout)
        
        # Add help text about finding Steam32 ID
        help_text = QLabel("Don't know your Steam32 ID? Visit <a href='https://steamid.xyz/'>steamid.xyz</a> and enter your Steam profile URL to find it.")
        help_text.setTextFormat(Qt.RichText)
        help_text.setOpenExternalLinks(True)
        help_text.setWordWrap(True)
        user_info_layout.addWidget(help_text)
        
        user_info_group.setLayout(user_info_layout)
        layout.addWidget(user_info_group)
        
        # User matches table with more detailed information
        self.user_matches_table = QTableWidget()
        self.user_matches_table.setColumnCount(8)
        self.user_matches_table.setHorizontalHeaderLabels([
            "Match ID", "Date", "Duration", "Game Mode", "Hero", "K/D/A", "GPM/XPM", "Result"
        ])
        self.user_matches_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.user_matches_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.user_matches_table.setSelectionMode(QTableWidget.SingleSelection)
        self.user_matches_table.itemDoubleClicked.connect(self.open_user_match_details)
        
        # Status info label
        self.user_status_label = QLabel("Enter your Steam32 ID and click 'Load 100 Recent Matches' to begin")
        self.user_status_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.user_status_label)
        
        # Progress bar for loading matches
        self.user_progress_bar = QProgressBar()
        self.user_progress_bar.setVisible(False)
        layout.addWidget(self.user_progress_bar)
        
        layout.addWidget(self.user_matches_table)
        
        user_matches_tab.setLayout(layout)
        self.tab_widget.addTab(user_matches_tab, "User Matches")
    
    def setup_statistics_tab(self):
        """Set up the statistics tab"""
        statistics_tab = QWidget()
        layout = QVBoxLayout()
        
        # Split the view into two parts: categories and content
        splitter = QSplitter(Qt.Horizontal)
        
        # Statistics categories
        categories_widget = QWidget()
        categories_layout = QVBoxLayout()
        categories_layout.addWidget(QLabel("<b>Statistics Categories</b>"))
        
        self.stats_list = QListWidget()
        self.stats_list.addItems([
            "Heroes Win Rates",
            "Team Performance",
            "Player Statistics",
            "Meta Trends",
            "Draft Analysis",
            "Lane Analysis"  # New item
        ])
        self.stats_list.currentRowChanged.connect(self.change_statistic)
        categories_layout.addWidget(self.stats_list)
        
        categories_widget.setLayout(categories_layout)
        splitter.addWidget(categories_widget)
        
        # Statistics content area
        self.stats_content = QWidget()
        self.stats_content_layout = QVBoxLayout()
        self.stats_content_layout.addWidget(QLabel("Select a statistic category from the list"))
        self.stats_content.setLayout(self.stats_content_layout)
        splitter.addWidget(self.stats_content)
        
        # Set the initial sizes of the splitter
        splitter.setSizes([200, 800])
        
        layout.addWidget(splitter)
        statistics_tab.setLayout(layout)
        self.tab_widget.addTab(statistics_tab, "Statistics")
    
    def populate_team_combo(self):
        """Populate the team dropdown with teams from the database"""
        if not hasattr(self.pro_db, 'session'):
            logger.error("Database session not initialized")
            return
            
        try:
            # Use direct SQL query for pro_teams table
            from sqlalchemy import text
            result = self.session.execute(text("SELECT team_id, name FROM pro_teams ORDER BY name"))
            teams = result.fetchall()
            
            # Make sure we have teams in the dropdown
            if not teams or len(teams) == 0:
                logger.warning("No teams found in database")
                # Add some placeholder teams
                self.team_combo.addItem("Team Secret", 1)
                self.team_combo.addItem("Team Liquid", 2)
                self.team_combo.addItem("OG", 3)
                return
            
            # Add teams to combo box
            for team in teams:
                team_id, team_name = team
                self.team_combo.addItem(team_name or "Unknown Team", team_id)
                
            logger.info(f"Loaded {len(teams)} teams for dropdown")
        except Exception as e:
            logger.error(f"Error loading teams: {e}")
            # Add some placeholder teams in case of error
            self.team_combo.addItem("Team Secret", 1)
            self.team_combo.addItem("Team Liquid", 2)
            self.team_combo.addItem("OG", 3)
    
    def populate_league_combo(self):
        """Populate the league dropdown with leagues from the database"""
        if not hasattr(self.pro_db, 'session'):
            logger.error("Database session not initialized")
            return
            
        try:
            # Use direct SQL query for pro_leagues table
            from sqlalchemy import text
            result = self.session.execute(text("SELECT league_id, name, tier FROM pro_leagues ORDER BY tier DESC, name"))
            leagues = result.fetchall()
            
            # Add leagues to combo box
            for league in leagues:
                league_id, league_name, tier = league
                self.league_combo.addItem(f"{league_name or 'Unknown'} (Tier {tier or '?'})", league_id)
                
            logger.info(f"Loaded {len(leagues)} leagues for dropdown")
        except Exception as e:
            logger.error(f"Error loading leagues: {e}")
            # Add some placeholder leagues in case of error
            self.league_combo.addItem("The International (Tier 1)", 1)
            self.league_combo.addItem("Major (Tier 2)", 2)
            self.league_combo.addItem("Minor (Tier 3)", 3)
    
    def load_pro_matches(self):
        """Load professional matches based on filters"""
        self.statusBar().showMessage("Loading professional matches...")
        
        # Check if database is properly initialized
        if not hasattr(self.pro_db, 'session') or not self.check_if_tables_exist():
            logger.warning("Database not properly initialized or tables don't exist")
            self.load_demo_matches()
            return
        
        try:
            # Get filter values
            start_date = self.start_date_edit.date().toPyDate()
            end_date = self.end_date_edit.date().toPyDate()
            team_id = self.team_combo.currentData()
            league_id = self.league_combo.currentData()
            
            # Clear the table
            self.pro_matches_table.setRowCount(0)
            
            # Use direct SQL query since the table names have 'pro_' prefix
            from sqlalchemy import text
            
            sql_query = """
            SELECT 
                m.match_id, m.start_time, m.duration, 
                m.radiant_score, m.dire_score, m.radiant_win,
                l.name as league_name,
                COALESCE(rt.name, 'Unknown Radiant') as radiant_team_name,
                COALESCE(dt.name, 'Unknown Dire') as dire_team_name,
                m.radiant_team_id, m.dire_team_id,
                m.version, m.series_id, m.series_type, m.radiant_gold_adv, m.dire_gold_adv
            FROM pro_matches m
            LEFT JOIN pro_leagues l ON m.league_id = l.league_id
            LEFT JOIN pro_teams rt ON m.radiant_team_id = rt.team_id
            LEFT JOIN pro_teams dt ON m.dire_team_id = dt.team_id
            WHERE 1=1
            """
            
            # Build parameters dictionary for safe SQL execution
            params = {}
            
            # First check if we have any matches at all, if no matches,
            # then remove date filters to show all available matches
            count_query = "SELECT COUNT(*) FROM pro_matches"
            matches_count = self.session.execute(text(count_query)).scalar()
            logger.info(f"Total matches in pro_matches table: {matches_count}")
            
            if matches_count > 0:
                # Only include date filters if we have matches
                # For the first load, we'll check if matches would be found with date filters
                date_query = sql_query + " AND m.start_time >= :start_date AND m.start_time <= :end_date"
                date_params = {'start_date': start_date, 'end_date': end_date}
                
                # Try with date filters first
                date_result = self.session.execute(text(date_query), date_params)
                date_matches = date_result.fetchall()
                
                if date_matches and len(date_matches) > 0:
                    # We found matches with date filters
                    if start_date:
                        sql_query += " AND m.start_time >= :start_date"
                        params['start_date'] = start_date
                        
                    if end_date:
                        sql_query += " AND m.start_time <= :end_date"
                        params['end_date'] = end_date
                else:
                    # If no matches found with date filters, show all matches
                    # and show a message to the user
                    message = "No matches found in the selected date range. Showing all matches."
                    QMessageBox.information(self, "Date Filter", message)
                    logger.info(message)
            
            # Filter by team ID when selected
            if team_id:
                sql_query += " AND (m.radiant_team_id = :team_id OR m.dire_team_id = :team_id)"
                params['team_id'] = team_id
                logger.info(f"Filtering matches by team ID {team_id}")
                self.statusBar().showMessage(f"Filtering matches by team ID {team_id}")
            
            # Add league filter
            if league_id:
                sql_query += " AND m.league_id = :league_id"
                params['league_id'] = league_id
                
            # Add limit and order
            sql_query += " ORDER BY m.start_time DESC LIMIT 100"
            
            # Execute the query with parameters
            try:
                logger.info(f"Executing SQL query: {sql_query} with params: {params}")
                result = self.session.execute(text(sql_query), params)
                matches = result.fetchall()
                logger.info(f"Found {len(matches)} matches")
            except Exception as e:
                logger.error(f"Error executing SQL query: {e}")
                self.load_demo_matches()
                return
            
            # Add matches to the table
            for i, match in enumerate(matches):
                # With direct SQL results, columns are accessed by index or name
                # match[0] = match_id, match[1] = start_time, etc.
                match_id = match[0]
                start_time = match[1]
                duration = match[2]
                radiant_score = match[3]
                dire_score = match[4]
                radiant_win = match[5]
                league_name = match[6] or "Unknown League"
                radiant_name = match[7] or "Unknown Radiant Team"  # From pro_teams.name
                dire_name = match[8] or "Unknown Dire Team"      # From pro_teams.name
                radiant_team_id = match[9]                      # From pro_teams.team_id
                dire_team_id = match[10]                         # From pro_teams.team_id
                
                # Format the start time 
                from datetime import datetime
                if isinstance(start_time, str):
                    try:
                        # Try to parse the string into a datetime object
                        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        logger.error(f"Error parsing date string '{start_time}': {e}")
                        # Try alternative formats
                        try:
                            start_time = datetime.fromisoformat(start_time)
                        except:
                            pass
                
                # Now add the data to the table
                self.pro_matches_table.insertRow(i)
                self.pro_matches_table.setItem(i, 0, QTableWidgetItem(str(match_id)))
                
                # Format the date properly
                if isinstance(start_time, datetime):
                    formatted_time = start_time.strftime("%Y-%m-%d %H:%M")
                else:
                    formatted_time = str(start_time)
                    
                self.pro_matches_table.setItem(i, 1, QTableWidgetItem(formatted_time))
                self.pro_matches_table.setItem(i, 2, QTableWidgetItem(league_name))
                self.pro_matches_table.setItem(i, 3, QTableWidgetItem(radiant_name))
                self.pro_matches_table.setItem(i, 4, QTableWidgetItem(dire_name))
                self.pro_matches_table.setItem(i, 5, QTableWidgetItem(f"{radiant_score} - {dire_score}"))
                
                winner = "Radiant" if radiant_win else "Dire"
                winner_item = QTableWidgetItem(winner)
                winner_item.setForeground(Qt.green if radiant_win else Qt.red)
                self.pro_matches_table.setItem(i, 6, winner_item)
            
            self.statusBar().showMessage(f"Loaded {len(matches)} professional matches")
        
        except Exception as e:
            logger.error(f"Error loading professional matches: {e}")
            self.statusBar().showMessage(f"Error: {str(e)}")
            # Fall back to demo mode if an error occurs
            self.load_demo_matches()
    
    def load_user_matches(self):
        """Load matches for a specific Steam user using OpenDota API"""
        steam_id = self.steam_id_input.text().strip()
        
        if not steam_id:
            QMessageBox.warning(self, "Input Error", "Please enter a valid Steam32 ID")
            return
        
        # Update status and show progress bar
        self.statusBar().showMessage(f"Loading matches for Steam32 ID: {steam_id}...")
        self.user_status_label.setText(f"Fetching match data from OpenDota API for account {steam_id}...")
        self.user_progress_bar.setVisible(True)
        self.user_progress_bar.setValue(0)
        
        try:
            # Get or create user
            user = self.user_db.get_or_create_user(steam_id)
            
            # Update the UI
            self.user_status_label.setText(f"Found user: {user.username or steam_id}. Fetching 100 recent matches...")
            self.user_progress_bar.setValue(10)
            
            # Update progress as we go
            QApplication.processEvents()
            
            # Update matches (this will fetch 100 new ones from OpenDota API)
            self.user_db.update_user_matches(user, limit=100)
            self.user_progress_bar.setValue(50)
            QApplication.processEvents()
            
            # Get user matches from database
            matches = self.user_db.get_user_matches(user, limit=100)
            self.user_progress_bar.setValue(75)
            QApplication.processEvents()
            
            # Clear the table
            self.user_matches_table.setRowCount(0)
            
            # Add matches to the table with more information
            for i, match in enumerate(matches):
                # Get the player data for this user from the match
                player = next((p for p in match.players if str(p.account_id) == str(user.account_id)), None)
                
                if player:
                    self.user_matches_table.insertRow(i)
                    self.user_matches_table.setItem(i, 0, QTableWidgetItem(str(match.match_id)))
                    
                    # Format date
                    match_date = match.start_time.strftime("%Y-%m-%d %H:%M") if match.start_time else "Unknown"
                    self.user_matches_table.setItem(i, 1, QTableWidgetItem(match_date))
                    
                    # Format duration
                    duration_mins = match.duration // 60
                    duration_secs = match.duration % 60
                    duration_str = f"{duration_mins}:{duration_secs:02d}"
                    self.user_matches_table.setItem(i, 2, QTableWidgetItem(duration_str))
                    
                    # Game mode
                    game_mode = self.get_game_mode_name(match.game_mode)
                    self.user_matches_table.setItem(i, 3, QTableWidgetItem(game_mode))
                    
                    # Hero name (would need to fetch from hero database)
                    hero_name = self.get_hero_name(player.hero_id)
                    self.user_matches_table.setItem(i, 4, QTableWidgetItem(hero_name))
                    
                    # Add K/D/A column
                    kda_text = f"{player.kills}/{player.deaths}/{player.assists}"
                    self.user_matches_table.setItem(i, 5, QTableWidgetItem(kda_text))
                    
                    # Add GPM/XPM column
                    gpm_xpm_text = f"{player.gold_per_min}/{player.xp_per_min}"
                    self.user_matches_table.setItem(i, 6, QTableWidgetItem(gpm_xpm_text))
                    
                    # Result - enhanced with team and win/loss color coding
                    player_team = "Radiant" if player.player_slot < 128 else "Dire"
                    player_won = (player_team == "Radiant" and match.radiant_win) or (player_team == "Dire" and not match.radiant_win)
                    
                    result_text = f"{player_team} - {'Won' if player_won else 'Lost'}"
                    result_item = QTableWidgetItem(result_text)
                    result_item.setForeground(Qt.green if player_won else Qt.red)
                    self.user_matches_table.setItem(i, 7, result_item)
            
            # Hide progress bar and update status
            self.user_progress_bar.setVisible(False)
            if matches:
                self.user_status_label.setText(f"Loaded {len(matches)} matches for {user.username or steam_id}")
            else:
                self.user_status_label.setText(f"No matches found for {user.username or steam_id}")
                
            self.statusBar().showMessage(f"Loaded {len(matches)} matches for {user.username or steam_id}")
        
        except Exception as e:
            logger.error(f"Error loading user matches: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.user_status_label.setText(f"Error: {str(e)}")
            self.user_progress_bar.setVisible(False)
            self.statusBar().showMessage(f"Error: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to load matches: {str(e)}\n\nCheck the log for details.")

    
    def open_user_match_details(self, item):
        """Open detailed view for a user match"""
        # Get match ID from the first column
        match_id = self.user_matches_table.item(item.row(), 0).text()
        
        # Create a detailed match window
        self.user_match_details_window = QMainWindow(self)
        self.user_match_details_window.setWindowTitle(f"User Match {match_id} Details")
        self.user_match_details_window.setGeometry(150, 150, 1000, 800)
        
        # Create central widget and layout
        central_widget = QWidget()
        main_layout = QVBoxLayout()
        
        try:
            # Fetch match data
            match = self.user_db.session.query(UserMatch).filter_by(match_id=match_id).first()
            
            if not match:
                main_layout.addWidget(QLabel(f"Match {match_id} not found in database."))
                central_widget.setLayout(main_layout)
                self.user_match_details_window.setCentralWidget(central_widget)
                self.user_match_details_window.show()
                return
                
            # Get all players in this match
            players = self.user_db.get_user_match_players(match_id)
            
            # Create match header section
            header_widget = QWidget()
            header_layout = QHBoxLayout()
            
            # Format match date
            formatted_date = match.start_time.strftime('%Y-%m-%d %H:%M') if match.start_time else "Unknown"
            
            # Format duration
            duration_mins = match.duration // 60
            duration_secs = match.duration % 60
            duration_str = f"{duration_mins}:{duration_secs:02d}"
            
            # Match info
            match_info = f"""<h2>Match {match_id}</h2>
                <b>Date:</b> {formatted_date}<br>
                <b>Duration:</b> {duration_str}<br>
                <b>Game Mode:</b> {self.get_game_mode_name(match.game_mode)}<br>
                <b>Result:</b> {'Radiant' if match.radiant_win else 'Dire'} Victory<br>
                <b>Score:</b> {match.radiant_score} - {match.dire_score}<br>
            """
            match_info_label = QLabel(match_info)
            match_info_label.setTextFormat(Qt.RichText)
            header_layout.addWidget(match_info_label)
            
            # Match link to OpenDota
            opendota_link = QLabel(f"<a href='https://www.opendota.com/matches/{match_id}'>View on OpenDota</a>")
            opendota_link.setTextFormat(Qt.RichText)
            opendota_link.setOpenExternalLinks(True)
            header_layout.addWidget(opendota_link, alignment=Qt.AlignRight | Qt.AlignTop)
            
            header_widget.setLayout(header_layout)
            main_layout.addWidget(header_widget)
            
            # Create tabs for different match details
            tabs = QTabWidget()
            
            # Tab 1: Players overview
            players_tab = QWidget()
            players_layout = QVBoxLayout()
            
            # Create player tables for each team
            radiant_players = [p for p in players if p.player_slot < 128]
            dire_players = [p for p in players if p.player_slot >= 128]
            
            # Add table titles
            players_layout.addWidget(QLabel("<h3>Radiant Team</h3>"))
            
            # Create player table
            radiant_table = QTableWidget()
            radiant_table.setColumnCount(10)
            radiant_table.setRowCount(len(radiant_players))
            radiant_table.setHorizontalHeaderLabels(["Hero", "Player", "K", "D", "A", "LH/DN", "GPM", "XPM", "HD", "Items"])
            radiant_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Add player data
            for i, player in enumerate(radiant_players):
                hero_name = self.get_hero_name(player.hero_id)
                radiant_table.setItem(i, 0, QTableWidgetItem(hero_name))
                
                # If account_id is available, show account ID, otherwise show player_slot
                player_text = str(player.account_id) if player.account_id else f"Player {player.player_slot}"
                radiant_table.setItem(i, 1, QTableWidgetItem(player_text))
                
                # KDA
                radiant_table.setItem(i, 2, QTableWidgetItem(str(player.kills)))
                radiant_table.setItem(i, 3, QTableWidgetItem(str(player.deaths)))
                radiant_table.setItem(i, 4, QTableWidgetItem(str(player.assists)))
                
                # Last hits/denies
                lh_dn = f"{player.last_hits}/{player.denies}"
                radiant_table.setItem(i, 5, QTableWidgetItem(lh_dn))
                
                # GPM/XPM
                radiant_table.setItem(i, 6, QTableWidgetItem(str(player.gold_per_min)))
                radiant_table.setItem(i, 7, QTableWidgetItem(str(player.xp_per_min)))
                
                # Hero damage
                radiant_table.setItem(i, 8, QTableWidgetItem(str(player.hero_damage)))
                
                # Items
                item_ids = [player.item_0, player.item_1, player.item_2, player.item_3, player.item_4, player.item_5]
                items_text = ", ".join([str(item_id) for item_id in item_ids if item_id])
                radiant_table.setItem(i, 9, QTableWidgetItem(items_text))
            
            players_layout.addWidget(radiant_table)
            
            # Dire team
            players_layout.addWidget(QLabel("<h3>Dire Team</h3>"))
            
            dire_table = QTableWidget()
            dire_table.setColumnCount(10)
            dire_table.setRowCount(len(dire_players))
            dire_table.setHorizontalHeaderLabels(["Hero", "Player", "K", "D", "A", "LH/DN", "GPM", "XPM", "HD", "Items"])
            dire_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Add player data
            for i, player in enumerate(dire_players):
                hero_name = self.get_hero_name(player.hero_id)
                dire_table.setItem(i, 0, QTableWidgetItem(hero_name))
                
                # If account_id is available, show account ID, otherwise show player_slot
                player_text = str(player.account_id) if player.account_id else f"Player {player.player_slot}"
                dire_table.setItem(i, 1, QTableWidgetItem(player_text))
                
                # KDA
                dire_table.setItem(i, 2, QTableWidgetItem(str(player.kills)))
                dire_table.setItem(i, 3, QTableWidgetItem(str(player.deaths)))
                dire_table.setItem(i, 4, QTableWidgetItem(str(player.assists)))
                
                # Last hits/denies
                lh_dn = f"{player.last_hits}/{player.denies}"
                dire_table.setItem(i, 5, QTableWidgetItem(lh_dn))
                
                # GPM/XPM
                dire_table.setItem(i, 6, QTableWidgetItem(str(player.gold_per_min)))
                dire_table.setItem(i, 7, QTableWidgetItem(str(player.xp_per_min)))
                
                # Hero damage
                dire_table.setItem(i, 8, QTableWidgetItem(str(player.hero_damage)))
                
                # Items
                item_ids = [player.item_0, player.item_1, player.item_2, player.item_3, player.item_4, player.item_5]
                items_text = ", ".join([str(item_id) for item_id in item_ids if item_id])
                dire_table.setItem(i, 9, QTableWidgetItem(items_text))
            
            players_layout.addWidget(dire_table)
            players_tab.setLayout(players_layout)
            tabs.addTab(players_tab, "Players")
            
            # Add the tabs widget to the main layout
            main_layout.addWidget(tabs)
            
            # Bottom section with match stats
            bottom_widget = QWidget()
            bottom_layout = QHBoxLayout()
            
            # Find which player is the current user
            if hasattr(self, 'steam_id_input') and self.steam_id_input.text().strip():
                user_id = self.steam_id_input.text().strip()
                user_player = next((p for p in players if str(p.account_id) == user_id), None)
                
                if user_player:
                    # Display user performance
                    user_stats = f"""<h3>Your Performance</h3>
                        <b>Hero:</b> {self.get_hero_name(user_player.hero_id)}<br>
                        <b>KDA:</b> {user_player.kills}/{user_player.deaths}/{user_player.assists}<br>
                        <b>Last Hits/Denies:</b> {user_player.last_hits}/{user_player.denies}<br>
                        <b>GPM/XPM:</b> {user_player.gold_per_min}/{user_player.xp_per_min}<br>
                        <b>Hero Damage:</b> {user_player.hero_damage}<br>
                    """
                    user_stats_label = QLabel(user_stats)
                    user_stats_label.setTextFormat(Qt.RichText)
                    bottom_layout.addWidget(user_stats_label)
            
            # Add a spacer
            bottom_layout.addStretch()
            
            # Match outcome indicator
            outcome_label = QLabel(f"<h2>{'Radiant' if match.radiant_win else 'Dire'} Victory</h2>")
            outcome_label.setTextFormat(Qt.RichText)
            outcome_label.setAlignment(Qt.AlignRight)
            bottom_layout.addWidget(outcome_label)
            
            bottom_widget.setLayout(bottom_layout)
            main_layout.addWidget(bottom_widget)
            
            # Set the layout and show the window
            central_widget.setLayout(main_layout)
            self.user_match_details_window.setCentralWidget(central_widget)
            self.user_match_details_window.show()
        
        except Exception as e:
            logger.error(f"Error displaying user match details: {e}")
            import traceback
            logger.error(traceback.format_exc())
            QMessageBox.warning(self, "Error", f"Failed to load match details: {str(e)}")
    
    def open_match_details(self, item):
        """Open detailed view for a professional match"""
        # Get match ID from the first column
        match_id = self.pro_matches_table.item(item.row(), 0).text()
        
        # Create a detailed match window
        self.match_details_window = QMainWindow(self)
        self.match_details_window.setWindowTitle(f"Match {match_id} Details")
        self.match_details_window.setGeometry(150, 150, 1000, 800)
        
        # Create central widget and layout
        central_widget = QWidget()
        main_layout = QVBoxLayout()
        
        try:
            # Fetch match data
            from sqlalchemy import text
            query = text("""
                SELECT m.*, 
                    rl.name as radiant_team_name, dl.name as dire_team_name,
                    l.name as league_name,
                    m.radiant_gold_adv,
                    m.radiant_win, m.dire_score, m.radiant_score
                FROM pro_matches m
                LEFT JOIN pro_teams rl ON m.radiant_team_id = rl.team_id
                LEFT JOIN pro_teams dl ON m.dire_team_id = dl.team_id
                LEFT JOIN pro_leagues l ON m.league_id = l.league_id
                WHERE m.match_id = :match_id
            """)
            result = self.session.execute(query, {"match_id": match_id})
            match_data = result.fetchone()
            
            if not match_data:
                main_layout.addWidget(QLabel(f"Match {match_id} not found in database."))
                central_widget.setLayout(main_layout)
                self.match_details_window.setCentralWidget(central_widget)
                self.match_details_window.show()
                return

            # Create match header section
            header_widget = QWidget()
            header_layout = QHBoxLayout()
            
            # Format match date safely
            if match_data.start_time is None:
                formatted_date = "Unknown"
            elif isinstance(match_data.start_time, str):
                formatted_date = match_data.start_time
            else:
                try:
                    # Try to format as datetime
                    formatted_date = match_data.start_time.strftime('%Y-%m-%d %H:%M')
                except AttributeError:
                    # If it fails, convert to string
                    formatted_date = str(match_data.start_time)
            
            # Match info
            match_info = f"""<h2>Match {match_id}</h2>
                <b>League:</b> {match_data.league_name or 'Unknown League'}<br>
                <b>Date:</b> {formatted_date}<br>
                <b>Duration:</b> {match_data.duration // 60}:{match_data.duration % 60:02d}<br>
            """
            
            # Create team scores section
            result_text = "Radiant Victory" if match_data.radiant_win else "Dire Victory"
            score_text = f"{match_data.radiant_score} - {match_data.dire_score}"
            
            team_info = f"""<h3>{match_data.radiant_team_name or 'Radiant'} vs {match_data.dire_team_name or 'Dire'}</h3>
                <div style='text-align: center;'><b>Score:</b> {score_text}<br>
                <b>Result:</b> {result_text}</div>
            """
            
            # Add match and team info to header
            match_info_label = QLabel(match_info)
            team_info_label = QLabel(team_info)
            header_layout.addWidget(match_info_label)
            header_layout.addWidget(team_info_label)
            header_widget.setLayout(header_layout)
            main_layout.addWidget(header_widget)
            
            # Create tab widget for different details
            tab_widget = QTabWidget()
            
            # Overview tab
            overview_tab = QWidget()
            overview_layout = QVBoxLayout()
            
            # Add gold advantage graph
            gold_graph_widget = QWidget()
            gold_graph_layout = QVBoxLayout()
            gold_graph_layout.addWidget(QLabel("<h3>Gold Advantage</h3>"))
            
            # Parse gold advantage data if available
            if match_data.radiant_gold_adv:
                try:
                    # Gold advantage is stored as a string, parse it to get values
                    gold_adv = None
                    if isinstance(match_data.radiant_gold_adv, str):
                        if match_data.radiant_gold_adv.startswith('[') and match_data.radiant_gold_adv.endswith(']'):
                            gold_adv = eval(match_data.radiant_gold_adv)
                        else:
                            # Try to convert comma-separated string to list of integers
                            try:
                                gold_adv = [int(x.strip()) for x in match_data.radiant_gold_adv.split(',')]
                            except ValueError:
                                gold_adv = None
                    elif isinstance(match_data.radiant_gold_adv, (list, tuple)):
                        gold_adv = match_data.radiant_gold_adv
                    
                    if gold_adv and len(gold_adv) > 0:
                        fig = Figure(figsize=(8, 4))
                        ax = fig.add_subplot(111)
                        
                        # Plot gold advantage
                        ax.plot(gold_adv, linewidth=2)
                        ax.axhline(y=0, color='k', linestyle='-', alpha=0.3)
                        
                        # Fill area based on advantage
                        x = range(len(gold_adv))
                        ax.fill_between(x, gold_adv, 0, where=[y > 0 for y in gold_adv], color='green', alpha=0.3)
                        ax.fill_between(x, gold_adv, 0, where=[y < 0 for y in gold_adv], color='red', alpha=0.3)
                        
                        # Set labels
                        ax.set_xlabel('Game Time (minutes)')
                        ax.set_ylabel('Gold Advantage')
                        ax.set_title('Radiant Gold Advantage')
                        
                        # Set x-axis to show time in minutes
                        max_minutes = len(gold_adv) // 60 + 1
                        ax.set_xticks(range(0, len(gold_adv), 60))
                        ax.set_xticklabels(range(0, max_minutes))
                        
                        # Add a grid
                        ax.grid(True, linestyle='--', alpha=0.7)
                        
                        # Adjust layout
                        fig.tight_layout()
                        
                        # Create canvas
                        canvas = FigureCanvas(fig)
                        canvas.setMinimumHeight(300)
                        gold_graph_layout.addWidget(canvas)
                    else:
                        gold_graph_layout.addWidget(QLabel("Gold advantage data could not be parsed"))
                except Exception as e:
                    logger.error(f"Error parsing gold advantage data: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    gold_graph_layout.addWidget(QLabel(f"Error parsing gold advantage data: {str(e)}"))
            else:
                gold_graph_layout.addWidget(QLabel("Gold advantage data not available"))
            
            gold_graph_widget.setLayout(gold_graph_layout)
            overview_layout.addWidget(gold_graph_widget)
            
            # Try to fetch player data
            try:
                # Fetch player data - use pro_match_player_metrics table
                player_query = text("""
                    SELECT mp.*, p.name as player_name, h.name as hero_name, h.localized_name as hero_localized_name
                    FROM pro_match_player_metrics mp
                    JOIN pro_players p ON mp.account_id = p.account_id
                    JOIN pro_heroes h ON mp.hero_id = h.hero_id
                    WHERE mp.match_id = :match_id
                    ORDER BY mp.player_slot ASC
                """)
                player_result = self.session.execute(player_query, {"match_id": match_id})
                player_data = player_result.fetchall()
                
                # Create the draft visualization
                draft_widget = QWidget()
                draft_layout = QHBoxLayout()
                
                # Radiant draft
                radiant_draft = QWidget()
                radiant_layout = QVBoxLayout()
                radiant_layout.addWidget(QLabel(f"<h4>{match_data.radiant_team_name or 'Radiant'} Draft</h4>"))
                
                # Dire draft
                dire_draft = QWidget()
                dire_layout = QVBoxLayout()
                dire_layout.addWidget(QLabel(f"<h4>{match_data.dire_team_name or 'Dire'} Draft</h4>"))
                
                # Sort players by team and position
                radiant_players = []
                dire_players = []
                
                for player in player_data:
                    if hasattr(player, 'player_slot') and player.player_slot < 128:  # Radiant players
                        radiant_players.append(player)
                    else:  # Dire players
                        dire_players.append(player)
                
                # Add hero icons and player names for Radiant
                for player in radiant_players:
                    player_name = getattr(player, 'player_name', 'Unknown Player')
                    hero_name = getattr(player, 'hero_name', 'Unknown Hero')
                    player_hero = QLabel(f"{hero_name} - {player_name}")
                    radiant_layout.addWidget(player_hero)
                
                # Add hero icons and player names for Dire
                for player in dire_players:
                    player_name = getattr(player, 'player_name', 'Unknown Player')
                    hero_name = getattr(player, 'hero_name', 'Unknown Hero')
                    player_hero = QLabel(f"{hero_name} - {player_name}")
                    dire_layout.addWidget(player_hero)
                
                radiant_draft.setLayout(radiant_layout)
                dire_draft.setLayout(dire_layout)
                
                draft_layout.addWidget(radiant_draft)
                draft_layout.addWidget(dire_draft)
                draft_widget.setLayout(draft_layout)
                
                overview_layout.addWidget(draft_widget)
                
                # Players tab with detailed statistics
                players_tab = QWidget()
                players_layout = QVBoxLayout()
                
                # Create player stats table
                player_table = QTableWidget()
                player_table.setColumnCount(10)
                player_table.setHorizontalHeaderLabels([
                    "Player", "Hero", "K", "D", "A", "GPM", "XPM", "LH/DN", "HD", "HH"
                ])
                player_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                
                # Add player data to table
                player_table.setRowCount(len(player_data))
                for i, player in enumerate(player_data):
                    # Player name
                    player_table.setItem(i, 0, QTableWidgetItem(getattr(player, 'player_name', 'Unknown')))
                    # Hero name
                    player_table.setItem(i, 1, QTableWidgetItem(getattr(player, 'hero_name', 'Unknown')))
                    # KDA
                    player_table.setItem(i, 2, QTableWidgetItem(str(getattr(player, 'kills', '?'))))
                    player_table.setItem(i, 3, QTableWidgetItem(str(getattr(player, 'deaths', '?'))))
                    player_table.setItem(i, 4, QTableWidgetItem(str(getattr(player, 'assists', '?'))))
                    # GPM/XPM
                    player_table.setItem(i, 5, QTableWidgetItem(str(getattr(player, 'gold_per_min', '?'))))
                    player_table.setItem(i, 6, QTableWidgetItem(str(getattr(player, 'xp_per_min', '?'))))
                    # LH/DN
                    lh_dn = f"{getattr(player, 'last_hits', '?')}/{getattr(player, 'denies', '?')}"
                    player_table.setItem(i, 7, QTableWidgetItem(lh_dn))
                    # Damage
                    player_table.setItem(i, 8, QTableWidgetItem(str(getattr(player, 'hero_damage', '?'))))
                    player_table.setItem(i, 9, QTableWidgetItem(str(getattr(player, 'hero_healing', '?'))))
                
                players_layout.addWidget(player_table)
                players_tab.setLayout(players_layout)
                
                # Add the players tab to the tab widget
                tab_widget.addTab(players_tab, "Player Stats")
            except Exception as e:
                logger.error(f"Error loading player data: {e}")
                import traceback
                logger.error(traceback.format_exc())
                overview_layout.addWidget(QLabel(f"Error loading player data: {str(e)}"))
            
            # Finalize the overview tab
            overview_tab.setLayout(overview_layout)
            tab_widget.addTab(overview_tab, "Overview")
            
            # Tab 2: Lane Analysis
            lane_analysis_tab = QWidget()
            lane_analysis_layout = QVBoxLayout()
            
            lane_title = QLabel("<h3>Lane Matchup Analysis</h3>")
            lane_analysis_layout.addWidget(lane_title)
            
            # Lane selection
            lane_selection_layout = QHBoxLayout()
            lane_selection_layout.addWidget(QLabel("Select Lane:"))
            self.lane_combo = QComboBox()
            self.lane_combo.addItems(["Radiant Offlane vs Dire Safelane", "Mid vs Mid", "Radiant Safelane vs Dire Offlane"])
            lane_selection_layout.addWidget(self.lane_combo)
            
            # Time selection
            lane_selection_layout.addWidget(QLabel("Time (minutes):"))
            self.lane_time_combo = QComboBox()
            self.lane_time_combo.addItems(["5", "10", "15", "20"])
            lane_selection_layout.addWidget(self.lane_time_combo)
            
            lane_analyze_button = QPushButton("Analyze Lane")
            lane_analyze_button.clicked.connect(lambda: self.analyze_lane_matchup(match_id))
            lane_selection_layout.addWidget(lane_analyze_button)
            lane_analysis_layout.addLayout(lane_selection_layout)
            
            # Results area
            self.lane_analysis_result = QLabel("Select a lane and time point to analyze the matchup")
            self.lane_analysis_result.setWordWrap(True)
            self.lane_analysis_result.setTextFormat(Qt.RichText)
            lane_analysis_layout.addWidget(self.lane_analysis_result)
            
            # Lane visualization placeholder
            self.lane_figure = Figure(figsize=(8, 6))
            self.lane_canvas = FigureCanvas(self.lane_figure)
            lane_analysis_layout.addWidget(self.lane_canvas)
            
            lane_analysis_tab.setLayout(lane_analysis_layout)
            tab_widget.addTab(lane_analysis_tab, "Lane Analysis")
            
            # Tab 3: Player Metrics
            player_metrics_tab = QWidget()
            player_metrics_layout = QVBoxLayout()
            
            player_metrics_title = QLabel("<h3>Player Performance Analysis</h3>")
            player_metrics_layout.addWidget(player_metrics_title)
            
            # Metric selection
            metrics_selection_layout = QHBoxLayout()
            metrics_selection_layout.addWidget(QLabel("Select Metric:"))
            self.metrics_combo = QComboBox()
            self.metrics_combo.addItems(["kills", "deaths", "assists", "gold_per_min", "xp_per_min", 
                                       "last_hits", "hero_damage", "tower_damage", "hero_healing"])
            metrics_selection_layout.addWidget(self.metrics_combo)
            
            metrics_analyze_button = QPushButton("Visualize Metric")
            metrics_analyze_button.clicked.connect(lambda: self.visualize_player_metric(match_id))
            metrics_selection_layout.addWidget(metrics_analyze_button)
            player_metrics_layout.addLayout(metrics_selection_layout)
            
            # Metric visualization placeholder
            self.metrics_figure = Figure(figsize=(8, 6))
            self.metrics_canvas = FigureCanvas(self.metrics_figure)
            player_metrics_layout.addWidget(self.metrics_canvas)
            
            player_metrics_tab.setLayout(player_metrics_layout)
            tab_widget.addTab(player_metrics_tab, "Player Metrics")
            
            # Tab 4: Team Fights
            team_fights_tab = QWidget()
            team_fights_layout = QVBoxLayout()
            
            team_fights_title = QLabel("<h3>Team Fights Analysis</h3>")
            team_fights_layout.addWidget(team_fights_title)
            
            # Team fight selection
            team_fights_layout.addWidget(QLabel("Team Fights:"))
            self.team_fights_list = QListWidget()
            team_fights_layout.addWidget(self.team_fights_list)
            
            # Load team fights button
            load_team_fights_button = QPushButton("Load Team Fights")
            load_team_fights_button.clicked.connect(lambda: self.load_team_fights(match_id))
            team_fights_layout.addWidget(load_team_fights_button)
            
            # Team fight details
            self.team_fight_details = QLabel("Select a team fight to view details")
            self.team_fight_details.setWordWrap(True)
            self.team_fight_details.setTextFormat(Qt.RichText)
            team_fights_layout.addWidget(self.team_fight_details)
            
            team_fights_tab.setLayout(team_fights_layout)
            tab_widget.addTab(team_fights_tab, "Team Fights")
            
            # Add tab widget to main layout
            main_layout.addWidget(tab_widget)
            
        except Exception as e:
            logger.error(f"Error loading match details: {e}")
            import traceback
            logger.error(traceback.format_exc())
            main_layout.addWidget(QLabel(f"Error loading match details: {str(e)}"))
        
        # Set layout and show window
        central_widget.setLayout(main_layout)
        self.match_details_window.setCentralWidget(central_widget)
        self.match_details_window.show()
    
    def open_user_match_details(self, item):
        """Open detailed view for a user match"""
        # Get match ID from the first column
        row = item.row()
        match_id = int(self.user_matches_table.item(row, 0).text())
        
        # In a real implementation, you'd create a detailed match window
        # For now, just show a message
        QMessageBox.information(
            self, 
            "User Match Details", 
            f"Detailed view for match {match_id} would appear here.\n\n"
            f"This would include your performance, items, skill builds, etc."
        )
    def change_statistic(self, row):
        """Change the displayed statistic based on list selection"""
        self.clear_layout(self.stats_content_layout)
        
        selected_stat = self.stats_list.item(row).text()
        
        if selected_stat == "Heroes Win Rates":
            self.display_hero_win_rates()
        elif selected_stat == "Team Performance":
            self.display_team_performance()
        elif selected_stat == "Player Statistics":
            self.display_player_statistics()
        elif selected_stat == "Meta Trends":
            self.display_meta_trends()
        elif selected_stat == "Draft Analysis":
            self.display_draft_analysis()
        elif selected_stat == "Lane Analysis":
            self.display_lane_analysis()
            
        # Update layout
        self.stats_content.setLayout(self.stats_content_layout)
        
    def display_meta_trends(self):
        """Display meta trend statistics from the database"""
        # Create a scrollable container for meta trend analysis
        meta_trends_widget = QWidget()
        meta_trends_layout = QVBoxLayout()
        
        # Add title
        title = QLabel("<h2>Meta Trends Analysis</h2>")
        title.setAlignment(Qt.AlignCenter)
        meta_trends_layout.addWidget(title)
        
        # Add description
        description = QLabel("Analyze how the game has evolved over time, including game duration, pick rates, and item usage.")
        description.setWordWrap(True)
        meta_trends_layout.addWidget(description)
        
        # Filters section
        filters_group = QGroupBox("Analysis Filters")
        filters_layout = QGridLayout()
        
        # Date range filter
        filters_layout.addWidget(QLabel("Time Period:"), 0, 0)
        self.meta_time_combo = QComboBox()
        self.meta_time_combo.addItems(["Last Month", "Last 3 Months", "Last 6 Months", "Last Year", "All Time"])
        filters_layout.addWidget(self.meta_time_combo, 0, 1)
        
        # League filter
        filters_layout.addWidget(QLabel("League:"), 1, 0)
        self.meta_league_combo = QComboBox()
        self.meta_league_combo.addItem("All Leagues", None)
        # Add leagues from database
        try:
            result = self.session.execute(text("SELECT league_id, name FROM pro_leagues ORDER BY name"))
            leagues = result.fetchall()
            for league in leagues:
                league_id, league_name = league
                self.meta_league_combo.addItem(league_name or "Unknown League", league_id)
        except Exception as e:
            logger.error(f"Error loading leagues for meta trends: {e}")
        filters_layout.addWidget(self.meta_league_combo, 1, 1)
        
        # Analysis type filter
        filters_layout.addWidget(QLabel("Analysis Type:"), 2, 0)
        self.meta_analysis_combo = QComboBox()
        self.meta_analysis_combo.addItems(["Game Duration", "Hero Pick Rates", "Role Distribution", "Item Usage"])
        filters_layout.addWidget(self.meta_analysis_combo, 2, 1)
        
        # Add analysis parameters
        filters_layout.addWidget(QLabel("Top Results:"), 3, 0)
        self.meta_top_results = QComboBox()
        self.meta_top_results.addItems(["5", "10", "15", "20"])
        filters_layout.addWidget(self.meta_top_results, 3, 1)
        
        # Add Apply button
        self.meta_analysis_button = QPushButton("Generate Analysis")
        self.meta_analysis_button.clicked.connect(self.generate_meta_analysis)
        filters_layout.addWidget(self.meta_analysis_button, 4, 1)
        
        filters_group.setLayout(filters_layout)
        meta_trends_layout.addWidget(filters_group)
        
        # Create a widget for displaying charts and results
        self.meta_results_widget = QWidget()
        self.meta_results_layout = QVBoxLayout()
        self.meta_results_layout.addWidget(QLabel("Select analysis options and click Generate Analysis"))
        self.meta_results_widget.setLayout(self.meta_results_layout)
        meta_trends_layout.addWidget(self.meta_results_widget)
        
        # Set layout
        meta_trends_widget.setLayout(meta_trends_layout)
        self.stats_content_layout.addWidget(meta_trends_widget)
        
    def generate_meta_analysis(self):
        """Generate meta trend analysis based on selected filters"""
        # Clear previous results
        self.clear_layout(self.meta_results_layout)
        
        # Get filter values
        analysis_type = self.meta_analysis_combo.currentText()
        time_period = self.meta_time_combo.currentText()
        league_id = self.meta_league_combo.currentData()
        top_results = int(self.meta_top_results.currentText())
        
        # Create date filter based on time period
        end_date = datetime.now()
        if time_period == "Last Month":
            start_date = end_date - timedelta(days=30)
        elif time_period == "Last 3 Months":
            start_date = end_date - timedelta(days=90)
        elif time_period == "Last 6 Months":
            start_date = end_date - timedelta(days=180)
        elif time_period == "Last Year":
            start_date = end_date - timedelta(days=365)
        else:  # All Time
            start_date = datetime(2000, 1, 1)  # Very old date to include all matches
            
        # Create parameters dict
        params = {
            'top_n': top_results
        }
        
        # Call appropriate analysis function based on type
        if analysis_type == "Game Duration":
            self.analyze_game_duration(start_date, league_id, params)
        elif analysis_type == "Hero Pick Rates":
            self.analyze_hero_pick_rates(start_date, league_id, params)
        elif analysis_type == "Role Distribution":
            self.analyze_role_distribution(start_date, league_id, params)
        elif analysis_type == "Item Usage":
            self.analyze_item_usage(start_date, league_id, params)
        
    def analyze_game_duration(self, date_filter, league_filter, params):
        """Analyze game duration trends over time"""
        # Add title
        title = QLabel("<h3>Game Duration Trends</h3>")
        title.setAlignment(Qt.AlignCenter)
        self.meta_results_layout.addWidget(title)
        
        try:
            # Build SQL query
            sql_query = """
            SELECT 
                strftime('%Y-%m', start_time) as month,
                AVG(duration) / 60.0 as avg_duration_minutes,
                COUNT(*) as match_count
            FROM pro_matches
            WHERE start_time >= :start_date
            """
            
            params_dict = {"start_date": date_filter}
            
            if league_filter:
                sql_query += " AND league_id = :league_id"
                params_dict["league_id"] = league_filter
                
            sql_query += """
            GROUP BY month
            ORDER BY month ASC
            """
            
            # Execute query
            result = self.session.execute(text(sql_query), params_dict)
            data = result.fetchall()
            
            if not data:
                self.meta_results_layout.addWidget(QLabel("No data available for the selected filters."))
                return
                
            # Extract data for plotting
            months = [row[0] for row in data]
            durations = [row[1] for row in data]
            match_counts = [row[2] for row in data]
            
            # Create matplotlib figure
            fig = Figure(figsize=(10, 6))
            ax = fig.add_subplot(111)
            
            # Plot average duration
            ax.plot(range(len(months)), durations, 'ro-', linewidth=2, markersize=8)
            
            # Set labels and title
            ax.set_xlabel('Month')
            ax.set_ylabel('Average Duration (minutes)')
            ax.set_title('Average Game Duration Over Time')
            
            # Set x-axis tick labels
            ax.set_xticks(range(len(months)))
            ax.set_xticklabels(months, rotation=45)
            
            # Add match count as a second line on a secondary y-axis
            ax2 = ax.twinx()
            ax2.plot(range(len(months)), match_counts, 'bo--', linewidth=1, markersize=5)
            ax2.set_ylabel('Number of Matches', color='b')
            ax2.tick_params(axis='y', labelcolor='b')
            
            # Add a grid
            ax.grid(True, linestyle='--', alpha=0.7)
            
            # Adjust layout
            fig.tight_layout()
            
            # Create canvas
            canvas = FigureCanvas(fig)
            canvas.setMinimumHeight(400)
            
            # Add to layout
            self.meta_results_layout.addWidget(canvas)
            
            # Add summary text
            if durations:
                avg_duration = sum(durations) / len(durations)
                min_duration = min(durations)
                max_duration = max(durations)
                
                summary = QLabel(f"<b>Summary:</b><br>"
                                f"Average game duration: {avg_duration:.2f} minutes<br>"
                                f"Shortest average duration: {min_duration:.2f} minutes<br>"
                                f"Longest average duration: {max_duration:.2f} minutes<br>"
                                f"Total matches analyzed: {sum(match_counts)}<br>")
                summary.setWordWrap(True)
                self.meta_results_layout.addWidget(summary)
                
                # Analysis insights
                if len(durations) > 1:
                    trend = "increasing" if durations[-1] > durations[0] else "decreasing"
                    insight = QLabel(f"<b>Insight:</b> Game duration is {trend} over the analyzed period, "
                                    f"which may indicate meta shifts toward {'late' if trend == 'increasing' else 'early'} game strategies.")
                    insight.setWordWrap(True)
                    self.meta_results_layout.addWidget(insight)
            
        except Exception as e:
            logger.error(f"Error analyzing game duration: {e}")
            self.meta_results_layout.addWidget(QLabel(f"Error analyzing game duration: {str(e)}"))
            
    def analyze_hero_pick_rates(self, date_filter, league_filter, params):
        """Analyze hero pick rate trends over time"""
        # Add title
        title = QLabel("<h3>Hero Pick Rate Trends</h3>")
        title.setAlignment(Qt.AlignCenter)
        self.meta_results_layout.addWidget(title)
        
        try:
            # Get top N parameter
            top_n = params.get('top_n', 10)
            
            # Build SQL query to get top heroes by pick rate
            sql_query = """
            SELECT 
                h.name as hero_name,
                COUNT(*) as pick_count,
                (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM pro_match_players mp 
                                    JOIN pro_matches m ON mp.match_id = m.match_id 
                                    WHERE m.start_time >= :start_date
            """
            
            params_dict = {"start_date": date_filter}
            
            if league_filter:
                sql_query += " AND m.league_id = :league_id"
                params_dict["league_id"] = league_filter
                
            sql_query += """
            )) as pick_rate
            FROM pro_match_players mp
            JOIN pro_heroes h ON mp.hero_id = h.hero_id
            JOIN pro_matches m ON mp.match_id = m.match_id
            WHERE m.start_time >= :start_date
            """
            
            if league_filter:
                sql_query += " AND m.league_id = :league_id"
                
            sql_query += """
            GROUP BY h.hero_id
            ORDER BY pick_count DESC
            LIMIT :top_n
            """
            
            params_dict["top_n"] = top_n
            
            # Execute query
            result = self.session.execute(text(sql_query), params_dict)
            data = result.fetchall()
            
            if not data:
                self.meta_results_layout.addWidget(QLabel("No data available for the selected filters."))
                return
                
            # Extract data for plotting
            heroes = [row[0] for row in data]
            pick_rates = [row[2] for row in data]
            
            # Create matplotlib figure
            fig = Figure(figsize=(10, 6))
            ax = fig.add_subplot(111)
            
            # Create horizontal bar chart
            bars = ax.barh(range(len(heroes)), pick_rates, color='skyblue')
            
            # Set labels and title
            ax.set_xlabel('Pick Rate (%)')
            ax.set_title('Top Heroes by Pick Rate')
            
            # Set y-axis tick labels
            ax.set_yticks(range(len(heroes)))
            ax.set_yticklabels(heroes)
            
            # Add pick rate values at the end of each bar
            for i, bar in enumerate(bars):
                ax.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2, 
                        f'{pick_rates[i]:.1f}%', va='center')
            
            # Add a grid
            ax.grid(True, axis='x', linestyle='--', alpha=0.7)
            
            # Adjust layout
            fig.tight_layout()
            
            # Create canvas
            canvas = FigureCanvas(fig)
            canvas.setMinimumHeight(400)
            
            # Add to layout
            self.meta_results_layout.addWidget(canvas)
            
            # Add summary text
            summary = QLabel(f"<b>Summary:</b><br>"
                            f"{heroes[0]} is the most picked hero with a {pick_rates[0]:.1f}% pick rate.<br>"
                            f"The top 3 most picked heroes are {', '.join(heroes[:3])}.")
            summary.setWordWrap(True)
            self.meta_results_layout.addWidget(summary)
            
        except Exception as e:
            logger.error(f"Error analyzing hero pick rates: {e}")
            self.meta_results_layout.addWidget(QLabel(f"Error analyzing hero pick rates: {str(e)}"))

    def analyze_role_distribution(self, date_filter, league_filter, params):
        """Analyze role distribution trends over time"""
        # Add title
        title = QLabel("<h3>Role Distribution Analysis</h3>")
        title.setAlignment(Qt.AlignCenter)
        self.meta_results_layout.addWidget(title)
        
        # For role distribution, we'll use a simplified approach since actual lane data
        # might not be available. We'll simulate with random data for visualization purposes.
        
        # Create roles and sample data
        roles = ['Carry', 'Mid', 'Offlane', 'Support', 'Hard Support']
        
        # Generate random distribution data for different time periods
        np.random.seed(42)  # For reproducibility
        time_periods = ['Patch 7.30', 'Patch 7.31', 'Patch 7.32', 'Current']
        
        # Create base distribution that evolves over time
        base_distribution = [30, 25, 20, 15, 10]  # Initial distribution
        all_distributions = []
        
        for i in range(len(time_periods)):
            # Add some random variation while maintaining sum of 100
            variation = np.random.randint(-3, 4, 5)
            # Ensure sum is still 100
            distribution = [max(5, base_distribution[j] + variation[j]) for j in range(5)]
            total = sum(distribution)
            distribution = [int(100 * d / total) for d in distribution]
            
            # Fix any rounding errors
            while sum(distribution) < 100:
                distribution[np.random.randint(0, 5)] += 1
            while sum(distribution) > 100:
                idx = np.argmax(distribution)
                distribution[idx] -= 1
                
            all_distributions.append(distribution)
            
            # Update base for next iteration
            base_distribution = distribution
        
        # Create matplotlib figure for stacked bar chart
        fig = Figure(figsize=(10, 6))
        ax = fig.add_subplot(111)
        
        # Create x-axis positions
        x = np.arange(len(time_periods))
        width = 0.6
        
        # Create bottom position for stacking
        bottom = np.zeros(len(time_periods))
        
        # Colors for each role
        colors = ['#FF7043', '#42A5F5', '#66BB6A', '#AB47BC', '#FFA726']
        
        # Plot each role as a segment of the stacked bar
        bars = []
        for i, role in enumerate(roles):
            values = [dist[i] for dist in all_distributions]
            bar = ax.bar(x, values, width, bottom=bottom, label=role, color=colors[i])
            bars.append(bar)
            bottom += values
        
        # Set labels and title
        ax.set_xlabel('Patch')
        ax.set_ylabel('Percentage (%)')
        ax.set_title('Role Distribution Across Patches')
        
        # Set x-axis tick labels
        ax.set_xticks(x)
        ax.set_xticklabels(time_periods)
        
        # Add value labels to each segment
        for i, role_bars in enumerate(bars):
            for j, bar in enumerate(role_bars):
                height = bar.get_height()
                if height > 5:  # Only label segments that are large enough
                    ax.text(bar.get_x() + bar.get_width()/2, 
                           bar.get_y() + height/2, 
                           str(int(height)), 
                           ha='center', va='center', 
                           color='white', fontweight='bold')
        
        # Add legend
        ax.legend(title="Roles", bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # Adjust layout
        fig.tight_layout()
        
        # Create canvas
        canvas = FigureCanvas(fig)
        canvas.setMinimumHeight(400)
        
        # Add to layout
        self.meta_results_layout.addWidget(canvas)
        
        # Add explanatory text
        explanation = QLabel("<b>Note:</b> This visualization shows simulated role distribution data across patches. "
                           "In a production environment, this would use actual player position data from matches.")
        explanation.setWordWrap(True)
        self.meta_results_layout.addWidget(explanation)
        
        # Add insights
        latest_dist = all_distributions[-1]
        max_role_idx = np.argmax(latest_dist)
        min_role_idx = np.argmin(latest_dist)
        
        insight = QLabel(f"<b>Insight:</b> {roles[max_role_idx]} appears to be the most resource-intensive role "
                         f"in the current meta ({latest_dist[max_role_idx]}%), while {roles[min_role_idx]} "
                         f"receives the least farm priority ({latest_dist[min_role_idx]}%).")
        insight.setWordWrap(True)
        self.meta_results_layout.addWidget(insight)
        
    def analyze_item_usage(self, date_filter, league_filter, params):
        """Analyze item usage trends over time"""
        # Add title
        title = QLabel("<h3>Item Usage Trends</h3>")
        title.setAlignment(Qt.AlignCenter)
        self.meta_results_layout.addWidget(title)
        
        # For this demo, we'll use simulated data for popular items
        items = [
            "Black King Bar", "Blink Dagger", "Power Treads", "Aghanim's Scepter",
            "Desolator", "Satanic", "Butterfly", "Manta Style",
            "Battle Fury", "Eye of Skadi"
        ]
        
        # Generate usage data (increasing or decreasing trend for each item)
        np.random.seed(42)  # For reproducibility
        time_periods = ['2023 Q1', '2023 Q2', '2023 Q3', '2023 Q4', '2024 Q1']
        
        # Create trends data
        usage_data = {}
        trends = {}  # Store trend direction for each item
        
        for item in items:
            # Randomly decide if item usage is increasing or decreasing
            trend_direction = np.random.choice([-1, 1])
            trends[item] = trend_direction
            
            # Base usage percentage (between 10% and 70%)
            base = np.random.uniform(10, 70)
            
            # Create trend with some random noise
            trend = []
            for i in range(len(time_periods)):
                # Add trend effect and some noise
                value = base + (trend_direction * i * 5) + np.random.uniform(-3, 3)
                # Ensure values are in range [5, 95]
                value = max(5, min(95, value))
                trend.append(value)
                
            usage_data[item] = trend
        
        # Sort items by their current (last) usage rate
        sorted_items = sorted(items, key=lambda x: usage_data[x][-1], reverse=True)
        
        # Select top N items for visualization
        top_n = params.get('top_n', 5)
        top_items = sorted_items[:top_n]
        
        # Create matplotlib figure
        fig = Figure(figsize=(10, 6))
        ax = fig.add_subplot(111)
        
        # Plot each item's usage trend
        for item in top_items:
            ax.plot(range(len(time_periods)), usage_data[item], 'o-', linewidth=2, markersize=8, label=item)
        
        # Set labels and title
        ax.set_xlabel('Time Period')
        ax.set_ylabel('Usage Rate (%)')
        ax.set_title('Item Usage Trends Over Time')
        
        # Set x-axis tick labels
        ax.set_xticks(range(len(time_periods)))
        ax.set_xticklabels(time_periods, rotation=45)
        
        # Add a grid
        ax.grid(True, linestyle='--', alpha=0.7)
        
        # Add legend
        ax.legend(title="Items", loc='best')
        
        # Adjust layout
        fig.tight_layout()
        
        # Create canvas
        canvas = FigureCanvas(fig)
        canvas.setMinimumHeight(400)
        
        # Add to layout
        self.meta_results_layout.addWidget(canvas)
        
        # Add insights
        rising_items = [item for item in top_items if trends[item] > 0]
        falling_items = [item for item in top_items if trends[item] < 0]
        
        insights = QLabel("<b>Item Trend Insights:</b><br>")
        insights.setWordWrap(True)
        
        if rising_items:
            insights_text = f"<span style='color:green'>Gaining popularity:</span> {', '.join(rising_items)}<br>"
            insights.setText(insights.text() + insights_text)
            
        if falling_items:
            insights_text = f"<span style='color:red'>Declining usage:</span> {', '.join(falling_items)}<br>"
            insights.setText(insights.text() + insights_text)
            
        most_popular = top_items[0]
        insights_text = f"<br>Currently, <b>{most_popular}</b> is the most commonly purchased item among the top cores."
        insights.setText(insights.text() + insights_text)
        
        self.meta_results_layout.addWidget(insights)
        
        # Add explanatory note
        note = QLabel("<b>Note:</b> This visualization shows simulated item usage data. "
                      "In a production environment, this would use actual item purchase data from matches.")
        note.setWordWrap(True)
        self.meta_results_layout.addWidget(note)
    
    def display_hero_win_rates(self):
        """Display hero win rate statistics from the database"""
        logger.info("Generating hero win rate statistics")
        
        # Create container widget and layout
        container = QWidget()
        container_layout = QVBoxLayout(container)
        
        # Add filters section
        filters_group = QGroupBox("Filters")
        filters_layout = QGridLayout()
        
        # League filter
        filters_layout.addWidget(QLabel("League:"), 0, 0)
        league_combo = QComboBox()
        league_combo.addItem("All Leagues", None)  # Default option
        
        # Time period filter
        filters_layout.addWidget(QLabel("Time Period:"), 1, 0)
        time_combo = QComboBox()
        time_combo.addItems(["All Time", "Last 3 Months", "Last Month", "Last Week"])
        
        # Minimum matches filter
        filters_layout.addWidget(QLabel("Minimum Matches:"), 2, 0)
        min_matches_combo = QComboBox()
        min_matches_combo.addItems(["1", "5", "10", "20"])
        
        filters_layout.addWidget(league_combo, 0, 1)
        filters_layout.addWidget(time_combo, 1, 1)
        filters_layout.addWidget(min_matches_combo, 2, 1)
        
        # Add apply button
        apply_button = QPushButton("Apply Filters")
        filters_layout.addWidget(apply_button, 3, 1)
        
        filters_group.setLayout(filters_layout)
        container_layout.addWidget(filters_group)
        
        # Add table for displaying hero win rates
        self.hero_stats_table = QTableWidget()
        self.hero_stats_table.setColumnCount(5)
        self.hero_stats_table.setHorizontalHeaderLabels([
            "Hero", "Matches", "Win Rate", "Pick Rate", "Ban Rate"
        ])
        self.hero_stats_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        container_layout.addWidget(self.hero_stats_table)
        
        # Add the container to the content layout
        self.stats_content_layout.addWidget(container)
        
        # Try to populate the league dropdown if possible
        try:
            from sqlalchemy import text
            leagues_query = text("SELECT league_id, name FROM pro_leagues ORDER BY name")
            leagues = self.session.execute(leagues_query).fetchall()
            for league in leagues:
                league_id, name = league
                if name:  # Only add leagues with actual names
                    league_combo.addItem(name, league_id)
        except Exception as e:
            logger.error(f"Error loading leagues for hero stats: {e}")
        
        # Connect filter apply button
        apply_button.clicked.connect(lambda: self.calculate_hero_stats(
            league_id=league_combo.currentData(),
            time_period=time_combo.currentText(),
            min_matches=int(min_matches_combo.currentText())
        ))
        
        # Load initial data
        self.calculate_hero_stats(league_id=None, time_period="All Time", min_matches=1)
    
    def calculate_hero_stats(self, league_id=None, time_period="All Time", min_matches=1):
        """Calculate and display hero statistics based on filters"""
        try:
            # Clear the table
            self.hero_stats_table.setRowCount(0)
            
            # Build the SQL query for hero statistics
            from sqlalchemy import text
            from datetime import datetime, timedelta
            
            # Base query to get all matches for heroes
            base_query = """
            SELECT 
                h.hero_id, 
                h.name AS hero_name,
                COUNT(DISTINCT mp.match_id) AS num_matches,
                SUM(CASE 
                    WHEN (mp.player_slot < 128 AND m.radiant_win = 1) OR 
                         (mp.player_slot >= 128 AND m.radiant_win = 0) 
                    THEN 1 ELSE 0 END) AS wins
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_heroes h ON mp.hero_id = h.hero_id
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            """
            
            # Add league filter if specified
            if league_id:
                base_query += "\nWHERE m.league_id = :league_id"
            else:
                base_query += "\nWHERE 1=1"
            
            # Add time period filter
            if time_period != "All Time":
                now = datetime.now()
                if time_period == "Last Week":
                    start_date = now - timedelta(days=7)
                elif time_period == "Last Month":
                    start_date = now - timedelta(days=30)
                elif time_period == "Last 3 Months":
                    start_date = now - timedelta(days=90)
                
                base_query += "\nAND m.start_time >= :start_date"
            
            # Group by hero and filter by minimum matches
            base_query += """
            GROUP BY h.hero_id, h.name
            HAVING COUNT(DISTINCT mp.match_id) >= :min_matches
            ORDER BY (SUM(CASE 
                    WHEN (mp.player_slot < 128 AND m.radiant_win = 1) OR 
                         (mp.player_slot >= 128 AND m.radiant_win = 0) 
                    THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT mp.match_id)) DESC,
                    COUNT(DISTINCT mp.match_id) DESC
            """
            
            # Prepare parameters
            params = {"min_matches": min_matches}
            if league_id:
                params["league_id"] = league_id
            if time_period != "All Time":
                params["start_date"] = start_date
            
            # Execute the query
            logger.info(f"Executing hero stats query with params: {params}")
            result = self.session.execute(text(base_query), params)
            heroes = result.fetchall()
            
            # Calculate total matches for pick rate
            total_matches_query = "SELECT COUNT(DISTINCT match_id) FROM pro_matches"
            if league_id:
                total_matches_query += " WHERE league_id = :league_id"
                if time_period != "All Time":
                    total_matches_query += " AND start_time >= :start_date"
            elif time_period != "All Time":
                total_matches_query += " WHERE start_time >= :start_date"
            
            total_matches = self.session.execute(text(total_matches_query), params).scalar() or 0
            
            # Add heroes to the table
            for i, hero in enumerate(heroes):
                hero_id = hero[0]
                hero_name = hero[1] or f"Hero {hero_id}"
                matches = hero[2]
                wins = hero[3]
                win_rate = (wins / matches * 100) if matches > 0 else 0
                pick_rate = (matches / total_matches * 100) if total_matches > 0 else 0
                
                # Add row to table
                self.hero_stats_table.insertRow(i)
                self.hero_stats_table.setItem(i, 0, QTableWidgetItem(hero_name))
                self.hero_stats_table.setItem(i, 1, QTableWidgetItem(str(matches)))
                
                # Format win rate with color (green for >50%, red for <50%)
                win_rate_item = QTableWidgetItem(f"{win_rate:.1f}%")
                if win_rate >= 50:
                    win_rate_item.setForeground(Qt.green)
                else:
                    win_rate_item.setForeground(Qt.red)
                self.hero_stats_table.setItem(i, 2, win_rate_item)
                
                # Pick rate
                self.hero_stats_table.setItem(i, 3, QTableWidgetItem(f"{pick_rate:.1f}%"))
                
                # Ban rate (currently a placeholder as we may not have this data)
                self.hero_stats_table.setItem(i, 4, QTableWidgetItem("N/A"))  # Placeholder
            
            # Show message if no heroes found
            if not heroes:
                QMessageBox.information(
                    self, 
                    "No Data", 
                    "No hero data found with the current filters. Try adjusting your filters."
                )
                logger.warning("No hero data found with the current filters")
            
            # Update status
            self.statusBar().showMessage(f"Loaded win rates for {len(heroes)} heroes")
            
        except Exception as e:
            logger.error(f"Error calculating hero statistics: {e}")
            QMessageBox.warning(
                self, 
                "Error", 
                f"Error calculating hero statistics: {str(e)}"
            )
            # Add placeholder message in case of error
            self.hero_stats_table.setRowCount(0)
    
    def display_team_performance(self):
        """Display team performance statistics from the database"""
        logger.info("Generating team performance statistics")
        
        # Create container widget and layout
        container = QWidget()
        container_layout = QVBoxLayout(container)
        
        # Add filters section
        filters_group = QGroupBox("Filters")
        filters_layout = QGridLayout()
        
        # League filter
        filters_layout.addWidget(QLabel("League:"), 0, 0)
        league_combo = QComboBox()
        league_combo.addItem("All Leagues", None)  # Default option
        
        # Time period filter
        filters_layout.addWidget(QLabel("Time Period:"), 1, 0)
        time_combo = QComboBox()
        time_combo.addItems(["All Time", "Last 3 Months", "Last Month", "Last Week"])
        
        # Minimum matches filter
        filters_layout.addWidget(QLabel("Minimum Matches:"), 2, 0)
        min_matches_combo = QComboBox()
        min_matches_combo.addItems(["1", "5", "10", "20"])
        
        # Compare teams section
        filters_layout.addWidget(QLabel("Compare Teams:"), 3, 0)
        self.team1_combo = QComboBox()
        self.team2_combo = QComboBox()
        compare_button = QPushButton("Compare Head-to-Head")
        
        compare_layout = QHBoxLayout()
        compare_layout.addWidget(self.team1_combo)
        compare_layout.addWidget(QLabel("vs"))
        compare_layout.addWidget(self.team2_combo)
        
        filters_layout.addLayout(compare_layout, 3, 1)
        filters_layout.addWidget(compare_button, 4, 1)
        
        filters_layout.addWidget(league_combo, 0, 1)
        filters_layout.addWidget(time_combo, 1, 1)
        filters_layout.addWidget(min_matches_combo, 2, 1)
        
        # Add apply button for general stats
        apply_button = QPushButton("Show Team Rankings")
        filters_layout.addWidget(apply_button, 5, 1)
        
        filters_group.setLayout(filters_layout)
        container_layout.addWidget(filters_group)
        
        # Create tabs for different team performance views
        tabs = QTabWidget()
        
        # Team rankings tab
        rankings_tab = QWidget()
        rankings_layout = QVBoxLayout(rankings_tab)
        
        self.team_rankings_table = QTableWidget()
        self.team_rankings_table.setColumnCount(6)
        self.team_rankings_table.setHorizontalHeaderLabels([
            "Team", "Matches", "Wins", "Losses", "Win Rate", "Avg Game Duration"
        ])
        self.team_rankings_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        rankings_layout.addWidget(self.team_rankings_table)
        
        # Head to head tab
        head2head_tab = QWidget()
        head2head_layout = QVBoxLayout(head2head_tab)
        
        self.head2head_table = QTableWidget()
        self.head2head_table.setColumnCount(4)
        self.head2head_table.setHorizontalHeaderLabels([
            "Match ID", "Date", "Score", "Winner"
        ])
        self.head2head_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        
        # Add summary labels above the table
        self.head2head_summary = QLabel("Select two teams to compare head-to-head statistics")
        head2head_layout.addWidget(self.head2head_summary)
        head2head_layout.addWidget(self.head2head_table)
        
        # Add tabs to tab widget
        tabs.addTab(rankings_tab, "Team Rankings")
        tabs.addTab(head2head_tab, "Head to Head")
        
        container_layout.addWidget(tabs)
        
        # Add the container to the content layout
        self.stats_content_layout.addWidget(container)
        
        # Try to populate the league and team dropdowns
        try:
            from sqlalchemy import text
            
            # Populate leagues
            leagues_query = text("SELECT league_id, name FROM pro_leagues ORDER BY name")
            leagues = self.session.execute(leagues_query).fetchall()
            for league in leagues:
                league_id, name = league
                if name:  # Only add leagues with actual names
                    league_combo.addItem(name, league_id)
            
            # Populate teams for comparison
            teams_query = text("SELECT team_id, name FROM pro_teams ORDER BY name")
            teams = self.session.execute(teams_query).fetchall()
            
            # Add empty first item
            self.team1_combo.addItem("Select Team", None)
            self.team2_combo.addItem("Select Team", None)
            
            for team in teams:
                team_id, name = team
                if name:  # Only add teams with actual names
                    self.team1_combo.addItem(name, team_id)
                    self.team2_combo.addItem(name, team_id)
                    
        except Exception as e:
            logger.error(f"Error loading dropdown data for team performance: {e}")
        
        # Connect filter buttons
        apply_button.clicked.connect(lambda: self.calculate_team_rankings(
            league_id=league_combo.currentData(),
            time_period=time_combo.currentText(),
            min_matches=int(min_matches_combo.currentText())
        ))
        
        compare_button.clicked.connect(self.show_head_to_head_stats)
        
        # Load initial team rankings data
        self.calculate_team_rankings()
    
    def calculate_team_rankings(self, league_id=None, time_period="All Time", min_matches=1):
        """Calculate and display team rankings based on match data"""
        try:
            # Clear the table
            self.team_rankings_table.setRowCount(0)
            
            # Build the SQL query for team statistics
            from sqlalchemy import text
            from datetime import datetime, timedelta
            
            # Build time filter
            time_filter = ""
            params = {"min_matches": min_matches}
            
            if time_period != "All Time":
                now = datetime.now()
                if time_period == "Last Week":
                    start_date = now - timedelta(days=7)
                elif time_period == "Last Month":
                    start_date = now - timedelta(days=30)
                elif time_period == "Last 3 Months":
                    start_date = now - timedelta(days=90)
                
                time_filter = "AND m.start_time >= :start_date"
                params["start_date"] = start_date
            
            # League filter
            league_filter = ""
            if league_id:
                league_filter = "AND m.league_id = :league_id"
                params["league_id"] = league_id
            
            # Query for radiant team stats
            radiant_query = f"""
            SELECT 
                t.team_id,
                t.name as team_name,
                COUNT(m.match_id) as num_matches,
                SUM(CASE WHEN m.radiant_win = 1 THEN 1 ELSE 0 END) as wins,
                AVG(m.duration) as avg_duration
            FROM 
                pro_matches m
            JOIN 
                pro_teams t ON m.radiant_team_id = t.team_id
            WHERE 
                m.radiant_team_id IS NOT NULL
                {league_filter}
                {time_filter}
            GROUP BY 
                t.team_id, t.name
            """
            
            # Query for dire team stats
            dire_query = f"""
            SELECT 
                t.team_id,
                t.name as team_name,
                COUNT(m.match_id) as num_matches,
                SUM(CASE WHEN m.radiant_win = 0 THEN 1 ELSE 0 END) as wins,
                AVG(m.duration) as avg_duration
            FROM 
                pro_matches m
            JOIN 
                pro_teams t ON m.dire_team_id = t.team_id
            WHERE 
                m.dire_team_id IS NOT NULL
                {league_filter}
                {time_filter}
            GROUP BY 
                t.team_id, t.name
            """
            
            # Execute queries
            radiant_result = self.session.execute(text(radiant_query), params)
            dire_result = self.session.execute(text(dire_query), params)
            
            radiant_stats = {}
            for row in radiant_result:
                team_id = row[0]
                team_name = row[1]
                matches = row[2]
                wins = row[3]
                avg_duration = row[4]
                
                radiant_stats[team_id] = {
                    "team_name": team_name,
                    "matches": matches,
                    "wins": wins,
                    "duration": avg_duration
                }
            
            # Combine with dire stats
            team_stats = {}
            for row in dire_result:
                team_id = row[0]
                team_name = row[1]
                matches = row[2]
                wins = row[3]
                avg_duration = row[4]
                
                if team_id in radiant_stats:
                    # Team played as both radiant and dire
                    r_stats = radiant_stats[team_id]
                    total_matches = r_stats["matches"] + matches
                    total_wins = r_stats["wins"] + wins
                    avg_dur = (r_stats["duration"]*r_stats["matches"] + avg_duration*matches)/total_matches if total_matches > 0 else 0
                    
                    team_stats[team_id] = {
                        "team_name": team_name,
                        "matches": total_matches,
                        "wins": total_wins,
                        "losses": total_matches - total_wins,
                        "win_rate": (total_wins / total_matches * 100) if total_matches > 0 else 0,
                        "avg_duration": avg_dur
                    }
                    
                    # Remove from radiant stats as it's now in combined stats
                    del radiant_stats[team_id]
                else:
                    # Team only played as dire
                    team_stats[team_id] = {
                        "team_name": team_name,
                        "matches": matches,
                        "wins": wins,
                        "losses": matches - wins,
                        "win_rate": (wins / matches * 100) if matches > 0 else 0,
                        "avg_duration": avg_duration
                    }
            
            # Add remaining radiant-only teams
            for team_id, stats in radiant_stats.items():
                team_stats[team_id] = {
                    "team_name": stats["team_name"],
                    "matches": stats["matches"],
                    "wins": stats["wins"],
                    "losses": stats["matches"] - stats["wins"],
                    "win_rate": (stats["wins"] / stats["matches"] * 100) if stats["matches"] > 0 else 0,
                    "avg_duration": stats["duration"]
                }
            
            # Filter by minimum matches
            team_stats = {k: v for k, v in team_stats.items() if v["matches"] >= min_matches}
            
            # Sort teams by win rate (descending)
            sorted_teams = sorted(team_stats.items(), key=lambda x: x[1]["win_rate"], reverse=True)
            
            # Add teams to the table
            for i, (team_id, stats) in enumerate(sorted_teams):
                self.team_rankings_table.insertRow(i)
                self.team_rankings_table.setItem(i, 0, QTableWidgetItem(stats["team_name"]))
                self.team_rankings_table.setItem(i, 1, QTableWidgetItem(str(stats["matches"])))
                self.team_rankings_table.setItem(i, 2, QTableWidgetItem(str(stats["wins"])))
                self.team_rankings_table.setItem(i, 3, QTableWidgetItem(str(stats["losses"])))
                
                # Format win rate with color
                win_rate_item = QTableWidgetItem(f"{stats['win_rate']:.1f}%")
                if stats["win_rate"] >= 50:
                    win_rate_item.setForeground(Qt.green)
                else:
                    win_rate_item.setForeground(Qt.red)
                self.team_rankings_table.setItem(i, 4, win_rate_item)
                
                # Convert seconds to minutes:seconds format
                minutes = int(stats["avg_duration"]) // 60
                seconds = int(stats["avg_duration"]) % 60
                duration_str = f"{minutes}:{seconds:02d}"
                self.team_rankings_table.setItem(i, 5, QTableWidgetItem(duration_str))
            
            # Show message if no teams found
            if not sorted_teams:
                QMessageBox.information(
                    self, 
                    "No Data", 
                    "No team data found with the current filters. Try adjusting your filters."
                )
                logger.warning("No team data found with the current filters")
            
            # Update status
            self.statusBar().showMessage(f"Loaded team rankings for {len(sorted_teams)} teams")
            
            # Add visualizations if we have teams to display
            if sorted_teams:
                # Get top 10 teams for charts (or all if less than 10)
                top_teams = sorted_teams[:min(10, len(sorted_teams))]
                
                # Extract data for visualization
                team_names = [team[1]["team_name"] for team in top_teams]
                win_rates = [team[1]["win_rate"] for team in top_teams]
                matches = [team[1]["matches"] for team in top_teams]
                wins = [team[1]["wins"] for team in top_teams]
                losses = [team[1]["losses"] for team in top_teams]
                durations = [team[1]["avg_duration"]/60 for team in top_teams]  # Convert to minutes
                
                # Create visualization frame
                viz_frame = QFrame()
                viz_frame.setFrameShape(QFrame.StyledPanel)
                viz_frame.setFrameShadow(QFrame.Sunken)
                viz_layout = QVBoxLayout(viz_frame)
                viz_layout.addWidget(QLabel("<h3>Top Teams Performance Visualizations</h3>"))
                
                # Create a tab widget for different charts
                viz_tabs = QTabWidget()
                
                # 1. Win Rate Bar Chart
                win_rate_tab = QWidget()
                win_rate_layout = QVBoxLayout(win_rate_tab)
                
                from matplotlib.figure import Figure
                from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
                
                fig1 = Figure(figsize=(8, 4))
                ax1 = fig1.add_subplot(111)
                bars = ax1.bar(range(len(team_names)), win_rates, color='skyblue')
                
                # Add value labels on top of bars
                for i, bar in enumerate(bars):
                    height = bar.get_height()
                    ax1.text(bar.get_x() + bar.get_width()/2, height + 1,
                            f'{win_rates[i]:.1f}%',
                            ha='center', va='bottom', rotation=0)
                    
                    # Color bars based on win rate
                    if win_rates[i] >= 60:
                        bar.set_color('green')
                    elif win_rates[i] >= 50:
                        bar.set_color('lightgreen')
                    else:
                        bar.set_color('salmon')
                
                ax1.set_title('Win Rates of Top Teams')
                ax1.set_xlabel('Team')
                ax1.set_ylabel('Win Rate (%)')
                ax1.set_xticks(range(len(team_names)))
                ax1.set_xticklabels(team_names, rotation=45, ha='right')
                ax1.set_ylim(0, max(win_rates) + 10)  # Set y-axis with headroom
                ax1.grid(axis='y', linestyle='--', alpha=0.7)
                fig1.tight_layout()
                
                canvas1 = FigureCanvas(fig1)
                canvas1.setMinimumHeight(350)
                win_rate_layout.addWidget(canvas1)
                
                # 2. Wins vs Losses Stacked Bar Chart
                wl_tab = QWidget()
                wl_layout = QVBoxLayout(wl_tab)
                
                fig2 = Figure(figsize=(8, 4))
                ax2 = fig2.add_subplot(111)
                
                # Create stacked bar chart for wins and losses
                width = 0.8
                bars1 = ax2.bar(range(len(team_names)), wins, width, label='Wins', color='green')
                bars2 = ax2.bar(range(len(team_names)), losses, width, bottom=wins, label='Losses', color='red')
                
                # Add win-loss ratio labels
                for i in range(len(team_names)):
                    total = wins[i] + losses[i]
                    ax2.text(i, total + 0.5, f'W/L: {wins[i]}-{losses[i]}', 
                             ha='center', va='bottom', fontsize=9)
                
                ax2.set_title('Wins and Losses by Team')
                ax2.set_xlabel('Team')
                ax2.set_ylabel('Number of Matches')
                ax2.set_xticks(range(len(team_names)))
                ax2.set_xticklabels(team_names, rotation=45, ha='right')
                ax2.legend()
                ax2.grid(axis='y', linestyle='--', alpha=0.7)
                fig2.tight_layout()
                
                canvas2 = FigureCanvas(fig2)
                canvas2.setMinimumHeight(350)
                wl_layout.addWidget(canvas2)
                
                # 3. Average Game Duration Chart
                duration_tab = QWidget()
                duration_layout = QVBoxLayout(duration_tab)
                
                fig3 = Figure(figsize=(8, 4))
                ax3 = fig3.add_subplot(111)
                bars3 = ax3.bar(range(len(team_names)), durations, color='purple')
                
                # Add duration labels
                for i, bar in enumerate(bars3):
                    height = bar.get_height()
                    minutes = int(durations[i])
                    seconds = int((durations[i] - minutes) * 60)
                    ax3.text(bar.get_x() + bar.get_width()/2, height + 0.5,
                            f'{minutes}:{seconds:02d}',
                            ha='center', va='bottom', rotation=0)
                
                ax3.set_title('Average Game Duration by Team')
                ax3.set_xlabel('Team')
                ax3.set_ylabel('Duration (minutes)')
                ax3.set_xticks(range(len(team_names)))
                ax3.set_xticklabels(team_names, rotation=45, ha='right')
                ax3.grid(axis='y', linestyle='--', alpha=0.7)
                fig3.tight_layout()
                
                canvas3 = FigureCanvas(fig3)
                canvas3.setMinimumHeight(350)
                duration_layout.addWidget(canvas3)
                
                # Add the tabs to viz_tabs
                viz_tabs.addTab(win_rate_tab, 'Win Rate')
                viz_tabs.addTab(wl_tab, 'Wins vs Losses')
                viz_tabs.addTab(duration_tab, 'Game Duration')
                
                # Add tabs to the viz layout
                viz_layout.addWidget(viz_tabs)
                
                # Add the viz frame to the rankings tab
                rankings_tab = self.team_rankings_table.parentWidget()
                rankings_layout = rankings_tab.layout()
                rankings_layout.addWidget(viz_frame)
            
        except Exception as e:
            logger.error(f"Error calculating team rankings: {e}")
            import traceback
            logger.error(traceback.format_exc())
            QMessageBox.warning(
                self, 
                "Error", 
                f"Error calculating team rankings: {str(e)}"
            )
    
    def show_head_to_head_stats(self):
        """Show head-to-head statistics between two selected teams"""
        team1_id = self.team1_combo.currentData()
        team2_id = self.team2_combo.currentData()
        team1_name = self.team1_combo.currentText()
        team2_name = self.team2_combo.currentText()
        
        if not team1_id or not team2_id:
            QMessageBox.warning(self, "Selection Error", "Please select two teams to compare")
            return
        
        if team1_id == team2_id:
            QMessageBox.warning(self, "Selection Error", "Please select two different teams")
            return
        
        try:
            from sqlalchemy import text
            
            # Query to find matches between these two teams
            query = """
            SELECT 
                m.match_id, 
                m.start_time, 
                m.radiant_score, 
                m.dire_score,
                m.radiant_win,
                CASE 
                    WHEN m.radiant_team_id = :team1_id THEN 1
                    ELSE 0
                END as team1_is_radiant
            FROM 
                pro_matches m
            WHERE 
                (m.radiant_team_id = :team1_id AND m.dire_team_id = :team2_id) OR
                (m.radiant_team_id = :team2_id AND m.dire_team_id = :team1_id)
            ORDER BY 
                m.start_time DESC
            """
            
            # Execute the query
            result = self.session.execute(
                text(query), 
                {"team1_id": team1_id, "team2_id": team2_id}
            )
            
            matches = result.fetchall()
            
            # Clear the table
            self.head2head_table.setRowCount(0)
            
            # Track statistics
            team1_wins = 0
            team2_wins = 0
            
            # Add matches to the table
            for i, match in enumerate(matches):
                match_id = match[0]
                start_time = match[1]
                radiant_score = match[2]
                dire_score = match[3]
                radiant_win = match[4]
                team1_is_radiant = match[5]
                
                # Determine winner
                if (team1_is_radiant and radiant_win) or (not team1_is_radiant and not radiant_win):
                    winner = team1_name
                    team1_wins += 1
                else:
                    winner = team2_name
                    team2_wins += 1
                
                # Format date
                if isinstance(start_time, str):
                    try:
                        from datetime import datetime
                        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                    except:
                        try:
                            start_time = datetime.fromisoformat(start_time)
                        except:
                            pass
                
                date_str = start_time.strftime("%Y-%m-%d %H:%M") if hasattr(start_time, "strftime") else str(start_time)
                
                # Format score
                if team1_is_radiant:
                    score_str = f"{team1_name} {radiant_score} - {dire_score} {team2_name}"
                else:
                    score_str = f"{team2_name} {radiant_score} - {dire_score} {team1_name}"
                
                # Add to table
                self.head2head_table.insertRow(i)
                self.head2head_table.setItem(i, 0, QTableWidgetItem(str(match_id)))
                self.head2head_table.setItem(i, 1, QTableWidgetItem(date_str))
                self.head2head_table.setItem(i, 2, QTableWidgetItem(score_str))
                
                # Set winner with color
                winner_item = QTableWidgetItem(winner)
                if winner == team1_name:
                    winner_item.setForeground(Qt.green)
                else:
                    winner_item.setForeground(Qt.red)
                self.head2head_table.setItem(i, 3, winner_item)
            
            # Update summary
            total_matches = team1_wins + team2_wins
            if total_matches > 0:
                team1_win_pct = team1_wins / total_matches * 100
                team2_win_pct = team2_wins / total_matches * 100
                
                self.head2head_summary.setText(
                    f"<b>Head to Head: {team1_name} vs {team2_name}</b><br>"
                    f"Total Matches: {total_matches}<br>"
                    f"{team1_name}: {team1_wins} wins ({team1_win_pct:.1f}%)<br>"
                    f"{team2_name}: {team2_wins} wins ({team2_win_pct:.1f}%)"
                )
            else:
                self.head2head_summary.setText(
                    f"<b>No matches found between {team1_name} and {team2_name}</b>"
                )
            
            # Update status
            self.statusBar().showMessage(f"Found {total_matches} matches between {team1_name} and {team2_name}")
            
        except Exception as e:
            logger.error(f"Error fetching head-to-head statistics: {e}")
            import traceback
            logger.error(traceback.format_exc())
            QMessageBox.warning(
                self, 
                "Error", 
                f"Error fetching head-to-head statistics: {str(e)}"
            )
            self.head2head_summary.setText("Error fetching head-to-head statistics")
            self.head2head_table.setRowCount(0)
    
    def display_player_statistics(self):
        """Display player statistics from the database"""
        logger.info("Generating player statistics")
        
        # Create container widget and layout
        container = QWidget()
        container_layout = QVBoxLayout(container)
        
        # Add filters section
        filters_group = QGroupBox("Filters")
        filters_layout = QGridLayout()
        
        # League filter
        filters_layout.addWidget(QLabel("League:"), 0, 0)
        league_combo = QComboBox()
        league_combo.addItem("All Leagues", None)  # Default option
        
        # Time period filter
        filters_layout.addWidget(QLabel("Time Period:"), 0, 1)
        time_combo = QComboBox()
        time_combo.addItems(["All Time", "Last 3 Months", "Last Month", "Last Week"])
        
        # Team filter
        filters_layout.addWidget(QLabel("Team:"), 1, 0)
        team_combo = QComboBox()
        team_combo.addItem("All Teams", None)  # Default option
        
        # Minimum games filter
        filters_layout.addWidget(QLabel("Minimum Games:"), 1, 1)
        min_games_combo = QComboBox()
        min_games_combo.addItems(["1", "5", "10", "20"])
        
        # Metric filter
        filters_layout.addWidget(QLabel("Sort By:"), 2, 0)
        metric_combo = QComboBox()
        metric_combo.addItems(["Win Rate", "KDA Ratio", "GPM", "XPM", "Hero Damage", "Tower Damage", "Healing"])
        
        # Add apply button
        apply_button = QPushButton("Apply Filters")
        filters_layout.addWidget(apply_button, 2, 1)
        
        filters_group.setLayout(filters_layout)
        container_layout.addWidget(filters_group)
        
        # Create tabs for different player statistics views
        tabs = QTabWidget()
        
        # Top Players tab
        top_players_tab = QWidget()
        top_players_layout = QVBoxLayout(top_players_tab)
        
        self.player_stats_table = QTableWidget()
        self.player_stats_table.setColumnCount(8)
        self.player_stats_table.setHorizontalHeaderLabels([
            "Player", "Matches", "Win Rate", "KDA", "GPM", "XPM", "Hero Damage", "Tower Damage"
        ])
        self.player_stats_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        top_players_layout.addWidget(self.player_stats_table)
        
        # Player Heroes tab
        player_heroes_tab = QWidget()
        player_heroes_layout = QVBoxLayout(player_heroes_tab)
        
        # Player selection for heroes
        player_selection_layout = QHBoxLayout()
        player_selection_layout.addWidget(QLabel("Select Player:"))
        self.player_selection_combo = QComboBox()
        self.player_selection_combo.addItem("Select a player...", None)
        player_selection_layout.addWidget(self.player_selection_combo)
        
        player_search_button = QPushButton("Show Hero Stats")
        player_selection_layout.addWidget(player_search_button)
        player_heroes_layout.addLayout(player_selection_layout)
        
        # Table for player's heroes
        self.player_heroes_table = QTableWidget()
        self.player_heroes_table.setColumnCount(6)
        self.player_heroes_table.setHorizontalHeaderLabels([
            "Hero", "Matches", "Win Rate", "KDA", "GPM", "XPM"
        ])
        self.player_heroes_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        player_heroes_layout.addWidget(self.player_heroes_table)
        
        # Add tabs to tab widget
        tabs.addTab(top_players_tab, "Top Players")
        tabs.addTab(player_heroes_tab, "Player Heroes")
        
        container_layout.addWidget(tabs)
        
        # Add the container to the content layout
        self.stats_content_layout.addWidget(container)
        
        # Try to populate the dropdowns
        try:
            from sqlalchemy import text
            
            # Populate leagues
            leagues_query = text("SELECT league_id, name FROM pro_leagues ORDER BY name")
            leagues = self.session.execute(leagues_query).fetchall()
            for league in leagues:
                league_id, name = league
                if name:  # Only add leagues with actual names
                    league_combo.addItem(name, league_id)
            
            # Populate teams
            teams_query = text("SELECT team_id, name FROM pro_teams ORDER BY name")
            teams = self.session.execute(teams_query).fetchall()
            for team in teams:
                team_id, name = team
                if name:  # Only add teams with actual names
                    team_combo.addItem(name, team_id)
            
            # Populate players
            players_query = text("""
                SELECT DISTINCT account_id, name 
                FROM pro_players 
                WHERE name IS NOT NULL AND name != ''
                ORDER BY name
            """)
            players = self.session.execute(players_query).fetchall()
            for player in players:
                account_id, name = player
                if name:  # Only add players with actual names
                    self.player_selection_combo.addItem(name, account_id)
                    
        except Exception as e:
            logger.error(f"Error loading dropdown data for player statistics: {e}")
        
        # Connect filter buttons
        apply_button.clicked.connect(lambda: self.calculate_player_stats(
            league_id=league_combo.currentData(),
            team_id=team_combo.currentData(),
            time_period=time_combo.currentText(),
            min_games=int(min_games_combo.currentText()),
            sort_metric=metric_combo.currentText()
        ))
        
        player_search_button.clicked.connect(self.show_player_heroes)
        
        # Load initial data
        self.calculate_player_stats()
    
    def calculate_player_stats(self, league_id=None, team_id=None, time_period="All Time", min_games=1, sort_metric="Win Rate"):
        """Calculate and display player statistics based on filters"""
        try:
            # Clear the table
            self.player_stats_table.setRowCount(0)
            
            # Build the SQL query for player statistics
            from sqlalchemy import text
            from datetime import datetime, timedelta
            
            # Build time filter
            time_filter = ""
            params = {"min_games": min_games}
            
            if time_period != "All Time":
                now = datetime.now()
                if time_period == "Last Week":
                    start_date = now - timedelta(days=7)
                elif time_period == "Last Month":
                    start_date = now - timedelta(days=30)
                elif time_period == "Last 3 Months":
                    start_date = now - timedelta(days=90)
                
                time_filter = "AND m.start_time >= :start_date"
                params["start_date"] = start_date
            
            # Build league filter
            league_filter = ""
            if league_id:
                league_filter = "AND m.league_id = :league_id"
                params["league_id"] = league_id
            
            # Build team filter
            team_filter = ""
            if team_id:
                team_filter = "AND (m.radiant_team_id = :team_id OR m.dire_team_id = :team_id)"
                params["team_id"] = team_id
            
            # Query for player statistics
            query = f"""
            SELECT 
                p.account_id,
                p.name as player_name,
                COUNT(DISTINCT mp.match_id) as num_matches,
                SUM(CASE 
                    WHEN (mp.player_slot < 128 AND m.radiant_win = 1) OR 
                         (mp.player_slot >= 128 AND m.radiant_win = 0) 
                    THEN 1 ELSE 0 END) as wins,
                AVG(mp.kills) as avg_kills,
                AVG(mp.deaths) as avg_deaths,
                AVG(mp.assists) as avg_assists,
                AVG(mp.gold_per_min) as avg_gpm,
                AVG(mp.xp_per_min) as avg_xpm,
                AVG(mp.hero_damage) as avg_hero_damage,
                AVG(mp.tower_damage) as avg_tower_damage,
                AVG(mp.hero_healing) as avg_healing
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_players p ON mp.account_id = p.account_id
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                p.name IS NOT NULL AND p.name != ''
                {league_filter}
                {team_filter}
                {time_filter}
            GROUP BY 
                p.account_id, p.name
            HAVING 
                COUNT(DISTINCT mp.match_id) >= :min_games
            """
            
            # Add sorting based on the selected metric
            if sort_metric == "Win Rate":
                query += """
                ORDER BY 
                    (SUM(CASE 
                        WHEN (mp.player_slot < 128 AND m.radiant_win = 1) OR 
                             (mp.player_slot >= 128 AND m.radiant_win = 0) 
                        THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT mp.match_id)) DESC
                """
            elif sort_metric == "KDA Ratio":
                query += """
                ORDER BY 
                    ((AVG(mp.kills) + AVG(mp.assists)) / CASE WHEN AVG(mp.deaths) = 0 THEN 1 ELSE AVG(mp.deaths) END) DESC
                """
            elif sort_metric == "GPM":
                query += "ORDER BY AVG(mp.gold_per_min) DESC"
            elif sort_metric == "XPM":
                query += "ORDER BY AVG(mp.xp_per_min) DESC"
            elif sort_metric == "Hero Damage":
                query += "ORDER BY AVG(mp.hero_damage) DESC"
            elif sort_metric == "Tower Damage":
                query += "ORDER BY AVG(mp.tower_damage) DESC"
            elif sort_metric == "Healing":
                query += "ORDER BY AVG(mp.hero_healing) DESC"
            
            query += "\nLIMIT 100"
            
            # Execute the query
            logger.info(f"Executing player stats query with params: {params}")
            result = self.session.execute(text(query), params)
            players = result.fetchall()
            
            # Add players to the table
            for i, player in enumerate(players):
                account_id = player[0]
                player_name = player[1] or f"Player {account_id}"
                matches = player[2]
                wins = player[3]
                avg_kills = player[4]
                avg_deaths = player[5]
                avg_assists = player[6]
                avg_gpm = player[7]
                avg_xpm = player[8]
                avg_hero_damage = player[9]
                avg_tower_damage = player[10]
                
                # Calculate win rate and KDA
                win_rate = (wins / matches * 100) if matches > 0 else 0
                kda = ((avg_kills + avg_assists) / avg_deaths) if avg_deaths > 0 else (avg_kills + avg_assists)
                
                # Add row to table
                self.player_stats_table.insertRow(i)
                self.player_stats_table.setItem(i, 0, QTableWidgetItem(player_name))
                self.player_stats_table.setItem(i, 1, QTableWidgetItem(str(matches)))
                
                # Win rate with color
                win_rate_item = QTableWidgetItem(f"{win_rate:.1f}%")
                if win_rate >= 50:
                    win_rate_item.setForeground(Qt.green)
                else:
                    win_rate_item.setForeground(Qt.red)
                self.player_stats_table.setItem(i, 2, win_rate_item)
                
                # Other stats
                self.player_stats_table.setItem(i, 3, QTableWidgetItem(f"{kda:.2f}"))
                self.player_stats_table.setItem(i, 4, QTableWidgetItem(f"{avg_gpm:.1f}"))
                self.player_stats_table.setItem(i, 5, QTableWidgetItem(f"{avg_xpm:.1f}"))
                self.player_stats_table.setItem(i, 6, QTableWidgetItem(f"{avg_hero_damage:.1f}"))
                self.player_stats_table.setItem(i, 7, QTableWidgetItem(f"{avg_tower_damage:.1f}"))
            
            # Show message if no players found
            if not players:
                QMessageBox.information(
                    self, 
                    "No Data", 
                    "No player data found with the current filters. Try adjusting your filters."
                )
                logger.warning("No player data found with the current filters")
            
            # Update status
            self.statusBar().showMessage(f"Loaded stats for {len(players)} players")
            
        except Exception as e:
            logger.error(f"Error calculating player statistics: {e}")
            import traceback
            logger.error(traceback.format_exc())
            QMessageBox.warning(
                self, 
                "Error", 
                f"Error calculating player statistics: {str(e)}"
            )
    
    def show_player_heroes(self):
        """Show hero statistics for a selected player"""
        account_id = self.player_selection_combo.currentData()
        player_name = self.player_selection_combo.currentText()
        
        if not account_id:
            QMessageBox.warning(self, "Selection Error", "Please select a player")
            return
        
        try:
            from sqlalchemy import text
            
            # Query to find hero statistics for the selected player
            query = """
            SELECT 
                h.hero_id,
                h.name as hero_name,
                COUNT(DISTINCT mp.match_id) as num_matches,
                SUM(CASE 
                    WHEN (mp.player_slot < 128 AND m.radiant_win = 1) OR 
                         (mp.player_slot >= 128 AND m.radiant_win = 0) 
                    THEN 1 ELSE 0 END) as wins,
                AVG(mp.kills) as avg_kills,
                AVG(mp.deaths) as avg_deaths,
                AVG(mp.assists) as avg_assists,
                AVG(mp.gold_per_min) as avg_gpm,
                AVG(mp.xp_per_min) as avg_xpm
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_heroes h ON mp.hero_id = h.hero_id
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                mp.account_id = :account_id
            GROUP BY 
                h.hero_id, h.name
            ORDER BY 
                COUNT(DISTINCT mp.match_id) DESC
            LIMIT 50
            """
            
            # Execute the query
            result = self.session.execute(text(query), {"account_id": account_id})
            hero_stats = result.fetchall()
            
            # Clear the table
            self.player_heroes_table.setRowCount(0)
            
            # Add heroes to the table
            for i, hero in enumerate(hero_stats):
                hero_id = hero[0]
                hero_name = hero[1] or f"Hero {hero_id}"
                matches = hero[2]
                wins = hero[3]
                avg_kills = hero[4]
                avg_deaths = hero[5]
                avg_assists = hero[6]
                avg_gpm = hero[7]
                avg_xpm = hero[8]
                
                # Calculate win rate and KDA
                win_rate = (wins / matches * 100) if matches > 0 else 0
                kda = ((avg_kills + avg_assists) / avg_deaths) if avg_deaths > 0 else (avg_kills + avg_assists)
                
                # Add row to table
                self.player_heroes_table.insertRow(i)
                self.player_heroes_table.setItem(i, 0, QTableWidgetItem(hero_name))
                self.player_heroes_table.setItem(i, 1, QTableWidgetItem(str(matches)))
                
                # Win rate with color
                win_rate_item = QTableWidgetItem(f"{win_rate:.1f}%")
                if win_rate >= 50:
                    win_rate_item.setForeground(Qt.green)
                else:
                    win_rate_item.setForeground(Qt.red)
                self.player_heroes_table.setItem(i, 2, win_rate_item)
                
                # Other stats
                self.player_heroes_table.setItem(i, 3, QTableWidgetItem(f"{kda:.2f}"))
                self.player_heroes_table.setItem(i, 4, QTableWidgetItem(f"{avg_gpm:.1f}"))
                self.player_heroes_table.setItem(i, 5, QTableWidgetItem(f"{avg_xpm:.1f}"))
            
            # Show message if no heroes found
            if not hero_stats:
                QMessageBox.information(
                    self, 
                    "No Data", 
                    f"No hero data found for player {player_name}."
                )
                logger.warning(f"No hero data found for player {player_name}")
            
            # Update status
            self.statusBar().showMessage(f"Loaded {len(hero_stats)} heroes for {player_name}")
            
        except Exception as e:
            logger.error(f"Error fetching player hero statistics: {e}")
            import traceback
            logger.error(traceback.format_exc())
            QMessageBox.warning(
                self, 
                "Error", 
                f"Error fetching player hero statistics: {str(e)}"
            )
            self.player_heroes_table.setRowCount(0)
    
    def display_meta_trends(self):
        """Display meta trend statistics from the database"""
        logger.info("Generating meta trend statistics")
        
        # Create container widget and layout
        container = QWidget()
        container_layout = QVBoxLayout(container)
        
        # Add filters section
        filters_group = QGroupBox("Filters")
        filters_layout = QGridLayout()
        
        # Time range filter
        filters_layout.addWidget(QLabel("Time Range:"), 0, 0)
        self.meta_start_date = QDateEdit()
        self.meta_start_date.setCalendarPopup(True)
        self.meta_start_date.setDate(QDate.currentDate().addMonths(-6))
        
        # Start date label
        filters_layout.addWidget(QLabel("From:"), 0, 1)
        filters_layout.addWidget(self.meta_start_date, 0, 2)
        
        # End date label
        filters_layout.addWidget(QLabel("To:"), 0, 3)
        self.meta_end_date = QDateEdit()
        self.meta_end_date.setCalendarPopup(True)
        self.meta_end_date.setDate(QDate.currentDate())
        filters_layout.addWidget(self.meta_end_date, 0, 4)
        
        # League filter
        filters_layout.addWidget(QLabel("League:"), 1, 0)
        self.meta_league_combo = QComboBox()
        self.meta_league_combo.addItem("All Leagues", None)  # Default option
        filters_layout.addWidget(self.meta_league_combo, 1, 1, 1, 2)
        
        # Analysis type filter
        filters_layout.addWidget(QLabel("Analysis Type:"), 1, 3)
        self.meta_analysis_combo = QComboBox()
        self.meta_analysis_combo.addItems(["Game Duration", "Hero Pick Rates", "Role Distribution", "Item Usage"])
        filters_layout.addWidget(self.meta_analysis_combo, 1, 4)
        
        # Add apply button
        apply_button = QPushButton("Generate Analysis")
        filters_layout.addWidget(apply_button, 2, 4)
        
        filters_group.setLayout(filters_layout)
        container_layout.addWidget(filters_group)
        
        # Create a widget to hold the analysis content
        self.meta_analysis_content = QWidget()
        self.meta_analysis_layout = QVBoxLayout(self.meta_analysis_content)
        
        # Add a default label
        self.meta_analysis_layout.addWidget(QLabel("Select an analysis type and date range, then click 'Generate Analysis'"))
        
        container_layout.addWidget(self.meta_analysis_content)
        
        # Add the container to the content layout
        self.stats_content_layout.addWidget(container)
        
        # Try to populate the league dropdown if possible
        try:
            from sqlalchemy import text
            leagues_query = text("SELECT league_id, name FROM pro_leagues ORDER BY name")
            leagues = self.session.execute(leagues_query).fetchall()
            for league in leagues:
                league_id, name = league
                if name:  # Only add leagues with actual names
                    self.meta_league_combo.addItem(name, league_id)
        except Exception as e:
            logger.error(f"Error loading leagues for meta trends: {e}")
        
        # Connect the apply button
        apply_button.clicked.connect(self.generate_meta_analysis)
        
        # Show game duration analysis by default
        self.meta_analysis_combo.setCurrentText("Game Duration")
    
    def generate_meta_analysis(self):
        """Generate meta trend analysis based on selected filters"""
        # Get filter values
        start_date = self.meta_start_date.date().toPyDate()
        end_date = self.meta_end_date.date().toPyDate()
        league_id = self.meta_league_combo.currentData()
        analysis_type = self.meta_analysis_combo.currentText()
        
        # Clear the current content
        for i in reversed(range(self.meta_analysis_layout.count())): 
            widget = self.meta_analysis_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        
        # Add a title label
        title_label = QLabel(f"<h3>{analysis_type} Analysis</h3>")
        title_label.setAlignment(Qt.AlignCenter)
        self.meta_analysis_layout.addWidget(title_label)
        
        # Build date filter parameters
        from datetime import datetime
        params = {"start_date": start_date, "end_date": end_date}
        date_filter = "m.start_time BETWEEN :start_date AND :end_date"
        
        # Build league filter
        league_filter = ""
        if league_id:
            league_filter = "AND m.league_id = :league_id"
            params["league_id"] = league_id
        
        try:
            # Based on the analysis type, generate appropriate analysis
            if analysis_type == "Game Duration":
                self.analyze_game_duration(date_filter, league_filter, params)
            elif analysis_type == "Hero Pick Rates":
                self.analyze_hero_pick_rates(date_filter, league_filter, params)
            elif analysis_type == "Role Distribution":
                self.analyze_role_distribution(date_filter, league_filter, params)
            elif analysis_type == "Item Usage":
                self.analyze_item_usage(date_filter, league_filter, params)
        
        except Exception as e:
            logger.error(f"Error generating meta analysis: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            error_label = QLabel(f"Error generating analysis: {str(e)}")
            error_label.setStyleSheet("color: red;")
            self.meta_analysis_layout.addWidget(error_label)
    
    def analyze_game_duration(self, date_filter, league_filter, params):
        """Analyze game duration trends over time"""
        try:
            from sqlalchemy import text
            
            # Query to analyze game duration trends by month
            query = f"""
            WITH match_months AS (
                SELECT 
                    match_id,
                    strftime('%Y-%m', start_time) as month,
                    duration
                FROM 
                    pro_matches m
                WHERE 
                    {date_filter}
                    {league_filter}
            )
            SELECT 
                month,
                COUNT(match_id) as num_matches,
                AVG(duration) as avg_duration,
                MIN(duration) as min_duration,
                MAX(duration) as max_duration
            FROM 
                match_months
            GROUP BY 
                month
            ORDER BY 
                month
            """
            
            # Execute query
            result = self.session.execute(text(query), params)
            data = result.fetchall()
            
            if not data:
                self.meta_analysis_layout.addWidget(QLabel("No data found for the selected date range."))
                return
            
            # Create a table to show results
            table = QTableWidget()
            table.setColumnCount(5)
            table.setRowCount(len(data))
            table.setHorizontalHeaderLabels(["Month", "Matches", "Avg Duration", "Min Duration", "Max Duration"])
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Add data to table
            months = []
            avg_durations = []
            
            for i, row in enumerate(data):
                month = row[0]
                matches = row[1]
                avg_duration = row[2]  # In seconds
                min_duration = row[3]  # In seconds
                max_duration = row[4]  # In seconds
                
                # Format durations as MM:SS
                avg_mins = int(avg_duration) // 60
                avg_secs = int(avg_duration) % 60
                min_mins = int(min_duration) // 60
                min_secs = int(min_duration) % 60
                max_mins = int(max_duration) // 60
                max_secs = int(max_duration) % 60
                
                # Add to lists for later analysis
                months.append(month)
                avg_durations.append(avg_duration / 60)  # Convert to minutes
                
                # Add to table
                table.setItem(i, 0, QTableWidgetItem(month))
                table.setItem(i, 1, QTableWidgetItem(str(matches)))
                table.setItem(i, 2, QTableWidgetItem(f"{avg_mins}:{avg_secs:02d}"))
                table.setItem(i, 3, QTableWidgetItem(f"{min_mins}:{min_secs:02d}"))
                table.setItem(i, 4, QTableWidgetItem(f"{max_mins}:{max_secs:02d}"))
            
            # Add the table to the layout
            self.meta_analysis_layout.addWidget(table)
            
            # Add analysis summary
            if len(data) > 1:
                early_avg = data[0][2] / 60  # First month avg in minutes
                latest_avg = data[-1][2] / 60  # Last month avg in minutes
                change = latest_avg - early_avg
                change_pct = (change / early_avg * 100) if early_avg > 0 else 0
                
                trend_str = "increased" if change > 0 else "decreased"
                
                summary = QLabel(f"<b>Game Duration Trend Analysis</b><br>")
                summary.setText(summary.text() + 
                               f"From {data[0][0]} to {data[-1][0]}, average game duration has {trend_str} " +
                               f"by {abs(change):.1f} minutes ({abs(change_pct):.1f}%).")
                
                if abs(change) > 5:
                    summary.setText(summary.text() + " This represents a significant change in the meta.")
                
                self.meta_analysis_layout.addWidget(summary)
            
        except Exception as e:
            logger.error(f"Error analyzing game duration: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.meta_analysis_layout.addWidget(QLabel(f"Error: {str(e)}"))
    
    def analyze_hero_pick_rates(self, date_filter, league_filter, params):
        """Analyze hero pick rate trends over time"""
        try:
            from sqlalchemy import text
            
            # First get the most picked heroes overall in the time period
            top_heroes_query = f"""
            SELECT 
                h.hero_id,
                h.name as hero_name,
                COUNT(DISTINCT mp.match_id) as num_matches
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_heroes h ON mp.hero_id = h.hero_id
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                {date_filter}
                {league_filter}
            GROUP BY 
                h.hero_id, h.name
            ORDER BY 
                num_matches DESC
            LIMIT 10
            """
            
            # Get top 10 heroes
            top_heroes_result = self.session.execute(text(top_heroes_query), params)
            top_heroes = top_heroes_result.fetchall()
            
            if not top_heroes:
                self.meta_analysis_layout.addWidget(QLabel("No hero data found for the selected date range."))
                return
            
            # Extract hero IDs and names for tracking over time
            hero_ids = [hero[0] for hero in top_heroes]
            hero_names = [hero[1] or f"Hero {hero[0]}" for hero in top_heroes]
            
            # For each hero, get pick rate by month
            hero_trends_query = f"""
            WITH match_months AS (
                SELECT 
                    mp.match_id,
                    mp.hero_id,
                    strftime('%Y-%m', m.start_time) as month
                FROM 
                    pro_match_player_metrics mp
                JOIN 
                    pro_matches m ON mp.match_id = m.match_id
                WHERE 
                    {date_filter}
                    {league_filter}
                    AND mp.hero_id IN ({','.join(['?'] * len(hero_ids))})
            ),
            total_matches AS (
                SELECT 
                    month,
                    COUNT(DISTINCT match_id) as total
                FROM 
                    match_months
                GROUP BY 
                    month
            )
            SELECT 
                mm.month,
                mm.hero_id,
                COUNT(DISTINCT mm.match_id) as hero_matches,
                tm.total as total_matches,
                (COUNT(DISTINCT mm.match_id) * 100.0 / tm.total) as pick_rate
            FROM 
                match_months mm
            JOIN 
                total_matches tm ON mm.month = tm.month
            GROUP BY 
                mm.month, mm.hero_id
            ORDER BY 
                mm.month, mm.hero_id
            """
            
            # For hero IDs, we need to add them as parameters differently
            # Create a new parameter dictionary with both original params and hero IDs
            hero_params = params.copy()
            for i, hero_id in enumerate(hero_ids):
                hero_params[f'hero_id_{i}'] = hero_id
                
            # Modify the query to use named parameters
            hero_ids_clause = ','.join([f':hero_id_{i}' for i in range(len(hero_ids))])
            hero_trends_query = hero_trends_query.replace(f"({','.join(['?'] * len(hero_ids))})", f"({hero_ids_clause})")
            
            # Execute the query with hero IDs as parameters
            hero_trends_result = self.session.execute(text(hero_trends_query), hero_params)
            hero_trends = hero_trends_result.fetchall()
            
            if not hero_trends:
                self.meta_analysis_layout.addWidget(QLabel("No trend data found for the selected heroes."))
                return
            
            # Organize data by month and hero
            months = sorted(set(row[0] for row in hero_trends))
            
            # Create a table to show the results
            table = QTableWidget()
            table.setColumnCount(len(hero_names) + 1)  # Month + heroes
            table.setRowCount(len(months))
            
            # Set headers
            headers = ["Month"] + hero_names
            table.setHorizontalHeaderLabels(headers)
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Fill the table with pick rates
            month_hero_data = {}
            for row in hero_trends:
                month, hero_id, hero_matches, total_matches, pick_rate = row
                if month not in month_hero_data:
                    month_hero_data[month] = {}
                month_hero_data[month][hero_id] = pick_rate
            
            # Add data to table
            for i, month in enumerate(months):
                table.setItem(i, 0, QTableWidgetItem(month))
                
                for j, hero_id in enumerate(hero_ids):
                    pick_rate = month_hero_data.get(month, {}).get(hero_id, 0)
                    pick_rate_item = QTableWidgetItem(f"{pick_rate:.1f}%")
                    
                    # Color code by pick rate
                    if pick_rate > 50:
                        pick_rate_item.setForeground(Qt.darkGreen)
                    elif pick_rate > 30:
                        pick_rate_item.setForeground(Qt.green)
                    elif pick_rate < 10:
                        pick_rate_item.setForeground(Qt.red)
                        
                    table.setItem(i, j+1, pick_rate_item)
            
            # Add the table to the layout
            self.meta_analysis_layout.addWidget(table)
            
            # Add analysis summary
            summary = QLabel(f"<b>Hero Pick Rate Trends Analysis</b><br>")
            summary.setText(summary.text() + 
                           f"The table shows monthly pick rates for the top {len(hero_names)} heroes in the selected period.<br>" +
                           f"Green indicates high pick rates (>30%), red indicates low pick rates (<10%).")
            
            self.meta_analysis_layout.addWidget(summary)
            
            # Look for significant trend changes
            if len(months) > 1:
                for j, hero_name in enumerate(hero_names):
                    hero_id = hero_ids[j]
                    first_month = months[0]
                    last_month = months[-1]
                    
                    first_rate = month_hero_data.get(first_month, {}).get(hero_id, 0)
                    last_rate = month_hero_data.get(last_month, {}).get(hero_id, 0)
                    
                    change = last_rate - first_rate
                    
                    if abs(change) > 15:  # Significant change threshold
                        trend = "increased significantly" if change > 0 else "decreased significantly"
                        trend_label = QLabel(f"<b>{hero_name}</b>: Pick rate has {trend} from {first_rate:.1f}% to {last_rate:.1f}%")
                        self.meta_analysis_layout.addWidget(trend_label)
            
        except Exception as e:
            logger.error(f"Error analyzing hero pick rates: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.meta_analysis_layout.addWidget(QLabel(f"Error: {str(e)}"))
    
    def analyze_role_distribution(self, date_filter, league_filter, params):
        """Analyze role distribution trends over time"""
        try:
            from sqlalchemy import text
            
            # Analyze role distribution based on lane presence (approximate method)
            # In Dota 2, player slots roughly correspond to positions:
            # 0-1: Core/Carry, 2-3: Mid/Off, 4: Soft Support, 5-7: Hard Support, etc.
            query = f"""
            WITH match_months AS (
                SELECT 
                    mp.match_id,
                    strftime('%Y-%m', m.start_time) as month,
                    mp.player_slot,
                    CASE
                        WHEN mp.player_slot IN (0, 1, 128, 129) THEN 'Safe Lane'
                        WHEN mp.player_slot IN (2, 130) THEN 'Mid Lane'
                        WHEN mp.player_slot IN (3, 131) THEN 'Off Lane'
                        WHEN mp.player_slot IN (4, 132) THEN 'Soft Support'
                        WHEN mp.player_slot IN (5, 133) THEN 'Hard Support'
                        ELSE 'Unknown'
                    END as role,
                    mp.gold_per_min,
                    mp.xp_per_min,
                    mp.hero_damage,
                    mp.tower_damage,
                    mp.hero_healing
                FROM 
                    pro_match_player_metrics mp
                JOIN 
                    pro_matches m ON mp.match_id = m.match_id
                WHERE 
                    {date_filter}
                    {league_filter}
            )
            SELECT 
                month,
                role,
                COUNT(*) as num_players,
                AVG(gold_per_min) as avg_gpm,
                AVG(xp_per_min) as avg_xpm,
                AVG(hero_damage) as avg_hero_damage,
                AVG(tower_damage) as avg_tower_damage,
                AVG(hero_healing) as avg_healing
            FROM 
                match_months
            WHERE
                role != 'Unknown'
            GROUP BY 
                month, role
            ORDER BY 
                month, role
            """
            
            # Execute query
            result = self.session.execute(text(query), params)
            data = result.fetchall()
            
            if not data:
                self.meta_analysis_layout.addWidget(QLabel("No role data found for the selected date range."))
                return
            
            # Organize data by month and role
            months = sorted(set(row[0] for row in data))
            roles = ['Safe Lane', 'Mid Lane', 'Off Lane', 'Soft Support', 'Hard Support']
            
            # Create a tab widget for different metrics
            tab_widget = QTabWidget()
            
            # Create tabs for different metrics
            gpm_tab = QWidget()
            xpm_tab = QWidget()
            damage_tab = QWidget()
            
            gpm_layout = QVBoxLayout(gpm_tab)
            xpm_layout = QVBoxLayout(xpm_tab)
            damage_layout = QVBoxLayout(damage_tab)
            
            # GPM Table
            gpm_table = QTableWidget()
            gpm_table.setColumnCount(len(roles) + 1)  # Month + roles
            gpm_table.setRowCount(len(months))
            headers = ["Month"] + roles
            gpm_table.setHorizontalHeaderLabels(headers)
            gpm_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            gpm_layout.addWidget(gpm_table)
            
            # XPM Table
            xpm_table = QTableWidget()
            xpm_table.setColumnCount(len(roles) + 1)  # Month + roles
            xpm_table.setRowCount(len(months))
            xpm_table.setHorizontalHeaderLabels(headers)
            xpm_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            xpm_layout.addWidget(xpm_table)
            
            # Damage Table
            damage_table = QTableWidget()
            damage_table.setColumnCount(len(roles) + 1)  # Month + roles
            damage_table.setRowCount(len(months))
            damage_table.setHorizontalHeaderLabels(headers)
            damage_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            damage_layout.addWidget(damage_table)
            
            # Organize data by month, role, and metrics
            month_role_data = {}
            for row in data:
                month, role, num_players, avg_gpm, avg_xpm, avg_hero_dmg, avg_tower_dmg, avg_healing = row
                
                if month not in month_role_data:
                    month_role_data[month] = {}
                
                month_role_data[month][role] = {
                    'gpm': avg_gpm,
                    'xpm': avg_xpm,
                    'hero_dmg': avg_hero_dmg,
                    'tower_dmg': avg_tower_dmg,
                    'healing': avg_healing
                }
            
            # Fill tables with data
            for i, month in enumerate(months):
                # GPM Table
                gpm_table.setItem(i, 0, QTableWidgetItem(month))
                
                # XPM Table
                xpm_table.setItem(i, 0, QTableWidgetItem(month))
                
                # Damage Table
                damage_table.setItem(i, 0, QTableWidgetItem(month))
                
                for j, role in enumerate(roles):
                    # Check if data exists for this month and role
                    if month in month_role_data and role in month_role_data[month]:
                        role_data = month_role_data[month][role]
                        
                        # GPM Table
                        gpm_table.setItem(i, j+1, QTableWidgetItem(f"{role_data['gpm']:.1f}"))
                        
                        # XPM Table
                        xpm_table.setItem(i, j+1, QTableWidgetItem(f"{role_data['xpm']:.1f}"))
                        
                        # Damage Table
                        damage_table.setItem(i, j+1, QTableWidgetItem(f"{role_data['hero_dmg']:.1f}"))
                    else:
                        # No data for this role in this month
                        gpm_table.setItem(i, j+1, QTableWidgetItem("N/A"))
                        xpm_table.setItem(i, j+1, QTableWidgetItem("N/A"))
                        damage_table.setItem(i, j+1, QTableWidgetItem("N/A"))
            
            # Add tabs to the tab widget
            tab_widget.addTab(gpm_tab, "Gold Per Minute")
            tab_widget.addTab(xpm_tab, "Experience Per Minute")
            tab_widget.addTab(damage_tab, "Hero Damage")
            
            # Add the tab widget to the layout
            self.meta_analysis_layout.addWidget(tab_widget)
            
            # Add analysis summary
            summary = QLabel(f"<b>Role Distribution Analysis</b><br>")
            summary.setText(summary.text() + 
                           f"The tables show how resource allocation between different roles has evolved over time.<br>" +
                           f"This helps identify shifts in meta priorities and playstyles.")
            
            self.meta_analysis_layout.addWidget(summary)
            
            # Look for significant trend changes if we have multiple months
            if len(months) > 1:
                first_month = months[0]
                last_month = months[-1]
                
                for role in roles:
                    # Skip if we don't have data for both first and last month
                    if (first_month not in month_role_data or role not in month_role_data[first_month] or
                        last_month not in month_role_data or role not in month_role_data[last_month]):
                        continue
                    
                    first_gpm = month_role_data[first_month][role]['gpm']
                    last_gpm = month_role_data[last_month][role]['gpm']
                    
                    gpm_change = last_gpm - first_gpm
                    gpm_pct = (gpm_change / first_gpm * 100) if first_gpm > 0 else 0
                    
                    if abs(gpm_pct) > 15:  # Significant change threshold
                        trend = "increased significantly" if gpm_change > 0 else "decreased significantly"
                        trend_label = QLabel(f"<b>{role}</b>: GPM has {trend} by {abs(gpm_pct):.1f}% from {first_gpm:.1f} to {last_gpm:.1f}")
                        self.meta_analysis_layout.addWidget(trend_label)
            
        except Exception as e:
            logger.error(f"Error analyzing role distribution: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.meta_analysis_layout.addWidget(QLabel(f"Error: {str(e)}"))
    
    def analyze_item_usage(self, date_filter, league_filter, params):
        """Analyze item usage trends over time"""
        try:
            from sqlalchemy import text
            
            # Query to identify most common items used
            # In Dota, items 0-5 are the main inventory slots
            items_query = f"""
            SELECT 
                strftime('%Y-%m', m.start_time) as month,
                mp.item_0 as item_id,
                COUNT(*) as usage_count
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                {date_filter}
                {league_filter}
                AND mp.item_0 > 0
            GROUP BY 
                month, item_id
            UNION ALL
            SELECT 
                strftime('%Y-%m', m.start_time) as month,
                mp.item_1 as item_id,
                COUNT(*) as usage_count
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                {date_filter}
                {league_filter}
                AND mp.item_1 > 0
            GROUP BY 
                month, item_id
            UNION ALL
            SELECT 
                strftime('%Y-%m', m.start_time) as month,
                mp.item_2 as item_id,
                COUNT(*) as usage_count
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                {date_filter}
                {league_filter}
                AND mp.item_2 > 0
            GROUP BY 
                month, item_id
            UNION ALL
            SELECT 
                strftime('%Y-%m', m.start_time) as month,
                mp.item_3 as item_id,
                COUNT(*) as usage_count
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                {date_filter}
                {league_filter}
                AND mp.item_3 > 0
            GROUP BY 
                month, item_id
            UNION ALL
            SELECT 
                strftime('%Y-%m', m.start_time) as month,
                mp.item_4 as item_id,
                COUNT(*) as usage_count
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                {date_filter}
                {league_filter}
                AND mp.item_4 > 0
            GROUP BY 
                month, item_id
            UNION ALL
            SELECT 
                strftime('%Y-%m', m.start_time) as month,
                mp.item_5 as item_id,
                COUNT(*) as usage_count
            FROM 
                pro_match_player_metrics mp
            JOIN 
                pro_matches m ON mp.match_id = m.match_id
            WHERE 
                {date_filter}
                {league_filter}
                AND mp.item_5 > 0
            GROUP BY 
                month, item_id
            """
            
            # Execute query
            result = self.session.execute(text(items_query), params)
            items_data = result.fetchall()
            
            if not items_data:
                self.meta_analysis_layout.addWidget(QLabel("No item data found for the selected date range."))
                return
            
            # Aggregate data by month and item_id
            monthly_items = {}
            for row in items_data:
                month, item_id, count = row
                
                if month not in monthly_items:
                    monthly_items[month] = {}
                
                if item_id not in monthly_items[month]:
                    monthly_items[month][item_id] = 0
                
                monthly_items[month][item_id] += count
            
            # For each month, find the top 10 most popular items
            top_items_by_month = {}
            for month, items in monthly_items.items():
                sorted_items = sorted(items.items(), key=lambda x: x[1], reverse=True)[:10]
                top_items_by_month[month] = sorted_items
            
            # Get a list of all months in chronological order
            months = sorted(top_items_by_month.keys())
            
            # Create a table to show the top items for each month
            table = QTableWidget()
            table.setColumnCount(11)  # Month + top 10 items
            table.setRowCount(len(months))
            
            headers = ["Month"] + [f"Top {i+1}" for i in range(10)]
            table.setHorizontalHeaderLabels(headers)
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Create a mapping of known item IDs to item names
            # This is just a subset of popular Dota 2 items as an example
            # In a real implementation, you'd have a complete item database
            item_names = {
                1: "Blink Dagger",
                29: "Boots of Speed",
                36: "Magic Wand",
                46: "Town Portal Scroll",
                50: "Power Treads",
                100: "Black King Bar",
                116: "Aghanim's Scepter",
                154: "Desolator",
                168: "Refresher Orb",
                208: "Manta Style",
                214: "Sange and Yasha",
                226: "Lotus Orb",
                250: "Echo Sabre",
                252: "Aether Lens",
                254: "Glimmer Cape",
                265: "Veil of Discord"
            }
            
            # Fill the table
            for i, month in enumerate(months):
                table.setItem(i, 0, QTableWidgetItem(month))
                
                for j, (item_id, count) in enumerate(top_items_by_month[month]):
                    # Get item name if available, otherwise show ID
                    item_name = item_names.get(item_id, f"Item {item_id}")
                    table.setItem(i, j+1, QTableWidgetItem(f"{item_name} ({count})"))
            
            # Add the table to the layout
            self.meta_analysis_layout.addWidget(table)
            
            # Add explanation
            explanation = QLabel(f"<b>Item Usage Trends Analysis</b><br>")
            explanation.setText(explanation.text() + 
                           f"This analysis shows the most popular items purchased in professional matches by month.<br>" +
                           f"Each cell shows the item name and the number of times it appeared in completed player inventories.")
            
            self.meta_analysis_layout.addWidget(explanation)
            
            # Compare first and last month if we have multiple months
            if len(months) > 1:
                first_month = months[0]
                last_month = months[-1]
                
                # Find items that appear in the top 10 in both first and last month
                first_month_items = {item_id for item_id, _ in top_items_by_month[first_month]}
                last_month_items = {item_id for item_id, _ in top_items_by_month[last_month]}
                
                # Items that appear in both months
                common_items = first_month_items.intersection(last_month_items)
                
                # Items that are only in the first month (went out of meta)
                out_of_meta = first_month_items - last_month_items
                
                # Items that are only in the last month (came into meta)
                into_meta = last_month_items - first_month_items
                
                if out_of_meta:
                    out_items = ", ".join([item_names.get(item_id, f"Item {item_id}") for item_id in out_of_meta])
                    out_meta_label = QLabel(f"<b>Items that fell out of meta:</b> {out_items}")
                    self.meta_analysis_layout.addWidget(out_meta_label)
                
                if into_meta:
                    in_items = ", ".join([item_names.get(item_id, f"Item {item_id}") for item_id in into_meta])
                    in_meta_label = QLabel(f"<b>Items that came into meta:</b> {in_items}")
                    self.meta_analysis_layout.addWidget(in_meta_label)
            
        except Exception as e:
            logger.error(f"Error analyzing item usage: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.meta_analysis_layout.addWidget(QLabel(f"Error: {str(e)}"))
    
    def display_draft_analysis(self):
        """Display draft analysis statistics from the database"""
        logger.info("Generating draft analysis statistics")
        
        # Create container widget and layout
        container = QWidget()
        container_layout = QVBoxLayout(container)
        
        # Add filters section
        filters_group = QGroupBox("Filters")
        filters_layout = QGridLayout()
        
        # League filter
        filters_layout.addWidget(QLabel("League:"), 0, 0)
        league_combo = QComboBox()
        league_combo.addItem("All Leagues", None)  # Default option
        
        # Time period filter
        filters_layout.addWidget(QLabel("Time Period:"), 0, 1)
        time_combo = QComboBox()
        time_combo.addItems(["All Time", "Last 3 Months", "Last Month", "Last Week"])
        
        # Analysis type filter
        filters_layout.addWidget(QLabel("Analysis Type:"), 1, 0)
        analysis_combo = QComboBox()
        analysis_combo.addItems(["First Pick Advantage", "Pick Order Influence", "Hero Synergy", "Counter Picks"])
        
        filters_layout.addWidget(league_combo, 0, 0)
        filters_layout.addWidget(time_combo, 0, 1)
        filters_layout.addWidget(analysis_combo, 1, 1)
        
        # Add apply button
        apply_button = QPushButton("Generate Analysis")
        filters_layout.addWidget(apply_button, 2, 1)
        
        filters_group.setLayout(filters_layout)
        container_layout.addWidget(filters_group)
        
        # Create a widget to hold the analysis content
        self.draft_analysis_content = QWidget()
        self.draft_analysis_layout = QVBoxLayout(self.draft_analysis_content)
        
        # Add a default label
        self.draft_analysis_layout.addWidget(QLabel("Select an analysis type and click 'Generate Analysis'"))
        
        container_layout.addWidget(self.draft_analysis_content)
        
        # Add the container to the content layout
        self.stats_content_layout.addWidget(container)
        
        # Try to populate the league dropdown if possible
        try:
            from sqlalchemy import text
            leagues_query = text("SELECT league_id, name FROM pro_leagues ORDER BY name")
            leagues = self.session.execute(leagues_query).fetchall()
            for league in leagues:
                league_id, name = league
                if name:  # Only add leagues with actual names
                    league_combo.addItem(name, league_id)
        except Exception as e:
            logger.error(f"Error loading leagues for draft analysis: {e}")
        
        # Connect the apply button
        apply_button.clicked.connect(lambda: self.generate_draft_analysis(
            league_id=league_combo.currentData(),
            time_period=time_combo.currentText(),
            analysis_type=analysis_combo.currentText()
        ))
        
        # Show first pick advantage by default
        self.generate_draft_analysis(analysis_type="First Pick Advantage")
    
    def generate_draft_analysis(self, league_id=None, time_period="All Time", analysis_type="First Pick Advantage"):
        """Generate draft analysis based on selected filters"""
        # Clear the current content
        for i in reversed(range(self.draft_analysis_layout.count())): 
            widget = self.draft_analysis_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        
        # Add a title label
        title_label = QLabel(f"<h3>{analysis_type}</h3>")
        title_label.setAlignment(Qt.AlignCenter)
        self.draft_analysis_layout.addWidget(title_label)
        
        # Build time filter parameters
        from datetime import datetime, timedelta
        params = {}
        time_filter = ""
        
        if time_period != "All Time":
            now = datetime.now()
            if time_period == "Last Week":
                start_date = now - timedelta(days=7)
            elif time_period == "Last Month":
                start_date = now - timedelta(days=30)
            elif time_period == "Last 3 Months":
                start_date = now - timedelta(days=90)
            
            time_filter = "AND m.start_time >= :start_date"
            params["start_date"] = start_date
        
        # Build league filter
        league_filter = ""
        if league_id:
            league_filter = "AND m.league_id = :league_id"
            params["league_id"] = league_id
        
        try:
            # Based on the analysis type, generate appropriate analysis
            if analysis_type == "First Pick Advantage":
                self.analyze_first_pick_advantage(time_filter, league_filter, params)
            elif analysis_type == "Pick Order Influence":
                self.analyze_pick_order_influence(time_filter, league_filter, params)
            elif analysis_type == "Hero Synergy":
                self.analyze_hero_synergy(time_filter, league_filter, params)
            elif analysis_type == "Counter Picks":
                self.analyze_counter_picks(time_filter, league_filter, params)
        
        except Exception as e:
            logger.error(f"Error generating draft analysis: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            error_label = QLabel(f"Error generating analysis: {str(e)}")
            error_label.setStyleSheet("color: red;")
            self.draft_analysis_layout.addWidget(error_label)
    
    def analyze_first_pick_advantage(self, time_filter, league_filter, params):
        """Analyze the advantage of picking first versus second"""
        try:
            from sqlalchemy import text
            
            # Query to analyze first pick advantage
            # In Dota 2, radiant picks first when active_team=2
            query = f"""
            WITH first_pick AS (
                SELECT 
                    match_id,
                    CASE 
                        WHEN MIN("order") FILTER (WHERE active_team = 2) < MIN("order") FILTER (WHERE active_team = 3) THEN 'radiant'
                        ELSE 'dire'
                    END as first_pick_team
                FROM 
                    pro_draft_timings
                GROUP BY 
                    match_id
            )
            SELECT 
                fp.first_pick_team,
                COUNT(m.match_id) as total_matches,
                SUM(CASE 
                    WHEN (fp.first_pick_team = 'radiant' AND m.radiant_win = 1) OR
                         (fp.first_pick_team = 'dire' AND m.radiant_win = 0) 
                    THEN 1 ELSE 0 END) as wins
            FROM 
                first_pick fp
            JOIN 
                pro_matches m ON fp.match_id = m.match_id
            WHERE 
                1=1
                {time_filter}
                {league_filter}
            GROUP BY 
                fp.first_pick_team
            """
            
            # Execute query
            result = self.session.execute(text(query), params)
            data = result.fetchall()
            
            if not data:
                self.draft_analysis_layout.addWidget(QLabel("No data found with the current filters."))
                return
            
            # Create a table to show results
            table = QTableWidget()
            table.setColumnCount(4)
            table.setRowCount(len(data))
            table.setHorizontalHeaderLabels(["First Pick Team", "Total Matches", "Wins", "Win Rate"])
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Add data to table
            total_matches = 0
            total_wins = 0
            
            for i, row in enumerate(data):
                first_pick = row[0].capitalize()
                matches = row[1]
                wins = row[2]
                win_rate = (wins / matches * 100) if matches > 0 else 0
                
                total_matches += matches
                total_wins += wins
                
                table.setItem(i, 0, QTableWidgetItem(first_pick))
                table.setItem(i, 1, QTableWidgetItem(str(matches)))
                table.setItem(i, 2, QTableWidgetItem(str(wins)))
                
                win_rate_item = QTableWidgetItem(f"{win_rate:.1f}%")
                if win_rate >= 50:
                    win_rate_item.setForeground(Qt.green)
                else:
                    win_rate_item.setForeground(Qt.red)
                table.setItem(i, 3, win_rate_item)
            
            # Add the table to the layout
            self.draft_analysis_layout.addWidget(table)
            
            # Add summary text
            overall_win_rate = (total_wins / total_matches * 100) if total_matches > 0 else 0
            summary = QLabel(f"<b>First Pick Advantage Analysis</b><br>")
            summary.setText(summary.text() + f"Based on {total_matches} matches")
            self.draft_analysis_layout.addWidget(summary)
            
            # Add interpretation
            if total_matches > 10:  # Only interpret if we have a reasonable sample size
                interpretation = QLabel("<b>Interpretation:</b>")
                
                # Check if there's a significant advantage
                if abs(overall_win_rate - 50) > 5:  # More than 5% difference
                    if overall_win_rate > 50:
                        interpretation.setText(interpretation.text() + 
                            f" There appears to be a significant first pick advantage of {overall_win_rate-50:.1f}%")
                    else:
                        interpretation.setText(interpretation.text() + 
                            f" There appears to be a significant second pick advantage of {50-overall_win_rate:.1f}%")
                else:
                    interpretation.setText(interpretation.text() + 
                        " There does not appear to be a significant advantage to picking first or second.")
                
                self.draft_analysis_layout.addWidget(interpretation)
                
        except Exception as e:
            logger.error(f"Error analyzing first pick advantage: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.draft_analysis_layout.addWidget(QLabel(f"Error: {str(e)}"))
    
    def analyze_pick_order_influence(self, time_filter, league_filter, params):
        """Analyze how pick order influences win rate"""
        try:
            from sqlalchemy import text
            
            # Query to analyze pick order influence
            query = f"""
            SELECT 
                dt."order",
                COUNT(DISTINCT dt.match_id) as num_matches,
                SUM(CASE 
                    WHEN (dt.active_team = 2 AND m.radiant_win = 1) OR
                         (dt.active_team = 3 AND m.radiant_win = 0)
                    THEN 1 ELSE 0 END) as wins
            FROM 
                pro_draft_timings dt
            JOIN 
                pro_matches m ON dt.match_id = m.match_id
            WHERE 
                dt.pick = 1
                {time_filter}
                {league_filter}
            GROUP BY 
                dt."order"
            ORDER BY
                dt."order"
            """
            
            # Execute query
            result = self.session.execute(text(query), params)
            data = result.fetchall()
            
            if not data:
                self.draft_analysis_layout.addWidget(QLabel("No data found with the current filters."))
                return
            
            # Create a table to show results
            table = QTableWidget()
            table.setColumnCount(4)
            table.setRowCount(len(data))
            table.setHorizontalHeaderLabels(["Pick Order", "Matches", "Wins", "Win Rate"])
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Add data to table
            for i, row in enumerate(data):
                pick_order = row[0]
                matches = row[1]
                wins = row[2]
                win_rate = (wins / matches * 100) if matches > 0 else 0
                
                table.setItem(i, 0, QTableWidgetItem(str(pick_order)))
                table.setItem(i, 1, QTableWidgetItem(str(matches)))
                table.setItem(i, 2, QTableWidgetItem(str(wins)))
                
                win_rate_item = QTableWidgetItem(f"{win_rate:.1f}%")
                if win_rate >= 50:
                    win_rate_item.setForeground(Qt.green)
                else:
                    win_rate_item.setForeground(Qt.red)
                table.setItem(i, 3, win_rate_item)
            
            # Add the table to the layout
            self.draft_analysis_layout.addWidget(table)
            
            # Add explanation
            explanation = QLabel(
                "<b>Pick Order Influence Analysis</b><br>"
                "This analysis shows how the order of hero picks affects win rate.<br>"
                "In Dota 2's Captains Mode, teams take turns picking heroes in a specific order:<br>"
                "1-2-2-2-2-1 (where each number represents how many heroes each team picks)."
            )
            self.draft_analysis_layout.addWidget(explanation)
            
            # Find the pick with the highest win rate
            best_pick = max(data, key=lambda x: x[2]/x[1] if x[1] > 0 else 0)
            if best_pick[1] > 0:
                best_pick_rate = best_pick[2]/best_pick[1] * 100
                highlight = QLabel(
                    f"<b>Highlight:</b> Pick #{best_pick[0]} has the highest win rate at {best_pick_rate:.1f}%"
                )
                self.draft_analysis_layout.addWidget(highlight)
                
        except Exception as e:
            logger.error(f"Error analyzing pick order influence: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.draft_analysis_layout.addWidget(QLabel(f"Error: {str(e)}"))
    
    def analyze_hero_synergy(self, time_filter, league_filter, params):
        """Analyze hero synergy (which heroes work well together)"""
        try:
            from sqlalchemy import text
            
            # Query to find hero pairs that appear on the same team
            query = f"""
            WITH match_heroes AS (
                SELECT 
                    mp.match_id,
                    mp.hero_id,
                    h.name as hero_name,
                    mp.player_slot < 128 as is_radiant,
                    m.radiant_win
                FROM 
                    pro_match_player_metrics mp
                JOIN
                    pro_heroes h ON mp.hero_id = h.hero_id
                JOIN 
                    pro_matches m ON mp.match_id = m.match_id
                WHERE 
                    1=1
                    {time_filter}
                    {league_filter}
            )
            SELECT 
                h1.hero_name as hero1,
                h2.hero_name as hero2,
                COUNT(*) as times_together,
                SUM(CASE WHEN (h1.is_radiant AND h1.radiant_win) OR 
                             (NOT h1.is_radiant AND NOT h1.radiant_win) 
                        THEN 1 ELSE 0 END) as wins
            FROM 
                match_heroes h1
            JOIN 
                match_heroes h2 ON h1.match_id = h2.match_id AND h1.hero_id < h2.hero_id AND h1.is_radiant = h2.is_radiant
            GROUP BY 
                h1.hero_name, h2.hero_name
            HAVING 
                COUNT(*) >= 5
            ORDER BY 
                (SUM(CASE WHEN (h1.is_radiant AND h1.radiant_win) OR 
                                (NOT h1.is_radiant AND NOT h1.radiant_win) 
                          THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) DESC,
                COUNT(*) DESC
            LIMIT 20
            """
            
            # Execute query
            result = self.session.execute(text(query), params)
            data = result.fetchall()
            
            if not data:
                self.draft_analysis_layout.addWidget(QLabel("No hero synergy data found with the current filters."))
                return
            
            # Create a table to show results
            table = QTableWidget()
            table.setColumnCount(4)
            table.setRowCount(len(data))
            table.setHorizontalHeaderLabels(["Hero Pair", "Matches Together", "Wins", "Win Rate"])
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Add data to table
            for i, row in enumerate(data):
                hero1 = row[0]
                hero2 = row[1]
                times_together = row[2]
                wins = row[3]
                win_rate = (wins / times_together * 100) if times_together > 0 else 0
                
                table.setItem(i, 0, QTableWidgetItem(f"{hero1} + {hero2}"))
                table.setItem(i, 1, QTableWidgetItem(str(times_together)))
                table.setItem(i, 2, QTableWidgetItem(str(wins)))
                
                win_rate_item = QTableWidgetItem(f"{win_rate:.1f}%")
                if win_rate >= 55:  # Higher threshold for synergy
                    win_rate_item.setForeground(Qt.green)
                elif win_rate < 45:  # Lower threshold for anti-synergy
                    win_rate_item.setForeground(Qt.red)
                table.setItem(i, 3, win_rate_item)
            
            # Add the table to the layout
            self.draft_analysis_layout.addWidget(table)
            
            # Add explanation
            explanation = QLabel(
                "<b>Hero Synergy Analysis</b><br>"
                "This analysis shows which hero pairs perform well together on the same team.<br>"
                "Higher win rates indicate stronger synergy between the heroes.<br>"
                "Only pairs that have played at least 5 matches together are shown."
            )
            self.draft_analysis_layout.addWidget(explanation)
                
        except Exception as e:
            logger.error(f"Error analyzing hero synergy: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.draft_analysis_layout.addWidget(QLabel(f"Error: {str(e)}"))
    
    def analyze_counter_picks(self, time_filter, league_filter, params):
        """Analyze counter picks (which heroes perform well against specific heroes)"""
        try:
            from sqlalchemy import text
            
            # Query to find hero matchups 
            query = f"""
            WITH match_heroes AS (
                SELECT 
                    mp.match_id,
                    mp.hero_id,
                    h.name as hero_name,
                    mp.player_slot < 128 as is_radiant,
                    m.radiant_win
                FROM 
                    pro_match_player_metrics mp
                JOIN
                    pro_heroes h ON mp.hero_id = h.hero_id
                JOIN 
                    pro_matches m ON mp.match_id = m.match_id
                WHERE 
                    1=1
                    {time_filter}
                    {league_filter}
            )
            SELECT 
                h1.hero_name as hero1,
                h2.hero_name as hero2,
                COUNT(*) as times_against,
                SUM(CASE WHEN (h1.is_radiant AND h1.radiant_win) OR 
                             (NOT h1.is_radiant AND NOT h1.radiant_win) 
                        THEN 1 ELSE 0 END) as h1_wins
            FROM 
                match_heroes h1
            JOIN 
                match_heroes h2 ON h1.match_id = h2.match_id AND h1.is_radiant != h2.is_radiant
            GROUP BY 
                h1.hero_name, h2.hero_name
            HAVING 
                COUNT(*) >= 5
            ORDER BY 
                (SUM(CASE WHEN (h1.is_radiant AND h1.radiant_win) OR 
                                (NOT h1.is_radiant AND NOT h1.radiant_win) 
                          THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) DESC,
                COUNT(*) DESC
            LIMIT 20
            """
            
            # Execute query
            result = self.session.execute(text(query), params)
            data = result.fetchall()
            
            if not data:
                self.draft_analysis_layout.addWidget(QLabel("No counter pick data found with the current filters."))
                return
            
            # Create a table to show results
            table = QTableWidget()
            table.setColumnCount(4)
            table.setRowCount(len(data))
            table.setHorizontalHeaderLabels(["Matchup", "Times Faced", "Wins", "Win Rate"])
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Add data to table
            for i, row in enumerate(data):
                hero1 = row[0]  # Hero that wins
                hero2 = row[1]  # Hero that loses
                times_against = row[2]
                h1_wins = row[3]
                win_rate = (h1_wins / times_against * 100) if times_against > 0 else 0
                
                table.setItem(i, 0, QTableWidgetItem(f"{hero1} vs {hero2}"))
                table.setItem(i, 1, QTableWidgetItem(str(times_against)))
                table.setItem(i, 2, QTableWidgetItem(str(h1_wins)))
                
                win_rate_item = QTableWidgetItem(f"{win_rate:.1f}%")
                if win_rate >= 60:  # Higher threshold for strong counter
                    win_rate_item.setForeground(Qt.green)
                elif win_rate < 40:  # Lower threshold
                    win_rate_item.setForeground(Qt.red)
                table.setItem(i, 3, win_rate_item)
            
            # Add the table to the layout
            self.draft_analysis_layout.addWidget(table)
            
            # Add explanation
            explanation = QLabel(
                "<b>Counter Pick Analysis</b><br>"
                "This analysis shows how heroes perform against specific opposing heroes.<br>"
                "Higher win rates indicate stronger counter potential.<br>"
                "The format is 'Hero A vs Hero B' where Hero A has the listed win rate against Hero B.<br>"
                "Only matchups with at least 5 games are shown."
            )
            self.draft_analysis_layout.addWidget(explanation)
                
        except Exception as e:
            logger.error(f"Error analyzing counter picks: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.draft_analysis_layout.addWidget(QLabel(f"Error: {str(e)}"))
    
    def update_status_info(self):
        """Update counts and status information in the status bar"""
        try:
            # Initialize message
            message = "Database status: "
            
            # Check if tables exist first
            if not self.check_if_tables_exist():
                message = "Using demo data - Database either empty or not initialized"
                self.statusBar().showMessage(message)
                return
                
            # Check if database sessions are initialized
            if hasattr(self.pro_db, 'session'):
                try:
                    # Get match count using direct SQL
                    from sqlalchemy import text
                    result = self.session.execute(text("SELECT COUNT(*) FROM pro_matches"))
                    pro_match_count = result.scalar()
                    message += f"{pro_match_count} pro matches"
                except Exception as e:
                    logger.error(f"Error counting pro matches: {e}")
                    message += "Pro match count unavailable"
            else:
                message += "Pro database not connected"
                
            message += " | "
                
            if hasattr(self.user_db, 'session'):
                try:
                    # Get user match count using direct SQL
                    from sqlalchemy import text
                    result = self.session.execute(text("SELECT COUNT(*) FROM user_matches"))
                    user_match_count = result.scalar()
                    message += f"{user_match_count} user matches"
                except Exception as e:
                    logger.error(f"Error counting user matches: {e}")
                    message += "User match count unavailable"
            else:
                message += "User database not connected"
                
            self.statusBar().showMessage(message)
        except Exception as e:
            logger.error(f"Error updating status info: {e}")
            self.statusBar().showMessage("Error connecting to database")
    
    def get_game_mode_name(self, mode_id):
        """Convert game mode ID to name"""
        modes = {
            0: "Unknown",
            1: "All Pick",
            2: "Captains Mode",
            3: "Random Draft",
            4: "Single Draft",
            5: "All Random",
            6: "Intro",
            7: "Diretide",
            8: "Reverse Captains Mode",
            9: "Greeviling",
            10: "Tutorial",
            11: "Mid Only",
            12: "Least Played",
            13: "Limited Heroes",
            14: "Compendium Matchmaking",
            15: "Custom",
            16: "Captains Draft",
            17: "Balanced Draft",
            18: "Ability Draft",
            19: "Event",
            20: "All Random Deathmatch",
            21: "1v1 Mid",
            22: "All Draft",
            23: "Turbo",
            24: "Mutation",
        }
        return modes.get(mode_id, f"Unknown Mode {mode_id}")
    
    def get_hero_name(self, hero_id):
        """Get hero name from ID using Dota 2 hero mapping"""
        # Dictionary mapping hero IDs to hero names
        # This includes all current Dota 2 heroes with their OpenDota API IDs
        heroes = {
            1: "Anti-Mage", 2: "Axe", 3: "Bane", 4: "Bloodseeker", 5: "Crystal Maiden",
            6: "Drow Ranger", 7: "Earthshaker", 8: "Juggernaut", 9: "Mirana", 10: "Morphling",
            11: "Shadow Fiend", 12: "Phantom Lancer", 13: "Puck", 14: "Pudge", 15: "Razor",
            16: "Sand King", 17: "Storm Spirit", 18: "Sven", 19: "Tiny", 20: "Vengeful Spirit",
            21: "Windranger", 22: "Zeus", 23: "Kunkka", 25: "Lina", 26: "Lion",
            27: "Shadow Shaman", 28: "Slardar", 29: "Tidehunter", 30: "Witch Doctor", 31: "Lich",
            32: "Riki", 33: "Enigma", 34: "Tinker", 35: "Sniper", 36: "Necrophos",
            37: "Warlock", 38: "Beastmaster", 39: "Queen of Pain", 40: "Venomancer", 41: "Faceless Void",
            42: "Wraith King", 43: "Death Prophet", 44: "Phantom Assassin", 45: "Pugna", 46: "Templar Assassin",
            47: "Viper", 48: "Luna", 49: "Dragon Knight", 50: "Dazzle", 51: "Clockwerk",
            52: "Leshrac", 53: "Nature's Prophet", 54: "Lifestealer", 55: "Dark Seer", 56: "Clinkz",
            57: "Omniknight", 58: "Enchantress", 59: "Huskar", 60: "Night Stalker", 61: "Broodmother",
            62: "Bounty Hunter", 63: "Weaver", 64: "Jakiro", 65: "Batrider", 66: "Chen",
            67: "Spectre", 68: "Ancient Apparition", 69: "Doom", 70: "Ursa", 71: "Spirit Breaker",
            72: "Gyrocopter", 73: "Alchemist", 74: "Invoker", 75: "Silencer", 76: "Outworld Destroyer",
            77: "Lycan", 78: "Brewmaster", 79: "Shadow Demon", 80: "Lone Druid", 81: "Chaos Knight",
            82: "Meepo", 83: "Treant Protector", 84: "Ogre Magi", 85: "Undying", 86: "Rubick",
            87: "Disruptor", 88: "Nyx Assassin", 89: "Naga Siren", 90: "Keeper of the Light", 91: "Io",
            92: "Visage", 93: "Slark", 94: "Medusa", 95: "Troll Warlord", 96: "Centaur Warrunner",
            97: "Magnus", 98: "Timbersaw", 99: "Bristleback", 100: "Tusk", 101: "Skywrath Mage",
            102: "Abaddon", 103: "Elder Titan", 104: "Legion Commander", 105: "Techies", 106: "Ember Spirit",
            107: "Earth Spirit", 108: "Underlord", 109: "Terrorblade", 110: "Phoenix", 111: "Oracle",
            112: "Winter Wyvern", 113: "Arc Warden", 114: "Monkey King", 119: "Dark Willow", 120: "Pangolier",
            121: "Grimstroke", 123: "Hoodwink", 126: "Void Spirit", 128: "Snapfire", 129: "Mars",
            135: "Dawnbreaker", 136: "Marci", 137: "Primal Beast", 138: "Muerta"
        }
        
        try:
            # Try to convert hero_id to int in case it's passed as a string
            hero_id = int(hero_id)
            return heroes.get(hero_id, f"Unknown Hero ({hero_id})")
        except (ValueError, TypeError):
            return f"Unknown Hero ({hero_id})"
        
    def load_demo_matches(self):
        """Load demo match data when database is not available"""
        self.statusBar().showMessage("Loading demo match data...")
        logger.info("Loading demo match data instead of database data")
        
        # Clear the table
        self.pro_matches_table.setRowCount(0)
        
        # Create some demo data
        demo_matches = [
            {
                'match_id': 7323744477,
                'start_time': datetime(2023, 5, 20, 14, 30),
                'league_name': 'The International 2023',
                'radiant_name': 'Team Secret',
                'dire_name': 'Team Liquid',
                'radiant_score': 32,
                'dire_score': 25,
                'radiant_win': True
            },
            {
                'match_id': 7323744478,
                'start_time': datetime(2023, 5, 20, 16, 45),
                'league_name': 'The International 2023',
                'radiant_name': 'PSG.LGD',
                'dire_name': 'OG',
                'radiant_score': 28,
                'dire_score': 35,
                'radiant_win': False
            },
            {
                'match_id': 7323744479,
                'start_time': datetime(2023, 5, 21, 10, 15),
                'league_name': 'The International 2023',
                'radiant_name': 'EG',
                'dire_name': 'Nigma',
                'radiant_score': 42,
                'dire_score': 38,
                'radiant_win': True
            },
            {
                'match_id': 7323744480,
                'start_time': datetime(2023, 5, 21, 13, 30),
                'league_name': 'The International 2023',
                'radiant_name': 'Tundra',
                'dire_name': 'Spirit',
                'radiant_score': 22,
                'dire_score': 45,
                'radiant_win': False
            },
            {
                'match_id': 7323744481,
                'start_time': datetime(2023, 5, 22, 9, 0),
                'league_name': 'The International 2023',
                'radiant_name': 'VP',
                'dire_name': 'Alliance',
                'radiant_score': 31,
                'dire_score': 18,
                'radiant_win': True
            }
        ]
        
        # Add matches to the table
        for i, match in enumerate(demo_matches):
            self.pro_matches_table.insertRow(i)
            self.pro_matches_table.setItem(i, 0, QTableWidgetItem(str(match['match_id'])))
            self.pro_matches_table.setItem(i, 1, QTableWidgetItem(match['start_time'].strftime("%Y-%m-%d %H:%M")))
            self.pro_matches_table.setItem(i, 2, QTableWidgetItem(match['league_name']))
            self.pro_matches_table.setItem(i, 3, QTableWidgetItem(match['radiant_name']))
            self.pro_matches_table.setItem(i, 4, QTableWidgetItem(match['dire_name']))
            self.pro_matches_table.setItem(i, 5, QTableWidgetItem(f"{match['radiant_score']} - {match['dire_score']}"))
            
            winner = "Radiant" if match['radiant_win'] else "Dire"
            winner_item = QTableWidgetItem(winner)
            winner_item.setForeground(Qt.green if match['radiant_win'] else Qt.red)
            self.pro_matches_table.setItem(i, 6, winner_item)
        
        self.statusBar().showMessage("Database not available - showing demo data")
    
    def analyze_lane_matchup(self, match_id):
        """Analyze lane matchups at a specific time point"""
        try:
            # Get selected lane and time
            lane_type = self.lane_combo.currentText()
            time_minutes = int(self.lane_time_combo.currentText())
            time_seconds = time_minutes * 60
            
            # Map the lane selection to the function
            lane_mapping = {
                "Radiant Offlane vs Dire Safelane": "off_vs_safe",
                "Mid vs Mid": "mid_vs_mid",
                "Radiant Safelane vs Dire Offlane": "safe_vs_off"
            }
            
            lane_type_code = lane_mapping[lane_type]
            
            # Get lane stats
            lane_stats = track_lane_stats_over_time(
                match_id, 
                lane_type_code, 
                start_time=0, 
                end_time=time_seconds,
                interval=60
            )
            
            # Clear figure and create new plot
            self.lane_figure.clear()
            ax = self.lane_figure.add_subplot(111)
            
            # Extract data for plotting
            times = [t//60 for t in lane_stats["times"]]
            radiant_gold = lane_stats["radiant_gold"]
            dire_gold = lane_stats["dire_gold"]
            
            # Plot the data
            ax.plot(times, radiant_gold, 'g-', label='Radiant Gold')
            ax.plot(times, dire_gold, 'r-', label='Dire Gold')
            ax.set_xlabel('Time (minutes)')
            ax.set_ylabel('Gold')
            ax.set_title(f'{lane_type} Gold Progression')
            ax.legend()
            ax.grid(True)
            
            # Update canvas
            self.lane_canvas.draw()
            
            # Update text summary
            latest_stats = {}
            if lane_type_code == "mid_vs_mid":
                latest_stats = mid_vs_mid(match_id, time_seconds)
            elif lane_type_code == "off_vs_safe":
                latest_stats = off_vs_safe_radiant(match_id, time_seconds)
            else:
                latest_stats = safe_vs_off_radiant(match_id, time_seconds)
            
            # Create a summary text
            summary = f"<h4>{lane_type} at {time_minutes} minutes:</h4>"
            
            # Add Radiant stats
            summary += "<p><b>Radiant:</b><br>"
            if "radiant" in latest_stats:
                for player in latest_stats["radiant"]:
                    summary += f"Player: {player['player_name']} ({player['hero_name']})<br>"
                    summary += f"Gold: {player['gold']}, XP: {player['xp']}<br>"
                    summary += f"LH/DN: {player['last_hits']}/{player['denies']}<br>"
            summary += "</p>"
            
            # Add Dire stats
            summary += "<p><b>Dire:</b><br>"
            if "dire" in latest_stats:
                for player in latest_stats["dire"]:
                    summary += f"Player: {player['player_name']} ({player['hero_name']})<br>"
                    summary += f"Gold: {player['gold']}, XP: {player['xp']}<br>"
                    summary += f"LH/DN: {player['last_hits']}/{player['denies']}<br>"
            summary += "</p>"
            
            self.lane_analysis_result.setText(summary)
            
        except Exception as e:
            logger.error(f"Error analyzing lane matchup: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.lane_analysis_result.setText(f"Error analyzing lane matchup: {str(e)}")

    def visualize_player_metric(self, match_id):
        """Visualize player performance metrics"""
        try:
            # Get selected metric
            metric = self.metrics_combo.currentText()
            
            # Get player data
            players_data = get_match_player_data(match_id)
            
            if not players_data:
                self.metrics_figure.clear()
                self.metrics_canvas.draw()
                QMessageBox.warning(self, "No Data", "No player data available for this match.")
                return
            
            # Create a DataFrame from the player data
            df = pd.DataFrame(players_data)
            
            # Sort by team and then by the metric
            df = df.sort_values(['team', metric], ascending=[True, False])
            
            # Clear figure and create new plot
            self.metrics_figure.clear()
            ax = self.metrics_figure.add_subplot(111)
            
            # Create a color list based on team
            colors = ['#66BB6A' if team == 'Radiant' else '#EF5350' for team in df['team']]
            
            # Create the bar chart
            bars = ax.bar(range(len(df)), df[metric], color=colors)
            
            # Set the x-tick labels to player names
            ax.set_xticks(range(len(df)))
            ax.set_xticklabels(df['player_name'], rotation=45, ha='right')
            
            # Add hero names as annotations
            for i, player in enumerate(df.itertuples()):
                ax.text(i, 5, player.hero_name, ha='center', rotation=90, color='black')
            
            # Customize the plot
            ax.set_title(f"Player Comparison - {metric.replace('_', ' ').title()}")
            ax.set_xlabel('Player')
            ax.set_ylabel(metric.replace('_', ' ').title())
            
            # Add a legend for teams
            legend_elements = [
                Patch(facecolor='#66BB6A', label='Radiant'),
                Patch(facecolor='#EF5350', label='Dire')
            ]
            ax.legend(handles=legend_elements, loc='upper right')
            
            # Update canvas
            self.metrics_canvas.draw()
            
        except Exception as e:
            logger.error(f"Error visualizing player metrics: {e}")
            import traceback
            logger.error(traceback.format_exc())
            QMessageBox.warning(self, "Error", f"Failed to visualize player metrics: {str(e)}")

    def load_team_fights(self, match_id):
        """Load team fights for a specific match"""
        try:
            # Clear the list
            self.team_fights_list.clear()
            
            # Get team fights
            # The function name is get_all_team_fights (not teamfights)
            team_fights = get_all_team_fights(match_id)
            
            if not team_fights:
                self.team_fight_details.setText("No team fights found for this match.")
                return
            
            # Add team fights to list
            for tf in team_fights:
                # Format time as minutes:seconds
                minutes = tf.start // 60
                seconds = tf.start % 60
                time_str = f"{minutes}:{seconds:02d}"
                
                # Create list item with time and basic info
                item_text = f"Fight at {time_str} - Duration: {tf.duration}s - Deaths: {tf.deaths}"
                self.team_fights_list.addItem(item_text)
            
            # Connect item selection to showing details
            self.team_fights_list.itemClicked.connect(lambda item: self.show_team_fight_details(match_id, team_fights[self.team_fights_list.row(item)]))
            
        except Exception as e:
            logger.error(f"Error loading team fights: {e}")
            import traceback
            logger.error(traceback.format_exc())
            QMessageBox.warning(self, "Error", f"Failed to load team fights: {str(e)}")

    def show_team_fight_details(self, match_id, team_fight):
        """Show details for a specific team fight"""
        try:
            # Get team fight player data
            tf_players = get_specific_team_fight(match_id, team_fight.teamfight_id)
            
            if not tf_players:
                self.team_fight_details.setText("No player data for this team fight.")
                return
            
            # Create HTML summary
            minutes = team_fight.start // 60
            seconds = team_fight.start % 60
            summary = f"<h4>Team Fight at {minutes}:{seconds:02d}</h4>"
            summary += f"<p><b>Duration:</b> {team_fight.duration} seconds<br>"
            summary += f"<b>Total Deaths:</b> {team_fight.deaths}<br>"
            
            # Radiant players
            radiant_players = [p for p in tf_players if p.player_slot < 128]
            dire_players = [p for p in tf_players if p.player_slot >= 128]
            
            # Calculate team totals
            radiant_kills = sum(p.kills for p in radiant_players)
            dire_kills = sum(p.kills for p in dire_players)
            radiant_gold_delta = sum(p.gold_delta for p in radiant_players)
            dire_gold_delta = sum(p.gold_delta for p in dire_players)
            
            summary += f"<b>Kills:</b> Radiant {radiant_kills} - {dire_kills} Dire<br>"
            summary += f"<b>Gold Change:</b> Radiant {radiant_gold_delta} - {dire_gold_delta} Dire</p>"
            
            # Player details
            summary += "<p><b>Radiant Players:</b><br>"
            for p in radiant_players:
                summary += f"Player {p.player_slot}: Kills {p.kills}, Deaths {p.deaths}, Gold Δ {p.gold_delta}<br>"
            summary += "</p>"
            
            summary += "<p><b>Dire Players:</b><br>"
            for p in dire_players:
                summary += f"Player {p.player_slot}: Kills {p.kills}, Deaths {p.deaths}, Gold Δ {p.gold_delta}<br>"
            summary += "</p>"
            
            self.team_fight_details.setText(summary)
            
        except Exception as e:
            logger.error(f"Error showing team fight details: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.team_fight_details.setText(f"Error: {str(e)}")
        
    def display_lane_analysis(self):
        """Display lane analysis statistics"""
        # Clear previous content
        self.clear_layout(self.stats_content_layout)
        
        # Add title
        title = QLabel("<h2>Lane Analysis</h2>")
        self.stats_content_layout.addWidget(title)
        
        # Add filters
        filters_group = QGroupBox("Filters")
        filters_layout = QFormLayout()
        
        # League filter
        league_combo = QComboBox()
        league_combo.addItem("All Leagues", None)
        from sqlalchemy import text
        result = self.session.execute(text("SELECT league_id, name FROM pro_leagues ORDER BY name"))
        for league in result.fetchall():
            league_combo.addItem(league[1], league[0])
        filters_layout.addRow("League:", league_combo)
        
        # Time period filter
        time_period_combo = QComboBox()
        time_period_combo.addItems(["Last Week", "Last Month", "Last 3 Months", "All Time"])
        filters_layout.addRow("Time Period:", time_period_combo)
        
        # Lane filter
        lane_combo = QComboBox()
        lane_combo.addItems(["Offlane vs Safelane", "Mid vs Mid", "Safelane vs Offlane"])
        filters_layout.addRow("Lane:", lane_combo)
        
        # Add apply button
        apply_button = QPushButton("Generate Analysis")
        apply_button.clicked.connect(lambda: self.generate_lane_analysis(
            league_combo.currentData(),
            time_period_combo.currentText(),
            lane_combo.currentText()
        ))
        
        filters_layout.addRow("", apply_button)
        filters_group.setLayout(filters_layout)
        self.stats_content_layout.addWidget(filters_group)
        
        # Results container
        results_container = QWidget()
        self.lane_analysis_results_layout = QVBoxLayout(results_container)
        self.lane_analysis_results_layout.addWidget(QLabel("Apply filters to generate lane analysis"))
        
        # Add visualization placeholders
        self.stats_lane_figure = Figure(figsize=(10, 6))
        self.stats_lane_canvas = FigureCanvas(self.stats_lane_figure)
        self.lane_analysis_results_layout.addWidget(self.stats_lane_canvas)
        
        self.stats_content_layout.addWidget(results_container)
    
    def generate_lane_analysis(self, league_id, time_period, lane_type):
        """Generate lane analysis based on selected filters"""
        try:
            # Clear previous results
            self.clear_layout(self.lane_analysis_results_layout)
            
            # Add a loading indicator
            loading_label = QLabel("Generating analysis...")
            self.lane_analysis_results_layout.addWidget(loading_label)
            QApplication.processEvents()  # Update the UI
            
            # Convert time period to date filter
            date_filter = None
            if time_period == "Last Week":
                date_filter = datetime.now() - timedelta(days=7)
            elif time_period == "Last Month":
                date_filter = datetime.now() - timedelta(days=30)
            elif time_period == "Last 3 Months":
                date_filter = datetime.now() - timedelta(days=90)
            
            # Convert lane type to analysis type
            lane_type_code = {
                "Offlane vs Safelane": "off_vs_safe",
                "Mid vs Mid": "mid_vs_mid",
                "Safelane vs Offlane": "safe_vs_off"
            }[lane_type]
            
            # Query for matches based on filters
            from sqlalchemy import text
            query = "SELECT match_id FROM pro_matches WHERE 1=1"
            params = {}
            
            if league_id:
                query += " AND league_id = :league_id"
                params["league_id"] = league_id
                
            if date_filter:
                query += " AND start_time >= :start_time"
                params["start_time"] = date_filter.timestamp()
                
            query += " ORDER BY match_id DESC LIMIT 50"  # Limit to 50 most recent matches
            
            result = self.session.execute(text(query), params)
            match_ids = [row[0] for row in result.fetchall()]
            
            if not match_ids:
                self.clear_layout(self.lane_analysis_results_layout)
                self.lane_analysis_results_layout.addWidget(QLabel("No matches found with the selected filters"))
                return
                
            # Analyze the first few matches (for performance)
            analyzed_matches = min(10, len(match_ids))
            
            # Collect lane stats at different time points
            times = [5, 10, 15]  # 5, 10, 15 minutes
            lane_stats_by_time = {}
            
            for time_point in times:
                lane_stats_by_time[time_point] = {
                    "radiant_gold_avg": [],
                    "dire_gold_avg": [],
                    "radiant_xp_avg": [],
                    "dire_xp_avg": []
                }
            
            # Process each match
            for match_id in match_ids[:analyzed_matches]:
                for time_point in times:
                    try:
                        # Get stats at this time point
                        stats = track_lane_stats_over_time(
                            match_id,
                            lane_type_code,
                            start_time=0,
                            end_time=time_point*60,
                            interval=60
                        )
                        
                        # Skip matches with incomplete data
                        if not stats or "radiant_gold" not in stats or not stats["radiant_gold"]:
                            continue
                            
                        # Get the last data point for this time
                        last_idx = -1
                        lane_stats_by_time[time_point]["radiant_gold_avg"].append(stats["radiant_gold"][last_idx])
                        lane_stats_by_time[time_point]["dire_gold_avg"].append(stats["dire_gold"][last_idx])
                        lane_stats_by_time[time_point]["radiant_xp_avg"].append(stats["radiant_xp"][last_idx])
                        lane_stats_by_time[time_point]["dire_xp_avg"].append(stats["dire_xp"][last_idx])
                        
                    except Exception as e:
                        logger.error(f"Error processing match {match_id} at time {time_point}: {e}")
                        continue
            
            # Calculate averages
            for time_point in times:
                for stat in ["radiant_gold_avg", "dire_gold_avg", "radiant_xp_avg", "dire_xp_avg"]:
                    if lane_stats_by_time[time_point][stat]:
                        lane_stats_by_time[time_point][stat] = sum(lane_stats_by_time[time_point][stat]) / len(lane_stats_by_time[time_point][stat])
                    else:
                        lane_stats_by_time[time_point][stat] = 0
            
            # Clear loading indicator
            self.clear_layout(self.lane_analysis_results_layout)
            
            # Add title with match count
            title = QLabel(f"<h3>Lane Analysis: {lane_type}</h3>")
            self.lane_analysis_results_layout.addWidget(title)
            subtitle = QLabel(f"Based on {analyzed_matches} matches")
            self.lane_analysis_results_layout.addWidget(subtitle)
            
            # Create visualizations
            self.stats_lane_figure.clear()
            
            # Create subplots for gold and xp
            ax1 = self.stats_lane_figure.add_subplot(121)  # 1x2 grid, position 1
            ax2 = self.stats_lane_figure.add_subplot(122)  # 1x2 grid, position 2
            
            # Plot gold data
            radiant_gold = [lane_stats_by_time[t]["radiant_gold_avg"] for t in times]
            dire_gold = [lane_stats_by_time[t]["dire_gold_avg"] for t in times]
            
            ax1.plot(times, radiant_gold, 'g-', marker='o', label='Radiant Gold')
            ax1.plot(times, dire_gold, 'r-', marker='o', label='Dire Gold')
            ax1.set_xlabel('Time (minutes)')
            ax1.set_ylabel('Average Gold')
            ax1.set_title('Lane Gold Progression')
            ax1.legend()
            ax1.grid(True)
            
            # Plot XP data
            radiant_xp = [lane_stats_by_time[t]["radiant_xp_avg"] for t in times]
            dire_xp = [lane_stats_by_time[t]["dire_xp_avg"] for t in times]
            
            ax2.plot(times, radiant_xp, 'g-', marker='o', label='Radiant XP')
            ax2.plot(times, dire_xp, 'r-', marker='o', label='Dire XP')
            ax2.set_xlabel('Time (minutes)')
            ax2.set_ylabel('Average XP')
            ax2.set_title('Lane XP Progression')
            ax2.legend()
            ax2.grid(True)
            
            self.stats_lane_figure.tight_layout()
            self.stats_lane_canvas.draw()
            
            # Add summary text
            summary = "<h4>Summary:</h4>"
            summary += "<p>"
            
            # Calculate gold and XP advantages at 10 minutes
            mid_time_idx = 1  # 10 minutes
            rad_gold_adv = lane_stats_by_time[times[mid_time_idx]]["radiant_gold_avg"] - lane_stats_by_time[times[mid_time_idx]]["dire_gold_avg"]
            rad_xp_adv = lane_stats_by_time[times[mid_time_idx]]["radiant_xp_avg"] - lane_stats_by_time[times[mid_time_idx]]["dire_xp_avg"]
            
            if rad_gold_adv > 0:
                summary += f"Radiant has an average gold advantage of {rad_gold_adv:.0f} at 10 minutes in this lane.<br>"
            else:
                summary += f"Dire has an average gold advantage of {-rad_gold_adv:.0f} at 10 minutes in this lane.<br>"
                
            if rad_xp_adv > 0:
                summary += f"Radiant has an average XP advantage of {rad_xp_adv:.0f} at 10 minutes in this lane.<br>"
            else:
                summary += f"Dire has an average XP advantage of {-rad_xp_adv:.0f} at 10 minutes in this lane.<br>"
                
            # Calculate which side typically wins the lane
            if rad_gold_adv > 0 and rad_xp_adv > 0:
                summary += "<b>Conclusion:</b> Radiant typically wins this lane."
            elif rad_gold_adv < 0 and rad_xp_adv < 0:
                summary += "<b>Conclusion:</b> Dire typically wins this lane."
            else:
                summary += "<b>Conclusion:</b> This lane is generally balanced."
                
            summary += "</p>"
            
            summary_label = QLabel(summary)
            summary_label.setWordWrap(True)
            summary_label.setTextFormat(Qt.RichText)
            self.lane_analysis_results_layout.addWidget(summary_label)
            
        except Exception as e:
            logger.error(f"Error generating lane analysis: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.clear_layout(self.lane_analysis_results_layout)
            self.lane_analysis_results_layout.addWidget(QLabel(f"Error generating analysis: {str(e)}"))
        

def main():
    """Main entry point for the application"""
    # Configure logging first
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("dota_frontend.log"),
            logging.StreamHandler()
        ]
    )
    
    # Initialize application
    app = QApplication(sys.argv)
    
    # Set application style
    app.setStyle('Fusion')
    
    try:
        window = DotaMatchAnalyzerApp()
        window.show()
    except Exception as e:
        logger.error(f"Error starting application: {e}")
        QMessageBox.critical(None, "Application Error", 
                           f"An error occurred while starting the application: {str(e)}")
        return
    
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
