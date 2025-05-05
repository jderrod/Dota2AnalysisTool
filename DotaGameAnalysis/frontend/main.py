"""
Dota 2 Match Analyzer - Desktop Frontend

This application provides a desktop interface for exploring and analyzing
Dota 2 match data stored in the database.
"""
import sys
import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, relationship, scoped_session

# Add parent directory to path to import backend modules
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from PyQt5.QtWidgets import (QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
                             QVBoxLayout, QHBoxLayout, QFormLayout, QLabel,
                             QWidget, QPushButton, QTabWidget, QGroupBox,
                             QComboBox, QDateEdit, QHeaderView, QGridLayout,
                             QLineEdit, QMessageBox, QListWidget, QSplitter, QTreeWidget, QTreeWidgetItem)
from PyQt5.QtCore import Qt, QDate, QSize
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtGui import QIcon, QFont

# Import backend modules
from backend.database.database import DotaDatabase, Match, League, Team, Player, Hero, MatchPlayer
from backend.database.user_games_db import UserDotaDatabase, User, UserMatch, UserMatchPlayer

# Configure logger
logger = logging.getLogger(__name__)


class DotaMatchAnalyzerApp(QMainWindow):
    """Main application window for the Dota 2 Match Analyzer"""
    
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
        
        # Steam ID input
        steam_id_layout = QHBoxLayout()
        steam_id_layout.addWidget(QLabel("Steam ID:"))
        self.steam_id_input = QLineEdit()
        self.steam_id_input.setPlaceholderText("Enter Steam ID (e.g., 76561197960435530)")
        steam_id_layout.addWidget(self.steam_id_input)
        
        self.load_user_button = QPushButton("Load User Matches")
        self.load_user_button.clicked.connect(self.load_user_matches)
        steam_id_layout.addWidget(self.load_user_button)
        
        layout.addLayout(steam_id_layout)
        
        # User matches table
        self.user_matches_table = QTableWidget()
        self.user_matches_table.setColumnCount(6)
        self.user_matches_table.setHorizontalHeaderLabels([
            "Match ID", "Date", "Duration", "Game Mode", "Hero", "Result"
        ])
        self.user_matches_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.user_matches_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.user_matches_table.setSelectionMode(QTableWidget.SingleSelection)
        self.user_matches_table.itemDoubleClicked.connect(self.open_user_match_details)
        
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
            "Draft Analysis"
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
        """Load matches for a specific Steam user"""
        steam_id = self.steam_id_input.text().strip()
        
        if not steam_id:
            QMessageBox.warning(self, "Input Error", "Please enter a valid Steam ID")
            return
        
        self.statusBar().showMessage(f"Loading matches for Steam ID: {steam_id}...")
        
        try:
            # Get or create user
            user = self.user_db.get_or_create_user(steam_id)
            
            # Update matches (this will fetch new ones from API if available)
            self.user_db.update_user_matches(user, limit=20)
            
            # Get user matches from database
            matches = self.user_db.get_user_matches(user, limit=50)
            
            # Clear the table
            self.user_matches_table.setRowCount(0)
            
            # Add matches to the table
            for i, match in enumerate(matches):
                # Get the player data for this user
                player = next((p for p in match.players if p.account_id == user.account_id), None)
                
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
                    
                    # Result
                    player_team = "Radiant" if player.player_slot < 128 else "Dire"
                    player_won = (player_team == "Radiant" and match.radiant_win) or (player_team == "Dire" and not match.radiant_win)
                    
                    result_item = QTableWidgetItem("Won" if player_won else "Lost")
                    result_item.setForeground(Qt.green if player_won else Qt.red)
                    self.user_matches_table.setItem(i, 5, result_item)
            
            self.status_bar.showMessage(f"Loaded {len(matches)} matches for {user.username or steam_id}")
        
        except Exception as e:
            logger.error(f"Error loading user matches: {e}")
            self.status_bar.showMessage(f"Error: {str(e)}")
    
    def open_match_details(self, item):
        """Open detailed view for a professional match"""
        row = item.row()
        match_id = int(self.pro_matches_table.item(row, 0).text())
        
        # In a real implementation, you'd create a detailed match window
        # For now, just show a message
        QMessageBox.information(
            self, 
            "Match Details", 
            f"Detailed view for match {match_id} would appear here.\n\n"
            f"This would include drafts, player stats, gold/xp graphs, etc."
        )
    
    def open_user_match_details(self, item):
        """Open detailed view for a user match"""
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
        statistic = self.stats_list.item(row).text()
        
        # Clear the current content
        for i in reversed(range(self.stats_content_layout.count())): 
            widget = self.stats_content_layout.itemAt(i).widget()
            if widget:
                widget.deleteLater()
        
        # Add a label with the selected statistic
        title_label = QLabel(f"<h2>{statistic}</h2>")
        title_label.setAlignment(Qt.AlignCenter)
        self.stats_content_layout.addWidget(title_label)
        
        # Based on the selected statistic, display different content
        if statistic == "Heroes Win Rates":
            self.display_hero_win_rates()
        elif statistic == "Team Performance":
            self.display_team_performance()
        elif statistic == "Player Statistics":
            self.display_player_statistics()
        elif statistic == "Meta Trends":
            self.display_meta_trends()
        elif statistic == "Draft Analysis":
            self.display_draft_analysis()
    
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
            self.statusBar().showMessage(f"Loaded performance stats for {len(sorted_teams)} teams")
            
        except Exception as e:
            logger.error(f"Error calculating team statistics: {e}")
            import traceback
            logger.error(traceback.format_exc())
            QMessageBox.warning(
                self, 
                "Error", 
                f"Error calculating team statistics: {str(e)}"
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
                    # Get match count
                    pro_match_count = self.session.query(Match).count()
                    message += f"{pro_match_count} pro matches"
                except Exception as e:
                    logger.error(f"Error counting pro matches: {e}")
                    message += "Pro match count unavailable"
            else:
                message += "Pro database not connected"
                
            message += " | "
                
            if hasattr(self.user_db, 'session'):
                try:
                    # Get user match count
                    user_match_count = self.session.query(UserMatch).count()
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
        """Get hero name from ID (placeholder function)"""
        # In a real implementation, you would query the database for hero names
        # For now, return a placeholder
        return f"Hero {hero_id}"
        
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
