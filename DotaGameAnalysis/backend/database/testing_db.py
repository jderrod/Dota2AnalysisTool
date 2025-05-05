import os
import json
import logging
from datetime import datetime
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    ForeignKey,
    DateTime,
    JSON,
    UniqueConstraint
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
import requests
import time
from datetime import timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("database_pro_teams")

# Initialize SQLAlchemy Base
Base = declarative_base()

# -----------------------
# Database Schema Classes
# -----------------------

class League(Base):
    """League table for storing tournament information"""
    __tablename__ = 'leagues'
    
    id = Column(Integer, primary_key=True)
    league_id = Column(Integer, unique=True)
    name = Column(String(255))
    tier = Column(Integer)
    region = Column(String(50))
    
    # Relationships
    matches = relationship("Match", back_populates="league")
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    prize_pool = Column(Integer)
    
    def __repr__(self):
        return (f"<League(id={self.id}, name='{self.name}', tier={self.tier}, "
                f"start_date={self.start_date}, end_date={self.end_date}, "
                f"prize_pool={self.prize_pool})>")


class Team(Base):
    """Team table for storing team information"""
    __tablename__ = 'teams'
    
    id = Column(Integer, primary_key=True)
    rank = Column(Integer)
    team_id = Column(Integer, unique=True)
    name = Column(String(255))
    tag = Column(String(50))
    logo_url = Column(String(255), nullable=True)
    players = relationship("Player", back_populates="team")
    
    def __repr__(self):
        return f"<Team(id={self.id}, name='{self.name}')>"


class Match(Base):
    """Match table for storing basic match information"""
    __tablename__ = 'matches'
    
    id = Column(Integer, primary_key=True)
    version = Column(Integer)
    match_id = Column(Integer, unique=True)
    start_time = Column(DateTime)
    duration = Column(Integer)  # in seconds
    
    # League information
    league_id = Column(Integer, ForeignKey('leagues.league_id'))
    league = relationship("League", back_populates="matches")  # back_populates must match League.matches
    
    series_id = Column(Integer, nullable=True)
    series_type = Column(Integer, nullable=True)
    
    # Team information
    radiant_team_id = Column(Integer, ForeignKey('teams.team_id'), nullable=True)
    dire_team_id = Column(Integer, ForeignKey('teams.team_id'), nullable=True)
    radiant_gold_adv = Column(Integer, nullable=True)
    dire_gold_adv = Column(Integer, nullable=True)
    
    # Match results
    radiant_score = Column(Integer)
    dire_score = Column(Integer)
    radiant_win = Column(Boolean)
    
    # Game version
    game_version = Column(Integer, nullable=True)

    def __repr__(self):
        return (f"<Match(match_id={self.match_id}, "
                f"radiant_team_id={self.radiant_team_id}, "
                f"dire_team_id={self.dire_team_id})>")


class Player(Base):
    """Player table for storing player information"""
    __tablename__ = 'players'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.match_id'), nullable=True)
    account_id = Column(Integer, unique=True)
    name = Column(String(255))
    personaname = Column(String(255), nullable=True)
    country_code = Column(String(10), nullable=True)
    team_id = Column(Integer, ForeignKey('teams.team_id'), nullable=True)
    
    # Relationships
    match_players = relationship("MatchPlayer", back_populates="player")
    
    # Relationship to Team
    team = relationship("Team", back_populates="players")
    
    def __repr__(self):
        return f"<Player(id={self.id}, name='{self.name}')>"


class Hero(Base):
    """Hero table for storing hero information"""
    __tablename__ = 'heroes'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.match_id'), nullable=True)
    hero_id = Column(Integer, unique=True)
    name = Column(String(255))
    localized_name = Column(String(255))
    primary_attr = Column(String(10))
    attack_type = Column(String(50))
    roles = Column(JSON)  # Store roles as a JSON array
    
    def __repr__(self):
        return f"<Hero(id={self.id}, localized_name='{self.localized_name}')>"


class MatchPlayer(Base):
    """
    MatchPlayer table for storing player performance in a match.
    Uses a unique constraint on (match_id, account_id, hero_id) to avoid duplicates.
    """
    __tablename__ = 'match_player_metrics'
    __table_args__ = (
        UniqueConstraint('match_id', 'account_id', 'hero_id', name='uix_match_player'),
    )

    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.match_id'))
    account_id = Column(Integer, ForeignKey('players.account_id'))
    hero_id = Column(Integer, ForeignKey('heroes.hero_id'))
    player_slot = Column(Integer)  # 0-127 are Radiant, 128-255 are Dire
    
    # Basic performance metrics
    kills = Column(Integer)
    deaths = Column(Integer)
    assists = Column(Integer)
    last_hits = Column(Integer)
    denies = Column(Integer)
    gold_per_min = Column(Integer)
    xp_per_min = Column(Integer)
    hero_damage = Column(Integer)
    tower_damage = Column(Integer)
    hero_healing = Column(Integer)
    level = Column(Integer)
    
    # Additional detailed metrics
    total_gold = Column(Integer)         # Total gold earned
    total_xp = Column(Integer)           # Total experience earned
    neutral_kills = Column(Integer)      # Neutral creep kills
    tower_kills = Column(Integer)        # Number of towers killed
    courier_kills = Column(Integer)      # Courier kills
    lane_kills = Column(Integer)         # Lane kills
    hero_kills = Column(Integer)         # Hero kills (if tracked separately)
    observer_kills = Column(Integer)     # Observer kills
    sentry_kills = Column(Integer)       # Sentry kills
    stuns = Column(Float)                # Total stuns applied (in seconds or count)
    actions_per_min = Column(Float)      # Actions per minute
    
    # Benchmarks (raw percentage values for performance comparison)
    bench_gold_pct = Column(Float)       
    bench_xp_pct = Column(Float)
    bench_kills_pct = Column(Float)
    bench_last_hits_pct = Column(Float)
    bench_hero_damage_pct = Column(Float)
    bench_hero_healing_pct = Column(Float)
    bench_tower_damage_pct = Column(Float)
    
    # Items
    item_0 = Column(Integer)
    item_1 = Column(Integer)
    item_2 = Column(Integer)
    item_3 = Column(Integer)
    item_4 = Column(Integer)
    item_5 = Column(Integer)

    # Detailed logs stored as JSON
    purchase_log = Column(JSON, nullable=True)
    lane_pos = Column(JSON, nullable=True)
    kill_log = Column(JSON, nullable=True)
    
    # Relationship to Player
    player = relationship("Player", back_populates="match_players")


    def __repr__(self):
        return (f"<MatchPlayer(match_id={self.match_id}, account_id={self.account_id}, "
                f"hero_id={self.hero_id})>")


class DraftTiming(Base):
    __tablename__ = 'draft_timings'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.match_id'), nullable=True)
    order = Column(Integer, nullable=False)
    pick = Column(Boolean, nullable=False)
    active_team = Column(Integer, nullable=False)
    hero_id = Column(Integer, nullable=False)
    player_slot = Column(Integer, nullable=True)
    extra_time = Column(Integer, nullable=False)
    total_time_taken = Column(Integer, nullable=False)
    
    def __repr__(self):
        return (f"<DraftTiming(order={self.order}, pick={self.pick}, "
                f"active_team={self.active_team}, hero_id={self.hero_id}, "
                f"player_slot={self.player_slot}, extra_time={self.extra_time}, "
                f"total_time_taken={self.total_time_taken})>")



class TeamFight(Base):
    """Table for storing teamfight-level information in a match."""
    __tablename__ = 'teamfights'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer)  # Optionally add a ForeignKey('matches.match_id') later
    start = Column(Integer, nullable=False)      # In-game time when the teamfight started
    end = Column(Integer, nullable=False)        # In-game time when the teamfight ended
    last_death = Column(Integer, nullable=True)  # In-game time of the last death in the fight
    deaths = Column(Integer, nullable=False)     # Total number of deaths in the teamfight
    
    # Relationship to teamfight players
    players = relationship("TeamFightPlayer", back_populates="teamfight")
    
    def __repr__(self):
        return (f"<TeamFight(match_id={self.match_id}, start={self.start}, end={self.end}, "
                f"last_death={self.last_death}, deaths={self.deaths})>")


class TeamFightPlayer(Base):
    """Table for storing per-player statistics within a teamfight."""
    __tablename__ = 'teamfight_players'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.match_id'), nullable=True)
    teamfight_id = Column(Integer, ForeignKey('teamfights.id'))  # Link to the corresponding teamfight
    
    deaths_pos = Column(JSON, nullable=True)
    ability_uses = Column(JSON, nullable=True)
    ability_targets = Column(JSON, nullable=True)
    item_uses = Column(JSON, nullable=True)
    killed = Column(JSON, nullable=True)
    deaths = Column(Integer, nullable=False)
    buybacks = Column(Integer, nullable=False)
    damage = Column(Integer, nullable=False)
    healing = Column(Integer, nullable=False)
    gold_delta = Column(Integer, nullable=False)
    xp_delta = Column(Integer, nullable=False)
    xp_start = Column(Integer, nullable=False)
    xp_end = Column(Integer, nullable=False)
    
    # Relationship back to teamfight
    teamfight = relationship("TeamFight", back_populates="players")
    
    def __repr__(self):
        return (f"<TeamFightPlayer(teamfight_id={self.teamfight_id}, "
                f"deaths={self.deaths}, damage={self.damage}, healing={self.healing}, "
                f"ability_uses={self.ability_uses}, item_uses={self.item_uses}, "
                f"killed={self.killed}, buybacks={self.buybacks}, "
                f"gold_delta={self.gold_delta}, xp_delta={self.xp_delta})>")


class Objective(Base):
    __tablename__ = 'objectives'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer)  # Optionally add ForeignKey('matches.match_id') later
    time = Column(Integer, nullable=False)
    type = Column(String(100), nullable=False)
    slot = Column(Integer, nullable=True)
    key = Column(String(255), nullable=True)
    player_slot = Column(Integer, nullable=True)
    unit = Column(String(255), nullable=True)
    value = Column(Integer, nullable=True)
    killer = Column(Integer, nullable=True)
    team = Column(Integer, nullable=True)
    
    def __repr__(self):
        return f"<Objective(time={self.time}, type='{self.type}', key='{self.key}')>"


class ChatWheel(Base):
    __tablename__ = 'chatwheel'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer)  # Optionally add ForeignKey('matches.match_id') later
    time = Column(Integer, nullable=False)   # Time of the chat event
    type = Column(String(50), nullable=False)# Should be "chatwheel" for these events
    key = Column(String(50), nullable=False) # The key value (as a string, e.g., "142831")
    slot = Column(Integer, nullable=True)    # Chat slot (if available)
    player_slot = Column(Integer, nullable=True)
    
    def __repr__(self):
        return (f"<ChatWheel(match_id={self.match_id}, time={self.time}, "
                f"type='{self.type}', key='{self.key}', slot={self.slot}, "
                f"player_slot={self.player_slot})>")


class TimevsStats(Base):
    __tablename__ = 'timevsstats'
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.match_id'))
    player_id = Column(Integer, ForeignKey('players.id'))
    player_name = Column(String(100), nullable=False)
    player_slot = Column(Integer, nullable=False)
    time = Column(Integer, nullable=False)
    starting_lane = Column(Integer, nullable=False)
    gold = Column(Integer, nullable=True)
    last_hits = Column(Integer, nullable=True)
    denies = Column(Integer, nullable=True)
    xp = Column(Integer, nullable=True)
    event_type = Column(String(20), nullable=True)
    killed_hero = Column(String(100), nullable=True)
    purchased_item = Column(String(100), nullable=True)
    rune_type = Column(Integer, nullable=True)

    
    def __repr__(self):
        return (f"<TimevsStats(match_id={self.match_id}, player_slot={self.player_slot}, "
                f"time={self.time}, gold={self.gold}, last_hits={self.last_hits}, "
                f"denies={self.denies}, xp={self.xp}, event_type='{self.event_type}', "
                f"killed_hero='{self.killed_hero}', purchased_item='{self.purchased_item}', "
                f"rune_type={self.rune_type})>")


# ----------------------------
# Database Connection & Setup
# ----------------------------

class DotaDatabase:
    """
    A class to manage database operations for Dota 2 match data.
    """
    
    def __init__(self, db_url=None):
        """
        Initialize the database connection.
        
        Args:
            db_url (str, optional): Database connection URL.
                                    Defaults to a SQLite file 'dota_test.db' in a 'data' directory.
        """
        if not db_url:
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            db_url = f"sqlite:///{os.path.join(data_dir, 'dota_test.db')}"
            
        self.engine = create_engine(db_url, echo=False)
        self.Session = sessionmaker(bind=self.engine)
        logger.setLevel(logging.WARNING)
        
    def create_tables(self):
        """Create all database tables if they do not exist."""
        Base.metadata.create_all(self.engine)
        logger.info("Database tables created")
        
    def update_all_tables(self, days_to_look_back=7):
        """
        Update all tables in the database with the latest data from JSON files
        without dropping any tables or losing existing data.
        Also fetches recent matches from OpenDota API.
        
        Args:
            days_to_look_back (int): Number of days to look back for new matches. Default is 1 for daily updates.
        """
        # Now fetch recent matches from OpenDota API for all teams
        print(f"\nChecking for new matches from the past {days_to_look_back} day(s)...")
        OPENDOTA_API_BASE = "https://api.opendota.com/api"
        api_match_count = 0
        
        # Calculate the cutoff time for recent matches
        now = datetime.now()
        cutoff_time = now - timedelta(days=days_to_look_back)
        cutoff_timestamp = int(cutoff_time.timestamp())
        
        # Track newly added matches across all teams in this update run
        newly_added_match_ids = set()
        
        # Get all teams from database
        session = self.Session()
        teams = session.query(Team).all()
        session.close()
        
        for team in teams:
            print(f"Checking for recent matches for team: {team.name} (ID: {team.team_id})")
            
            try:
                # Get existing matches for this team to avoid duplicates
                session = self.Session()
                existing_match_ids = {
                    row[0] for row in session.query(Match.match_id).filter(
                        (Match.radiant_team_id == team.team_id) | 
                        (Match.dire_team_id == team.team_id)
                    ).all()
                }
                session.close()
                
                # Fetch recent matches from OpenDota API
                matches_url = f"{OPENDOTA_API_BASE}/teams/{team.team_id}/matches"
                response = requests.get(matches_url)
                
                if response.status_code != 200:
                    print(f"Error fetching matches for {team.name}: {response.status_code}")
                    continue
                    
                matches = response.json()
                
                # Filter matches by date and check if they're already in database
                new_match_ids = []
                
                for match in matches:
                    match_id = match.get("match_id")
                    start_time = match.get("start_time")
                    
                    # Skip if match is already in the database or has been added in this run
                    if match_id not in existing_match_ids and match_id not in newly_added_match_ids:
                        # Only add matches from the specified time period
                        if start_time and start_time >= cutoff_timestamp:
                            new_match_ids.append(match_id)
                        elif start_time:
                            # Once we hit older matches, we can stop checking (matches are in descending order)
                            break
                
                if new_match_ids:
                    print(f"Found {len(new_match_ids)} new matches for {team.name} in the past {days_to_look_back} day(s)")
                    
                    # Fetch full match details for each new match
                    for match_id in new_match_ids:
                        print(f"Fetching details for match {match_id}...")
                        match_url = f"{OPENDOTA_API_BASE}/matches/{match_id}"
                        
                        # Add a small delay to avoid rate limiting
                        time.sleep(1)
                        
                        match_response = requests.get(match_url)
                        if match_response.status_code != 200:
                            print(f"Error fetching match {match_id}: {match_response.status_code}")
                            continue
                            
                        match_details = match_response.json()
                        
                        # Double-check if match is already in database
                        session = self.Session()
                        existing_match = session.query(Match).filter_by(match_id=match_id).first()
                        session.close()
                        
                        if existing_match:
                            print(f"Match {match_id} was added by another process, skipping.")
                            continue
                        
                        # Create a temporary file to store match details
                        temp_dir = "temp_matches"
                        os.makedirs(temp_dir, exist_ok=True)
                        temp_file = os.path.join(temp_dir, f"match_{match_id}.json")
                        
                        with open(temp_file, "w", encoding="utf-8") as f:
                            json.dump(match_details, f)
                        
                        # Process the match data
                        try:
                            populate_from_json(temp_file)
                            populate_time_vs_stats(temp_file)
                            api_match_count += 1
                            # Track this match as newly added
                            newly_added_match_ids.add(match_id)
                            print(f"Added match {match_id} to database")
                        except Exception as e:
                            print(f"Error processing match {match_id}: {e}")
                        
                        # Remove temporary file
                        os.remove(temp_file)
                else:
                    print(f"No new matches found for {team.name} in the past {days_to_look_back} day(s)")
                    
            except Exception as e:
                print(f"Error checking matches for {team.name}: {e}")

        # Clean up temp directory if it exists and is empty
        temp_dir = "temp_matches"
        if os.path.exists(temp_dir) and not os.listdir(temp_dir):
            os.rmdir(temp_dir)
            
        print(f"Added {api_match_count} new matches from OpenDota API")
        
        print("Database update complete!")
        return api_match_count  # Return total number of updates
        
    def search_matches(self, criteria=None, limit=20, order_by_recent=True):
        """
        Search for matches based on criteria.
        
        Args:
            criteria (dict): Dictionary of search criteria.
                Supported keys include:
                - team_id: Filter by radiant or dire team ID
                - team_name: Filter by team name (case-insensitive)
                - player_id: Filter by player account ID
                - player_name: Filter by player name or persona name
                - hero_id: Filter by hero ID
                - hero_name: Filter by hero name
                - min_date: Minimum date (datetime or string YYYY-MM-DD)
                - max_date: Maximum date (datetime or string YYYY-MM-DD)
                - min_duration: Minimum match duration in seconds
                - max_duration: Maximum match duration in seconds
                - league_id: Filter by league ID
            limit (int): Maximum number of matches to return (default: 20)
            order_by_recent (bool): If True, order by most recent matches first
            
        Returns:
            list: List of Match objects matching the criteria
        """
        from sqlalchemy import or_, and_, desc
        from datetime import datetime
        
        session = self.Session()
        query = session.query(Match)
        
        if criteria is None:
            criteria = {}
        
        # Filter by team ID or name
        if 'team_id' in criteria:
            team_id = criteria['team_id']
            query = query.filter(or_(Match.radiant_team_id == team_id, Match.dire_team_id == team_id))
        elif 'team_name' in criteria:
            team_name = criteria['team_name']
            # Find team IDs matching the name
            team_query = session.query(Team.team_id).filter(Team.name.ilike(f'%{team_name}%'))
            team_ids = [t[0] for t in team_query.all()]
            if team_ids:
                team_filter = or_(*[
                    or_(Match.radiant_team_id == tid, Match.dire_team_id == tid) 
                    for tid in team_ids
                ])
                query = query.filter(team_filter)
        
        # Filter by player ID or name
        if 'player_id' in criteria or 'player_name' in criteria:
            # We need to join with MatchPlayer and Player tables
            query = query.join(MatchPlayer, MatchPlayer.match_id == Match.match_id)
            query = query.join(Player, Player.account_id == MatchPlayer.account_id)
            
            if 'player_id' in criteria:
                query = query.filter(Player.account_id == criteria['player_id'])
            elif 'player_name' in criteria:
                player_name = criteria['player_name']
                query = query.filter(or_(
                    Player.name.ilike(f'%{player_name}%'),
                    Player.personaname.ilike(f'%{player_name}%')
                ))
        
        # Filter by hero ID or name
        if 'hero_id' in criteria or 'hero_name' in criteria:
            if not any(t.name == 'match_player_metrics' for t in query._join_entities):
                query = query.join(MatchPlayer, MatchPlayer.match_id == Match.match_id)
            
            query = query.join(Hero, Hero.hero_id == MatchPlayer.hero_id)
            
            if 'hero_id' in criteria:
                query = query.filter(Hero.hero_id == criteria['hero_id'])
            elif 'hero_name' in criteria:
                hero_name = criteria['hero_name']
                query = query.filter(or_(
                    Hero.name.ilike(f'%{hero_name}%'),
                    Hero.localized_name.ilike(f'%{hero_name}%')
                ))
        
        # Filter by date range
        if 'min_date' in criteria:
            min_date = criteria['min_date']
            if isinstance(min_date, str):
                min_date = datetime.strptime(min_date, '%Y-%m-%d')
            query = query.filter(Match.start_time >= min_date)
            
        if 'max_date' in criteria:
            max_date = criteria['max_date']
            if isinstance(max_date, str):
                max_date = datetime.strptime(max_date, '%Y-%m-%d')
                # Set to end of day
                max_date = max_date.replace(hour=23, minute=59, second=59)
            query = query.filter(Match.start_time <= max_date)
        
        # Filter by duration
        if 'min_duration' in criteria:
            query = query.filter(Match.duration >= criteria['min_duration'])
            
        if 'max_duration' in criteria:
            query = query.filter(Match.duration <= criteria['max_duration'])
            
        # Filter by league
        if 'league_id' in criteria:
            query = query.filter(Match.league_id == criteria['league_id'])
        
        # Order by most recent matches first if requested
        if order_by_recent:
            query = query.order_by(desc(Match.start_time))
        
        # Apply limit
        if limit:
            query = query.limit(limit)
        
        # Execute query and get results
        matches = query.all()
        
        # Close session
        session.close()
        
        return matches
        
    def get_recent_matches(self, days=30, team_id=None, team_name=None, limit=20):
        """
        Get recent matches from the past number of days, optionally filtered by team.
        
        Args:
            days (int): Number of days to look back
            team_id (int): Optional team ID to filter by
            team_name (str): Optional team name to filter by (ignored if team_id is provided)
            limit (int): Maximum number of matches to return
            
        Returns:
            list: List of Match objects
        """
        from datetime import datetime, timedelta
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Build criteria dict
        criteria = {
            'min_date': start_date,
            'max_date': end_date
        }
        
        if team_id:
            criteria['team_id'] = team_id
        elif team_name:
            criteria['team_name'] = team_name
        
        # Use the search_matches method
        return self.search_matches(criteria, limit=limit)
        
    def get_match_with_details(self, match_id):
        """
        Get a match by ID with associated player details.
        
        Args:
            match_id (int): The match ID
            
        Returns:
            dict: Match data with player details
        """
        session = self.Session()
        
        # Get the match
        match = session.query(Match).filter(Match.match_id == match_id).first()
        if not match:
            session.close()
            return None
        
        # Get team names
        radiant_team = None
        dire_team = None
        
        if match.radiant_team_id:
            radiant_team = session.query(Team).filter(Team.team_id == match.radiant_team_id).first()
        
        if match.dire_team_id:
            dire_team = session.query(Team).filter(Team.team_id == match.dire_team_id).first()
        
        # Get player details
        players = session.query(
            MatchPlayer, Player, Hero
        ).join(
            Player, Player.account_id == MatchPlayer.account_id
        ).join(
            Hero, Hero.hero_id == MatchPlayer.hero_id
        ).filter(
            MatchPlayer.match_id == match_id
        ).all()
        
        # Format result
        radiant_players = []
        dire_players = []
        
        for mp, player, hero in players:
            player_data = {
                'account_id': player.account_id,
                'name': player.name or player.personaname or 'Unknown',
                'hero_id': hero.hero_id,
                'hero_name': hero.localized_name or hero.name,
                'kills': mp.kills,
                'deaths': mp.deaths,
                'assists': mp.assists,
                'net_worth': mp.total_gold,
                'gpm': mp.gold_per_min,
                'xpm': mp.xp_per_min,
                'hero_damage': mp.hero_damage,
                'tower_damage': mp.tower_damage,
                'hero_healing': mp.hero_healing,
                'last_hits': mp.last_hits,
                'denies': mp.denies,
                'level': mp.level
            }
            
            # Add to appropriate team list (Radiant players have player_slots 0-127)
            if mp.player_slot < 128:
                radiant_players.append(player_data)
            else:
                dire_players.append(player_data)
        
        # Format match data
        match_data = {
            'match_id': match.match_id,
            'start_time': match.start_time,
            'duration': match.duration,
            'radiant_team': {
                'id': match.radiant_team_id,
                'name': radiant_team.name if radiant_team else 'Radiant',
                'tag': radiant_team.tag if radiant_team else None,
            },
            'dire_team': {
                'id': match.dire_team_id,
                'name': dire_team.name if dire_team else 'Dire',
                'tag': dire_team.tag if dire_team else None,
            },
            'score': {
                'radiant': match.radiant_score,
                'dire': match.dire_score
            },
            'winner': 'radiant' if match.radiant_win else 'dire',
            'radiant_players': radiant_players,
            'dire_players': dire_players,
            'league_id': match.league_id,
            'series_id': match.series_id,
            'series_type': match.series_type
        }
        
        session.close()
        return match_data

# ---------------------------
# Data Population / Upsert
# ---------------------------

def populate_from_json(json_path):
    """
    Parse the given JSON file and upsert the data into the database.
    Checks for duplicates and updates them if needed (e.g., for MatchPlayer).
    """
    db = DotaDatabase()
    session = db.Session()

    # 1. Load JSON
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    match_id = data.get("match_id")
    if not match_id:
        logger.warning("No match_id found in JSON. Skipping.")
        return

    # 2. Upsert the Match
    existing_match = session.query(Match).filter_by(match_id=match_id).one_or_none()
    if existing_match:
        # Update relevant fields if desired
        existing_match.duration = data.get("duration", existing_match.duration)
        existing_match.radiant_score = data.get("radiant_score", existing_match.radiant_score)
        existing_match.dire_score = data.get("dire_score", existing_match.dire_score)
        existing_match.radiant_win = data.get("radiant_win", existing_match.radiant_win)
        # You can update more fields here as needed
        session.commit()
        match_record = existing_match
        logger.info(f"Updated existing match {match_id}")
    else:
        # Insert new match
        match_record = Match(
            version=data.get("version"),
            match_id=match_id,
            start_time=datetime.now(),
            duration=data.get("duration", 0),
            radiant_score=data.get("radiant_score", 0),
            dire_score=data.get("dire_score", 0),
            radiant_win=data.get("radiant_win", False),
            game_version=data.get("version"),
            radiant_gold_adv=(data.get("radiant_gold_adv")[-1] if data.get("radiant_gold_adv") else 0),
            dire_gold_adv=0  # or parse from the JSON if you have dire gold adv
        )
        session.add(match_record)
        session.commit()
        logger.info(f"Inserted new match {match_id}")

    # 3. DraftTiming
    for dt in data.get("draft_timings", []):
        draft_timing = DraftTiming(
            match_id=match_id,
            order=dt["order"],
            pick=dt["pick"],
            active_team=dt["active_team"],
            hero_id=dt["hero_id"],
            player_slot=dt.get("player_slot"),
            extra_time=dt["extra_time"],
            total_time_taken=dt["total_time_taken"]
        )
        session.add(draft_timing)
    session.commit()

    # 4. TeamFights & TeamFightPlayers
    for tf in data.get("teamfights", []):
        teamfight = TeamFight(
            match_id=match_id,
            start=tf["start"],
            end=tf["end"],
            last_death=tf.get("last_death"),
            deaths=tf["deaths"]
        )
        session.add(teamfight)
        session.commit()  # commit to get teamfight.id
        
        # Process players for this teamfight
        for tfp in tf.get("players", []):
            tf_player = TeamFightPlayer(
                teamfight_id=teamfight.id,
                match_id=match_id,
                deaths=tfp.get("deaths", 0),
                buybacks=tfp.get("buybacks", 0),
                damage=tfp.get("damage", 0),
                healing=tfp.get("healing", 0),
                gold_delta=tfp.get("gold_delta", 0),
                xp_delta=tfp.get("xp_delta", 0),
                xp_start=tfp.get("xp_start", 0),
                xp_end=tfp.get("xp_end", 0),
                deaths_pos=tfp.get("deaths_pos"),
                ability_uses=tfp.get("ability_uses"),
                ability_targets=tfp.get("ability_targets"),
                item_uses=tfp.get("item_uses"),
                killed=tfp.get("killed")
            )
            session.add(tf_player)
        session.commit()  # This commit should be inside the outer loop, but still in the tf loop


    # 5. Objectives
    for obj in data.get("objectives", []):
        objective = Objective(
            match_id=match_id,
            time=obj["time"],
            type=obj["type"],
            slot=obj.get("slot"),
            key=str(obj.get("key")) if obj.get("key") is not None else None,
            player_slot=obj.get("player_slot"),
            unit=obj.get("unit"),
            value=obj.get("value"),
            killer=obj.get("killer"),
            team=obj.get("team")
        )
        session.add(objective)
    session.commit()

    # 6. ChatWheel
    for chat in data.get("chat", []):
        if chat["type"] == "chatwheel":
            cw = ChatWheel(
                match_id=match_id,
                time=chat["time"],
                type=chat["type"],
                key=str(chat["key"]),
                slot=chat.get("slot"),
                player_slot=chat.get("player_slot")
            )
            session.add(cw)
    session.commit()

    # 7. Players, Heroes, and MatchPlayer upsert
    for p in data.get("players", []):
        account_id = p.get("account_id")
        hero_id = p.get("hero_id")

        # Upsert Player
        existing_player = session.query(Player).filter_by(account_id=account_id).one_or_none()
        if not existing_player:
            new_player = Player(
                match_id=match_id,
                account_id=account_id,
                name=p.get("name"),
                personaname=p.get("personaname"),
                country_code=p.get("country_code"),
                team_id=p.get("team_id")
            )
            session.add(new_player)
            session.commit()
            player_id = new_player.account_id
        else:
            player_id = existing_player.account_id

        # Upsert Hero
        existing_hero = session.query(Hero).filter_by(hero_id=hero_id).one_or_none()
        if not existing_hero:
            new_hero = Hero(
                match_id=match_id,
                hero_id=hero_id,
                name=p.get("hero_name"),
                localized_name=p.get("localized_name"),
                primary_attr=p.get("primary_attr", "unknown"),
                attack_type=p.get("attack_type", "unknown"),
                roles=p.get("roles", [])
            )
            session.add(new_hero)
            session.commit()
            hero_db_id = new_hero.hero_id
        else:
            hero_db_id = existing_hero.hero_id

        # Extract benchmark percentages
        benchmarks = p.get("benchmarks", {})
        bench_gold_pct = benchmarks.get("gold_per_min", {}).get("pct", 0.0)
        bench_xp_pct = benchmarks.get("xp_per_min", {}).get("pct", 0.0)
        bench_kills_pct = benchmarks.get("kills_per_min", {}).get("pct", 0.0)
        bench_last_hits_pct = benchmarks.get("last_hits_per_min", {}).get("pct", 0.0)
        bench_hero_damage_pct = benchmarks.get("hero_damage_per_min", {}).get("pct", 0.0)
        bench_hero_healing_pct = benchmarks.get("hero_healing_per_min", {}).get("pct", 0.0)
        bench_tower_damage_pct = benchmarks.get("tower_damage", {}).get("pct", 0.0)

        # Check if MatchPlayer already exists
        existing_mp = session.query(MatchPlayer).filter_by(
            match_id=match_id,
            account_id=player_id,
            hero_id=hero_db_id
        ).one_or_none()

        if existing_mp:
            # Update fields if you want to reflect new data
            existing_mp.kills = p.get("kills", existing_mp.kills)
            existing_mp.deaths = p.get("deaths", existing_mp.deaths)
            existing_mp.assists = p.get("assists", existing_mp.assists)
            existing_mp.last_hits = p.get("last_hits", existing_mp.last_hits)
            existing_mp.denies = p.get("denies", existing_mp.denies)
            existing_mp.gold_per_min = p.get("gold_per_min", existing_mp.gold_per_min)
            existing_mp.xp_per_min = p.get("xp_per_min", existing_mp.xp_per_min)
            existing_mp.hero_damage = p.get("hero_damage", existing_mp.hero_damage)
            existing_mp.tower_damage = p.get("tower_damage", existing_mp.tower_damage)
            existing_mp.hero_healing = p.get("hero_healing", existing_mp.hero_healing)
            existing_mp.level = p.get("level", existing_mp.level)
            existing_mp.total_gold = p.get("total_gold", existing_mp.total_gold)
            existing_mp.total_xp = p.get("total_xp", existing_mp.total_xp)
            existing_mp.neutral_kills = p.get("neutral_kills", existing_mp.neutral_kills)
            existing_mp.tower_kills = p.get("tower_kills", existing_mp.tower_kills)
            existing_mp.courier_kills = p.get("courier_kills", existing_mp.courier_kills)
            existing_mp.lane_kills = p.get("lane_kills", existing_mp.lane_kills)
            existing_mp.hero_kills = p.get("hero_kills", existing_mp.hero_kills)
            existing_mp.observer_kills = p.get("observer_kills", existing_mp.observer_kills)
            existing_mp.sentry_kills = p.get("sentry_kills", existing_mp.sentry_kills)
            existing_mp.stuns = p.get("stuns", existing_mp.stuns)
            existing_mp.actions_per_min = p.get("actions_per_min", existing_mp.actions_per_min)
            existing_mp.bench_gold_pct = bench_gold_pct
            existing_mp.bench_xp_pct = bench_xp_pct
            existing_mp.bench_kills_pct = bench_kills_pct
            existing_mp.bench_last_hits_pct = bench_last_hits_pct
            existing_mp.bench_hero_damage_pct = bench_hero_damage_pct
            existing_mp.bench_hero_healing_pct = bench_hero_healing_pct
            existing_mp.bench_tower_damage_pct = bench_tower_damage_pct
            existing_mp.item_0 = p.get("item_0", existing_mp.item_0)
            existing_mp.item_1 = p.get("item_1", existing_mp.item_1)
            existing_mp.item_2 = p.get("item_2", existing_mp.item_2)
            existing_mp.item_3 = p.get("item_3", existing_mp.item_3)
            existing_mp.item_4 = p.get("item_4", existing_mp.item_4)
            existing_mp.item_5 = p.get("item_5", existing_mp.item_5)
            existing_mp.purchase_log = p.get("purchase_log", existing_mp.purchase_log)
            existing_mp.lane_pos = p.get("lane_pos", existing_mp.lane_pos)
            existing_mp.kill_log = p.get("kill_log", existing_mp.kill_log)
            logger.info(f"Updated MatchPlayer for match_id={match_id}, account_id={player_id}")
        else:
            # Insert a new MatchPlayer
            match_player = MatchPlayer(
                match_id=match_id,
                account_id=player_id,
                hero_id=hero_db_id,
                player_slot=p.get("player_slot"),
                kills=p.get("kills", 0),
                deaths=p.get("deaths", 0),
                assists=p.get("assists", 0),
                last_hits=p.get("last_hits", 0),
                denies=p.get("denies", 0),
                gold_per_min=p.get("gold_per_min", 0),
                xp_per_min=p.get("xp_per_min", 0),
                hero_damage=p.get("hero_damage", 0),
                tower_damage=p.get("tower_damage", 0),
                hero_healing=p.get("hero_healing", 0),
                level=p.get("level", 0),
                total_gold=p.get("total_gold", 0),
                total_xp=p.get("total_xp", 0),
                neutral_kills=p.get("neutral_kills", 0),
                tower_kills=p.get("tower_kills", 0),
                courier_kills=p.get("courier_kills", 0),
                lane_kills=p.get("lane_kills", 0),
                hero_kills=p.get("hero_kills", 0),
                observer_kills=p.get("observer_kills", 0),
                sentry_kills=p.get("sentry_kills", 0),
                stuns=p.get("stuns", 0.0),
                actions_per_min=p.get("actions_per_min", 0.0),
                bench_gold_pct=bench_gold_pct,
                bench_xp_pct=bench_xp_pct,
                bench_kills_pct=bench_kills_pct,
                bench_last_hits_pct=bench_last_hits_pct,
                bench_hero_damage_pct=bench_hero_damage_pct,
                bench_hero_healing_pct=bench_hero_healing_pct,
                bench_tower_damage_pct=bench_tower_damage_pct,
                item_0=p.get("item_0"),
                item_1=p.get("item_1"),
                item_2=p.get("item_2"),
                item_3=p.get("item_3"),
                item_4=p.get("item_4"),
                item_5=p.get("item_5"),
                purchase_log=p.get("purchase_log"),
                lane_pos=p.get("lane_pos"),
                kill_log=p.get("kill_log")
            )
            session.add(match_player)
            logger.info(f"Inserted new MatchPlayer for match_id={match_id}, account_id={player_id}")

        session.commit()

    logger.info(f"Finished populating data from {json_path}.")

def populate_time_vs_stats(json_path):
    """
    Load the match JSON from the given file path and insert time-series stats
    for each player into the TimevsStats table.
    This function adds:
      - A baseline row for each time entry (with gold, last_hits, denies, xp)
      - Additional rows for each event in kills_log, purchase_log, and runes_log.
    
    The player's account_id and name (from "personaname" or "name") are saved
    into the TimevsStats table.
    
    Args:
        json_path (str): Path to the match JSON file.
    """
    # Create a new database session
    db = DotaDatabase()
    session = db.Session()
    
    # Load JSON data from file
    with open(json_path, "r", encoding="utf-8") as f:
        match_data = json.load(f)
    
    match_id = match_data.get("match_id")
    if not match_id:
        print(f"No match_id found in {json_path}. Skipping.")
        session.close()
        return

    players = match_data.get("players", [])
    
    for player in players:
        # Get player identification from the JSON
        player_slot = player.get("player_slot")
        account_id = player.get("account_id")
        # Use "personaname" if available; if not, fall back to "name"
        player_name = player.get("personaname") or player.get("name") or "Unknown"
        
        # Baseline time-series arrays
        times = player.get("times", [])
        gold_series = player.get("gold_t", [])
        lh_series = player.get("lh_t", [])
        dn_series = player.get("dn_t", [])
        xp_series = player.get("xp_t", [])
        # Determine starting lane; if not provided, default to 0
        starting_lane = player.get("lane", 0)
        
        # Insert baseline rows for each time entry
        for i, t in enumerate(times):
            tvs = TimevsStats(
                match_id=match_id,
                player_id=account_id,
                player_name=player_name,
                player_slot=player_slot,
                time=t,
                starting_lane=starting_lane,
                gold=gold_series[i] if i < len(gold_series) else None,
                last_hits=lh_series[i] if i < len(lh_series) else None,
                denies=dn_series[i] if i < len(dn_series) else None,
                xp=xp_series[i] if i < len(xp_series) else None,
                event_type=None,
                killed_hero=None,
                purchased_item=None,
                rune_type=None
            )
            session.add(tvs)
        
        # Process kill events
        for event in player.get("kills_log", []):
            event_time = event.get("time")
            if event_time is not None:
                tvs_event = TimevsStats(
                    match_id=match_id,
                    player_id=account_id,
                    player_name=player_name,
                    player_slot=player_slot,
                    time=event_time,
                    starting_lane=starting_lane,
                    gold=None,
                    last_hits=None,
                    denies=None,
                    xp=None,
                    event_type="kill",
                    killed_hero=event.get("key"),
                    purchased_item=None,
                    rune_type=None
                )
                session.add(tvs_event)
        
        # Process purchase events
        for event in player.get("purchase_log", []):
            event_time = event.get("time")
            if event_time is not None:
                tvs_event = TimevsStats(
                    match_id=match_id,
                    player_id=account_id,
                    player_name=player_name,
                    player_slot=player_slot,
                    time=event_time,
                    starting_lane=starting_lane,
                    gold=None,
                    last_hits=None,
                    denies=None,
                    xp=None,
                    event_type="purchase",
                    killed_hero=None,
                    purchased_item=event.get("key"),
                    rune_type=None
                )
                session.add(tvs_event)
        
        # Process rune events
        for event in player.get("runes_log", []):
            event_time = event.get("time")
            if event_time is not None:
                tvs_event = TimevsStats(
                    match_id=match_id,
                    player_id=account_id,
                    player_name=player_name,
                    player_slot=player_slot,
                    time=event_time,
                    starting_lane=starting_lane,
                    gold=None,
                    last_hits=None,
                    denies=None,
                    xp=None,
                    event_type="rune",
                    killed_hero=None,
                    purchased_item=None,
                    rune_type=event.get("key")
                )
                session.add(tvs_event)
    
    session.commit()
    session.close()

def populate_team_from_json(json_path):
    """
    Parse a team info JSON file and upsert the team data into the database.
    Expects keys like "team_id", "team_name", "team_logo", and "match_ids".
    """
    db = DotaDatabase()
    session = db.Session()
    
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Check if the JSON file contains team info by looking for a "team_id" and "match_ids" keys.
    if "team_id" not in data or "match_ids" not in data:
        logger.warning(f"File {json_path} does not appear to be a team info file. Skipping.")
        session.close()
        return
    
    rank = data.get("rank", 0)
    team_id = data["team_id"]
    team_name = data.get("team_name", "Unknown")
    team_logo = data.get("team_logo")  # Adjust the key if your file uses "team_logo" or "logo_url"
    
    # Check if the team already exists in the database.
    existing_team = session.query(Team).filter_by(team_id=team_id).one_or_none()
    if existing_team:
        # Update existing team record
        existing_team.name = team_name
        existing_team.logo_url = team_logo
        existing_team.rank = rank
        logger.info(f"Updated existing team {team_name} with ID {team_id}")
    else:
        # Insert new team record
        new_team = Team(
            team_id=team_id,
            name=team_name,
            logo_url=team_logo,
            tag=team_name.replace(" ", "_"),
            rank=rank
        )
        session.add(new_team)
        logger.info(f"Inserted new team {team_name} with ID {team_id}")
    
    session.commit()
    session.close()

if __name__ == "__main__":
    # Create a database instance
    db = DotaDatabase()
    
    # Update all tables without dropping them
    update_count = db.update_all_tables()
    
    print(f"Database update complete. Made {update_count} updates in total.")