import os
import json
import logging
import datetime
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    DateTime,
    JSON,
    Boolean,
    UniqueConstraint,
    text
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.sql import func
import requests
from contextlib import contextmanager
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging - suppress console output as per user preference
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("user_games_db.log")
    ]
)
logger = logging.getLogger("user_games_db")

# Initialize SQLAlchemy Base
Base = declarative_base()

# -----------------------
# Database Schema Classes
# -----------------------

class User(Base):
    """User table for storing Steam user information"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    steam_id = Column(String(30), unique=True, nullable=False)
    account_id = Column(String(30), unique=True)  # Dota 2 account ID (different from Steam ID)
    username = Column(String(100))
    avatar = Column(String(255))
    profile_url = Column(String(255))
    created_at = Column(DateTime, default=datetime.datetime.now())           
    last_login = Column(DateTime, default=datetime.datetime.now())
    
    # Relationships
    matches = relationship("UserMatch", back_populates="user")
    
    def __repr__(self):
        return f"<User(steam_id='{self.steam_id}', username='{self.username}')>"

class UserMatch(Base):
    """Table for storing user match information."""
    __tablename__ = 'user_matches'
    
    match_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=True)
    start_time = Column(DateTime)
    duration = Column(Integer)  # In seconds
    radiant_win = Column(Boolean)
    game_mode = Column(Integer)
    lobby_type = Column(Integer)
    radiant_score = Column(Integer)
    dire_score = Column(Integer)
    
    # Relationships
    user = relationship("User", back_populates="matches")
    players = relationship("UserMatchPlayer", back_populates="match")
    teamfights = relationship("UserTeamFight", back_populates="match") 
    draft_timings = relationship("UserDraftTiming", back_populates="match")
    
    def __repr__(self):
        return f"<UserMatch(match_id={self.match_id}, user_id={self.user_id})>"

class UserMatchPlayer(Base):
    """Table for storing player performance in a user's match (similar to MatchPlayer in pro database)"""
    __tablename__ = 'user_match_players'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('user_matches.match_id'))
    account_id = Column(Integer)  
    hero_id = Column(Integer)     
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
    
    # Relationship to Match
    match = relationship("UserMatch", back_populates="players")
    
    def __repr__(self):
        return f"<UserMatchPlayer(match_id={self.match_id}, hero_id={self.hero_id})>"

class UserTimevsStats(Base):
    """Time-series statistics for user matches, similar to TimevsStats in pro database"""
    __tablename__ = 'user_time_vs_stats'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('user_matches.match_id'))
    player_id = Column(String(30))  # Steam account ID
    player_name = Column(String(255))
    player_slot = Column(Integer)
    time = Column(Integer)  # Time in seconds
    starting_lane = Column(String(20))  # safe, mid, off, jungle, roam
    
    # Time-series stats
    gold = Column(Integer, nullable=True)
    last_hits = Column(Integer, nullable=True)
    denies = Column(Integer, nullable=True)
    xp = Column(Integer, nullable=True)
    
    # Event data
    event_type = Column(String(20), nullable=True)  # kill, death, purchase, rune
    killed_hero = Column(String(50), nullable=True)
    purchased_item = Column(String(50), nullable=True)
    rune_type = Column(Integer, nullable=True)
    
    def __repr__(self):
        return f"<UserTimevsStats(match_id={self.match_id}, player_id={self.player_id}, time={self.time})>"

class User_Objective(Base):
    __tablename__ = 'user_objectives'
    
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
class UserTeamFight(Base):
    """Table for storing teamfight-level information in a user match."""
    __tablename__ = 'user_teamfights'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('user_matches.match_id'))
    start = Column(Integer, nullable=False)  # Start time in seconds
    end = Column(Integer, nullable=False)    # End time in seconds
    last_death = Column(Integer, nullable=True)  # Time of last death in fight
    deaths = Column(Integer, nullable=False)     # Total deaths in fight
    
    # Relationships
    match = relationship("UserMatch", back_populates="teamfights")
    players = relationship("UserTeamFightPlayer", back_populates="teamfight")
    
    def __repr__(self):
        return f"<UserTeamFight(match_id={self.match_id}, start={self.start}, end={self.end})>"

class UserTeamFightPlayer(Base):
    """Table for storing per-player statistics within a teamfight in user matches."""
    __tablename__ = 'user_teamfight_players'
    
    id = Column(Integer, primary_key=True)
    teamfight_id = Column(Integer, ForeignKey('user_teamfights.id'))
    player_slot = Column(Integer)
    
    # Pre-fight and post-fight stats
    deaths = Column(Integer, nullable=False)
    buybacks = Column(Integer, nullable=False)
    damage = Column(Integer, nullable=False)
    healing = Column(Integer, nullable=False)
    gold_delta = Column(Integer, nullable=False)
    xp_delta = Column(Integer, nullable=False)
    gold_start = Column(Integer, nullable=False)
    gold_end = Column(Integer, nullable=False)
    xp_start = Column(Integer, nullable=False)
    xp_end = Column(Integer, nullable=False)
    
    # Relationship to teamfight
    teamfight = relationship("UserTeamFight", back_populates="players")
    
    def __repr__(self):
        return f"<UserTeamFightPlayer(teamfight_id={self.teamfight_id}, player_slot={self.player_slot})>"

class UserDraftTiming(Base):
    """Draft timing information for user matches, if available."""
    __tablename__ = 'user_draft_timings'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('user_matches.match_id'), nullable=True)
    order = Column(Integer, nullable=False)  # Draft order
    pick = Column(Boolean, nullable=False)   # True for pick, False for ban
    active_team = Column(Integer, nullable=False)  # 0 for Radiant, 1 for Dire
    hero_id = Column(Integer, nullable=False)
    player_slot = Column(Integer, nullable=True)
    extra_time = Column(Integer, nullable=False)  # Extra time used
    total_time_taken = Column(Integer, nullable=False)  # Total time taken
    
    match = relationship("UserMatch", back_populates="draft_timings")
    
    def __repr__(self):
        return f"<UserDraftTiming(match_id={self.match_id}, order={self.order}, pick={self.pick})>"

class UserChatWheel(Base):
    __tablename__ = 'user_chatwheel'
    
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

class UserDotaDatabase:
    """Database manager for user's Dota 2 matches, mirroring DotaDatabase for pro matches"""
    
    def __init__(self, db_url=None, engine=None, session=None):
        """Initialize the user database
        
        Args:
            db_url (str, optional): Database connection URL. If None, uses the same as pro database.
            engine: Existing SQLAlchemy engine to reuse
            session: Existing SQLAlchemy session to reuse
        """
        if engine:
            # Reuse existing engine
            self.engine = engine
        else:
            # Use the same database location as pro matches
            if not db_url:
                data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
                os.makedirs(data_dir, exist_ok=True)
                db_url = f"sqlite:///{os.path.join(data_dir, 'dota_matches.db')}"
            
            self.engine = create_engine(db_url, echo=False)  # echo=False to suppress logs
        
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        
        # Create or reuse session
        if session:
            self.session = session
        else:
            Session = sessionmaker(bind=self.engine)
            self.session = scoped_session(Session)
            
        logger.setLevel(logging.WARNING)  # Suppress logs
        
        # Steam API key
        self.api_key = os.getenv('STEAMAPI')
        if not self.api_key:
            logger.warning("STEAMAPI key not found in environment variables")
    
    def close(self):
        """Close the database session"""
        self.session.close()
    
    def get_or_create_user(self, steam_id, user_info=None):
        """Get or create a user based on Steam ID"""
        user = self.session.query(User).filter_by(steam_id=steam_id).first()
        
        if not user:
            if not user_info:
                user_info = self._get_steam_user_info(steam_id)
            
            # Convert 64-bit Steam ID to 32-bit account ID used by Dota 2 API
            account_id = self._steam_id_to_account_id(steam_id)
            
            user = User(
                steam_id=steam_id,
                account_id=account_id,
                username=user_info.get('personaname', '') if user_info else '',
                avatar=user_info.get('avatarfull', '') if user_info else '',
                profile_url=user_info.get('profileurl', '') if user_info else ''
            )
            self.session.add(user)
            self.session.commit()
        
        return user
    
    def _steam_id_to_account_id(self, steam_id):
        """Convert 64-bit Steam ID to 32-bit account ID used by Dota 2 API"""
        try:
            return str(int(steam_id) - 76561197960265728)
        except (ValueError, TypeError):
            logger.error(f"Could not convert Steam ID: {steam_id}")
            return None
    
    def _get_steam_user_info(self, steam_id):
        """Get user info from Steam API"""
        if not self.api_key:
            logger.error("No Steam API key available")
            return None
        
        url = f"https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key={self.api_key}&steamids={steam_id}"
        try:
            response = requests.get(url)
            if response.ok:
                data = response.json()
                if data.get('response', {}).get('players'):
                    return data['response']['players'][0]
            logger.error(f"Failed to get Steam user info: {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching Steam user info: {str(e)}")
        
        return None
    
    def fetch_user_matches(self, user, limit=100):
        """Fetch recent matches for a user from Dota 2 API"""
        if not self.api_key:
            logger.error("No Steam API key available")
            return []
        
        if not user.account_id:
            logger.error(f"No account ID available for user {user.id}")
            return []
        
        url = f"https://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1/?key={self.api_key}&account_id={user.account_id}&matches_requested={limit}"
        try:
            response = requests.get(url)
            if response.ok:
                data = response.json()
                if data.get('result', {}).get('status') == 1:
                    return data['result']['matches']
            logger.error(f"Failed to get match history: {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching match history: {str(e)}")
        
        return []
    
    def fetch_match_details(self, match_id):
        """Fetch details for a specific match from Dota 2 API"""
        if not self.api_key:
            logger.error("No Steam API key available")
            return None
        
        url = f"https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/v1/?key={self.api_key}&match_id={match_id}"
        try:
            response = requests.get(url)
            if response.ok:
                data = response.json()
                if data.get('result'):
                    return data['result']
            logger.error(f"Failed to get match details: {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching match details: {str(e)}")
        
        return None
    
    def process_match_details(self, match_details, user=None):
        """Process match details into database tables (similar to process_match in pro database)"""
        if not match_details:
            logger.error("No match details provided")
            return None
        
        match_id = match_details.get('match_id')
        if not match_id:
            logger.error("Match ID not found in match details")
            return None
        
        # Check if match already exists
        existing_match = self.session.query(UserMatch).filter_by(match_id=match_id).first()
        if existing_match:
            logger.info(f"Match {match_id} already exists in database")
            return existing_match
        
        # Create UserMatch record
        start_time = datetime.datetime.fromtimestamp(match_details.get('start_time', 0))
        duration = match_details.get('duration', 0)
        
        # If user is provided, link the match to them
        user_id = user.id if user else None
        
        user_match = UserMatch(
            match_id=match_id,
            user_id=user_id,
            start_time=start_time,
            duration=duration,
            game_mode=match_details.get('game_mode', 0),
            lobby_type=match_details.get('lobby_type', 0),
            radiant_score=match_details.get('radiant_score', 0),
            dire_score=match_details.get('dire_score', 0),
            radiant_win=match_details.get('radiant_win', False),
            match_data=match_details
        )
        
        self.session.add(user_match)
        
        # Process players in match
        for player in match_details.get('players', []):
            self._process_player(match_id, player)
        
        # Process time series data if available
        if 'players' in match_details:
            for player in match_details['players']:
                self._process_timeseries_data(match_id, player)
        
        # Process teamfights if available
        if 'teamfights' in match_details:
            self._process_teamfights(match_id, match_details['teamfights'])
            
        # Process draft timing if available
        if 'picks_bans' in match_details:
            self._process_draft_timing(match_id, match_details['picks_bans'])
        
        self.session.commit()
        return user_match
    
    def _process_player(self, match_id, player_data):
        """Process player data for a match (similar to process_player in pro database)"""
        account_id = player_data.get('account_id')
        player_slot = player_data.get('player_slot', 0)
        is_radiant = player_slot < 128
        
        # Determine lane and role
        lane = "unknown"
        lane_role = player_data.get('lane_role', 0)
        
        if lane_role == 1:
            lane = "safe"
        elif lane_role == 2:
            lane = "mid"
        elif lane_role == 3:
            lane = "off"
        elif lane_role == 4:
            lane = "jungle"
        
        role = "core" if lane_role in [1, 2, 3] else "support"
        
        # Create player record
        player = UserMatchPlayer(
            match_id=match_id,
            account_id=str(account_id) if account_id else None,
            player_slot=player_slot,
            hero_id=player_data.get('hero_id', 0),
            team=0 if is_radiant else 1,  # 0 for Radiant, 1 for Dire
            lane=lane,
            lane_role=lane_role,
            role=role,
            is_roaming=player_data.get('is_roaming', False),
            kills=player_data.get('kills', 0),
            deaths=player_data.get('deaths', 0),
            assists=player_data.get('assists', 0),
            gold_per_min=player_data.get('gold_per_min', 0),
            xp_per_min=player_data.get('xp_per_min', 0),
            last_hits=player_data.get('last_hits', 0),
            denies=player_data.get('denies', 0),
            item_0=player_data.get('item_0', 0),
            item_1=player_data.get('item_1', 0),
            item_2=player_data.get('item_2', 0),
            item_3=player_data.get('item_3', 0),
            item_4=player_data.get('item_4', 0),
            item_5=player_data.get('item_5', 0),
            purchase_log=player_data.get('purchase_log'),
            lane_pos=player_data.get('lane_pos'),
            kill_log=player_data.get('kills_log')
        )
        
        self.session.add(player)
    
    def _process_timeseries_data(self, match_id, player_data):
        """Process time-series data for a player (similar to process_timeseries_data in pro database)"""
        account_id = player_data.get('account_id')
        player_slot = player_data.get('player_slot', 0)
        player_name = player_data.get('personaname', '')
        
        # Determine starting lane
        lane_role = player_data.get('lane_role', 0)
        starting_lane = "unknown"
        
        if lane_role == 1:
            starting_lane = "safe"
        elif lane_role == 2:
            starting_lane = "mid"
        elif lane_role == 3:
            starting_lane = "off"
        elif lane_role == 4:
            starting_lane = "jungle"
        
        # Process gold/xp/cs snapshots if available
        snapshots = player_data.get('gold_t', [])
        for idx, gold in enumerate(snapshots):
            if idx % 60 == 0:  # Record only every minute to save space
                minute = idx // 60
                xp = player_data.get('xp_t', [])[idx] if idx < len(player_data.get('xp_t', [])) else None
                
                ts_entry = UserTimevsStats(
                    match_id=match_id,
                    player_id=str(account_id) if account_id else None,
                    player_name=player_name,
                    player_slot=player_slot,
                    time=idx,
                    starting_lane=starting_lane,
                    gold=gold,
                    xp=xp,
                    last_hits=None,  # Not typically available in time series
                    denies=None,    # Not typically available in time series
                    event_type="snapshot"
                )
                self.session.add(ts_entry)
        
        # Process kill events
        for event in player_data.get('kills_log', []):
            event_time = event.get('time')
            if event_time is not None:
                ts_entry = UserTimevsStats(
                    match_id=match_id,
                    player_id=str(account_id) if account_id else None,
                    player_name=player_name,
                    player_slot=player_slot,
                    time=event_time,
                    starting_lane=starting_lane,
                    event_type="kill",
                    killed_hero=event.get('key')
                )
                self.session.add(ts_entry)
        
        # Process purchase events
        for event in player_data.get('purchase_log', []):
            event_time = event.get('time')
            if event_time is not None:
                ts_entry = UserTimevsStats(
                    match_id=match_id,
                    player_id=str(account_id) if account_id else None,
                    player_name=player_name,
                    player_slot=player_slot,
                    time=event_time,
                    starting_lane=starting_lane,
                    event_type="purchase",
                    purchased_item=event.get('key')
                )
                self.session.add(ts_entry)
    
    def _process_teamfights(self, match_id, teamfights_data):
        """Process teamfight data from match details"""
        if not teamfights_data:
            return
            
        for tf_data in teamfights_data:
            # Create teamfight record
            teamfight = UserTeamFight(
                match_id=match_id,
                start=tf_data.get('start', 0),
                end=tf_data.get('end', 0),
                last_death=tf_data.get('last_death', 0),
                deaths=tf_data.get('deaths', 0)
            )
            
            self.session.add(teamfight)
            self.session.flush()  # To get the teamfight ID
            
            # Process player data for this teamfight
            for player_slot, player_data in tf_data.get('players', {}).items():
                if not player_data:
                    continue
                    
                player_slot = int(player_slot)  # Convert string keys to int if necessary
                
                tf_player = UserTeamFightPlayer(
                    teamfight_id=teamfight.id,
                    player_slot=player_slot,
                    deaths=player_data.get('deaths', 0),
                    buybacks=player_data.get('buybacks', 0),
                    damage=player_data.get('damage', 0),
                    healing=player_data.get('healing', 0),
                    gold_delta=player_data.get('gold_delta', 0),
                    xp_delta=player_data.get('xp_delta', 0),
                    gold_start=player_data.get('gold_t', [0])[0] if player_data.get('gold_t') else 0,
                    gold_end=player_data.get('gold_t', [0])[-1] if player_data.get('gold_t') else 0,
                    xp_start=player_data.get('xp_t', [0])[0] if player_data.get('xp_t') else 0,
                    xp_end=player_data.get('xp_t', [0])[-1] if player_data.get('xp_t') else 0
                )
                
                self.session.add(tf_player)
    
    def _process_draft_timing(self, match_id, draft_data):
        """Process draft timing data from match details"""
        if not draft_data:
            return
            
        for idx, pick_ban in enumerate(draft_data):
            draft_timing = UserDraftTiming(
                match_id=match_id,
                order=idx,
                pick=pick_ban.get('is_pick', False),
                active_team=pick_ban.get('team', 0),  # 0 for Radiant, 1 for Dire
                hero_id=pick_ban.get('hero_id', 0),
                player_slot=pick_ban.get('player_slot'),
                extra_time=pick_ban.get('extra_time', 0),
                total_time_taken=pick_ban.get('total_time_taken', 0)
            )
            
            self.session.add(draft_timing)
            
    def get_match_teamfights(self, match_id):
        """Get all teamfights for a specific match"""
        return self.session.query(UserTeamFight).filter_by(match_id=match_id).all()
        
    def get_teamfight_players(self, teamfight_id):
        """Get all player stats for a specific teamfight"""
        return self.session.query(UserTeamFightPlayer).filter_by(teamfight_id=teamfight_id).all()
        
    def get_match_draft(self, match_id):
        """Get draft information for a match"""
        return self.session.query(UserDraftTiming).filter_by(match_id=match_id).order_by(UserDraftTiming.order).all()
    
    def update_user_matches(self, user, limit=20):
        """Fetch and update matches for a user"""
        if not user:
            logger.error("No user provided")
            return 0
            
        matches = self.fetch_user_matches(user, limit=limit)
        logger.info(f"Found {len(matches)} matches for user {user.id}")
        
        count = 0
        for match_data in matches:
            match_id = match_data['match_id']
            
            # Check if match already exists
            existing_match = self.session.query(UserMatch).filter_by(match_id=match_id).first()
            if existing_match:
                logger.debug(f"Match {match_id} already exists, skipping")
                continue
            
            # Fetch full match details
            match_details = self.fetch_match_details(match_id)
            if match_details:
                self.process_match_details(match_details, user)
                count += 1
                # Sleep to avoid API rate limits
                time.sleep(1)
                
        logger.info(f"Added {count} new matches for user {user.id}")
        return count
    
    def get_user_matches(self, user, limit=20):
        """Get user's matches from the database"""
        if not user:
            return []
        return self.session.query(UserMatch).filter_by(user_id=user.id).order_by(UserMatch.start_time.desc()).limit(limit).all()
    
    def get_user_match_players(self, match_id):
        """Get all players in a specific match"""
        return self.session.query(UserMatchPlayer).filter_by(match_id=match_id).all()
    
    def get_match_lane_statistics(self, match_id):
        """Get lane statistics for a match (similar to lane analysis in pro database)"""
        players = self.get_user_match_players(match_id)
        radiant_players = [p for p in players if p.team == 0]
        dire_players = [p for p in players if p.team == 1]
        
        lanes = {}
        
        # Organize players by lane
        for team_name, team_players in [("Radiant", radiant_players), ("Dire", dire_players)]:
            lanes[team_name] = {
                "safe": None,
                "mid": None,
                "off": None,
                "jungle": None,
                "roam": None
            }
            
            for player in team_players:
                lane = player.lane
                if lane in lanes[team_name]:
                    lanes[team_name][lane] = player
        
        return lanes
    
    def get_lane_matchups(self, match_id):
        """Get lane matchups for a match (similar to your lane analysis)"""
        lanes = self.get_match_lane_statistics(match_id)
        
        # Create matchups
        matchups = {
            "mid": (lanes["Radiant"]["mid"], lanes["Dire"]["mid"]),
            "radiant_safe": (lanes["Radiant"]["safe"], lanes["Dire"]["off"]),
            "dire_safe": (lanes["Dire"]["safe"], lanes["Radiant"]["off"])
        }
        
        return matchups
    
    def get_all_player_slots(self, match_id):
        """Get all players in a match organized by slots (similar to your function)"""
        players = self.get_user_match_players(match_id)
        slots = {}
        
        for player in players:
            slots[player.player_slot] = player
            
        return slots
    
    def get_all_player_lanes(self, match_id):
        """Get all players in a match organized by lanes (similar to your function)"""
        players = self.get_user_match_players(match_id)
        lanes = {}
        
        for player in players:
            lanes[player.player_slot] = player.lane
            
        return lanes
    
    def match_player_lanes(self, match_id):
        """Match players by lanes (similar to your function)"""
        slots = self.get_all_player_slots(match_id)
        lanes = self.get_all_player_lanes(match_id)
        
        radiant_players = {}
        dire_players = {}
        
        for slot, lane in lanes.items():
            player = slots[slot]
            
            if slot < 128:  # Radiant
                radiant_players[lane] = player
            else:  # Dire
                dire_players[lane] = player
                
        return {
            "radiant": radiant_players,
            "dire": dire_players
        }
    
    def update_all_users(self, limit_per_user=20):
        """Update matches for all users in the database"""
        users = self.session.query(User).all()
        total_updates = 0
        
        for user in users:
            logger.info(f"Updating matches for user {user.username} (ID: {user.id})")
            updates = self.update_user_matches(user, limit=limit_per_user)
            total_updates += updates
            
        return total_updates

if __name__ == "__main__":
    # Create a database instance
    # Get the current directory of this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Create data path if it doesn't exist
    data_dir = os.path.join(current_dir, 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    db_file = os.path.join(data_dir, 'dota_matches.db')
    
    # Create the engine
    engine = create_engine(f'sqlite:///{db_file}')
    
    print("Creating user tables in the existing database...")
    # This will only create tables that don't exist yet
    Base.metadata.create_all(engine)
    print("User tables have been created successfully")
    
    print("User database setup complete!")
    print("You can now add your Dota 2 matches and create user accounts later.")