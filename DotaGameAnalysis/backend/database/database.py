"""
Dota 2 Professional Match Database

This module handles database operations for storing and retrieving Dota 2 professional match data.
"""
import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, ForeignKey, DateTime, Text, JSON, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize SQLAlchemy
Base = declarative_base()

# Define database schema
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
    
    def __repr__(self):
        return f"<League(id={self.id}, name='{self.name}', tier={self.tier})>"

class Team(Base):
    """Team table for storing team information"""
    __tablename__ = 'teams'
    
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, unique=True)
    name = Column(String(255))
    tag = Column(String(50))
    logo_url = Column(String(255), nullable=True)
    
    # Relationships
    radiant_matches = relationship("Match", foreign_keys="Match.radiant_team_id", back_populates="radiant_team")
    dire_matches = relationship("Match", foreign_keys="Match.dire_team_id", back_populates="dire_team")
    
    def __repr__(self):
        return f"<Team(id={self.id}, name='{self.name}')>"

class Match(Base):
    """Match table for storing basic match information"""
    __tablename__ = 'matches'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, unique=True)
    start_time = Column(DateTime)
    duration = Column(Integer)  # in seconds
    
    # League information
    league_id = Column(Integer, ForeignKey('leagues.league_id'))
    series_id = Column(Integer, nullable=True)
    series_type = Column(Integer, nullable=True)
    
    # Team information
    radiant_team_id = Column(Integer, ForeignKey('teams.team_id'), nullable=True)
    dire_team_id = Column(Integer, ForeignKey('teams.team_id'), nullable=True)
    
    # Match results
    radiant_score = Column(Integer)
    dire_score = Column(Integer)
    radiant_win = Column(Boolean)
    
    # Game version
    game_version = Column(Integer, nullable=True)
    
    # Match data (JSON for flexibility)
    match_data = Column(JSON, nullable=True)
    
    # Timestamps
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    league = relationship("League", back_populates="matches")
    radiant_team = relationship("Team", foreign_keys=[radiant_team_id], back_populates="radiant_matches")
    dire_team = relationship("Team", foreign_keys=[dire_team_id], back_populates="dire_matches")
    players = relationship("MatchPlayer", back_populates="match")
    
    def __repr__(self):
        return f"<Match(match_id={self.match_id}, radiant={self.radiant_team_id}, dire={self.dire_team_id})>"

class Player(Base):
    """Player table for storing player information"""
    __tablename__ = 'players'
    
    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, unique=True)
    name = Column(String(255))
    personaname = Column(String(255), nullable=True)
    country_code = Column(String(10), nullable=True)
    
    # Relationships
    match_players = relationship("MatchPlayer", back_populates="player")
    
    def __repr__(self):
        return f"<Player(id={self.id}, name='{self.name}')>"

class Hero(Base):
    """Hero table for storing hero information"""
    __tablename__ = 'heroes'
    
    id = Column(Integer, primary_key=True)
    hero_id = Column(Integer, unique=True)
    name = Column(String(255))
    localized_name = Column(String(255))
    primary_attr = Column(String(10))
    attack_type = Column(String(50))
    roles = Column(JSON)  # Store roles as a JSON array
    
    # Relationships
    match_players = relationship("MatchPlayer", back_populates="hero")
    
    def __repr__(self):
        return f"<Hero(id={self.id}, name='{self.localized_name}')>"

class MatchPlayer(Base):
    """MatchPlayer table for storing player performance in a match"""
    __tablename__ = 'match_players'
    
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('matches.match_id'))
    account_id = Column(Integer, ForeignKey('players.account_id'))
    hero_id = Column(Integer, ForeignKey('heroes.hero_id'))
    player_slot = Column(Integer)  # 0-127 are Radiant, 128-255 are Dire
    
    # Performance metrics
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
    
    # Items
    item_0 = Column(Integer)
    item_1 = Column(Integer)
    item_2 = Column(Integer)
    item_3 = Column(Integer)
    item_4 = Column(Integer)
    item_5 = Column(Integer)
    
    # Detailed data
    player_data = Column(JSON, nullable=True)
    
    # Relationships
    match = relationship("Match", back_populates="players")
    player = relationship("Player", back_populates="match_players")
    hero = relationship("Hero", back_populates="match_players")
    
    def __repr__(self):
        return f"<MatchPlayer(match_id={self.match_id}, account_id={self.account_id}, hero_id={self.hero_id})>"


class DotaDatabase:
    """
    A class to manage database operations for Dota 2 match data.
    """
    
    def __init__(self, db_url=None):
        """
        Initialize the database connection.
        
        Args:
            db_url (str, optional): Database connection URL. Defaults to SQLite.
        """
        if not db_url:
            # Default to SQLite database in the data directory
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            db_url = f"sqlite:///{data_dir}/dota_matches.db"
            
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        
    def create_tables(self):
        """Create all database tables."""
        Base.metadata.create_all(self.engine)
        logger.info("Database tables created")
        
    def insert_match_summary(self, match_data):
        """
        Insert a match summary into the database.
        
        Args:
            match_data (dict): Match data from the OpenDota API.
            
        Returns:
            Match: The inserted Match object.
        """
        session = self.Session()
        
        try:
            # Check if match already exists
            existing_match = session.query(Match).filter_by(match_id=match_data['match_id']).first()
            if existing_match:
                logger.info(f"Match {match_data['match_id']} already exists in database")
                return existing_match
            
            # Create or get league
            league = session.query(League).filter_by(league_id=match_data['leagueid']).first()
            if not league:
                league = League(
                    league_id=match_data['leagueid'],
                    name=match_data.get('league_name', 'Unknown'),
                    tier=match_data.get('league_tier', 0),
                    region=match_data.get('region', 'Unknown')
                )
                session.add(league)
            
            # Create or get teams
            radiant_team = None
            dire_team = None
            
            if match_data.get('radiant_team_id'):
                radiant_team = session.query(Team).filter_by(team_id=match_data['radiant_team_id']).first()
                if not radiant_team:
                    radiant_team = Team(
                        team_id=match_data['radiant_team_id'],
                        name=match_data.get('radiant_name', 'Unknown Radiant'),
                        tag=match_data.get('radiant_team_tag', '')
                    )
                    session.add(radiant_team)
            
            if match_data.get('dire_team_id'):
                dire_team = session.query(Team).filter_by(team_id=match_data['dire_team_id']).first()
                if not dire_team:
                    dire_team = Team(
                        team_id=match_data['dire_team_id'],
                        name=match_data.get('dire_name', 'Unknown Dire'),
                        tag=match_data.get('dire_team_tag', '')
                    )
                    session.add(dire_team)
            
            # Create match
            match = Match(
                match_id=match_data['match_id'],
                start_time=datetime.fromtimestamp(match_data['start_time']),
                duration=match_data['duration'],
                league_id=match_data['leagueid'],
                series_id=match_data.get('series_id'),
                series_type=match_data.get('series_type'),
                radiant_team_id=match_data.get('radiant_team_id'),
                dire_team_id=match_data.get('dire_team_id'),
                radiant_score=match_data.get('radiant_score', 0),
                dire_score=match_data.get('dire_score', 0),
                radiant_win=match_data['radiant_win'],
                game_version=match_data.get('version')
            )
            
            session.add(match)
            session.commit()
            logger.info(f"Inserted match {match_data['match_id']} into database")
            return match
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting match {match_data.get('match_id')}: {e}")
            return None
        finally:
            session.close()
    
    def insert_match_details(self, match_details):
        """
        Insert detailed match information, including player performances.
        
        Args:
            match_details (dict): Detailed match data from the OpenDota API.
            
        Returns:
            Match: The inserted or updated Match object.
        """
        session = self.Session()
        
        try:
            # Check if match already exists
            match = session.query(Match).filter_by(match_id=match_details['match_id']).first()
            
            if not match:
                # Create a basic match record first
                match = Match(
                    match_id=match_details['match_id'],
                    start_time=datetime.fromtimestamp(match_details['start_time']),
                    duration=match_details['duration'],
                    radiant_score=match_details.get('radiant_score', 0),
                    dire_score=match_details.get('dire_score', 0),
                    radiant_win=match_details['radiant_win'],
                    game_version=match_details.get('version'),
                    match_data=match_details
                )
                session.add(match)
            else:
                # Update existing match with full details
                match.match_data = match_details
            
            # Process players
            for player_data in match_details.get('players', []):
                account_id = player_data.get('account_id')
                
                # Skip anonymous players
                if not account_id:
                    continue
                
                # Create or get player
                player = session.query(Player).filter_by(account_id=account_id).first()
                if not player:
                    player = Player(
                        account_id=account_id,
                        name=player_data.get('name', ''),
                        personaname=player_data.get('personaname', '')
                    )
                    session.add(player)
                
                # Create or get hero
                hero_id = player_data.get('hero_id')
                hero = session.query(Hero).filter_by(hero_id=hero_id).first()
                if not hero:
                    # We should have a complete hero list loaded separately,
                    # but for now create a minimal hero record
                    hero = Hero(
                        hero_id=hero_id,
                        name=f"hero_{hero_id}",
                        localized_name=f"Hero {hero_id}",
                        primary_attr="unknown",
                        attack_type="unknown",
                        roles=[]
                    )
                    session.add(hero)
                
                # Create match player record
                match_player = MatchPlayer(
                    match_id=match_details['match_id'],
                    account_id=account_id,
                    hero_id=hero_id,
                    player_slot=player_data.get('player_slot', 0),
                    kills=player_data.get('kills', 0),
                    deaths=player_data.get('deaths', 0),
                    assists=player_data.get('assists', 0),
                    last_hits=player_data.get('last_hits', 0),
                    denies=player_data.get('denies', 0),
                    gold_per_min=player_data.get('gold_per_min', 0),
                    xp_per_min=player_data.get('xp_per_min', 0),
                    hero_damage=player_data.get('hero_damage', 0),
                    tower_damage=player_data.get('tower_damage', 0),
                    hero_healing=player_data.get('hero_healing', 0),
                    level=player_data.get('level', 0),
                    item_0=player_data.get('item_0', 0),
                    item_1=player_data.get('item_1', 0),
                    item_2=player_data.get('item_2', 0),
                    item_3=player_data.get('item_3', 0),
                    item_4=player_data.get('item_4', 0),
                    item_5=player_data.get('item_5', 0),
                    player_data=player_data
                )
                
                session.add(match_player)
            
            session.commit()
            logger.info(f"Inserted detailed match data for match {match_details['match_id']}")
            return match
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting match details for {match_details.get('match_id')}: {e}")
            return None
        finally:
            session.close()
    
    def batch_insert_matches(self, matches):
        """
        Insert multiple match summaries into the database.
        
        Args:
            matches (list): List of match data from the OpenDota API.
            
        Returns:
            int: Number of matches successfully inserted.
        """
        success_count = 0
        
        for match_data in matches:
            if self.insert_match_summary(match_data):
                success_count += 1
                
        logger.info(f"Batch inserted {success_count} out of {len(matches)} matches")
        return success_count
    
    def batch_insert_match_details(self, match_details_list):
        """
        Insert multiple match details into the database.
        
        Args:
            match_details_list (list): List of match details from the OpenDota API.
            
        Returns:
            int: Number of match details successfully inserted.
        """
        success_count = 0
        
        for match_details in match_details_list:
            if self.insert_match_details(match_details):
                success_count += 1
                
        logger.info(f"Batch inserted {success_count} out of {len(match_details_list)} match details")
        return success_count
    
    def get_matches_by_date_range(self, start_date, end_date):
        """
        Get matches within a date range.
        
        Args:
            start_date (datetime): Start date.
            end_date (datetime): End date.
            
        Returns:
            list: List of Match objects.
        """
        session = self.Session()
        
        try:
            matches = session.query(Match).filter(
                Match.start_time >= start_date,
                Match.start_time <= end_date
            ).all()
            
            return matches
        finally:
            session.close()
    
    def get_team_statistics(self, team_id):
        """
        Get statistics for a specific team.
        
        Args:
            team_id (int): Team ID.
            
        Returns:
            dict: Team statistics.
        """
        session = self.Session()
        
        try:
            # Get all matches involving this team
            radiant_matches = session.query(Match).filter(Match.radiant_team_id == team_id).all()
            dire_matches = session.query(Match).filter(Match.dire_team_id == team_id).all()
            
            total_matches = len(radiant_matches) + len(dire_matches)
            
            if total_matches == 0:
                return {"error": f"No matches found for team {team_id}"}
            
            # Calculate win count
            radiant_wins = sum(1 for match in radiant_matches if match.radiant_win)
            dire_wins = sum(1 for match in dire_matches if not match.radiant_win)
            total_wins = radiant_wins + dire_wins
            
            # Calculate average match duration
            total_duration = sum(match.duration for match in radiant_matches + dire_matches)
            avg_duration = total_duration / total_matches if total_matches > 0 else 0
            
            # Calculate average scores
            radiant_team_scores = [match.radiant_score for match in radiant_matches]
            dire_team_scores = [match.dire_score for match in dire_matches]
            
            opponent_radiant_scores = [match.dire_score for match in radiant_matches]
            opponent_dire_scores = [match.radiant_score for match in dire_matches]
            
            team_scores = radiant_team_scores + dire_team_scores
            opponent_scores = opponent_radiant_scores + opponent_dire_scores
            
            avg_team_score = sum(team_scores) / total_matches if total_matches > 0 else 0
            avg_opponent_score = sum(opponent_scores) / total_matches if total_matches > 0 else 0
            
            return {
                "team_id": team_id,
                "total_matches": total_matches,
                "total_wins": total_wins,
                "total_losses": total_matches - total_wins,
                "win_rate": (total_wins / total_matches) if total_matches > 0 else 0,
                "avg_duration": avg_duration,
                "avg_team_score": avg_team_score,
                "avg_opponent_score": avg_opponent_score
            }
        finally:
            session.close()
    
    def get_player_statistics(self, account_id):
        """
        Get statistics for a specific player.
        
        Args:
            account_id (int): Player account ID.
            
        Returns:
            dict: Player statistics.
        """
        session = self.Session()
        
        try:
            # Get all matches involving this player
            match_players = session.query(MatchPlayer).filter(MatchPlayer.account_id == account_id).all()
            
            if not match_players:
                return {"error": f"No matches found for player {account_id}"}
            
            total_matches = len(match_players)
            
            # Calculate averages
            total_kills = sum(mp.kills for mp in match_players)
            total_deaths = sum(mp.deaths for mp in match_players)
            total_assists = sum(mp.assists for mp in match_players)
            total_gpm = sum(mp.gold_per_min for mp in match_players)
            total_xpm = sum(mp.xp_per_min for mp in match_players)
            
            # Count wins
            wins = 0
            for mp in match_players:
                match = session.query(Match).filter(Match.match_id == mp.match_id).first()
                if match:
                    is_radiant = mp.player_slot < 128
                    if (is_radiant and match.radiant_win) or (not is_radiant and not match.radiant_win):
                        wins += 1
            
            # Calculate most played heroes
            hero_counts = {}
            for mp in match_players:
                hero_id = mp.hero_id
                hero_counts[hero_id] = hero_counts.get(hero_id, 0) + 1
            
            most_played_heroes = sorted(hero_counts.items(), key=lambda x: x[1], reverse=True)
            
            return {
                "account_id": account_id,
                "total_matches": total_matches,
                "wins": wins,
                "losses": total_matches - wins,
                "win_rate": (wins / total_matches) if total_matches > 0 else 0,
                "avg_kills": total_kills / total_matches if total_matches > 0 else 0,
                "avg_deaths": total_deaths / total_matches if total_matches > 0 else 0,
                "avg_assists": total_assists / total_matches if total_matches > 0 else 0,
                "avg_gpm": total_gpm / total_matches if total_matches > 0 else 0,
                "avg_xpm": total_xpm / total_matches if total_matches > 0 else 0,
                "most_played_heroes": most_played_heroes[:5]  # Top 5 most played heroes
            }
        finally:
            session.close()
    
    def get_hero_statistics(self):
        """
        Get statistics for all heroes.
        
        Returns:
            list: List of hero statistics.
        """
        session = self.Session()
        
        try:
            # Get all heroes
            heroes = session.query(Hero).all()
            
            hero_stats = []
            
            for hero in heroes:
                # Get all matches where this hero was played
                match_players = session.query(MatchPlayer).filter(MatchPlayer.hero_id == hero.hero_id).all()
                
                if not match_players:
                    continue
                
                total_matches = len(match_players)
                
                # Calculate averages
                total_kills = sum(mp.kills for mp in match_players)
                total_deaths = sum(mp.deaths for mp in match_players)
                total_assists = sum(mp.assists for mp in match_players)
                
                # Count wins
                wins = 0
                for mp in match_players:
                    match = session.query(Match).filter(Match.match_id == mp.match_id).first()
                    if match:
                        is_radiant = mp.player_slot < 128
                        if (is_radiant and match.radiant_win) or (not is_radiant and not match.radiant_win):
                            wins += 1
                
                hero_stats.append({
                    "hero_id": hero.hero_id,
                    "name": hero.localized_name,
                    "total_matches": total_matches,
                    "wins": wins,
                    "losses": total_matches - wins,
                    "win_rate": (wins / total_matches) if total_matches > 0 else 0,
                    "avg_kills": total_kills / total_matches if total_matches > 0 else 0,
                    "avg_deaths": total_deaths / total_matches if total_matches > 0 else 0,
                    "avg_assists": total_assists / total_matches if total_matches > 0 else 0
                })
            
            return hero_stats
        finally:
            session.close()


if __name__ == "__main__":
    # Initialize database
    db = DotaDatabase()
    db.create_tables()
    
    # Example: Print some basic statistics
    print("Database initialized. Tables created.")
