from flask import Blueprint, request, jsonify, current_app
import os
import sys
import json

# Add the parent directory to sys.path to import the database modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the UserDotaDatabase class from the existing module
from database.user_games_db import UserDotaDatabase, User

# Create a Blueprint for player routes
player_routes = Blueprint('player_routes', __name__)

@player_routes.route('/api/players/save', methods=['POST'])
def save_player():
    """Save a player's Steam ID to the database using the existing UserDotaDatabase class"""
    data = request.get_json()
    steam_id = data.get('steamId')
    
    if not steam_id:
        return jsonify({'error': 'Steam ID is required'}), 400
    
    try:
        # Create a database instance
        db = UserDotaDatabase()
        
        # Use the existing get_or_create_user method
        user = db.get_or_create_user(steam_id)
        
        # Create a response dictionary before closing the session
        player_data = {
            'id': user.id,
            'steam_id': user.steam_id,
            'account_id': user.account_id,
            'username': user.username,
            'avatar': user.avatar
        }
        
        # Close the database connection
        db.close()
        
        # Return success response with user info
        return jsonify({
            'success': True, 
            'message': 'Player saved successfully',
            'player': player_data
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@player_routes.route('/api/players', methods=['GET'])
def get_players():
    """Get all players from the database using UserDotaDatabase"""
    try:
        # Create a database instance
        db = UserDotaDatabase()
        
        # Use the session to query all users
        users = db.session.query(User).all()
        
        # Convert user objects to dictionaries - Extract all data before closing session
        result = [
            {
                "id": user.id,
                "steam_id": user.steam_id,
                "account_id": user.account_id,
                "username": user.username,
                "avatar": user.avatar,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "last_login": user.last_login.isoformat() if user.last_login else None
            } 
            for user in users
        ]
        
        # Close the database connection
        db.close()
        
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@player_routes.route('/api/players/<steam_id>', methods=['GET'])
def get_player(steam_id):
    """Get a specific player by Steam ID using UserDotaDatabase"""
    try:
        # Create a database instance
        db = UserDotaDatabase()
        
        # Query for the user with the provided steam_id
        user = db.session.query(User).filter_by(steam_id=steam_id).first()
        
        if not user:
            db.close()
            return jsonify({'error': 'Player not found'}), 404
        
        # Get the player's matches (up to 10 most recent)
        user_matches = db.get_user_matches(user, limit=10)
        
        # Format the response - Extract all data before closing session
        recent_matches = [{
            "match_id": match.match_id,
            "start_time": match.start_time.isoformat() if match.start_time else None,
            "duration": match.duration,
            "radiant_win": match.radiant_win
        } for match in user_matches]
        
        result = {
            "id": user.id,
            "steam_id": user.steam_id,
            "account_id": user.account_id,
            "username": user.username,
            "avatar": user.avatar,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "last_login": user.last_login.isoformat() if user.last_login else None,
            "match_count": len(user_matches),
            "recent_matches": recent_matches
        }
        
        # Close the database connection
        db.close()
        
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@player_routes.route('/api/players/<steam_id>/update-matches', methods=['POST'])
def update_player_matches(steam_id):
    """Update matches for a player using the existing UserDotaDatabase class"""
    try:
        # Create a database instance
        db = UserDotaDatabase()
        
        # Find the user
        user = db.session.query(User).filter_by(steam_id=steam_id).first()
        
        if not user:
            db.close()
            return jsonify({'error': 'Player not found'}), 404
        
        # Update the user's matches (default: 20 most recent)
        match_count = db.update_user_matches(user)
        
        # Store the count before closing the session
        count_value = match_count
        
        # Close the database connection
        db.close()
        
        return jsonify({
            'success': True, 
            'message': f'Updated {count_value} matches for player',
            'match_count': count_value
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@player_routes.route('/api/players/<steam_id>/analyze', methods=['GET'])
def analyze_player(steam_id):
    """Analyze match data for a specific player using your existing lane analysis"""
    try:
        # Create a database instance
        db = UserDotaDatabase()
        
        # Find the user
        user = db.session.query(User).filter_by(steam_id=steam_id).first()
        
        if not user:
            db.close()
            return jsonify({'error': 'Player not found'}), 404
        
        # Extract user data we need before further operations
        user_info = {
            'steam_id': user.steam_id,
            'account_id': user.account_id,
            'username': user.username
        }
        
        # Get the player's matches
        matches = db.get_user_matches(user)
        
        if not matches:
            db.close()
            return jsonify({'error': 'No matches found for this player'}), 404
        
        # Analysis results
        results = {
            'player_info': user_info,
            'match_count': len(matches),
            'lane_analysis': {},
            'match_analysis': []
        }
        
        # Analyze each match
        for match in matches:
            # Get lane matchups for this match
            try:
                lane_matchups = db.match_player_lanes(match.match_id)
                
                # Get player performance in this match
                match_players = db.get_user_match_players(match.match_id)
                
                # Find the player's performance
                player_performance = None
                for mp in match_players:
                    if str(mp.account_id) == str(user_info['account_id']):
                        player_performance = mp
                        break
                
                # Add match analysis - Extract all the data we need
                if player_performance:
                    match_analysis = {
                        'match_id': match.match_id,
                        'hero_id': player_performance.hero_id,
                        'kills': player_performance.kills,
                        'deaths': player_performance.deaths,
                        'assists': player_performance.assists,
                        'kda': (player_performance.kills + player_performance.assists) / max(player_performance.deaths, 1),
                        'gpm': player_performance.gold_per_min,
                        'xpm': player_performance.xp_per_min,
                        'lane': player_performance.lane if hasattr(player_performance, 'lane') else 'unknown'
                    }
                    results['match_analysis'].append(match_analysis)
            except Exception as match_error:
                # Continue with other matches if one fails
                continue
                
        # Close the database connection
        db.close()
        
        return jsonify(results), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500