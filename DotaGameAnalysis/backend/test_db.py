import os
import sys
from database.user_games_db import UserDotaDatabase, User

def test_db_connection():
    """Test if we can create a database connection and save a user"""
    try:
        print("Creating database instance...")
        db = UserDotaDatabase()
        
        print("Database instance created successfully")
        print(f"Engine: {db.engine}")
        
        # Try a simple user creation
        test_steam_id = "76561198045776458"  # Test Steam ID
        
        print(f"Attempting to get or create user with Steam ID: {test_steam_id}")
        user = db.get_or_create_user(test_steam_id)
        
        print(f"Success! User created/retrieved: {user}")
        print(f"User ID: {user.id}")
        print(f"Steam ID: {user.steam_id}")
        print(f"Account ID: {user.account_id}")
        
        db.close()
        print("Database connection closed successfully")
        return True
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_db_connection()
