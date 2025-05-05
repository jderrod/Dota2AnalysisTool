import os
import sys

# Add the parent directory to path so imports work correctly
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database.database_pro_teams import DotaDatabase

def run_update():
    db = DotaDatabase()
    db.update_all_tables()
    print("Database update completed successfully")

if __name__ == "__main__":
    run_update()
