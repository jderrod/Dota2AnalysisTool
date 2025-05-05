import sqlite3
import json
import os

# Path to the database
base_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_dir, 'backend', 'database', 'data', 'dota_matches.db')

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Fetch first non-null match_data
cursor.execute("SELECT match_data FROM pro_matches WHERE match_data IS NOT NULL LIMIT 1")
row = cursor.fetchone()
if not row:
    print('No match_data found.')
else:
    md = row[0]
    print('Raw match_data slice:', md[:500])
    try:
        data = json.loads(md)
        print('\nTop-level keys:', list(data.keys()))
        # Attempt common nested keys
        if 'radiant_team' in data:
            print('radiant_team:', data['radiant_team'])
        if 'dire_team' in data:
            print('dire_team:', data['dire_team'])
        # Check if teams list exists
        if 'teams' in data:
            print('First team entry:', data['teams'][0])
    except Exception as e:
        print('JSON parsing error:', e)

conn.close()
