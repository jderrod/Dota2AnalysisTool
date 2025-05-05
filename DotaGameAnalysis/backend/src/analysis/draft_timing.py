from sqlalchemy import func, desc
import sys
import os
# Add the parent directory to sys.path to import database modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from database.database_pro_teams import DotaDatabase, DraftTiming
def get_picked_heros(match_id):
    """
    Get the most picked heroes from the DraftTiming table for a given match_id.
    
    This function filters for rows where 'pick' is True (or 1) within the specified match.
    It returns a list of tuples containing:
      (hero_id, pick_count, pick_percentage)
    
    Args:
        match_id (int): The match id to filter by.
    
    Returns:
        list of tuples: Each tuple has (hero_id, pick_count, pick_percentage)
    """
    db = DotaDatabase()
    session = db.Session()

    # Query to get count of picks per hero in the specified match
    picks_query = session.query(
        DraftTiming.hero_id,
        func.count(DraftTiming.id).label("pick_count")
    ).filter(
        DraftTiming.match_id == match_id,
        DraftTiming.pick == True  # Assuming pick is stored as a boolean (True/False)
    ).group_by(
        DraftTiming.hero_id
    ).order_by(
        desc("pick_count")
    ).all()

    # Get the total number of picks in this match (only count when pick is True)
    total_picks = session.query(
        func.count(DraftTiming.id)
    ).filter(
        DraftTiming.match_id == match_id,
        DraftTiming.pick == True
    ).scalar()

    session.close()

    # Calculate percentage and build the result list
    result = []
    for hero_id, pick_count in picks_query:
        percentage = (pick_count / total_picks) * 100 if total_picks else 0
        result.append((hero_id, pick_count, percentage))
    
    return result

def get_picked_heros_over_all_games():
    """
    Get the most picked heroes from the DraftTiming table for a given match_id.
    
    This function filters for rows where 'pick' is True (or 1) within the specified match.
    It returns a list of tuples containing:
      (hero_id, pick_count, pick_percentage)
    
    Args:
        match_id (int): The match id to filter by.
    
    Returns:
        list of tuples: Each tuple has (hero_id, pick_count, pick_percentage)
    """
    db = DotaDatabase()
    session = db.Session()

    # Query to get count of picks per hero in the specified match
    picks_query = session.query(
        DraftTiming.hero_id,
        func.count(DraftTiming.id).label("pick_count")
    ).filter(
        DraftTiming.pick == True  # Assuming pick is stored as a boolean (True/False)
    ).group_by(
        DraftTiming.hero_id
    ).order_by(
        desc("pick_count")
    ).all()

    # Get the total number of picks in this match (only count when pick is True)
    total_picks = session.query(
        func.count(DraftTiming.id)
    ).filter(
        DraftTiming.pick == True  # Assuming pick is stored as a boolean (True/False)
    ).scalar()

    session.close()

    # Calculate percentage and build the result list
    result = []
    for hero_id, pick_count in picks_query:
        percentage = (pick_count / total_picks) * 1000 if total_picks else 0
        result.append((hero_id, pick_count, percentage))
    
    return result

# Example usage:
if __name__ == '__main__':
    match_id = 8190432750  # Replace with a real match id from your data
    picked_heros = get_picked_heros(match_id)
    heros_over_all_games = get_picked_heros_over_all_games()
    for hero_id, count, pct in picked_heros:
        print(f"Hero ID {hero_id}: {count} picks ({pct:.2f}%)")
    print("\n")
    for hero_id, count, pct in heros_over_all_games:
        print(f"Hero ID {hero_id}: {count} picks ({pct:.2f}%)")