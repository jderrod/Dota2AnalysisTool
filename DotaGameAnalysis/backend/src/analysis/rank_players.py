from sqlalchemy import func, desc
from sqlalchemy import func, desc
import sys
import os
# Add the parent directory to sys.path to import database modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from database.database_pro_teams import DotaDatabase, MatchPlayer

def rank_players_by_benchmarks():
    """
    Rank players based on a weighted average of their benchmark percentages across all matches.
    The benchmark columns considered are:
      - bench_gold_pct
      - bench_xp_pct
      - bench_kills_pct
      - bench_last_hits_pct
      - bench_hero_damage_pct
      - bench_hero_healing_pct
      - bench_tower_damage_pct

    You can adjust the weights as needed.

    Returns:
        A list of tuples (account_id, overall_avg, match_count)
        sorted by overall_avg in descending order.
    """
    # Define weights for each benchmark column.
    weights = {
        "bench_gold_pct": 1.0,
        "bench_xp_pct": 1.0,
        "bench_kills_pct": 1.0,
        "bench_last_hits_pct": 1.0,
        "bench_hero_damage_pct": 1.0,
        "bench_hero_healing_pct": 1.0,
        "bench_tower_damage_pct": 1.0
    }
    
    db = DotaDatabase()
    session = db.Session()

    # Query each player's average benchmark stats from the match_player_metrics table.
    results = session.query(
        MatchPlayer.account_id,
        func.count(MatchPlayer.match_id).label("match_count"),
        func.avg(MatchPlayer.bench_gold_pct).label("avg_gold"),
        func.avg(MatchPlayer.bench_xp_pct).label("avg_xp"),
        func.avg(MatchPlayer.bench_kills_pct).label("avg_kills"),
        func.avg(MatchPlayer.bench_last_hits_pct).label("avg_last_hits"),
        func.avg(MatchPlayer.bench_hero_damage_pct).label("avg_hero_damage"),
        func.avg(MatchPlayer.bench_hero_healing_pct).label("avg_hero_healing"),
        func.avg(MatchPlayer.bench_tower_damage_pct).label("avg_tower_damage")
    ).group_by(
        MatchPlayer.account_id
    ).all()

    session.close()

    ranked_data = []
    for row in results:
        # Use 0 for None values
        avg_gold = row.avg_gold if row.avg_gold is not None else 0
        avg_xp = row.avg_xp if row.avg_xp is not None else 0
        avg_kills = row.avg_kills if row.avg_kills is not None else 0
        avg_last_hits = row.avg_last_hits if row.avg_last_hits is not None else 0
        avg_hero_damage = row.avg_hero_damage if row.avg_hero_damage is not None else 0
        avg_hero_healing = row.avg_hero_healing if row.avg_hero_healing is not None else 0
        avg_tower_damage = row.avg_tower_damage if row.avg_tower_damage is not None else 0

        # Calculate weighted sum and total weight.
        weighted_sum = (
            avg_gold * weights["bench_gold_pct"] +
            avg_xp * weights["bench_xp_pct"] +
            avg_kills * weights["bench_kills_pct"] +
            avg_last_hits * weights["bench_last_hits_pct"] +
            avg_hero_damage * weights["bench_hero_damage_pct"] +
            avg_hero_healing * weights["bench_hero_healing_pct"] +
            avg_tower_damage * weights["bench_tower_damage_pct"]
        )
        total_weight = sum(weights.values())
        overall_avg = weighted_sum / total_weight

        ranked_data.append((row.account_id, overall_avg, row.match_count))
    
    # Sort the players by overall average benchmark in descending order.
    ranked_data.sort(key=lambda x: x[1], reverse=True)
    
    return ranked_data

def assign_star_ratings(ranked_data):
    """
    Given a sorted list of players (descending by overall benchmark score),
    assign a star rating based on their percentile rank.
    
    Star ratings:
      - Top 20%: 5 stars
      - 20%-40%: 4 stars
      - 40%-60%: 3 stars
      - 60%-80%: 2 stars
      - Bottom 20%: 1 star

    Args:
        ranked_data (list): List of tuples (account_id, overall_avg, match_count)

    Returns:
        list: List of tuples (account_id, overall_avg, match_count, star_rating)
    """
    total = len(ranked_data)
    rated_players = []
    
    for index, (account_id, overall_avg, match_count) in enumerate(ranked_data):
        percentile = index / total  # 0 for best, nearly 1 for worst (since sorted descending)
        if percentile < 0.1:
            stars = 5
        elif percentile < 0.2:
            stars = 4.5
        elif percentile < 0.3:
            stars = 4
        elif percentile < 0.4:
            stars = 3.5
        elif percentile < 0.5:
            stars = 3
        elif percentile < 0.6:
            stars = 2.5
        elif percentile < 0.7:
            stars = 2
        elif percentile < 0.8:
            stars = 1.5
        else:
            stars = 1
        rated_players.append((account_id, overall_avg, match_count, stars))
    
    return rated_players

if __name__ == "__main__":
    # Get ranked player data from the database.
    rankings = rank_players_by_benchmarks()
    # Assign star ratings based on percentile rank.
    rated_players = assign_star_ratings(rankings)
    
    print("Player Rankings with Star Ratings:")
    for account_id, overall_avg, match_count, stars in rated_players:
        print(f"Account ID {account_id}: Overall Avg = {overall_avg:.3f}, "
              f"Matches = {match_count}, Star Rating = {stars} star(s)")
