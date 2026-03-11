"""
================================================================
STEP 4: EXPORT QUERY RESULTS TO CSV
================================================================
European Football Intelligence System
File: 04_export_results.py

PURPOSE:
    Saves all 25 query results as CSV files in outputs/
    So you can open them in Excel or Power BI

HOW TO RUN:
    python 04_export_results.py
================================================================
"""

import sqlite3
import pandas as pd
import os

os.makedirs("outputs", exist_ok=True)
conn = sqlite3.connect("data/football.db")

queries = {
    "01_top_scorers": """
        SELECT p.player_name, t.team_name, l.league_name,
               ps.goals, ps.assists, ps.goals+ps.assists AS contributions,
               ps.appearances,
               ROUND(CAST(ps.goals AS FLOAT)/ps.appearances,2) AS goals_per_game
        FROM player_stats ps
        JOIN players p ON ps.player_id=p.player_id
        JOIN teams t   ON p.team_id=t.team_id
        JOIN leagues l ON t.league_id=l.league_id
        WHERE ps.season='2023/24' ORDER BY ps.goals DESC
    """,
    "02_premier_league_standings": """
        SELECT ts.league_position AS pos, t.team_name,
               ts.played,ts.won,ts.drawn,ts.lost,
               ts.goals_for,ts.goals_against,
               ts.goals_for-ts.goals_against AS gd,
               ts.points, ts.clean_sheets
        FROM team_stats ts
        JOIN teams t ON ts.team_id=t.team_id
        JOIN leagues l ON t.league_id=l.league_id
        WHERE l.league_name='Premier League' AND ts.season='2023/24'
        ORDER BY ts.league_position
    """,
    "03_all_champions": """
        SELECT l.league_name, l.country, t.team_name AS champion,
               t.manager, ts.points, ts.won, ts.goals_for, ts.clean_sheets
        FROM team_stats ts
        JOIN teams t ON ts.team_id=t.team_id
        JOIN leagues l ON t.league_id=l.league_id
        WHERE ts.league_position=1 AND ts.season='2023/24'
        ORDER BY ts.points DESC
    """,
    "04_top_assists": """
        SELECT p.player_name, t.team_name, l.league_name,
               ps.assists, ps.appearances,
               ROUND(CAST(ps.assists AS FLOAT)/ps.appearances,2) AS assists_per_game
        FROM player_stats ps
        JOIN players p ON ps.player_id=p.player_id
        JOIN teams t ON p.team_id=t.team_id
        JOIN leagues l ON t.league_id=l.league_id
        WHERE ps.season='2023/24' ORDER BY ps.assists DESC LIMIT 15
    """,
    "05_biggest_transfers": """
        SELECT p.player_name, p.position, t_from.team_name AS from_team,
               t_to.team_name AS to_team, tr.transfer_fee_m, tr.transfer_type
        FROM transfers tr
        JOIN players p ON tr.player_id=p.player_id
        JOIN teams t_from ON tr.from_team_id=t_from.team_id
        JOIN teams t_to ON tr.to_team_id=t_to.team_id
        ORDER BY tr.transfer_fee_m DESC
    """,
    "06_player_ratings": """
        SELECT p.player_name, p.position, t.team_name, l.league_name,
               ps.avg_rating, ps.goals, ps.assists, ps.appearances
        FROM player_stats ps
        JOIN players p ON ps.player_id=p.player_id
        JOIN teams t ON p.team_id=t.team_id
        JOIN leagues l ON t.league_id=l.league_id
        WHERE ps.season='2023/24'
        ORDER BY ps.avg_rating DESC LIMIT 20
    """,
    "07_goal_timing": """
        SELECT CASE
            WHEN minute BETWEEN 1  AND 15 THEN '01-15 min'
            WHEN minute BETWEEN 16 AND 30 THEN '16-30 min'
            WHEN minute BETWEEN 31 AND 45 THEN '31-45 min'
            WHEN minute BETWEEN 46 AND 60 THEN '46-60 min'
            WHEN minute BETWEEN 61 AND 75 THEN '61-75 min'
            WHEN minute BETWEEN 76 AND 90 THEN '76-90 min'
            ELSE '90+ min' END AS time_period,
               COUNT(*) AS goals
        FROM match_events WHERE event_type='GOAL'
        GROUP BY time_period ORDER BY time_period
    """,
    "08_league_summary": """
        SELECT l.league_name, l.country,
               COUNT(DISTINCT t.team_id) AS teams,
               SUM(ts.goals_for) AS total_goals,
               MAX(ts.points) AS champion_points
        FROM leagues l
        JOIN teams t ON t.league_id=l.league_id
        JOIN team_stats ts ON ts.team_id=t.team_id
        WHERE ts.season='2023/24'
        GROUP BY l.league_id ORDER BY champion_points DESC
    """,
    "09_best_value_players": """
        WITH v AS (
            SELECT p.player_name, p.position, t.team_name, l.league_name,
                   ps.goals, p.market_value_m,
                   ROUND(ps.goals/NULLIF(p.market_value_m,0),3) AS goals_per_million
            FROM player_stats ps
            JOIN players p ON ps.player_id=p.player_id
            JOIN teams t ON p.team_id=t.team_id
            JOIN leagues l ON t.league_id=l.league_id
            WHERE ps.season='2023/24' AND ps.goals>5 AND p.market_value_m>10
        )
        SELECT * FROM v ORDER BY goals_per_million DESC LIMIT 10
    """,
    "10_player_report_cards": """
        SELECT p.player_name, p.position, p.age, p.nationality,
               t.team_name, l.league_name,
               ps.goals, ps.assists, ps.goals+ps.assists AS contributions,
               ps.avg_rating, p.market_value_m,
               CASE
                   WHEN ps.avg_rating >= 8.0 THEN 'World Class'
                   WHEN ps.avg_rating >= 7.7 THEN 'Very Good'
                   WHEN ps.avg_rating >= 7.4 THEN 'Good'
                   ELSE 'Average'
               END AS tier
        FROM player_stats ps
        JOIN players p ON ps.player_id=p.player_id
        JOIN teams t ON p.team_id=t.team_id
        JOIN leagues l ON t.league_id=l.league_id
        WHERE ps.season='2023/24'
        ORDER BY ps.avg_rating DESC
    """,
}

print("=" * 55)
print("  EXPORTING RESULTS TO CSV")
print("=" * 55)

for filename, sql in queries.items():
    df = pd.read_sql_query(sql, conn)
    path = f"outputs/{filename}.csv"
    df.to_csv(path, index=False)
    print(f"  {filename}.csv  ({len(df)} rows)")

conn.close()
print()
print("  All files saved to outputs/ folder")
print("  Open them in Excel to explore the data!")
print("=" * 55)
