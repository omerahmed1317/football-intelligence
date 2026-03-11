"""
================================================================
STEP 3: RUN 25 SQL QUERIES
================================================================
European Football Intelligence System
File: 03_run_queries.py

PURPOSE:
    Runs 25 professional SQL queries covering:
    - Basic SELECT & filtering
    - GROUP BY & aggregation
    - JOINs (INNER, LEFT, multiple tables)
    - Subqueries
    - CTEs (Common Table Expressions)
    - Window Functions (RANK, ROW_NUMBER, LAG)
    - CASE WHEN statements
    - HAVING clauses

HOW TO RUN:
    python 03_run_queries.py

INTERVIEW TIP:
    Every query here answers a real business question.
    When asked "show me your SQL skills", open this file
    and walk through 3-4 queries explaining the WHY.
================================================================
"""

import sqlite3
import pandas as pd
import os

os.makedirs("outputs", exist_ok=True)
conn = sqlite3.connect("data/football.db")

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 120)
pd.set_option("display.max_rows", 20)

def run_query(title, sql, show_rows=10):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)
    df = pd.read_sql_query(sql, conn)
    print(df.head(show_rows).to_string(index=False))
    print(f"  [{len(df)} rows returned]")
    return df

results = {}

# ============================================================
# BASIC QUERIES (1-5)
# ============================================================

results["Q01"] = run_query(
    "Q01 | Top 10 Scorers Across All 5 Leagues",
    """
    SELECT
        p.player_name,
        t.team_name,
        l.league_name,
        ps.goals,
        ps.assists,
        ps.goals + ps.assists AS goal_contributions,
        ps.appearances,
        ROUND(CAST(ps.goals AS FLOAT) / ps.appearances, 2) AS goals_per_game
    FROM player_stats ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN teams t   ON p.team_id    = t.team_id
    JOIN leagues l ON t.league_id  = l.league_id
    WHERE ps.season = '2023/24'
    ORDER BY ps.goals DESC
    LIMIT 10
    """
)

results["Q02"] = run_query(
    "Q02 | League Standings - Premier League 2023/24",
    """
    SELECT
        ts.league_position  AS pos,
        t.team_name,
        ts.played           AS pld,
        ts.won              AS w,
        ts.drawn            AS d,
        ts.lost             AS l,
        ts.goals_for        AS gf,
        ts.goals_against    AS ga,
        ts.goals_for - ts.goals_against AS gd,
        ts.points           AS pts,
        ts.clean_sheets     AS cs
    FROM team_stats ts
    JOIN teams t ON ts.team_id = t.team_id
    JOIN leagues l ON t.league_id = l.league_id
    WHERE l.league_name = 'Premier League'
    AND ts.season = '2023/24'
    ORDER BY ts.league_position
    """
)

results["Q03"] = run_query(
    "Q03 | All League Champions 2023/24",
    """
    SELECT
        l.league_name,
        l.country,
        t.team_name         AS champion,
        t.manager,
        ts.points,
        ts.won,
        ts.goals_for,
        ts.clean_sheets
    FROM team_stats ts
    JOIN teams t   ON ts.team_id  = t.team_id
    JOIN leagues l ON t.league_id = l.league_id
    WHERE ts.league_position = 1
    AND ts.season = '2023/24'
    ORDER BY ts.points DESC
    """
)

results["Q04"] = run_query(
    "Q04 | Players With Most Assists Per League",
    """
    SELECT
        p.player_name,
        t.team_name,
        l.league_name,
        ps.assists,
        ps.appearances,
        ROUND(CAST(ps.assists AS FLOAT) / ps.appearances, 2) AS assists_per_game
    FROM player_stats ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN teams t   ON p.team_id    = t.team_id
    JOIN leagues l ON t.league_id  = l.league_id
    WHERE ps.season = '2023/24'
    ORDER BY ps.assists DESC
    LIMIT 10
    """
)

results["Q05"] = run_query(
    "Q05 | Most Disciplined Teams (Fewest Yellow Cards)",
    """
    SELECT
        t.team_name,
        l.league_name,
        SUM(ps.yellow_cards) AS total_yellows,
        SUM(ps.red_cards)    AS total_reds,
        COUNT(p.player_id)   AS squad_size
    FROM player_stats ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN teams t   ON p.team_id    = t.team_id
    JOIN leagues l ON t.league_id  = l.league_id
    WHERE ps.season = '2023/24'
    GROUP BY t.team_id
    ORDER BY total_yellows ASC
    LIMIT 10
    """
)

# ============================================================
# INTERMEDIATE QUERIES (6-12)
# ============================================================

results["Q06"] = run_query(
    "Q06 | Home vs Away Performance (All Teams)",
    """
    SELECT
        t.team_name,
        l.league_name,
        SUM(CASE WHEN m.home_team_id = t.team_id THEN m.home_goals ELSE 0 END) AS home_goals_scored,
        SUM(CASE WHEN m.away_team_id = t.team_id THEN m.away_goals ELSE 0 END) AS away_goals_scored,
        SUM(CASE WHEN m.home_team_id = t.team_id AND m.home_goals > m.away_goals THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN m.away_team_id = t.team_id AND m.away_goals > m.home_goals THEN 1 ELSE 0 END) AS away_wins
    FROM matches m
    JOIN teams t   ON (t.team_id = m.home_team_id OR t.team_id = m.away_team_id)
    JOIN leagues l ON t.league_id = l.league_id
    WHERE m.season = '2023/24'
    GROUP BY t.team_id
    HAVING home_wins + away_wins > 3
    ORDER BY home_wins DESC
    LIMIT 10
    """
)

results["Q07"] = run_query(
    "Q07 | Most Valuable Players by Position",
    """
    SELECT
        p.position,
        p.player_name,
        t.team_name,
        p.market_value_m    AS value_million,
        p.wage_weekly_k     AS weekly_wage_k,
        ps.avg_rating
    FROM players p
    JOIN player_stats ps ON p.player_id = ps.player_id
    JOIN teams t         ON p.team_id   = t.team_id
    WHERE ps.season = '2023/24'
    AND p.market_value_m = (
        SELECT MAX(p2.market_value_m)
        FROM players p2
        WHERE p2.position = p.position
    )
    ORDER BY p.market_value_m DESC
    """
)

results["Q08"] = run_query(
    "Q08 | Transfer Analysis - Biggest Fees Paid",
    """
    SELECT
        p.player_name,
        p.position,
        p.nationality,
        t_from.team_name    AS sold_by,
        t_to.team_name      AS bought_by,
        tr.transfer_fee_m   AS fee_million,
        tr.transfer_type,
        tr.transfer_date
    FROM transfers tr
    JOIN players p          ON tr.player_id     = p.player_id
    JOIN teams t_from       ON tr.from_team_id  = t_from.team_id
    JOIN teams t_to         ON tr.to_team_id    = t_to.team_id
    WHERE tr.transfer_type = 'Permanent'
    ORDER BY tr.transfer_fee_m DESC
    """
)

results["Q09"] = run_query(
    "Q09 | Players Performing Above Their League Average (Subquery)",
    """
    SELECT
        p.player_name,
        t.team_name,
        l.league_name,
        ps.goals,
        ps.avg_rating,
        ROUND(league_avg.avg_goals, 2) AS league_avg_goals
    FROM player_stats ps
    JOIN players p  ON ps.player_id = p.player_id
    JOIN teams t    ON p.team_id    = t.team_id
    JOIN leagues l  ON t.league_id  = l.league_id
    JOIN (
        SELECT
            l2.league_id,
            AVG(ps2.goals) AS avg_goals
        FROM player_stats ps2
        JOIN players p2 ON ps2.player_id = p2.player_id
        JOIN teams t2   ON p2.team_id    = t2.team_id
        JOIN leagues l2 ON t2.league_id  = l2.league_id
        WHERE ps2.season = '2023/24'
        GROUP BY l2.league_id
    ) league_avg ON l.league_id = league_avg.league_id
    WHERE ps.goals > league_avg.avg_goals
    AND ps.season = '2023/24'
    ORDER BY ps.goals DESC
    LIMIT 15
    """
)

results["Q10"] = run_query(
    "Q10 | Goal Timing Analysis - When Do Goals Happen?",
    """
    SELECT
        CASE
            WHEN me.minute BETWEEN 1  AND 15  THEN '01-15 min'
            WHEN me.minute BETWEEN 16 AND 30  THEN '16-30 min'
            WHEN me.minute BETWEEN 31 AND 45  THEN '31-45 min'
            WHEN me.minute BETWEEN 46 AND 60  THEN '46-60 min'
            WHEN me.minute BETWEEN 61 AND 75  THEN '61-75 min'
            WHEN me.minute BETWEEN 76 AND 90  THEN '76-90 min'
            ELSE '90+ min'
        END AS time_period,
        COUNT(*) AS goals_scored,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM match_events WHERE event_type = 'GOAL'), 1) AS percentage
    FROM match_events me
    WHERE me.event_type = 'GOAL'
    GROUP BY time_period
    ORDER BY time_period
    """
)

results["Q11"] = run_query(
    "Q11 | Teams With Best Attack vs Best Defence",
    """
    SELECT
        t.team_name,
        l.league_name,
        ts.goals_for        AS attack_goals,
        ts.goals_against    AS defence_goals,
        ts.goals_for - ts.goals_against AS goal_diff,
        ts.clean_sheets,
        ts.points,
        CASE
            WHEN ts.goals_for >= 80   THEN 'Elite Attack'
            WHEN ts.goals_for >= 65   THEN 'Strong Attack'
            ELSE 'Average Attack'
        END AS attack_rating,
        CASE
            WHEN ts.goals_against <= 30 THEN 'Elite Defence'
            WHEN ts.goals_against <= 45 THEN 'Strong Defence'
            ELSE 'Average Defence'
        END AS defence_rating
    FROM team_stats ts
    JOIN teams t   ON ts.team_id  = t.team_id
    JOIN leagues l ON t.league_id = l.league_id
    WHERE ts.season = '2023/24'
    ORDER BY ts.goals_for DESC
    LIMIT 15
    """
)

results["Q12"] = run_query(
    "Q12 | Nationality Distribution Across Top 5 Leagues",
    """
    SELECT
        p.nationality,
        COUNT(p.player_id)                                      AS num_players,
        ROUND(COUNT(p.player_id) * 100.0 / (SELECT COUNT(*) FROM players), 1) AS percentage,
        SUM(ps.goals)                                           AS total_goals,
        ROUND(AVG(p.market_value_m), 1)                        AS avg_market_value_m
    FROM players p
    JOIN player_stats ps ON p.player_id = ps.player_id
    WHERE ps.season = '2023/24'
    GROUP BY p.nationality
    HAVING num_players >= 2
    ORDER BY num_players DESC
    LIMIT 15
    """
)

# ============================================================
# ADVANCED QUERIES (13-19) — CTEs & Window Functions
# ============================================================

results["Q13"] = run_query(
    "Q13 | CTE: Player Rankings Within Their League (Window Function)",
    """
    WITH ranked_players AS (
        SELECT
            p.player_name,
            t.team_name,
            l.league_name,
            ps.goals,
            ps.assists,
            ps.goals + ps.assists AS contributions,
            RANK() OVER (
                PARTITION BY l.league_id
                ORDER BY ps.goals DESC
            ) AS goals_rank_in_league,
            RANK() OVER (
                ORDER BY ps.goals DESC
            ) AS overall_rank
        FROM player_stats ps
        JOIN players p ON ps.player_id = p.player_id
        JOIN teams t   ON p.team_id    = t.team_id
        JOIN leagues l ON t.league_id  = l.league_id
        WHERE ps.season = '2023/24'
    )
    SELECT *
    FROM ranked_players
    WHERE goals_rank_in_league <= 3
    ORDER BY league_name, goals_rank_in_league
    """
)

results["Q14"] = run_query(
    "Q14 | CTE: Best Value Players (Goals per Million Market Value)",
    """
    WITH value_analysis AS (
        SELECT
            p.player_name,
            p.position,
            t.team_name,
            l.league_name,
            ps.goals,
            ps.assists,
            p.market_value_m,
            ROUND(ps.goals / NULLIF(p.market_value_m, 0), 3)  AS goals_per_million,
            ROUND((ps.goals + ps.assists) / NULLIF(p.market_value_m, 0), 3) AS contributions_per_million
        FROM player_stats ps
        JOIN players p ON ps.player_id = p.player_id
        JOIN teams t   ON p.team_id    = t.team_id
        JOIN leagues l ON t.league_id  = l.league_id
        WHERE ps.season = '2023/24'
        AND ps.goals > 5
        AND p.market_value_m > 10
    )
    SELECT *
    FROM value_analysis
    ORDER BY goals_per_million DESC
    LIMIT 10
    """
)

results["Q15"] = run_query(
    "Q15 | CTE: Team Points Per Game & Title Race Gap",
    """
    WITH points_analysis AS (
        SELECT
            t.team_name,
            l.league_name,
            ts.points,
            ts.played,
            ROUND(CAST(ts.points AS FLOAT) / ts.played, 2) AS points_per_game,
            ts.league_position,
            MAX(ts.points) OVER (PARTITION BY l.league_id) AS leader_points
        FROM team_stats ts
        JOIN teams t   ON ts.team_id  = t.team_id
        JOIN leagues l ON t.league_id = l.league_id
        WHERE ts.season = '2023/24'
    )
    SELECT
        league_name,
        team_name,
        points,
        points_per_game,
        league_position,
        leader_points - points AS points_behind_leader
    FROM points_analysis
    WHERE league_position <= 5
    ORDER BY league_name, league_position
    """
)

results["Q16"] = run_query(
    "Q16 | Window Function: Running Goal Tally by Player",
    """
    SELECT
        p.player_name,
        t.team_name,
        l.league_name,
        ps.goals,
        ps.appearances,
        SUM(ps.goals) OVER (
            PARTITION BY l.league_id
            ORDER BY ps.goals DESC
        ) AS running_league_goals,
        ROW_NUMBER() OVER (
            PARTITION BY l.league_id
            ORDER BY ps.goals DESC
        ) AS league_rank
    FROM player_stats ps
    JOIN players p ON ps.player_id = p.player_id
    JOIN teams t   ON p.team_id    = t.team_id
    JOIN leagues l ON t.league_id  = l.league_id
    WHERE ps.season = '2023/24'
    AND ps.goals > 0
    ORDER BY l.league_name, league_rank
    LIMIT 20
    """
)

results["Q17"] = run_query(
    "Q17 | Head-to-Head Record Between Teams",
    """
    SELECT
        t_home.team_name    AS team_a,
        t_away.team_name    AS team_b,
        m.home_goals        AS team_a_goals,
        m.away_goals        AS team_b_goals,
        CASE
            WHEN m.home_goals > m.away_goals THEN t_home.team_name || ' WIN'
            WHEN m.away_goals > m.home_goals THEN t_away.team_name || ' WIN'
            ELSE 'DRAW'
        END AS result,
        m.match_date,
        m.attendance
    FROM matches m
    JOIN teams t_home ON m.home_team_id = t_home.team_id
    JOIN teams t_away ON m.away_team_id = t_away.team_id
    WHERE (m.home_team_id IN (1,2,3,11,21,31,41) AND m.away_team_id IN (1,2,3,11,21,31,41))
    ORDER BY m.match_date
    LIMIT 15
    """
)

results["Q18"] = run_query(
    "Q18 | CTE: Manager ROI - Points Per Million Spent on Squad",
    """
    WITH squad_values AS (
        SELECT
            t.team_id,
            t.team_name,
            t.manager,
            l.league_name,
            SUM(p.market_value_m) AS total_squad_value
        FROM players p
        JOIN teams t   ON p.team_id   = t.team_id
        JOIN leagues l ON t.league_id = l.league_id
        GROUP BY t.team_id
    ),
    team_performance AS (
        SELECT
            team_id,
            points,
            league_position
        FROM team_stats
        WHERE season = '2023/24'
    )
    SELECT
        sv.team_name,
        sv.manager,
        sv.league_name,
        ROUND(sv.total_squad_value, 1)  AS squad_value_m,
        tp.points,
        tp.league_position,
        ROUND(tp.points / sv.total_squad_value * 100, 2) AS points_per_100m
    FROM squad_values sv
    JOIN team_performance tp ON sv.team_id = tp.team_id
    ORDER BY points_per_100m DESC
    LIMIT 10
    """
)

results["Q19"] = run_query(
    "Q19 | Age Analysis - Young Stars vs Veterans",
    """
    SELECT
        CASE
            WHEN p.age <= 21 THEN 'Young Star (U21)'
            WHEN p.age <= 25 THEN 'Rising Star (22-25)'
            WHEN p.age <= 29 THEN 'Prime Age (26-29)'
            WHEN p.age <= 33 THEN 'Experienced (30-33)'
            ELSE 'Veteran (34+)'
        END AS age_group,
        COUNT(p.player_id)              AS num_players,
        ROUND(AVG(p.market_value_m), 1) AS avg_value_m,
        ROUND(AVG(ps.goals), 1)         AS avg_goals,
        ROUND(AVG(ps.avg_rating), 2)    AS avg_rating
    FROM players p
    JOIN player_stats ps ON p.player_id = ps.player_id
    WHERE ps.season = '2023/24'
    GROUP BY age_group
    ORDER BY avg_value_m DESC
    """
)

# ============================================================
# EXPERT QUERIES (20-25)
# ============================================================

results["Q20"] = run_query(
    "Q20 | Transfer ROI - Did the Fee Match the Performance?",
    """
    WITH transfer_performance AS (
        SELECT
            p.player_name,
            p.position,
            t_to.team_name      AS current_team,
            tr.transfer_fee_m,
            ps.goals,
            ps.assists,
            ps.avg_rating,
            ps.appearances,
            ROUND((ps.goals + ps.assists) / NULLIF(tr.transfer_fee_m, 0) * 10, 2) AS performance_per_10m
        FROM transfers tr
        JOIN players p      ON tr.player_id     = p.player_id
        JOIN teams t_to     ON tr.to_team_id    = t_to.team_id
        JOIN player_stats ps ON p.player_id     = ps.player_id
        WHERE tr.transfer_fee_m > 0
        AND ps.season = '2023/24'
    )
    SELECT
        player_name,
        position,
        current_team,
        transfer_fee_m,
        goals,
        assists,
        avg_rating,
        performance_per_10m,
        CASE
            WHEN performance_per_10m >= 3.0 THEN 'Excellent Value'
            WHEN performance_per_10m >= 1.5 THEN 'Good Value'
            WHEN performance_per_10m >= 0.5 THEN 'Fair Value'
            ELSE 'Poor Value'
        END AS value_verdict
    FROM transfer_performance
    ORDER BY performance_per_10m DESC
    """
)

results["Q21"] = run_query(
    "Q21 | CTE Chain: Complete Player Report Card",
    """
    WITH scoring AS (
        SELECT player_id,
               goals, assists, appearances,
               ROUND(CAST(goals AS FLOAT)/NULLIF(appearances,0), 2) AS gpg,
               goals + assists AS contributions
        FROM player_stats WHERE season = '2023/24'
    ),
    rating_tier AS (
        SELECT player_id,
               avg_rating,
               NTILE(4) OVER (ORDER BY avg_rating DESC) AS rating_quartile
        FROM player_stats WHERE season = '2023/24'
    ),
    final_report AS (
        SELECT
            p.player_name,
            p.position,
            p.age,
            p.nationality,
            t.team_name,
            l.league_name,
            s.goals, s.assists, s.contributions, s.gpg,
            rt.avg_rating,
            p.market_value_m,
            CASE rt.rating_quartile
                WHEN 1 THEN 'World Class'
                WHEN 2 THEN 'Very Good'
                WHEN 3 THEN 'Good'
                ELSE 'Average'
            END AS performance_tier
        FROM scoring s
        JOIN rating_tier rt ON s.player_id    = rt.player_id
        JOIN players p      ON s.player_id    = p.player_id
        JOIN teams t        ON p.team_id      = t.team_id
        JOIN leagues l      ON t.league_id    = l.league_id
    )
    SELECT * FROM final_report
    WHERE performance_tier = 'World Class'
    ORDER BY contributions DESC
    LIMIT 15
    """
)

results["Q22"] = run_query(
    "Q22 | Match Goals Distribution Per League",
    """
    SELECT
        l.league_name,
        COUNT(m.match_id)                                   AS matches_played,
        SUM(m.home_goals + m.away_goals)                    AS total_goals,
        ROUND(AVG(m.home_goals + m.away_goals), 2)          AS avg_goals_per_match,
        MAX(m.home_goals + m.away_goals)                    AS highest_scoring_match,
        SUM(CASE WHEN m.home_goals = m.away_goals THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN m.home_goals > m.away_goals THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN m.home_goals < m.away_goals THEN 1 ELSE 0 END) AS away_wins
    FROM matches m
    JOIN leagues l ON m.league_id = l.league_id
    WHERE m.season = '2023/24'
    GROUP BY l.league_id
    ORDER BY avg_goals_per_match DESC
    """
)

results["Q23"] = run_query(
    "Q23 | Free Transfer Successes (Best Free Signings by Goals)",
    """
    SELECT
        p.player_name,
        p.position,
        p.nationality,
        t_from.team_name    AS left_from,
        t_to.team_name      AS joined,
        tr.transfer_date,
        ps.goals,
        ps.assists,
        ps.avg_rating,
        p.market_value_m    AS current_value_m,
        'FREE TRANSFER'     AS cost
    FROM transfers tr
    JOIN players p          ON tr.player_id     = p.player_id
    JOIN teams t_from       ON tr.from_team_id  = t_from.team_id
    JOIN teams t_to         ON tr.to_team_id    = t_to.team_id
    JOIN player_stats ps    ON p.player_id      = ps.player_id
    WHERE tr.transfer_fee_m = 0
    AND tr.transfer_type IN ('Free', 'Contract')
    AND ps.season = '2023/24'
    ORDER BY ps.goals DESC
    """
)

results["Q24"] = run_query(
    "Q24 | Window Function: Team Form (Points Accumulation Trend)",
    """
    WITH match_points AS (
        SELECT
            t.team_id,
            t.team_name,
            l.league_name,
            m.match_date,
            CASE
                WHEN m.home_team_id = t.team_id AND m.home_goals > m.away_goals THEN 3
                WHEN m.away_team_id = t.team_id AND m.away_goals > m.home_goals THEN 3
                WHEN m.home_goals = m.away_goals THEN 1
                ELSE 0
            END AS points_earned,
            ROW_NUMBER() OVER (PARTITION BY t.team_id ORDER BY m.match_date) AS match_num
        FROM matches m
        JOIN teams t ON (t.team_id = m.home_team_id OR t.team_id = m.away_team_id)
        JOIN leagues l ON t.league_id = l.league_id
        WHERE m.season = '2023/24'
        AND t.team_id IN (1, 11, 21, 31, 41)
    )
    SELECT
        team_name,
        league_name,
        match_num,
        points_earned,
        SUM(points_earned) OVER (
            PARTITION BY team_id
            ORDER BY match_num
        ) AS cumulative_points
    FROM match_points
    WHERE match_num <= 10
    ORDER BY team_name, match_num
    """
)

results["Q25"] = run_query(
    "Q25 | FINAL: Complete League Intelligence Summary",
    """
    WITH league_summary AS (
        SELECT
            l.league_name,
            l.country,
            COUNT(DISTINCT t.team_id)           AS num_teams,
            SUM(ts.goals_for)                   AS total_goals,
            ROUND(AVG(m_sub.avg_att), 0)        AS avg_attendance,
            MAX(ts.points)                      AS champion_points,
            ROUND(AVG(p.market_value_m), 1)     AS avg_player_value_m,
            SUM(tr.transfer_fee_m)              AS total_transfer_spend_m
        FROM leagues l
        JOIN teams t        ON t.league_id      = l.league_id
        JOIN team_stats ts  ON ts.team_id       = t.team_id
        JOIN players p      ON p.team_id        = t.team_id
        LEFT JOIN transfers tr ON tr.to_team_id = t.team_id
        LEFT JOIN (
            SELECT league_id, AVG(attendance) AS avg_att
            FROM matches
            GROUP BY league_id
        ) m_sub ON m_sub.league_id = l.league_id
        WHERE ts.season = '2023/24'
        GROUP BY l.league_id
    )
    SELECT
        league_name,
        country,
        num_teams,
        total_goals,
        avg_attendance,
        champion_points,
        avg_player_value_m,
        ROUND(COALESCE(total_transfer_spend_m, 0), 1) AS transfer_spend_m
    FROM league_summary
    ORDER BY champion_points DESC
    """
)

conn.close()

print()
print("=" * 70)
print("  ALL 25 QUERIES COMPLETED SUCCESSFULLY")
print("=" * 70)
print()
print("  QUERY CATEGORIES COVERED:")
print("  Basic (Q01-Q05)       - SELECT, JOIN, GROUP BY, ORDER BY")
print("  Intermediate (Q06-Q12)- CASE WHEN, Subqueries, HAVING")
print("  Advanced (Q13-Q19)    - CTEs, Window Functions, RANK()")
print("  Expert (Q20-Q25)      - CTE Chains, NTILE, Business Analysis")
print()
print("  Next: Run 04_export_results.py")
print("=" * 70)
