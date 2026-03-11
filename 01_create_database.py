"""
================================================================
STEP 1: CREATE FOOTBALL DATABASE
================================================================
European Football Intelligence System
File: 01_create_database.py

PURPOSE:
    Creates a professional SQLite database with 8 tables
    covering the Top 5 European Football Leagues (2023/24)

TABLES:
    1. leagues      - 5 leagues
    2. teams        - 50 clubs
    3. players      - 80 players with profiles
    4. matches      - 140+ match results
    5. player_stats - Goals, assists, ratings
    6. team_stats   - League standings
    7. transfers    - Transfer records
    8. match_events - Goals and cards

HOW TO RUN:
    python 01_create_database.py
================================================================
"""

import sqlite3
import os

os.makedirs("data", exist_ok=True)
os.makedirs("outputs", exist_ok=True)
os.makedirs("queries", exist_ok=True)

DB_PATH = "data/football.db"

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.executescript("""

CREATE TABLE leagues (
    league_id       INTEGER PRIMARY KEY,
    league_name     TEXT NOT NULL,
    country         TEXT NOT NULL,
    founded_year    INTEGER,
    num_teams       INTEGER,
    current_season  TEXT
);

CREATE TABLE teams (
    team_id             INTEGER PRIMARY KEY,
    team_name           TEXT NOT NULL,
    league_id           INTEGER,
    city                TEXT,
    stadium             TEXT,
    stadium_capacity    INTEGER,
    manager             TEXT,
    founded_year        INTEGER,
    market_value_m      REAL,
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);

CREATE TABLE players (
    player_id       INTEGER PRIMARY KEY,
    player_name     TEXT NOT NULL,
    team_id         INTEGER,
    nationality     TEXT,
    position        TEXT,
    age             INTEGER,
    height_cm       INTEGER,
    market_value_m  REAL,
    wage_weekly_k   REAL,
    contract_end    TEXT,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE TABLE matches (
    match_id        INTEGER PRIMARY KEY,
    league_id       INTEGER,
    season          TEXT,
    match_date      TEXT,
    home_team_id    INTEGER,
    away_team_id    INTEGER,
    home_goals      INTEGER,
    away_goals      INTEGER,
    attendance      INTEGER,
    referee         TEXT,
    FOREIGN KEY (league_id)    REFERENCES leagues(league_id),
    FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
);

CREATE TABLE player_stats (
    stat_id         INTEGER PRIMARY KEY,
    player_id       INTEGER,
    season          TEXT,
    appearances     INTEGER,
    goals           INTEGER,
    assists         INTEGER,
    yellow_cards    INTEGER,
    red_cards       INTEGER,
    minutes_played  INTEGER,
    pass_accuracy   REAL,
    shots_on_target INTEGER,
    avg_rating      REAL,
    FOREIGN KEY (player_id) REFERENCES players(player_id)
);

CREATE TABLE team_stats (
    stat_id             INTEGER PRIMARY KEY,
    team_id             INTEGER,
    season              TEXT,
    played              INTEGER,
    won                 INTEGER,
    drawn               INTEGER,
    lost                INTEGER,
    goals_for           INTEGER,
    goals_against       INTEGER,
    clean_sheets        INTEGER,
    points              INTEGER,
    league_position     INTEGER,
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);

CREATE TABLE transfers (
    transfer_id     INTEGER PRIMARY KEY,
    player_id       INTEGER,
    from_team_id    INTEGER,
    to_team_id      INTEGER,
    transfer_date   TEXT,
    transfer_fee_m  REAL,
    transfer_type   TEXT,
    season          TEXT,
    FOREIGN KEY (player_id)    REFERENCES players(player_id),
    FOREIGN KEY (from_team_id) REFERENCES teams(team_id),
    FOREIGN KEY (to_team_id)   REFERENCES teams(team_id)
);

CREATE TABLE match_events (
    event_id        INTEGER PRIMARY KEY,
    match_id        INTEGER,
    player_id       INTEGER,
    team_id         INTEGER,
    event_type      TEXT,
    minute          INTEGER,
    description     TEXT,
    FOREIGN KEY (match_id)  REFERENCES matches(match_id),
    FOREIGN KEY (player_id) REFERENCES players(player_id),
    FOREIGN KEY (team_id)   REFERENCES teams(team_id)
);

""")

conn.commit()
conn.close()

print("=" * 55)
print("  FOOTBALL DATABASE CREATED SUCCESSFULLY")
print("=" * 55)
print(f"  Location: {DB_PATH}")
print()
print("  Tables created:")
print("  1. leagues       5. player_stats")
print("  2. teams         6. team_stats")
print("  3. players       7. transfers")
print("  4. matches       8. match_events")
print()
print("  Next: Run 02_insert_data.py")
print("=" * 55)
