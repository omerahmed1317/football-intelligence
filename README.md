# European Football Intelligence System

A professional SQL analytics project covering the Top 5 European Football Leagues (2023/24 season).

## Live Stats
- **5 Leagues**: Premier League, La Liga, Serie A, Bundesliga, Ligue 1
- **50 Teams** across all leagues
- **80 Players** with real names and realistic stats
- **148 Match Results** with attendances
- **25 SQL Queries** from basic to expert level

## Tech Stack
Python | SQLite | Pandas | SQL (CTEs, Window Functions, Subqueries)

## Project Structure
```
football-intelligence/
├── data/
│   └── football.db          ← SQLite database (8 tables)
├── outputs/
│   └── *.csv                ← 10 exported query results
├── 01_create_database.py    ← Creates 8-table schema
├── 02_insert_data.py        ← Populates with 2023/24 data
├── 03_run_queries.py        ← Runs all 25 SQL queries
├── 04_export_results.py     ← Exports results to CSV
└── requirements.txt
```

## Database Schema (8 Tables)
| Table | Description |
|---|---|
| leagues | 5 top European leagues |
| teams | 50 clubs with stadiums and managers |
| players | 80 players with market values |
| matches | 148 match results |
| player_stats | Goals, assists, ratings per player |
| team_stats | Full league standings |
| transfers | 15 major summer transfers |
| match_events | Goals and cards with minute |

## SQL Concepts Demonstrated
| Level | Queries | Concepts |
|---|---|---|
| Basic | Q01-Q05 | SELECT, JOIN, GROUP BY, ORDER BY |
| Intermediate | Q06-Q12 | CASE WHEN, Subqueries, HAVING |
| Advanced | Q13-Q19 | CTEs, Window Functions, RANK() |
| Expert | Q20-Q25 | CTE Chains, NTILE, Business Analysis |

## Key Findings (2023/24)
- **Top Scorer**: Harry Kane — 36 goals (Bayern Munich)
- **Best Team**: Real Madrid — 95 points (La Liga)
- **Biggest Transfer**: Declan Rice — £105M (West Ham to Arsenal)
- **Best Value Player**: Harry Kane — 0.36 goals per £1M market value
- **Most Goals per Game**: Premier League — 3.54 avg
- **Unbeaten Champions**: Bayer Leverkusen — 28W 6D 0L

## How to Run
```bash
python 01_create_database.py
python 02_insert_data.py
python 03_run_queries.py
python 04_export_results.py
```

## GitHub
github.com/omerahmed1317/football-intelligence

---
*Built for data analytics portfolio — 2024*
