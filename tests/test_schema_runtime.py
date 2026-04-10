import sqlite3

from app.core.schema import ensure_runtime_db


def test_ensure_runtime_db_migrates_season_aware_unique_constraints(tmp_path):
    db_path = tmp_path / "runtime.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript(
        """
        CREATE TABLE seasons (
            id INTEGER PRIMARY KEY,
            year INTEGER NOT NULL UNIQUE,
            is_active INTEGER DEFAULT 0
        );

        CREATE TABLE players (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            player_type TEXT,
            position TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE fantasy_teams (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            owner TEXT
        );

        CREATE TABLE war_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            bat_war REAL,
            pit_war REAL,
            war REAL NOT NULL,
            war_diff REAL,
            UNIQUE(player_id, date)
        );

        CREATE TABLE team_war_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL NOT NULL,
            war_diff REAL,
            rank INTEGER,
            UNIQUE(team_id, date)
        );
        """
    )
    conn.executemany(
        "INSERT INTO seasons (id, year, is_active) VALUES (?, ?, ?)",
        [(1, 2026, 1), (2, 2027, 0)],
    )
    conn.execute("INSERT INTO players (id, name) VALUES (?, ?)", ('10001', '김투수'))
    conn.execute("INSERT INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)", ('준', '준팀', '준'))
    conn.execute(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        ('10001', 1, '2026-04-01', 1.0, 0.1),
    )
    conn.execute(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        ('준', 1, '2026-04-01', 1.0, 0.1, 1),
    )
    conn.commit()
    conn.close()

    ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        ('10001', 2, '2026-04-01', 2.0, 0.2),
    )
    conn.execute(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        ('준', 2, '2026-04-01', 2.0, 0.2, 1),
    )
    war_count = conn.execute("SELECT COUNT(*) FROM war_daily WHERE player_id = ? AND date = ?", ('10001', '2026-04-01')).fetchone()[0]
    team_count = conn.execute("SELECT COUNT(*) FROM team_war_daily WHERE team_id = ? AND date = ?", ('준', '2026-04-01')).fetchone()[0]
    conn.close()

    assert war_count == 2
    assert team_count == 2
