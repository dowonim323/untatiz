"""Database schema definitions for Untatiz (normalized Long format).

This module defines the new normalized schema that replaces the legacy
wide-format tables (where each date was a column).

Benefits of the new schema:
- No schema changes needed when adding new dates
- Standard SQL queries for date ranges
- Proper foreign key relationships
- Efficient indexing
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# SQL schema definition
SCHEMA_SQL = """
-- ============================================================
-- CORE TABLES
-- ============================================================

-- 1. 시즌 정보
CREATE TABLE IF NOT EXISTS seasons (
    id INTEGER PRIMARY KEY,
    year INTEGER NOT NULL UNIQUE,
    start_date TEXT,
    end_date TEXT,
    is_active INTEGER DEFAULT 0
);

-- 2. 선수 마스터
CREATE TABLE IF NOT EXISTS players (
    id TEXT PRIMARY KEY,           -- statiz player ID
    name TEXT NOT NULL,
    player_type TEXT CHECK(player_type IN ('bat', 'pit')),
    position TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_players_name ON players(name);
CREATE INDEX IF NOT EXISTS idx_players_type ON players(player_type);

-- 3. 일별 WAR 기록 (Long format - 핵심!)
CREATE TABLE IF NOT EXISTS war_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id TEXT NOT NULL REFERENCES players(id),
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    date TEXT NOT NULL,
    bat_war REAL,
    pit_war REAL,
    war REAL NOT NULL,
    war_diff REAL,  -- 전일 대비 변동
    UNIQUE(player_id, season_id, date)
);
CREATE INDEX IF NOT EXISTS idx_war_daily_date ON war_daily(date);
CREATE INDEX IF NOT EXISTS idx_war_daily_player_date ON war_daily(player_id, date);
CREATE INDEX IF NOT EXISTS idx_war_daily_season ON war_daily(season_id);

-- 4. 판타지 팀
CREATE TABLE IF NOT EXISTS fantasy_teams (
    id TEXT PRIMARY KEY,           -- '준', '뚝', '삼' 등
    name TEXT NOT NULL,
    owner TEXT,                    -- 팀 주인 이름
    created_at TEXT DEFAULT (datetime('now'))
);

-- 5. 드래프트
CREATE TABLE IF NOT EXISTS draft (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    team_id TEXT NOT NULL REFERENCES fantasy_teams(id),
    player_id TEXT NOT NULL REFERENCES players(id),
    round TEXT NOT NULL,           -- '용타1', '용투1', '1라운드' 등
    pick_order INTEGER,
    draft_type TEXT DEFAULT 'main',
    application_date TEXT,
    UNIQUE(season_id, team_id, round)
);
CREATE INDEX IF NOT EXISTS idx_draft_season_team ON draft(season_id, team_id);
CREATE INDEX IF NOT EXISTS idx_draft_player ON draft(player_id);

-- 6. 로스터 (시점별 소속)
CREATE TABLE IF NOT EXISTS roster (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id TEXT NOT NULL REFERENCES fantasy_teams(id),
    player_id TEXT NOT NULL REFERENCES players(id),
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    joined_date TEXT NOT NULL,
    left_date TEXT,                -- NULL이면 현재 소속
    CHECK(left_date IS NULL OR left_date > joined_date),
    UNIQUE(team_id, player_id, season_id, joined_date)
);
CREATE INDEX IF NOT EXISTS idx_roster_team ON roster(team_id);
CREATE INDEX IF NOT EXISTS idx_roster_player ON roster(player_id);
CREATE INDEX IF NOT EXISTS idx_roster_active ON roster(left_date);

-- 7. 거래 기록
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id TEXT NOT NULL REFERENCES players(id),
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    from_team_id TEXT REFERENCES fantasy_teams(id),  -- NULL이면 FA에서
    to_team_id TEXT REFERENCES fantasy_teams(id),    -- NULL이면 FA로
    transaction_date TEXT NOT NULL,
    war_at_transaction REAL,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_player ON transactions(player_id);
CREATE INDEX IF NOT EXISTS idx_transactions_season ON transactions(season_id);

-- 8. 팀 일별 WAR (집계 캐시)
CREATE TABLE IF NOT EXISTS team_war_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    team_id TEXT NOT NULL REFERENCES fantasy_teams(id),
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    date TEXT NOT NULL,
    total_war REAL NOT NULL,
    war_diff REAL,
    rank INTEGER,
    UNIQUE(team_id, season_id, date)
);
CREATE INDEX IF NOT EXISTS idx_team_war_date ON team_war_daily(date);
CREATE INDEX IF NOT EXISTS idx_team_war_team ON team_war_daily(team_id);

-- 9. GOAT/BOAT 기록
CREATE TABLE IF NOT EXISTS daily_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    record_type TEXT CHECK(record_type IN ('GOAT', 'BOAT')),
    team_id TEXT REFERENCES fantasy_teams(id),
    player_id TEXT NOT NULL REFERENCES players(id),
    war_diff REAL NOT NULL,
    UNIQUE(date, record_type, player_id)
);
CREATE INDEX IF NOT EXISTS idx_daily_records_date_type ON daily_records(date, record_type);

CREATE TABLE IF NOT EXISTS fa_config (
    season_id INTEGER PRIMARY KEY REFERENCES seasons(id),
    roster_size INTEGER DEFAULT 29,
    supplemental_bonus INTEGER DEFAULT 5,
    min_pitchers INTEGER DEFAULT 11,
    min_catchers INTEGER DEFAULT 2,
    min_infielders INTEGER DEFAULT 7,
    min_outfielders INTEGER DEFAULT 5
);

-- 10. 스크래퍼 상태 (단일 행)
CREATE TABLE IF NOT EXISTS scraper_status (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_updated_at TEXT NOT NULL,
    target_date TEXT NOT NULL,
    total_games INTEGER DEFAULT 0,
    updated_games INTEGER DEFAULT 0,
    war_status TEXT DEFAULT 'pending' CHECK (war_status IN ('pending', 'completed', 'no_games')),
    schedule_complete INTEGER DEFAULT 0,
    source_war_ready INTEGER DEFAULT 0,
    publish_ready_at TEXT,
    last_full_run_at TEXT,
    last_full_run_status TEXT CHECK (last_full_run_status IN ('success', 'failed')),
    last_error_message TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS source_team_snapshots (
    run_at TEXT NOT NULL,
    season_id INTEGER NOT NULL REFERENCES seasons(id),
    target_date TEXT NOT NULL,
    team_name TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('post_final')),
    war_hash TEXT NOT NULL,
    usage_hash TEXT NOT NULL,
    bat_pa_total INTEGER NOT NULL,
    pit_outs_total INTEGER NOT NULL,
    PRIMARY KEY (run_at, team_name)
);
CREATE INDEX IF NOT EXISTS idx_source_team_snapshots_lookup
    ON source_team_snapshots (season_id, target_date, phase, run_at, team_name);

-- 11. 일별 경기 결과
CREATE TABLE IF NOT EXISTS daily_games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_date TEXT NOT NULL,
    game_order INTEGER NOT NULL,
    away_team TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_score INTEGER,
    home_score INTEGER,
    game_status TEXT DEFAULT 'scheduled' CHECK (game_status IN ('scheduled', 'in_progress', 'final', 'postponed', 'cancelled')),
    war_updated INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(game_date, game_order)
);
CREATE INDEX IF NOT EXISTS idx_daily_games_date ON daily_games(game_date);

-- 12. 스크래퍼 로그
CREATE TABLE IF NOT EXISTS scraper_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    target_date TEXT NOT NULL,
    games_found INTEGER DEFAULT 0,
    games_updated INTEGER DEFAULT 0,
    war_status TEXT,
    duration_seconds REAL,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_scraper_log_date ON scraper_log(target_date);
CREATE INDEX IF NOT EXISTS idx_scraper_log_run ON scraper_log(run_at);

-- 13. 업데이트 로그 (레거시 호환)
CREATE TABLE IF NOT EXISTS update_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    update_type TEXT NOT NULL,     -- 'full', 'incremental', 'manual'
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT CHECK(status IN ('running', 'success', 'failed')),
    records_updated INTEGER,
    error_message TEXT
);

-- ============================================================
-- VIEWS
-- ============================================================

-- 현재 로스터 뷰
CREATE VIEW IF NOT EXISTS v_current_roster AS
SELECT 
    r.team_id,
    ft.name as team_name,
    r.player_id,
    p.name as player_name,
    p.player_type,
    d.round as draft_round
FROM roster r
JOIN fantasy_teams ft ON r.team_id = ft.id
JOIN players p ON r.player_id = p.id
LEFT JOIN draft d ON d.player_id = p.id AND d.team_id = r.team_id
WHERE r.left_date IS NULL;

-- 선수 최신 WAR 뷰
CREATE VIEW IF NOT EXISTS v_player_latest_war AS
SELECT 
    p.id,
    p.name,
    p.player_type,
    w.war,
    w.war_diff,
    w.date
FROM players p
JOIN war_daily w ON p.id = w.player_id
WHERE w.date = (SELECT MAX(date) FROM war_daily);

-- 팀 순위 뷰 (최신)
CREATE VIEW IF NOT EXISTS v_team_standings AS
SELECT 
    tw.team_id,
    ft.name as team_name,
    tw.total_war,
    tw.war_diff,
    tw.rank,
    tw.date
FROM team_war_daily tw
JOIN fantasy_teams ft ON tw.team_id = ft.id
WHERE tw.date = (SELECT MAX(date) FROM team_war_daily)
ORDER BY tw.rank;
"""

# Draft round order for proper sorting
DRAFT_ROUNDS = [
    '용타', '용투1', '용투2', '아쿼',
    '1R', '2R', '3R', '4R', '5R',
    '6R', '7R', '8R', '9R', '10R',
    '11R', '12R', '13R', '14R', '15R',
    '16R', '17R', '18R', '19R', '20R',
    '21R', '22R', '23R', '24R', '25R',
]

# Fantasy team mapping (short code -> full info)
FANTASY_TEAMS = {
    '옥': {'name': '옥', 'owner': '옥'},
    '무': {'name': '무', 'owner': '무'},
    '엉': {'name': '엉', 'owner': '엉'},
    '준': {'name': '준', 'owner': '준'},
    '뚝': {'name': '뚝', 'owner': '뚝'},
    '삼': {'name': '삼', 'owner': '삼'},
    '언': {'name': '언', 'owner': '언'},
    '홍': {'name': '홍', 'owner': '홍'},
    '코': {'name': '코', 'owner': '코'},
    '앙': {'name': '앙', 'owner': '앙'},
    '퐈': {'name': '퐈', 'owner': None},
}


def init_schema(conn) -> None:
    """Initialize database schema.
    
    Args:
        conn: SQLite connection
    """
    cursor = conn.cursor()
    cursor.executescript(SCHEMA_SQL)
    conn.commit()


def init_fantasy_teams(conn) -> None:
    """Initialize fantasy teams table.
    
    Args:
        conn: SQLite connection
    """
    cursor = conn.cursor()
    for team_id, info in FANTASY_TEAMS.items():
        cursor.execute(
            "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
            (team_id, info['name'], info['owner'])
        )
    conn.commit()


def init_season(conn, year: int, is_active: bool = True) -> int:
    """Initialize or get season.
    
    Args:
        conn: SQLite connection
        year: Season year
        is_active: Whether this is the active season
        
    Returns:
        int: Season ID
    """
    cursor = conn.cursor()
    
    # Deactivate all seasons if setting new active
    if is_active:
        cursor.execute("UPDATE seasons SET is_active = 0")
    
    # Insert or update season
    cursor.execute(
        """INSERT INTO seasons (year, is_active) VALUES (?, ?)
           ON CONFLICT(year) DO UPDATE SET is_active = ?""",
        (year, int(is_active), int(is_active))
    )
    conn.commit()
    
    # Get season ID
    cursor.execute("SELECT id FROM seasons WHERE year = ?", (year,))
    return cursor.fetchone()[0]


def ensure_runtime_db(db_path: Path, active_year: int) -> int:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        init_schema(conn)
        _migrate_runtime_tables(conn)
        init_fantasy_teams(conn)
        season_id = init_season(conn, active_year)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(war_daily)")
        war_daily_cols = {row[1] for row in cursor.fetchall()}
        if 'bat_war' not in war_daily_cols:
            cursor.execute("ALTER TABLE war_daily ADD COLUMN bat_war REAL")
        if 'pit_war' not in war_daily_cols:
            cursor.execute("ALTER TABLE war_daily ADD COLUMN pit_war REAL")
        _ensure_scraper_status_columns(conn)
        _ensure_source_team_snapshots_table(conn)
        cursor.execute(
            """INSERT OR IGNORE INTO fa_config
               (season_id, roster_size, supplemental_bonus, min_pitchers, min_catchers, min_infielders, min_outfielders)
               VALUES (?, 29, 5, 11, 2, 7, 5)""",
            (season_id,),
        )
        conn.commit()
        return season_id
    finally:
        conn.close()


__all__ = [
    'SCHEMA_SQL',
    'DRAFT_ROUNDS',
    'FANTASY_TEAMS',
    'init_schema',
    'init_fantasy_teams',
    'init_season',
    'ensure_runtime_db',
]


def _migrate_runtime_tables(conn: sqlite3.Connection) -> None:
    _drop_runtime_views(conn)
    _migrate_war_daily_table(conn)
    _migrate_team_war_daily_table(conn)
    _recreate_runtime_views(conn)


def _migrate_war_daily_table(conn: sqlite3.Connection) -> None:
    sql = _get_table_sql(conn, 'war_daily')
    if 'UNIQUE(player_id, season_id, date)' in sql:
        return

    conn.execute(
        """CREATE TABLE war_daily_new (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               player_id TEXT NOT NULL REFERENCES players(id),
               season_id INTEGER NOT NULL REFERENCES seasons(id),
               date TEXT NOT NULL,
               bat_war REAL,
               pit_war REAL,
               war REAL NOT NULL,
               war_diff REAL,
               UNIQUE(player_id, season_id, date)
           )"""
    )
    conn.execute(
        """INSERT INTO war_daily_new
           (id, player_id, season_id, date, bat_war, pit_war, war, war_diff)
           SELECT id, player_id, season_id, date, bat_war, pit_war, war, war_diff
           FROM war_daily
           ORDER BY season_id, player_id, date, id"""
    )
    conn.execute("DROP TABLE war_daily")
    conn.execute("ALTER TABLE war_daily_new RENAME TO war_daily")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_war_daily_date ON war_daily(date)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_war_daily_player_date ON war_daily(player_id, date)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_war_daily_season ON war_daily(season_id)")


def _migrate_team_war_daily_table(conn: sqlite3.Connection) -> None:
    sql = _get_table_sql(conn, 'team_war_daily')
    if 'UNIQUE(team_id, season_id, date)' in sql:
        return

    conn.execute(
        """CREATE TABLE team_war_daily_new (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               team_id TEXT NOT NULL REFERENCES fantasy_teams(id),
               season_id INTEGER NOT NULL REFERENCES seasons(id),
               date TEXT NOT NULL,
               total_war REAL NOT NULL,
               war_diff REAL,
               rank INTEGER,
               UNIQUE(team_id, season_id, date)
           )"""
    )
    conn.execute(
        """INSERT INTO team_war_daily_new
           (id, team_id, season_id, date, total_war, war_diff, rank)
           SELECT id, team_id, season_id, date, total_war, war_diff, rank
           FROM team_war_daily
           ORDER BY season_id, team_id, date, id"""
    )
    conn.execute("DROP TABLE team_war_daily")
    conn.execute("ALTER TABLE team_war_daily_new RENAME TO team_war_daily")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_team_war_date ON team_war_daily(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_team_war_team ON team_war_daily(team_id)")


def _get_table_sql(conn: sqlite3.Connection, table_name: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if row is None or row[0] is None:
        return ''
    return row[0]


def _drop_runtime_views(conn: sqlite3.Connection) -> None:
    conn.execute("DROP VIEW IF EXISTS v_player_latest_war")
    conn.execute("DROP VIEW IF EXISTS v_team_standings")


def _recreate_runtime_views(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE VIEW IF NOT EXISTS v_player_latest_war AS
        SELECT
            p.id,
            p.name,
            p.player_type,
            w.war,
            w.war_diff,
            w.date
        FROM players p
        JOIN war_daily w ON p.id = w.player_id
        WHERE w.date = (SELECT MAX(date) FROM war_daily);

        CREATE VIEW IF NOT EXISTS v_team_standings AS
        SELECT
            tw.team_id,
            ft.name as team_name,
            tw.total_war,
            tw.war_diff,
            tw.rank,
            tw.date
        FROM team_war_daily tw
        JOIN fantasy_teams ft ON tw.team_id = ft.id
        WHERE tw.date = (SELECT MAX(date) FROM team_war_daily)
        ORDER BY tw.rank;
        """
    )


def _ensure_scraper_status_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(scraper_status)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    column_defs = {
        'schedule_complete': "ALTER TABLE scraper_status ADD COLUMN schedule_complete INTEGER DEFAULT 0",
        'source_war_ready': "ALTER TABLE scraper_status ADD COLUMN source_war_ready INTEGER DEFAULT 0",
        'publish_ready_at': "ALTER TABLE scraper_status ADD COLUMN publish_ready_at TEXT",
        'last_full_run_at': "ALTER TABLE scraper_status ADD COLUMN last_full_run_at TEXT",
        'last_full_run_status': (
            "ALTER TABLE scraper_status ADD COLUMN last_full_run_status TEXT "
            "CHECK (last_full_run_status IN ('success', 'failed'))"
        ),
        'last_error_message': "ALTER TABLE scraper_status ADD COLUMN last_error_message TEXT",
    }
    for column_name, statement in column_defs.items():
        if column_name not in existing_cols:
            cursor.execute(statement)


def _ensure_source_team_snapshots_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS source_team_snapshots (
            run_at TEXT NOT NULL,
            season_id INTEGER NOT NULL REFERENCES seasons(id),
            target_date TEXT NOT NULL,
            team_name TEXT NOT NULL,
            phase TEXT NOT NULL CHECK (phase IN ('post_final')),
            war_hash TEXT NOT NULL,
            usage_hash TEXT NOT NULL,
            bat_pa_total INTEGER NOT NULL,
            pit_outs_total INTEGER NOT NULL,
            PRIMARY KEY (run_at, team_name)
        );
        CREATE INDEX IF NOT EXISTS idx_source_team_snapshots_lookup
            ON source_team_snapshots (season_id, target_date, phase, run_at, team_name);
        """
    )
