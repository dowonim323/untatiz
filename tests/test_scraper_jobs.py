from __future__ import annotations

import json
import sqlite3
from unittest.mock import patch

import pandas as pd

from app.core.db import DatabaseManager
from app.scraper.jobs import (
    _update_daily_records,
    _update_info_table,
    _update_team_war_daily,
    _update_war_daily,
    update_db,
)
from web.services.team_service import get_team_table_data


def _create_scraper_tables(db_path) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE scraper_status (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_updated_at TEXT NOT NULL,
            target_date TEXT NOT NULL,
            total_games INTEGER DEFAULT 0,
            updated_games INTEGER DEFAULT 0,
            war_status TEXT DEFAULT 'pending'
                CHECK (war_status IN ('pending', 'completed', 'no_games')),
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE daily_games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_date TEXT NOT NULL,
            game_order INTEGER NOT NULL,
            away_team TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_score INTEGER,
            home_score INTEGER,
            game_status TEXT DEFAULT 'scheduled'
                CHECK (
                    game_status IN ('scheduled', 'in_progress', 'final', 'postponed', 'cancelled')
                ),
            war_updated INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(game_date, game_order)
        );
        """
    )
    conn.commit()
    conn.close()


def _create_live_write_tables(db_path) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(
        """
        ALTER TABLE war_daily ADD COLUMN bat_war REAL;
        ALTER TABLE war_daily ADD COLUMN pit_war REAL;

        CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        );

        CREATE TABLE daily_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            record_type TEXT NOT NULL,
            team_id TEXT,
            player_id TEXT NOT NULL,
            war_diff REAL NOT NULL
        );

        CREATE TABLE scraper_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT NOT NULL,
            target_date TEXT NOT NULL,
            games_found INTEGER DEFAULT 0,
            games_updated INTEGER DEFAULT 0,
            war_status TEXT,
            duration_seconds REAL,
            error_message TEXT
        );
        """
    )
    conn.commit()
    conn.close()


def test_update_info_table_marks_completed_when_cancelled_games_remain(temp_db):
    _create_scraper_tables(temp_db)
    db = DatabaseManager(temp_db)

    year = 2026
    yesterday = f"{year}-03-29"
    today = f"{year}-03-30"

    with db.connection() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE players SET player_type = 'bat' WHERE id = '10002'")
        cursor.execute("UPDATE players SET player_type = 'pit' WHERE id = '10001'")
        cursor.executemany(
            """INSERT INTO war_daily
               (player_id, season_id, date, war, war_diff)
               VALUES (?, ?, ?, ?, ?)""",
            [
                ('10002', 1, yesterday, 1.0, 0.0),
                ('10002', 1, today, 1.4, 0.4),
                ('10001', 1, yesterday, 2.0, 0.0),
                ('10001', 1, today, 2.3, 0.3),
            ],
        )
        conn.commit()

    bat = pd.DataFrame(
        [{'Name': '박포수', 'Team': 'LG'}],
        index=pd.Index(['10002'], name='ID'),
    )
    pit = pd.DataFrame(
        [{'Name': '김투수', 'Team': 'KIA'}],
        index=pd.Index(['10001'], name='ID'),
    )
    games = pd.DataFrame(
        [
            ['LG', '3 : 1', 'KIA'],
            ['SSG', '우천취소', '두산'],
        ]
    )

    with patch('app.scraper.jobs.get_date', return_value='03/30'), patch(
        'app.scraper.jobs.get_kst_timestamp', return_value='2026-03-30T23:00:00+09:00'
    ):
        _update_info_table(db, bat, pit, games)

    with db.connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT war_status, updated_games, total_games FROM scraper_status WHERE id = 1'
        )
        status_row = cursor.fetchone()
        cursor.execute(
            """SELECT away_team, home_team, game_status, war_updated
               FROM daily_games
               ORDER BY game_order"""
        )
        game_rows = cursor.fetchall()

    assert status_row == ('completed', 1, 2)
    assert game_rows == [
        ('LG', 'KIA', 'final', 1),
        ('SSG', '두산', 'cancelled', 0),
    ]


def test_update_info_table_marks_no_games_when_all_games_cancelled(temp_db):
    _create_scraper_tables(temp_db)
    db = DatabaseManager(temp_db)
    games = pd.DataFrame(
        [
            ['LG', '우천취소', 'KIA'],
            ['SSG', '우천취소', '두산'],
        ]
    )

    with patch('app.scraper.jobs.get_date', return_value='03/30'), patch(
        'app.scraper.jobs.get_kst_timestamp', return_value='2026-03-30T23:00:00+09:00'
    ):
        _update_info_table(db, pd.DataFrame(), pd.DataFrame(), games)

    with db.connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT war_status, updated_games, total_games FROM scraper_status WHERE id = 1'
        )
        status_row = cursor.fetchone()
        cursor.execute(
            """SELECT away_team, home_team, game_status, war_updated
               FROM daily_games
               ORDER BY game_order"""
        )
        game_rows = cursor.fetchall()

    assert status_row == ('no_games', 0, 0)
    assert game_rows == [
        ('LG', 'KIA', 'cancelled', 0),
        ('SSG', '두산', 'cancelled', 0),
    ]


def test_update_info_table_can_see_uncommitted_war_rows_on_shared_connection(temp_db):
    _create_scraper_tables(temp_db)
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute("UPDATE players SET player_type = 'bat' WHERE id = '10002'")
    cursor.execute("UPDATE players SET player_type = 'pit' WHERE id = '10001'")
    cursor.executemany(
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('10002', 1, '2026-03-29', 1.0, 0.0),
            ('10001', 1, '2026-03-29', 2.0, 0.0),
        ],
    )
    conn.commit()

    bat = pd.DataFrame(
        [{'Name': '박포수', 'Team': 'LG'}],
        index=pd.Index(['10002'], name='ID'),
    )
    pit = pd.DataFrame(
        [{'Name': '김투수', 'Team': 'KIA'}],
        index=pd.Index(['10001'], name='ID'),
    )
    games = pd.DataFrame([['LG', '3 : 1', 'KIA']])

    cursor.executemany(
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('10002', 1, '2026-03-30', 1.4, 0.4),
            ('10001', 1, '2026-03-30', 2.3, 0.3),
        ],
    )

    db = DatabaseManager(temp_db)
    with patch('app.scraper.jobs.get_date', return_value='03/30'), patch(
        'app.scraper.jobs.get_kst_timestamp', return_value='2026-03-30T23:00:00+09:00'
    ):
        _update_info_table(db, bat, pit, games, conn=conn)

    status_row = conn.execute(
        'SELECT war_status, updated_games, total_games FROM scraper_status WHERE id = 1'
    ).fetchone()
    conn.close()

    assert status_row == ('completed', 1, 1)


def test_update_team_war_daily_can_see_uncommitted_fa_war_on_shared_connection(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.executescript(
        """
        ALTER TABLE war_daily ADD COLUMN bat_war REAL;
        ALTER TABLE war_daily ADD COLUMN pit_war REAL;
        CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        );
        """
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('19999', '퐈테스트선수', 'pit', 'P'),
    )
    cursor.execute(
        """INSERT INTO war_daily (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ('19999', 1, '2026-04-04', None, 1.25, 1.25, 1.25),
    )

    db = DatabaseManager(temp_db)
    _update_team_war_daily(
        db,
        pd.DataFrame(columns=pd.Index(['oWAR'])),
        pd.DataFrame([{'Name': '퐈테스트선수', 'WAR': 1.25}], index=pd.Index(['19999'], name='ID')),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        '2026-04-04',
        1,
        conn=conn,
    )

    fa_row = conn.execute(
        "SELECT total_war FROM team_war_daily WHERE team_id = '퐈' AND season_id = 1 AND date = '2026-04-04'"
    ).fetchone()
    conn.close()

    assert fa_row == (1.25,)


def test_update_db_rolls_back_main_writes_when_later_step_fails(temp_db):
    _create_scraper_tables(temp_db)
    _create_live_write_tables(temp_db)

    bat = pd.DataFrame(
        [{'Name': '테스트타자', 'POS': 'C', 'oWAR': 0.7, 'Team': 'LG'}],
        index=pd.Index(['29998'], name='ID'),
    )
    pit = pd.DataFrame(
        [{'Name': '테스트투수', 'WAR': 1.2, 'Team': 'KIA'}],
        index=pd.Index(['29999'], name='ID'),
    )
    games = pd.DataFrame([['LG', '3 : 1', 'KIA']])

    with patch('app.scraper.jobs.get_date', return_value='04/05'), patch(
        'app.scraper.jobs.maybe_run_daily_backup'
    ), patch('app.scraper.jobs._update_info_table', side_effect=RuntimeError('boom')):
        try:
            update_db(
                player_name=pd.DataFrame(),
                player_id=pd.DataFrame(),
                player_activation=pd.DataFrame(),
                live_war=pd.DataFrame(),
                current_war=pd.DataFrame(),
                bat=bat,
                pit=pit,
                games=games,
                db_path=temp_db,
            )
        except Exception as exc:
            assert str(exc) == 'boom'
        else:
            raise AssertionError('update_db should re-raise the main transaction failure')

    conn = sqlite3.connect(str(temp_db))
    player_row = conn.execute("SELECT name FROM players WHERE id = '29999'").fetchone()
    war_row = conn.execute(
        "SELECT war FROM war_daily WHERE player_id = '29999' AND date = '2026-04-05'"
    ).fetchone()
    team_rows = conn.execute("SELECT team_id, total_war FROM team_war_daily").fetchall()
    record_rows = conn.execute("SELECT player_id, war_diff FROM daily_records").fetchall()
    log_row = conn.execute(
        "SELECT war_status, error_message FROM scraper_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()

    assert player_row is None
    assert war_row is None
    assert team_rows == []
    assert record_rows == []
    assert log_row == ('failed', 'boom')


def test_update_war_daily_uses_previous_war_from_same_season(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE war_daily ADD COLUMN bat_war REAL")
    cursor.execute("ALTER TABLE war_daily ADD COLUMN pit_war REAL")
    cursor.execute("INSERT INTO seasons (id, year, is_active) VALUES (?, ?, ?)", (2, 2026, 0))
    cursor.executemany(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ('10001', 1, '2026-04-01', None, 0.4, 0.4, 0.4),
            ('10001', 2, '2026-04-01', None, 1.5, 1.5, 1.5),
        ],
    )
    conn.commit()
    conn.close()

    db = DatabaseManager(temp_db)
    pit = pd.DataFrame([{'Name': '김투수', 'WAR': 1.8}], index=pd.Index(['10001'], name='ID'))

    _update_war_daily(db, pd.DataFrame(columns=pd.Index(['oWAR'])), pit, '2026-04-02', 2)

    conn = sqlite3.connect(str(temp_db))
    row = conn.execute(
        "SELECT war, war_diff FROM war_daily WHERE player_id = ? AND season_id = ? AND date = ?",
        ('10001', 2, '2026-04-02'),
    ).fetchone()
    conn.close()

    assert row == (1.8, 0.3)


def test_update_war_daily_uses_latest_prior_existing_date_for_diff(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE war_daily ADD COLUMN bat_war REAL")
    cursor.execute("ALTER TABLE war_daily ADD COLUMN pit_war REAL")
    cursor.execute(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ('10001', 1, '2026-04-01', None, 0.4, 0.4, 0.4),
    )
    conn.commit()
    conn.close()

    db = DatabaseManager(temp_db)
    pit = pd.DataFrame([{'Name': '김투수', 'WAR': 1.0}], index=pd.Index(['10001'], name='ID'))

    _update_war_daily(db, pd.DataFrame(columns=pd.Index(['oWAR'])), pit, '2026-04-03', 1)

    conn = sqlite3.connect(str(temp_db))
    row = conn.execute(
        "SELECT war, war_diff FROM war_daily WHERE player_id = ? AND season_id = ? AND date = ?",
        ('10001', 1, '2026-04-03'),
    ).fetchone()
    conn.close()

    assert row == (1.0, 0.6)


def test_update_team_war_daily_matches_team_table_sum(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        );
        """
    )
    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('17000', '재입단선수', 'pitcher', 'P'),
    )
    cursor.executemany(
        """INSERT INTO roster
           (team_id, player_id, season_id, joined_date, left_date)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('무', '17000', 1, '2026-04-01', '2026-04-02'),
            ('무', '17000', 1, '2026-04-03', '2026-04-04'),
        ],
    )
    cursor.executemany(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            ('17000', 1, None, '무', '2026-04-01', 0.10),
            ('17000', 1, '무', None, '2026-04-02', 0.30),
            ('17000', 1, None, '무', '2026-04-03', 0.50),
            ('17000', 1, '무', None, '2026-04-04', 0.70),
        ],
    )
    cursor.executemany(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        [
            ('17000', 1, '2026-04-01', 0.10, 0.10),
            ('17000', 1, '2026-04-02', 0.30, 0.20),
            ('17000', 1, '2026-04-03', 0.50, 0.20),
            ('17000', 1, '2026-04-04', 0.70, 0.20),
        ],
    )
    conn.commit()
    conn.close()

    db = DatabaseManager(temp_db)
    bat = pd.DataFrame(columns=pd.Index(['oWAR']))
    pit = pd.DataFrame([{'WAR': 0.70}], index=pd.Index(['17000'], name='ID'))
    player_id = pd.DataFrame({'slot1': ['17000']}, index=pd.Index(['무']))
    player_activation = pd.DataFrame(index=pd.Index(['무']))
    current_war = pd.DataFrame({'total': [0.70]}, index=pd.Index(['무']))

    with patch('app.scraper.jobs.calculate_fa_war', return_value=0.0):
        _update_team_war_daily(
            db,
            bat,
            pit,
            player_id,
            player_activation,
            current_war,
            '2026-04-04',
            1,
        )

    conn = sqlite3.connect(str(temp_db))
    rows, _, _ = get_team_table_data(conn, '무', '2026-04-04', season_id=1)
    row_sum = sum(float(row['WAR']) for row in rows)
    stored_total = conn.execute(
        "SELECT total_war FROM team_war_daily WHERE team_id = ? AND season_id = ? AND date = ?",
        ('무', 1, '2026-04-04'),
    ).fetchone()[0]
    conn.close()

    assert row_sum == stored_total == 0.3


def test_update_team_war_daily_uses_latest_prior_existing_date_for_diff(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        );
        """
    )
    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16630', '버하겐', 'pitcher', 'P'),
    )
    cursor.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ('무', '16630', 1, '2026-03-30'),
    )
    cursor.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ('16630', 1, None, '무', '2026-03-30', 0.0),
    )
    cursor.execute(
        """INSERT INTO team_war_daily
           (team_id, season_id, date, total_war, war_diff, rank)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ('무', 1, '2026-04-01', 0.10, 0.10, 1),
    )
    cursor.executemany(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        [
            ('16630', 1, '2026-04-01', 0.10, 0.10),
            ('16630', 1, '2026-04-03', 0.30, 0.20),
        ],
    )
    conn.commit()
    conn.close()

    db = DatabaseManager(temp_db)
    empty_bat = pd.DataFrame({'oWAR': pd.Series(dtype=float)})
    empty_pit = pd.DataFrame({'WAR': pd.Series(dtype=float)})

    with patch('app.scraper.jobs.calculate_fa_war', return_value=0.0):
        _update_team_war_daily(
            db,
            empty_bat,
            empty_pit,
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            '2026-04-03',
            1,
        )

    conn = sqlite3.connect(str(temp_db))
    row = conn.execute(
        "SELECT total_war, war_diff FROM team_war_daily WHERE team_id = ? AND season_id = ? AND date = ?",
        ('무', 1, '2026-04-03'),
    ).fetchone()
    conn.close()

    assert row == (0.3, 0.2)


def test_update_team_war_daily_writes_all_canonical_teams(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        );
        """
    )
    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.executemany(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        [
            ('17000', '무선수', 'pitcher', 'P'),
            ('17001', '준선수', 'pitcher', 'P'),
        ],
    )
    cursor.executemany(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        [
            ('무', '17000', 1, '2026-04-01'),
            ('준', '17001', 1, '2026-04-01'),
        ],
    )
    cursor.executemany(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            ('17000', 1, None, '무', '2026-04-01', 0.0),
            ('17001', 1, None, '준', '2026-04-01', 0.0),
        ],
    )
    cursor.executemany(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        [
            ('17000', 1, '2026-04-04', 0.40, 0.10),
            ('17001', 1, '2026-04-04', 0.20, 0.05),
        ],
    )
    conn.commit()
    conn.close()

    db = DatabaseManager(temp_db)
    bat = pd.DataFrame(columns=pd.Index(['oWAR']))
    pit = pd.DataFrame(columns=pd.Index(['WAR']))
    player_id = pd.DataFrame({'slot1': ['17000']}, index=pd.Index(['무']))
    player_activation = pd.DataFrame(index=pd.Index(['무']))
    current_war = pd.DataFrame({'total': [0.40]}, index=pd.Index(['무']))

    with patch('app.scraper.jobs.calculate_fa_war', return_value=0.0):
        _update_team_war_daily(
            db,
            bat,
            pit,
            player_id,
            player_activation,
            current_war,
            '2026-04-04',
            1,
        )

    conn = sqlite3.connect(str(temp_db))
    rows = conn.execute(
        "SELECT team_id, total_war FROM team_war_daily WHERE season_id = ? AND date = ? ORDER BY team_id",
        (1, '2026-04-04'),
    ).fetchall()
    conn.close()

    assert ('무', 0.4) in rows
    assert ('준', 0.2) in rows
    assert ('퐈', 0.0) in rows


def test_update_team_war_daily_assigns_same_rank_to_tied_teams(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.executescript(
        """
        CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        );
        """
    )
    cursor.executemany(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        [
            ('무', '무팀', '무'),
            ('준', '준팀', '준'),
            ('뚝', '뚝팀', '뚝'),
        ],
    )
    cursor.executemany(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        [
            ('18000', '무선수', 'pitcher', 'P'),
            ('18001', '준선수', 'pitcher', 'P'),
            ('18002', '뚝선수', 'pitcher', 'P'),
        ],
    )
    cursor.executemany(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        [
            ('무', '18000', 1, '2026-04-01'),
            ('준', '18001', 1, '2026-04-01'),
            ('뚝', '18002', 1, '2026-04-01'),
        ],
    )
    cursor.executemany(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            ('18000', 1, None, '무', '2026-04-01', 0.0),
            ('18001', 1, None, '준', '2026-04-01', 0.0),
            ('18002', 1, None, '뚝', '2026-04-01', 0.0),
        ],
    )
    cursor.executemany(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        [
            ('18000', 1, '2026-04-04', 0.40, 0.10),
            ('18001', 1, '2026-04-04', 0.40, 0.10),
            ('18002', 1, '2026-04-04', 0.20, 0.05),
        ],
    )
    conn.commit()
    conn.close()

    db = DatabaseManager(temp_db)

    with patch('app.scraper.jobs.calculate_fa_war', return_value=0.0):
        _update_team_war_daily(
            db,
            pd.DataFrame(columns=pd.Index(['oWAR'])),
            pd.DataFrame(columns=pd.Index(['WAR'])),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            '2026-04-04',
            1,
        )

    conn = sqlite3.connect(str(temp_db))
    rows = conn.execute(
        "SELECT team_id, rank FROM team_war_daily WHERE season_id = ? AND date = ? ORDER BY team_id",
        (1, '2026-04-04'),
    ).fetchall()
    conn.close()

    assert ('무', 1) in rows
    assert ('준', 1) in rows
    assert ('뚝', 3) in rows


def test_update_daily_records_uses_pregame_roster_semantics(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE daily_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            record_type TEXT,
            team_id TEXT,
            player_id TEXT NOT NULL,
            war_diff REAL
        )
        """
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16630', '버하겐', 'pitcher', 'P'),
    )
    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ('무', '16630', 1, '2026-04-01'),
    )
    cursor.execute(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        ('16630', 1, '2026-04-01', 0.30, 0.20),
    )
    conn.commit()
    conn.close()

    db = DatabaseManager(temp_db)
    _update_daily_records(db, pd.DataFrame(), pd.DataFrame(), '2026-04-01', 1)

    conn = sqlite3.connect(str(temp_db))
    row = conn.execute(
        "SELECT record_type, team_id, player_id, war_diff FROM daily_records WHERE date = ?",
        ('2026-04-01',),
    ).fetchone()
    conn.close()

    assert row == ('GOAT', '무', '16630', 0.2)
