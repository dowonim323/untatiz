import sqlite3
from unittest.mock import patch

from web.services.league_service import (
    get_league_graph_data,
    get_league_table_data,
    get_league_weekly_data,
)


def test_get_league_table_data_uses_stored_tie_ranks(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )"""
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 1, '2026-04-04', 1.2, 0.1, 1),
            ('준', 1, '2026-04-04', 1.2, 0.2, 1),
            ('뚝', 1, '2026-04-04', 0.8, -0.1, 3),
            ('퐈', 1, '2026-04-04', 0.5, 0.0, None),
        ],
    )
    conn.commit()

    with patch('web.services.league_service.get_team_order', return_value=['무', '준', '뚝', '퐈']):
        rows, _, _ = get_league_table_data(conn, '2026-04-04', season_id=1)

    rank_by_team = {row['팀']: row['순위'] for row in rows}
    assert rank_by_team['무'] == 1
    assert rank_by_team['준'] == 1
    assert rank_by_team['뚝'] == 3
    assert rank_by_team['퐈'] == ''

    conn.close()


def test_get_league_weekly_data_uses_stored_tie_ranks_for_last_date(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )"""
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 1, '2026-04-03', 1.0, 0.1, 1),
            ('준', 1, '2026-04-03', 0.9, 0.1, 2),
            ('뚝', 1, '2026-04-03', 0.8, 0.1, 3),
            ('퐈', 1, '2026-04-03', 0.3, 0.0, None),
            ('무', 1, '2026-04-04', 1.2, 0.2, 1),
            ('준', 1, '2026-04-04', 1.2, 0.3, 1),
            ('뚝', 1, '2026-04-04', 0.7, -0.1, 3),
            ('퐈', 1, '2026-04-04', 0.4, 0.1, None),
        ],
    )
    conn.commit()

    with patch('web.services.league_service.get_team_order', return_value=['무', '준', '뚝', '퐈']):
        rows, _, _ = get_league_weekly_data(conn, '2026-04-04', season_id=1)

    rank_by_team = {row['팀']: row['순위'] for row in rows}
    assert rank_by_team['무'] == 1
    assert rank_by_team['준'] == 1
    assert rank_by_team['뚝'] == 3

    conn.close()


def test_get_league_weekly_data_keeps_opening_day_dates_when_history_is_short(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )"""
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 1, '2026-04-01', 1.0, 0.1, 1),
            ('준', 1, '2026-04-01', 0.8, 0.1, 2),
            ('퐈', 1, '2026-04-01', 0.3, 0.0, None),
            ('무', 1, '2026-04-02', 1.1, 0.1, 1),
            ('준', 1, '2026-04-02', 0.9, 0.1, 2),
            ('퐈', 1, '2026-04-02', 0.3, 0.0, None),
            ('무', 1, '2026-04-03', 1.3, 0.2, 1),
            ('준', 1, '2026-04-03', 1.0, 0.1, 2),
            ('퐈', 1, '2026-04-03', 0.4, 0.1, None),
        ],
    )
    conn.commit()

    with patch('web.services.league_service.get_team_order', return_value=['무', '준', '퐈']):
        rows, date_columns, selected_dates = get_league_weekly_data(conn, '2026-04-03', season_id=1)

    assert date_columns == ['2026-04-01', '2026-04-02', '2026-04-03']
    assert selected_dates == ['2026-04-01', '2026-04-02', '2026-04-03']
    assert [row['팀'] for row in rows] == ['무', '준']

    conn.close()


def test_get_league_graph_data_keeps_opening_day_dates_when_history_is_short(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )"""
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 1, '2026-04-01', 1.0, 0.1, 1),
            ('준', 1, '2026-04-01', 0.8, 0.1, 2),
            ('퐈', 1, '2026-04-01', 0.3, 0.0, None),
            ('무', 1, '2026-04-02', 1.1, 0.1, 1),
            ('준', 1, '2026-04-02', 0.9, 0.1, 2),
            ('퐈', 1, '2026-04-02', 0.3, 0.0, None),
            ('무', 1, '2026-04-03', 1.3, 0.2, 1),
            ('준', 1, '2026-04-03', 1.0, 0.1, 2),
            ('퐈', 1, '2026-04-03', 0.4, 0.1, None),
        ],
    )
    conn.commit()

    graph_data, date_columns, selected_dates = get_league_graph_data(
        conn,
        '2026-04-03',
        period='30',
        season_id=1,
    )

    assert date_columns == ['2026-04-01', '2026-04-02', '2026-04-03']
    assert selected_dates == ['2026-04-01', '2026-04-02', '2026-04-03']
    assert graph_data == {
        'dates': ['2026-04-01', '2026-04-02', '2026-04-03'],
        'teams': ['무', '준'],
        'data': [[1.0, 1.1, 1.3], [0.8, 0.9, 1.0]],
    }

    conn.close()


def test_get_league_graph_data_clamps_too_early_end_date_to_opening_day(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE team_war_daily (
            team_id TEXT NOT NULL,
            season_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_war REAL,
            war_diff REAL,
            rank INTEGER,
            PRIMARY KEY (team_id, season_id, date)
        )"""
    )
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', 1, '2026-04-01', 1.0, 0.1, 1),
            ('준', 1, '2026-04-01', 0.8, 0.1, 2),
            ('퐈', 1, '2026-04-01', 0.3, 0.0, None),
            ('무', 1, '2026-04-02', 1.1, 0.1, 1),
            ('준', 1, '2026-04-02', 0.9, 0.1, 2),
            ('퐈', 1, '2026-04-02', 0.3, 0.0, None),
            ('무', 1, '2026-04-03', 1.3, 0.2, 1),
            ('준', 1, '2026-04-03', 1.0, 0.1, 2),
            ('퐈', 1, '2026-04-03', 0.4, 0.1, None),
        ],
    )
    conn.commit()

    graph_data, date_columns, selected_dates = get_league_graph_data(
        conn,
        '2026-03-01',
        period='30',
        season_id=1,
    )

    assert date_columns == ['2026-04-01', '2026-04-02', '2026-04-03']
    assert selected_dates == ['2026-04-01']
    assert graph_data == {
        'dates': ['2026-04-01'],
        'teams': ['무', '준'],
        'data': [[1.0], [0.8]],
    }

    conn.close()
