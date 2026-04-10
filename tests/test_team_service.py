import sqlite3

from app.core.cache import invalidate_all
from web.services.team_service import get_team_graph_data, get_team_table_data, get_team_weekly_data


def test_get_team_weekly_data_keeps_draft_index_for_weekly_rows(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date)
        VALUES (?, ?, ?, ?)
        """,
        ('준', '10002', 1, '2025-03-20'),
    )
    cursor.execute(
        """
        INSERT INTO draft (
            season_id, team_id, player_id, round,
            pick_order, draft_type, application_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, '준', '10002', '1차1R', 1, 'main', '2025-03-20'),
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('10002', 1, '2025-03-20', 1.25, 0.15),
            ('10002', 1, '2025-03-21', 1.5, 0.25),
        ],
    )
    conn.commit()

    rows, date_columns, selected_dates = get_team_weekly_data(
        conn,
        '준',
        '2025-03-21',
        season_id=1,
    )

    assert date_columns == ['2025-03-20', '2025-03-21']
    assert selected_dates == ['2025-03-20', '2025-03-21']
    assert rows == [
        {
            '순위': 1,
            'index': '1차1R',
            'Name': '박포수',
            '2025-03-20': '1.25',
            '2025-03-21': '1.50',
        }
    ]

    conn.close()


def test_get_team_table_data_uses_team_contribution_since_join(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16630', '버하겐', 'pitcher', 'P'),
    )
    cursor.executemany(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date, left_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('퐈', '16630', 1, '2026-03-01', '2026-04-01'),
            ('무', '16630', 1, '2026-04-01', None),
        ],
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('16630', 1, '2026-03-31', 1.10, 0.10),
            ('16630', 1, '2026-04-04', 1.35, 0.05),
        ],
    )
    conn.commit()
    invalidate_all()

    rows, date_columns, selected_date = get_team_table_data(
        conn,
        '무',
        '2026-04-04',
        season_id=1,
    )

    assert date_columns == ['2026-03-31', '2026-04-04']
    assert selected_date == '2026-04-04'
    assert len(rows) == 1
    assert rows[0]['순위'] == 1
    assert rows[0]['index'] == ''
    assert rows[0]['Name'] == '버하겐'
    assert rows[0]['WAR'] == '0.25'
    assert rows[0]['변화량'] == '+0.25'
    assert rows[0]['변화량_색상'].startswith('rgb')

    conn.close()


def test_get_team_table_data_keeps_departed_player_with_frozen_contribution(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16349', '라일리', 'pitcher', 'P'),
    )
    cursor.execute(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date, left_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        ('무', '16349', 1, '2026-03-28', '2026-04-01'),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ('16349', 1, '무', None, '2026-04-01', 0.30),
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('16349', 1, '2026-03-27', 0.10, 0.10),
            ('16349', 1, '2026-03-31', 0.20, 0.10),
            ('16349', 1, '2026-04-01', 0.30, 0.20),
            ('16349', 1, '2026-04-04', 0.70, 0.40),
        ],
    )
    conn.commit()
    invalidate_all()

    rows, _, selected_date = get_team_table_data(conn, '무', '2026-04-04', season_id=1)

    assert selected_date == '2026-04-04'
    assert len(rows) == 1
    assert rows[0]['Name'] == '라일리'
    assert rows[0]['WAR'] == '0.10'
    assert rows[0]['변화량'] == '+0.00'

    conn.close()


def test_get_team_table_data_includes_same_day_acquisition_war(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

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
        ('무', '16630', 1, '2026-04-01'),
    )
    cursor.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ('16630', 1, None, '무', '2026-04-01', 0.08),
    )
    cursor.execute(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        ('16630', 1, '2026-04-01', 0.08, 0.08),
    )
    conn.commit()
    invalidate_all()

    rows, _, selected_date = get_team_table_data(conn, '무', '2026-04-01', season_id=1)

    assert selected_date == '2026-04-01'
    assert len(rows) == 1
    assert rows[0]['WAR'] == '0.08'
    assert rows[0]['변화량'] == '+0.08'

    conn.close()


def test_get_team_table_data_sums_multiple_stints_for_same_team(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('17000', '재입단선수', 'pitcher', 'P'),
    )
    cursor.executemany(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date, left_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('무', '17000', 1, '2026-04-01', '2026-04-02'),
            ('무', '17000', 1, '2026-04-03', '2026-04-04'),
        ],
    )
    cursor.executemany(
        """
        INSERT INTO transactions
        (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ('17000', 1, None, '무', '2026-04-01', 0.10),
            ('17000', 1, '무', None, '2026-04-02', 0.30),
            ('17000', 1, None, '무', '2026-04-03', 0.50),
            ('17000', 1, '무', None, '2026-04-04', 0.70),
        ],
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('17000', 1, '2026-04-01', 0.10, 0.10),
            ('17000', 1, '2026-04-02', 0.30, 0.20),
            ('17000', 1, '2026-04-03', 0.50, 0.20),
            ('17000', 1, '2026-04-04', 0.70, 0.20),
        ],
    )
    conn.commit()
    invalidate_all()

    rows, _, selected_date = get_team_table_data(conn, '무', '2026-04-04', season_id=1)

    assert selected_date == '2026-04-04'
    assert len(rows) == 1
    assert rows[0]['Name'] == '재입단선수'
    assert rows[0]['WAR'] == '0.30'
    assert rows[0]['변화량'] == '+0.00'

    conn.close()


def test_get_team_table_data_does_not_double_count_duplicate_draft_rows(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('18000', '중복드래프트선수', 'pitcher', 'P'),
    )
    cursor.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ('무', '18000', 1, '2026-04-01'),
    )
    cursor.executemany(
        """INSERT INTO draft
           (season_id, team_id, player_id, round, pick_order, draft_type, application_date)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (1, '무', '18000', '1차1R', 1, 'main', '2026-03-20'),
            (1, '무', '18000', '2차1R', 999, 'supplemental', '2026-06-01'),
        ],
    )
    cursor.executemany(
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('18000', 1, '2026-04-01', 0.10, 0.10),
            ('18000', 1, '2026-04-04', 0.40, 0.30),
        ],
    )
    conn.commit()
    invalidate_all()

    rows, _, _ = get_team_table_data(conn, '무', '2026-04-04', season_id=1)

    assert len(rows) == 1
    assert rows[0]['index'] == '1차1R'
    assert rows[0]['WAR'] == '0.40'

    conn.close()


def test_get_team_table_data_ignores_stale_transaction_war_snapshot(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

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
        ('무', '16630', 1, '2026-04-01'),
    )
    cursor.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ('16630', 1, None, '무', '2026-04-01', 9.99),
    )
    cursor.executemany(
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('16630', 1, '2026-03-31', 0.10, 0.10),
            ('16630', 1, '2026-04-01', 0.20, 0.10),
            ('16630', 1, '2026-04-04', 0.50, 0.30),
        ],
    )
    conn.commit()
    invalidate_all()

    rows, _, _ = get_team_table_data(conn, '무', '2026-04-04', season_id=1)

    assert len(rows) == 1
    assert rows[0]['WAR'] == '0.40'

    conn.close()


def test_get_team_weekly_data_keeps_departed_player_with_frozen_values(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('16349', '라일리', 'pitcher', 'P'),
    )
    cursor.execute(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date, left_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        ('무', '16349', 1, '2026-03-28', '2026-04-01'),
    )
    cursor.execute(
        """
        INSERT INTO transactions
        (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ('16349', 1, '무', None, '2026-04-01', 0.30),
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('16349', 1, '2026-03-27', 0.10, 0.10),
            ('16349', 1, '2026-03-31', 0.20, 0.10),
            ('16349', 1, '2026-04-01', 0.30, 0.20),
            ('16349', 1, '2026-04-02', 0.70, 0.40),
            ('16349', 1, '2026-04-03', 0.70, 0.00),
            ('16349', 1, '2026-04-04', 0.70, 0.00),
        ],
    )
    conn.commit()
    invalidate_all()

    rows, _, selected_dates = get_team_weekly_data(conn, '무', '2026-04-04', season_id=1)

    assert selected_dates == [
        '2026-03-27',
        '2026-03-31',
        '2026-04-01',
        '2026-04-02',
        '2026-04-03',
        '2026-04-04',
    ]
    assert rows == [
        {
            '순위': 1,
            'index': '',
            'Name': '라일리',
            '2026-03-27': '',
            '2026-03-31': '0.10',
            '2026-04-01': '0.10',
            '2026-04-02': '0.10',
            '2026-04-03': '0.10',
            '2026-04-04': '0.10',
        }
    ]

    conn.close()


def test_get_team_weekly_data_clamps_too_early_selected_date_to_opening_day(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date)
        VALUES (?, ?, ?, ?)
        """,
        ('준', '10002', 1, '2025-03-20'),
    )
    cursor.execute(
        """
        INSERT INTO draft (
            season_id, team_id, player_id, round,
            pick_order, draft_type, application_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, '준', '10002', '1차1R', 1, 'main', '2025-03-20'),
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('10002', 1, '2025-03-20', 1.25, 0.15),
            ('10002', 1, '2025-03-21', 1.50, 0.25),
            ('10002', 1, '2025-03-22', 1.75, 0.25),
        ],
    )
    conn.commit()
    invalidate_all()

    rows, date_columns, selected_dates = get_team_weekly_data(
        conn,
        '준',
        '2025-03-01',
        season_id=1,
    )

    assert date_columns == ['2025-03-20', '2025-03-21', '2025-03-22']
    assert selected_dates == ['2025-03-20']
    assert rows == [
        {
            '순위': 1,
            'index': '1차1R',
            'Name': '박포수',
            '2025-03-20': '1.25',
        }
    ]

    conn.close()


def test_get_team_graph_data_sums_multiple_stints_for_same_team(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('무', '무팀', '무'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('17000', '재입단선수', 'pitcher', 'P'),
    )
    cursor.executemany(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date, left_date)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('무', '17000', 1, '2026-04-01', '2026-04-02'),
            ('무', '17000', 1, '2026-04-03', '2026-04-04'),
        ],
    )
    cursor.executemany(
        """
        INSERT INTO transactions
        (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ('17000', 1, None, '무', '2026-04-01', 0.10),
            ('17000', 1, '무', None, '2026-04-02', 0.30),
            ('17000', 1, None, '무', '2026-04-03', 0.50),
            ('17000', 1, '무', None, '2026-04-04', 0.70),
        ],
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('17000', 1, '2026-04-01', 0.10, 0.10),
            ('17000', 1, '2026-04-02', 0.30, 0.20),
            ('17000', 1, '2026-04-03', 0.50, 0.20),
            ('17000', 1, '2026-04-04', 0.70, 0.20),
        ],
    )
    conn.commit()
    invalidate_all()

    graph_data, _, selected_dates = get_team_graph_data(
        conn,
        '무',
        '2026-04-04',
        period='all',
        season_id=1,
    )

    assert selected_dates == ['2026-04-01', '2026-04-02', '2026-04-03', '2026-04-04']
    assert graph_data == {
        'dates': ['2026-04-01', '2026-04-02', '2026-04-03', '2026-04-04'],
        'players': ['재입단선수'],
        'data': [[0.1, 0.1, 0.3, 0.3]],
    }

    conn.close()


def test_get_team_graph_data_keeps_opening_day_dates_when_30_day_history_is_short(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date)
        VALUES (?, ?, ?, ?)
        """,
        ('준', '10002', 1, '2025-03-20'),
    )
    cursor.execute(
        """
        INSERT INTO draft (
            season_id, team_id, player_id, round,
            pick_order, draft_type, application_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, '준', '10002', '1차1R', 1, 'main', '2025-03-20'),
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('10002', 1, '2025-03-20', 1.25, 0.15),
            ('10002', 1, '2025-03-21', 1.50, 0.25),
            ('10002', 1, '2025-03-22', 1.75, 0.25),
        ],
    )
    conn.commit()
    invalidate_all()

    graph_data, date_columns, selected_dates = get_team_graph_data(
        conn,
        '준',
        '2025-03-22',
        period='30',
        season_id=1,
    )

    assert date_columns == ['2025-03-20', '2025-03-21', '2025-03-22']
    assert selected_dates == ['2025-03-20', '2025-03-21', '2025-03-22']
    assert graph_data == {
        'dates': ['2025-03-20', '2025-03-21', '2025-03-22'],
        'players': ['박포수'],
        'data': [[1.25, 1.5, 1.75]],
    }

    conn.close()


def test_get_team_graph_data_clamps_too_early_end_date_to_opening_day(temp_db):
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date)
        VALUES (?, ?, ?, ?)
        """,
        ('준', '10002', 1, '2025-03-20'),
    )
    cursor.execute(
        """
        INSERT INTO draft (
            season_id, team_id, player_id, round,
            pick_order, draft_type, application_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, '준', '10002', '1차1R', 1, 'main', '2025-03-20'),
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ('10002', 1, '2025-03-20', 1.25, 0.15),
            ('10002', 1, '2025-03-21', 1.50, 0.25),
            ('10002', 1, '2025-03-22', 1.75, 0.25),
        ],
    )
    conn.commit()
    invalidate_all()

    graph_data, date_columns, selected_dates = get_team_graph_data(
        conn,
        '준',
        '2025-03-01',
        period='30',
        season_id=1,
    )

    assert date_columns == ['2025-03-20', '2025-03-21', '2025-03-22']
    assert selected_dates == ['2025-03-20']
    assert graph_data == {
        'dates': ['2025-03-20'],
        'players': ['박포수'],
        'data': [[1.25]],
    }

    conn.close()
