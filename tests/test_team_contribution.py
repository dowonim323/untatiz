import sqlite3

from app.services.team_contribution import (
    build_team_contribution_snapshot,
    calculate_team_contributed_war_as_of_date,
    calculate_team_contributed_war_diff_on_date,
    calculate_team_total_contribution,
    get_player_raw_daily_war_as_of_date,
    get_player_raw_daily_war_diff_on_date,
)


def test_raw_player_daily_war_helpers_distinguish_total_and_diff():
    player_history = [
        {'date': '2026-04-01', 'war': 0.1, 'war_diff': 0.1},
        {'date': '2026-04-03', 'war': 0.3, 'war_diff': 0.2},
    ]

    assert get_player_raw_daily_war_as_of_date(player_history, '2026-04-02') == 0.1
    assert get_player_raw_daily_war_as_of_date(player_history, '2026-04-03') == 0.3
    assert get_player_raw_daily_war_diff_on_date(player_history, '2026-04-02') == 0.0
    assert get_player_raw_daily_war_diff_on_date(player_history, '2026-04-03') == 0.2


def test_team_contributed_war_helpers_include_same_day_join_war(temp_db):
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
    cursor.executemany(
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('16630', 1, '2026-04-01', 0.08, 0.08),
            ('16630', 1, '2026-04-04', 0.20, 0.12),
        ],
    )
    conn.commit()

    assert calculate_team_contributed_war_as_of_date(conn, '16630', '무', 1, '2026-04-01') == 0.08
    assert calculate_team_contributed_war_diff_on_date(conn, '16630', '무', 1, '2026-04-01') == 0.08
    assert calculate_team_contributed_war_as_of_date(conn, '16630', '무', 1, '2026-04-04') == 0.20
    assert calculate_team_contributed_war_diff_on_date(conn, '16630', '무', 1, '2026-04-04') == 0.12

    conn.close()


def test_build_team_contribution_snapshot_exposes_explicit_raw_and_team_war_fields(temp_db):
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
    cursor.executemany(
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('16630', 1, '2026-04-01', 0.2, 0.1),
            ('16630', 1, '2026-04-04', 0.5, 0.3),
        ],
    )
    conn.commit()

    snapshot = build_team_contribution_snapshot(conn, '무', 1, '2026-04-04')

    assert snapshot == [
        {
            'player_id': '16630',
            'Name': '버하겐',
            'draft_order': None,
            'draft_round': '',
            'raw_player_daily_war_as_of_target_date': 0.5,
            'raw_player_daily_war_diff_on_target_date': 0.3,
            'team_contributed_war_as_of_target_date': 0.5,
            'team_contributed_war_diff_on_target_date': 0.3,
            'WAR': 0.5,
            '변화량': 0.3,
        }
    ]
    assert calculate_team_total_contribution(conn, '무', 1, '2026-04-04') == 0.5

    conn.close()


def test_departed_player_snapshot_keeps_team_diff_frozen(temp_db):
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
        """INSERT INTO war_daily (player_id, season_id, date, war, war_diff)
           VALUES (?, ?, ?, ?, ?)""",
        [
            ('16349', 1, '2026-03-27', 0.10, 0.10),
            ('16349', 1, '2026-03-31', 0.20, 0.10),
            ('16349', 1, '2026-04-01', 0.30, 0.20),
            ('16349', 1, '2026-04-04', 0.70, 0.40),
        ],
    )
    conn.commit()

    snapshot = build_team_contribution_snapshot(conn, '무', 1, '2026-04-04')

    assert snapshot == [
        {
            'player_id': '16349',
            'Name': '라일리',
            'draft_order': None,
            'draft_round': '',
            'raw_player_daily_war_as_of_target_date': 0.7,
            'raw_player_daily_war_diff_on_target_date': 0.4,
            'team_contributed_war_as_of_target_date': 0.1,
            'team_contributed_war_diff_on_target_date': 0.0,
            'WAR': 0.1,
            '변화량': 0.0,
        }
    ]

    conn.close()
