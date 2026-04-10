from __future__ import annotations

import sqlite3
from unittest.mock import patch

from app.core.cache import invalidate_after_update, invalidate_all
from web.services.player_service import get_player_data


def test_player_war_date_cache_is_cleared_by_post_update_invalidation(temp_db):
    invalidate_all()
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        ('10001', 1, '2025-03-20', 1.0, 0.1),
    )
    conn.commit()

    with patch('web.services.player_service.get_team_order', return_value=['준', '뚝', '재']):
        _, _, first_dates, _ = get_player_data(
            conn,
            selected_types=['bat', 'pit'],
            selected_teams=['준', '뚝', '재', '퐈'],
            selected_date='',
            search_query='',
            sort_by='WAR',
            sort_order='desc',
            season_id=1,
        )

    cursor.execute(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        ('10001', 1, '2025-03-21', 1.2, 0.2),
    )
    conn.commit()

    invalidate_after_update()

    with patch('web.services.player_service.get_team_order', return_value=['준', '뚝', '재']):
        _, _, second_dates, _ = get_player_data(
            conn,
            selected_types=['bat', 'pit'],
            selected_teams=['준', '뚝', '재', '퐈'],
            selected_date='',
            search_query='',
            sort_by='WAR',
            sort_order='desc',
            season_id=1,
        )
    conn.close()
    invalidate_all()

    assert first_dates == ['2025-03-20']
    assert second_dates == ['2025-03-20', '2025-03-21']


def test_player_default_sort_keeps_tied_war_players_in_rank_order(temp_db):
    invalidate_all()
    conn = sqlite3.connect(str(temp_db))
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        [
            ('20001', '나선수', 'bat', 'C'),
            ('20002', '가선수', 'bat', 'C'),
        ],
    )
    cursor.executemany(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        [
            ('준', '20001', 1, '2025-03-20'),
            ('뚝', '20002', 1, '2025-03-20'),
        ],
    )
    cursor.executemany(
        "INSERT INTO war_daily (player_id, season_id, date, war, war_diff) VALUES (?, ?, ?, ?, ?)",
        [
            ('20001', 1, '2025-03-20', 1.0, 0.1),
            ('20002', 1, '2025-03-20', 1.0, 0.1),
        ],
    )
    conn.commit()

    with patch('web.services.player_service.get_team_order', return_value=['준', '뚝', '재']):
        rows, _, date_columns, _ = get_player_data(
            conn,
            selected_types=['bat', 'pit'],
            selected_teams=['준', '뚝', '재', '퐈'],
            selected_date='',
            search_query='',
            sort_by='',
            sort_order='desc',
            season_id=1,
        )

    conn.close()
    invalidate_all()

    assert date_columns == ['2025-03-20']
    assert [row['Name'] for row in rows] == ['가선수', '나선수']
    assert [row['war_rank'] for row in rows] == [1, 2]
