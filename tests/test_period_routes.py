from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

from app.core.cache import invalidate_all
from app.core.schema import ensure_runtime_db
from web.app import create_app


def _build_app(db_path: Path):
    return create_app(config=SimpleNamespace(db_path=db_path, flask_secret_key="test-secret"))


def test_team_graph_route_keeps_full_available_dates_and_preserves_season(tmp_path):
    db_path = tmp_path / "team-period-routes.db"
    season_id = ensure_runtime_db(db_path, 2025)

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    cursor.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ('준', '준팀', '준'),
    )
    cursor.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ('10002', '박포수', 'bat', 'C'),
    )

    cursor.execute(
        """
        INSERT INTO roster (team_id, player_id, season_id, joined_date)
        VALUES (?, ?, ?, ?)
        """,
        ('준', '10002', season_id, '2025-03-20'),
    )
    cursor.execute(
        """
        INSERT INTO draft (
            season_id, team_id, player_id, round,
            pick_order, draft_type, application_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (season_id, '준', '10002', '1차1R', 1, 'main', '2025-03-20'),
    )
    cursor.executemany(
        """
        INSERT INTO war_daily (player_id, season_id, date, bat_war, pit_war, war, war_diff)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ('10002', season_id, '2025-03-20', 1.25, None, 1.25, 0.15),
            ('10002', season_id, '2025-03-21', 1.50, None, 1.50, 0.25),
            ('10002', season_id, '2025-03-22', 1.75, None, 1.75, 0.25),
        ],
    )
    conn.commit()
    conn.close()
    invalidate_all()

    app = _build_app(db_path)
    client = app.test_client()

    response = client.get('/category/team?team=준&sub=graph&period=30&date=2025-03-01&season=2025')

    assert response.status_code == 200
    content = response.get_data(as_text=True)
    assert 'value="2025-03-20"' in content
    assert 'const availableDates = ["2025-03-20", "2025-03-21", "2025-03-22"]' in content
    assert "fetch('/team_graph_data?team=준&period=30&season=2025&date=' + dateStr)" in content
    assert '/category/team?team=준&sub=graph&period=all&season=2025' in content


def test_league_graph_route_normalizes_end_date_and_preserves_season(tmp_path):
    db_path = tmp_path / "league-period-routes.db"
    season_id = ensure_runtime_db(db_path, 2025)

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    cursor.execute("DELETE FROM team_war_daily")
    cursor.executemany(
        "INSERT INTO team_war_daily (team_id, season_id, date, total_war, war_diff, rank) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ('무', season_id, '2025-03-20', 1.0, 0.1, 1),
            ('준', season_id, '2025-03-20', 0.8, 0.1, 2),
            ('퐈', season_id, '2025-03-20', 0.3, 0.0, None),
            ('무', season_id, '2025-03-21', 1.1, 0.1, 1),
            ('준', season_id, '2025-03-21', 0.9, 0.1, 2),
            ('퐈', season_id, '2025-03-21', 0.3, 0.0, None),
            ('무', season_id, '2025-03-22', 1.3, 0.2, 1),
            ('준', season_id, '2025-03-22', 1.0, 0.1, 2),
            ('퐈', season_id, '2025-03-22', 0.4, 0.1, None),
        ],
    )
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    response = client.get('/category/league?sub=graph&period=30&date=2025-03-01&season=2025')

    assert response.status_code == 200
    content = response.get_data(as_text=True)
    assert 'value="2025-03-20"' in content
    assert 'const availableDates = ["2025-03-20", "2025-03-21", "2025-03-22"]' in content
    assert "fetch('/graph_data?period=30&season=2025&date=' + dateStr)" in content
    assert '/category/league?sub=graph&period=all&season=2025' in content
