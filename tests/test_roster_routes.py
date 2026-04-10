from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

from app.core.schema import ensure_runtime_db
from web.app import create_app


def _build_app(db_path: Path):
    return create_app(config=SimpleNamespace(db_path=db_path, flask_secret_key="test-secret"))


def test_add_transaction_accepts_display_team_labels_and_stores_team_ids(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.execute(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, "2026-04-01", None, 0.0, 0.0, 0.0),
    )
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/add_transaction",
        data={
            "date": "2026/04/01",
            "name": "버하겐",
            "player_id": "16630",
            "old_team": "팀 퐈",
            "new_team": "팀 무",
            "season_id": str(season_id),
        },
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is True

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        """SELECT season_id, from_team_id, to_team_id, transaction_date
           FROM transactions
           WHERE player_id = ?""",
        ("16630",),
    ).fetchone()
    roster_rows = conn.execute(
        """SELECT team_id, joined_date, left_date
           FROM roster
           WHERE player_id = ? AND season_id = ?
           ORDER BY joined_date""",
        ("16630", season_id),
    ).fetchall()
    conn.close()

    assert row == (season_id, None, "무", "2026-04-01")
    assert roster_rows == [("무", "2026-04-01", None)]


def test_roster_template_uses_team_ids_for_transaction_select_values():
    content = (
        (Path(__file__).resolve().parents[1] / "web" / "templates" / "roster.html")
        .read_text(encoding="utf-8")
    )

    assert '<option value="퐈">팀 퐈</option>' in content
    assert '<option value="{{ team }}">팀 {{ team }}</option>' in content
    assert 'option value="팀 {{ team }}">팀 {{ team }}</option>' not in content


def test_update_transaction_accepts_display_team_labels_and_stores_team_ids(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.execute(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, "2026-04-01", None, 0.0, 0.0, 0.0),
    )
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "퐈", "2026-03-31", 0.0),
    )
    transaction_id = conn.execute(
        "SELECT id FROM transactions WHERE player_id = ?",
        ("16630",),
    ).fetchone()[0]
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/update_transaction",
        data={
            "transaction_id": str(transaction_id),
            "date": "2026/04/01",
            "name": "버하겐",
            "player_id": "16630",
            "old_team": "팀 퐈",
            "new_team": "팀 무",
        },
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is True

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT from_team_id, to_team_id, transaction_date FROM transactions WHERE id = ?",
        (transaction_id,),
    ).fetchone()
    roster_rows = conn.execute(
        """SELECT team_id, joined_date, left_date
           FROM roster
           WHERE player_id = ? AND season_id = ?
           ORDER BY joined_date""",
        ("16630", season_id),
    ).fetchall()
    conn.close()

    assert row == (None, "무", "2026-04-01")
    assert roster_rows == [("무", "2026-04-01", None)]


def test_delete_transaction_rebuilds_roster_for_player(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "무", "2026-04-01", 0.0),
    )
    transaction_id = conn.execute(
        "SELECT id FROM transactions WHERE player_id = ?",
        ("16630",),
    ).fetchone()[0]
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/delete_transaction",
        data={"transaction_id": str(transaction_id)},
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is True

    conn = sqlite3.connect(str(db_path))
    roster_rows = conn.execute(
        """SELECT team_id, joined_date, left_date
           FROM roster
           WHERE player_id = ? AND season_id = ?
           ORDER BY joined_date""",
        ("16630", season_id),
    ).fetchall()
    conn.close()

    assert roster_rows == []


def test_add_transaction_rejects_move_from_wrong_team(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("준", "준팀", "준"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ("준", "16630", season_id, "2026-03-28"),
    )
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/add_transaction",
        data={
            "date": "2026/04/01",
            "name": "버하겐",
            "player_id": "16630",
            "old_team": "팀 무",
            "new_team": "팀 퐈",
            "season_id": str(season_id),
        },
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is False

    conn = sqlite3.connect(str(db_path))
    transaction_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    roster_rows = conn.execute(
        "SELECT team_id, joined_date, left_date FROM roster WHERE player_id = ?",
        ("16630",),
    ).fetchall()
    conn.close()

    assert transaction_count == 0
    assert roster_rows == [("준", "2026-03-28", None)]


def test_add_transaction_rejects_duplicate_team_acquisition(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ("무", "16630", season_id, "2026-03-28"),
    )
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/add_transaction",
        data={
            "date": "2026/04/01",
            "name": "버하겐",
            "player_id": "16630",
            "old_team": "팀 퐈",
            "new_team": "팀 무",
            "season_id": str(season_id),
        },
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is False

    conn = sqlite3.connect(str(db_path))
    transaction_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    conn.close()

    assert transaction_count == 0


def test_update_transaction_rejects_invalid_history_change(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "무", "2026-04-01", 0.0),
    )
    transaction_id = conn.execute(
        "SELECT id FROM transactions WHERE player_id = ?",
        ("16630",),
    ).fetchone()[0]
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/update_transaction",
        data={
            "transaction_id": str(transaction_id),
            "date": "2026/04/01",
            "name": "버하겐",
            "player_id": "16630",
            "old_team": "팀 준",
            "new_team": "팀 무",
        },
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is False

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT from_team_id, to_team_id, transaction_date FROM transactions WHERE id = ?",
        (transaction_id,),
    ).fetchone()
    conn.close()

    assert row == (None, "무", "2026-04-01")


def test_add_transaction_rejects_second_move_on_same_date(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "무", "2026-04-01", 0.0),
    )
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/add_transaction",
        data={
            "date": "2026/04/01",
            "name": "버하겐",
            "player_id": "16630",
            "old_team": "팀 무",
            "new_team": "팀 퐈",
            "season_id": str(season_id),
        },
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is False
    assert "같은 날짜" in response.get_json()["message"]

    conn = sqlite3.connect(str(db_path))
    transaction_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    conn.close()

    assert transaction_count == 1


def test_update_transaction_rejects_same_day_collision(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.executemany(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            ("16630", season_id, None, "무", "2026-04-01", 0.0),
            ("16630", season_id, "무", None, "2026-04-02", 0.0),
        ],
    )
    transaction_id = conn.execute(
        "SELECT id FROM transactions WHERE player_id = ? AND transaction_date = ?",
        ("16630", "2026-04-02"),
    ).fetchone()[0]
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/update_transaction",
        data={
            "transaction_id": str(transaction_id),
            "date": "2026/04/01",
            "name": "버하겐",
            "player_id": "16630",
            "old_team": "팀 무",
            "new_team": "팀 퐈",
        },
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is False
    assert "같은 날짜" in response.get_json()["message"]

    conn = sqlite3.connect(str(db_path))
    preserved_row = conn.execute(
        "SELECT transaction_date FROM transactions WHERE id = ?",
        (transaction_id,),
    ).fetchone()
    conn.close()

    assert preserved_row == ("2026-04-02",)


def test_add_transaction_rejects_overlapping_existing_roster_intervals(tmp_path):
    db_path = tmp_path / "roster.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16630", "버하겐", "pit", "P"),
    )
    conn.executemany(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date, left_date) VALUES (?, ?, ?, ?, ?)",
        [
            ("준", "16630", season_id, "2026-03-28", None),
            ("무", "16630", season_id, "2026-03-30", None),
        ],
    )
    conn.commit()
    conn.close()

    app = _build_app(db_path)
    client = app.test_client()

    with client.session_transaction() as session:
        session["authenticated"] = True

    response = client.post(
        "/add_transaction",
        data={
            "date": "2026/04/01",
            "name": "버하겐",
            "player_id": "16630",
            "old_team": "팀 준",
            "new_team": "팀 퐈",
            "season_id": str(season_id),
        },
    )

    assert response.status_code == 200
    assert response.get_json()["success"] is False
    assert "기존 로스터 이력" in response.get_json()["message"]
