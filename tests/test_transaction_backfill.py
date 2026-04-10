from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from app.core.schema import ensure_runtime_db
from app.services.transaction_backfill import (
    backfill_transaction_history,
    rebuild_historical_records,
)
from web.app import create_app
from web.services.team_service import get_team_table_data


def _build_app(db_path: Path):
    return create_app(config=SimpleNamespace(db_path=db_path, flask_secret_key="test-secret"))


def _seed_player_history(
    conn: sqlite3.Connection,
    season_id: int,
    player_id: str = "16630",
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("무", "무팀", "무"),
    )
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        (player_id, "버하겐", "pit", "P"),
    )
    conn.executemany(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (player_id, season_id, "2026-04-02", None, 0.10, 0.10, 0.10),
            (player_id, season_id, "2026-04-03", None, 0.10, 0.10, 0.00),
            (player_id, season_id, "2026-04-04", None, 0.11, 0.11, 0.01),
        ],
    )


def test_add_transaction_backfills_historical_daily_and_team_totals(tmp_path):
    db_path = tmp_path / "backfill.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    _seed_player_history(conn, season_id)
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
    roster_rows = conn.execute(
        """SELECT team_id, joined_date, left_date
           FROM roster
           WHERE player_id = ?
           ORDER BY joined_date""",
        ("16630",),
    ).fetchall()
    goat_row = conn.execute(
        """SELECT team_id, war_diff
           FROM daily_records
           WHERE player_id = ? AND date = ? AND record_type = 'GOAT'""",
        ("16630", "2026-04-04"),
    ).fetchone()
    team_row = conn.execute(
        """SELECT total_war, war_diff, rank
           FROM team_war_daily
           WHERE team_id = ? AND season_id = ? AND date = ?""",
        ("무", season_id, "2026-04-04"),
    ).fetchone()
    fa_row = conn.execute(
        "SELECT total_war FROM team_war_daily WHERE team_id = '퐈' AND season_id = ? AND date = ?",
        (season_id, "2026-04-04"),
    ).fetchone()
    conn.close()

    assert roster_rows == [("무", "2026-04-01", None)]
    assert goat_row == ("무", 0.01)
    assert team_row == (0.11, 0.01, 1)
    assert fa_row == (0.0,)


def test_backfill_skips_when_transaction_is_after_latest_war_date(tmp_path):
    db_path = tmp_path / "backfill.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    _seed_player_history(conn, season_id)

    changed = backfill_transaction_history(conn, season_id, "2026-04-05")
    daily_count = conn.execute("SELECT COUNT(*) FROM daily_records").fetchone()[0]
    team_count = conn.execute("SELECT COUNT(*) FROM team_war_daily").fetchone()[0]
    conn.close()

    assert changed is False
    assert daily_count == 0
    assert team_count == 0


def test_update_transaction_backfills_from_earliest_effective_date(tmp_path):
    db_path = tmp_path / "backfill.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    _seed_player_history(conn, season_id)
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "무", "2026-04-04", 0.11),
    )
    conn.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ("무", "16630", season_id, "2026-04-04"),
    )
    conn.commit()
    transaction_id = conn.execute(
        "SELECT id FROM transactions WHERE player_id = ?",
        ("16630",),
    ).fetchone()[0]
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
    historical_row = conn.execute(
        """SELECT total_war, war_diff
           FROM team_war_daily
           WHERE team_id = ? AND season_id = ? AND date = ?""",
        ("무", season_id, "2026-04-02"),
    ).fetchone()
    goat_row = conn.execute(
        """SELECT team_id
           FROM daily_records
           WHERE player_id = ? AND date = ? AND record_type = 'GOAT'""",
        ("16630", "2026-04-02"),
    ).fetchone()
    conn.close()

    assert historical_row == (0.1, 0.1)
    assert goat_row == ("무",)


def test_backfill_team_total_matches_team_table_sum(tmp_path):
    db_path = tmp_path / "backfill.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    _seed_player_history(conn, season_id)
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "무", "2026-04-01", 0.0),
    )
    conn.commit()

    changed = backfill_transaction_history(conn, season_id, "2026-04-01")
    rows, _, _ = get_team_table_data(conn, '무', '2026-04-04', season_id=season_id)
    row_sum = sum(Decimal(str(row['WAR'])) for row in rows)
    team_total = conn.execute(
        "SELECT total_war FROM team_war_daily WHERE team_id = ? AND season_id = ? AND date = ?",
        ("무", season_id, "2026-04-04"),
    ).fetchone()[0]
    conn.close()

    assert changed is True
    assert row_sum == Decimal(str(team_total))


def test_backfill_uses_latest_prior_existing_date_for_team_diff(tmp_path):
    db_path = tmp_path / "backfill.db"
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
        ("무", "16630", season_id, "2026-03-30"),
    )
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "무", "2026-03-30", 0.0),
    )
    conn.executemany(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("16630", season_id, "2026-04-01", None, 0.10, 0.10, 0.10),
            ("16630", season_id, "2026-04-03", None, 0.30, 0.30, 0.20),
        ],
    )
    conn.commit()

    changed = backfill_transaction_history(conn, season_id, "2026-03-30")
    row = conn.execute(
        (
            "SELECT total_war, war_diff FROM team_war_daily "
            "WHERE team_id = ? AND season_id = ? AND date = ?"
        ),
        ("무", season_id, "2026-04-03"),
    ).fetchone()
    conn.close()

    assert changed is True
    assert row == (0.3, 0.2)


def test_backfill_without_roster_rebuild_keeps_same_day_player_in_fa(tmp_path):
    db_path = tmp_path / "backfill.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    _seed_player_history(conn, season_id)
    conn.execute(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, "2026-04-01", None, 0.08, 0.08, 0.08),
    )
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "무", "2026-04-01", 0.08),
    )
    conn.commit()

    changed = backfill_transaction_history(conn, season_id, "2026-04-01")
    team_total = conn.execute(
        "SELECT total_war FROM team_war_daily WHERE team_id = ? AND season_id = ? AND date = ?",
        ("무", season_id, "2026-04-01"),
    ).fetchone()[0]
    fa_total = conn.execute(
        "SELECT total_war FROM team_war_daily WHERE team_id = '퐈' AND season_id = ? AND date = ?",
        (season_id, "2026-04-01"),
    ).fetchone()[0]
    conn.close()

    assert changed is True
    assert team_total == 0.0
    assert fa_total == 0.0


def test_backfill_writes_unranked_fa_row_with_latest_prior_diff(tmp_path):
    db_path = tmp_path / "backfill.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO players (id, name, player_type, position) VALUES (?, ?, ?, ?)",
        ("16631", "퐈선수", "pit", "P"),
    )
    conn.executemany(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("16631", season_id, "2026-04-01", None, 0.20, 0.20, 0.20),
            ("16631", season_id, "2026-04-03", None, 0.45, 0.45, 0.25),
        ],
    )

    changed = backfill_transaction_history(conn, season_id, "2026-04-01")
    fa_rows = conn.execute(
        """SELECT date, total_war, war_diff, rank
           FROM team_war_daily
           WHERE team_id = '퐈' AND season_id = ?
           ORDER BY date""",
        (season_id,),
    ).fetchall()
    conn.close()

    assert changed is True
    assert fa_rows == [
        ("2026-04-01", 0.2, None, None),
        ("2026-04-03", 0.45, 0.25, None),
    ]


def test_rebuild_historical_records_recalculates_transaction_baselines(tmp_path):
    db_path = tmp_path / "backfill.db"
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
        ("무", "16630", season_id, "2026-04-01"),
    )
    conn.execute(
        """INSERT INTO transactions
           (player_id, season_id, from_team_id, to_team_id, transaction_date, war_at_transaction)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("16630", season_id, None, "무", "2026-04-01", 9.99),
    )
    conn.executemany(
        """INSERT INTO war_daily
           (player_id, season_id, date, bat_war, pit_war, war, war_diff)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("16630", season_id, "2026-03-31", None, 0.10, 0.10, 0.10),
            ("16630", season_id, "2026-04-01", None, 0.20, 0.20, 0.10),
            ("16630", season_id, "2026-04-04", None, 0.50, 0.50, 0.30),
        ],
    )
    conn.commit()

    changed = rebuild_historical_records(conn, season_id)
    transaction_row = conn.execute(
        "SELECT war_at_transaction FROM transactions WHERE player_id = ?",
        ("16630",),
    ).fetchone()
    team_row = conn.execute(
        "SELECT total_war FROM team_war_daily WHERE team_id = ? AND season_id = ? AND date = ?",
        ("무", season_id, "2026-04-04"),
    ).fetchone()
    conn.close()

    assert changed is True
    assert transaction_row == (0.1,)
    assert team_row == (0.4,)


def test_add_transaction_does_not_leave_active_fa_roster_row(tmp_path):
    db_path = tmp_path / "backfill.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    _seed_player_history(conn, season_id)
    conn.execute(
        "INSERT INTO roster (team_id, player_id, season_id, joined_date) VALUES (?, ?, ?, ?)",
        ("퐈", "16630", season_id, "2026-03-01"),
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
    roster_rows = conn.execute(
        """SELECT team_id, joined_date, left_date
           FROM roster
           WHERE player_id = ?
           ORDER BY joined_date""",
        ("16630",),
    ).fetchall()
    conn.close()

    assert roster_rows == [("무", "2026-04-01", None)]


def test_invalid_transaction_save_does_not_backfill_or_write(tmp_path):
    db_path = tmp_path / "backfill.db"
    season_id = ensure_runtime_db(db_path, 2026)

    conn = sqlite3.connect(str(db_path))
    _seed_player_history(conn, season_id)
    conn.execute(
        "INSERT OR IGNORE INTO fantasy_teams (id, name, owner) VALUES (?, ?, ?)",
        ("준", "준팀", "준"),
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
    daily_count = conn.execute("SELECT COUNT(*) FROM daily_records").fetchone()[0]
    team_count = conn.execute("SELECT COUNT(*) FROM team_war_daily").fetchone()[0]
    conn.close()

    assert transaction_count == 0
    assert roster_rows == [("준", "2026-03-28", None)]
    assert daily_count == 0
    assert team_count == 0
