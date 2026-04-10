from __future__ import annotations

import sqlite3

from app.services.team_war_daily_writer import write_team_war_daily_for_date
from app.services.war_calculator import calculate_fa_total_as_of


def backfill_transaction_history(
    conn: sqlite3.Connection,
    season_id: int,
    start_date: str,
) -> bool:
    recalculate_transaction_war_baselines(conn, season_id)

    dates = _get_affected_dates(conn, season_id, start_date)
    if not dates:
        return False

    for target_date in dates:
        _rebuild_daily_records_for_date(conn, season_id, target_date)
        _rebuild_team_war_daily_for_date(conn, season_id, target_date)

    return True


def recalculate_transaction_war_baselines(
    conn: sqlite3.Connection,
    season_id: int | None = None,
) -> bool:
    query = """
        SELECT id, player_id, season_id, transaction_date
        FROM transactions
    """
    params: tuple[int, ...] = ()
    if season_id is not None:
        query += " WHERE season_id = ?"
        params = (season_id,)
    query += " ORDER BY season_id, transaction_date, id"

    rows = conn.execute(query, params).fetchall()
    if not rows:
        return False

    for transaction_id, player_id, row_season_id, transaction_date in rows:
        war_row = conn.execute(
            """SELECT war
               FROM war_daily
               WHERE player_id = ?
                 AND season_id = ?
                 AND date < ?
               ORDER BY date DESC, id DESC
               LIMIT 1""",
            (player_id, row_season_id, transaction_date),
        ).fetchone()
        war_at_transaction = float(war_row[0]) if war_row and war_row[0] is not None else 0.0
        conn.execute(
            "UPDATE transactions SET war_at_transaction = ? WHERE id = ?",
            (war_at_transaction, transaction_id),
        )

    return True


def rebuild_historical_records(
    conn: sqlite3.Connection,
    season_id: int | None = None,
) -> bool:
    season_query = "SELECT DISTINCT season_id FROM war_daily"
    season_params: tuple[int, ...] = ()
    if season_id is not None:
        season_query += " WHERE season_id = ?"
        season_params = (season_id,)
    season_query += " ORDER BY season_id"

    season_rows = conn.execute(season_query, season_params).fetchall()
    if not season_rows:
        return False

    changed = False
    for (row_season_id,) in season_rows:
        earliest_row = conn.execute(
            "SELECT MIN(date) FROM war_daily WHERE season_id = ?",
            (row_season_id,),
        ).fetchone()
        earliest_date = earliest_row[0] if earliest_row else None
        if earliest_date:
            changed = backfill_transaction_history(conn, row_season_id, earliest_date) or changed

    return changed


def _get_affected_dates(conn: sqlite3.Connection, season_id: int, start_date: str) -> list[str]:
    rows = conn.execute(
        """SELECT DISTINCT date
           FROM war_daily
           WHERE season_id = ? AND date >= ?
           ORDER BY date""",
        (season_id, start_date),
    ).fetchall()
    return [row[0] for row in rows]


def _rebuild_daily_records_for_date(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
) -> None:
    conn.execute(
        "DELETE FROM daily_records WHERE date = ?",
        (target_date,),
    )

    diffs = conn.execute(
        """SELECT w.player_id, w.war_diff, r.team_id
           FROM war_daily w
           LEFT JOIN roster r
             ON r.player_id = w.player_id
            AND r.season_id = w.season_id
            AND r.joined_date <= ?
            AND (r.left_date IS NULL OR r.left_date > ?)
           WHERE w.date = ?
             AND w.season_id = ?
             AND w.war_diff IS NOT NULL
           ORDER BY w.war_diff DESC, w.player_id""",
        (target_date, target_date, target_date, season_id),
    ).fetchall()

    for player_id, war_diff, team_id in diffs:
        if war_diff > 0:
            conn.execute(
                """INSERT INTO daily_records
                   (date, record_type, team_id, player_id, war_diff)
                   VALUES (?, 'GOAT', ?, ?, ?)""",
                (target_date, team_id, player_id, war_diff),
            )
        elif war_diff < 0:
            conn.execute(
                """INSERT INTO daily_records
                   (date, record_type, team_id, player_id, war_diff)
                   VALUES (?, 'BOAT', ?, ?, ?)""",
                (target_date, team_id, player_id, war_diff),
            )


def _rebuild_team_war_daily_for_date(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
) -> None:
    conn.execute(
        "DELETE FROM team_war_daily WHERE season_id = ? AND date = ?",
        (season_id, target_date),
    )
    write_team_war_daily_for_date(
        conn,
        season_id,
        target_date,
        lambda: calculate_fa_total_as_of(conn, season_id, target_date),
    )


__all__ = [
    "backfill_transaction_history",
    "rebuild_historical_records",
    "recalculate_transaction_war_baselines",
]
