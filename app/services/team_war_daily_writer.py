from __future__ import annotations

import sqlite3
from collections.abc import Callable

from app.services.team_contribution import calculate_team_total_contribution, list_regular_team_ids


def get_previous_team_totals(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
) -> dict[str, float]:
    previous_date_row = conn.execute(
        """SELECT MAX(date)
           FROM team_war_daily
           WHERE season_id = ? AND date < ?""",
        (season_id, target_date),
    ).fetchone()
    previous_date = previous_date_row[0] if previous_date_row else None
    if previous_date is None:
        return {}

    rows = conn.execute(
        "SELECT team_id, total_war FROM team_war_daily WHERE season_id = ? AND date = ?",
        (season_id, previous_date),
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def assign_competition_ranks(
    team_wars: list[tuple[str, float, float | None]],
) -> list[tuple[int, str, float, float | None]]:
    ranked_teams = sorted(team_wars, key=lambda item: (-item[1], item[0]))
    ranked_rows: list[tuple[int, str, float, float | None]] = []
    last_total: float | None = None
    last_rank = 0

    for index, (team_id, total_war, war_diff) in enumerate(ranked_teams, start=1):
        if last_total is None or total_war != last_total:
            last_rank = index
            last_total = total_war
        ranked_rows.append((last_rank, team_id, total_war, war_diff))

    return ranked_rows


def build_regular_team_rows(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
    previous_totals: dict[str, float],
) -> list[tuple[str, float, float | None]]:
    rows: list[tuple[str, float, float | None]] = []
    for team_id in list_regular_team_ids(conn):
        total_war = round(calculate_team_total_contribution(conn, team_id, season_id, target_date), 2)
        previous_total = previous_totals.get(team_id)
        if previous_total is not None:
            war_diff = round(total_war - previous_total, 2)
        else:
            war_diff = round(total_war, 2)
        rows.append((team_id, total_war, war_diff))
    return rows


def build_fa_team_row(
    previous_totals: dict[str, float],
    fa_total: float,
) -> tuple[str, float, float | None, None]:
    previous_total = previous_totals.get('퐈')
    fa_diff = round(fa_total - previous_total, 2) if previous_total is not None else None
    return ('퐈', fa_total, fa_diff, None)


def write_team_war_daily_for_date(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
    fa_total_provider: Callable[[], float],
) -> None:
    previous_totals = get_previous_team_totals(conn, season_id, target_date)
    regular_rows = build_regular_team_rows(conn, season_id, target_date, previous_totals)
    fa_row = build_fa_team_row(previous_totals, round(float(fa_total_provider()), 2))

    for rank, team_id, total_war, war_diff in assign_competition_ranks(regular_rows):
        conn.execute(
            """INSERT OR REPLACE INTO team_war_daily
               (team_id, season_id, date, total_war, war_diff, rank)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (team_id, season_id, target_date, total_war, war_diff, rank),
        )

    conn.execute(
        """INSERT OR REPLACE INTO team_war_daily
           (team_id, season_id, date, total_war, war_diff, rank)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (fa_row[0], season_id, target_date, fa_row[1], fa_row[2], fa_row[3]),
    )


__all__ = [
    'assign_competition_ranks',
    'build_fa_team_row',
    'build_regular_team_rows',
    'get_previous_team_totals',
    'write_team_war_daily_for_date',
]
