from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any


def build_team_contribution_snapshot(
    conn: sqlite3.Connection,
    team_id: str,
    season_id: int,
    target_date: str,
) -> list[dict[str, Any]]:
    roster_rows = conn.execute(
        """SELECT
               r.player_id,
               p.name,
               r.joined_date,
               r.left_date
            FROM roster r
            JOIN players p ON p.id = r.player_id
            WHERE r.team_id = ?
              AND r.season_id = ?
              AND r.joined_date <= ?
            ORDER BY p.name, r.joined_date, r.id""",
        (team_id, season_id, target_date),
    ).fetchall()
    if not roster_rows:
        return []

    player_ids = sorted({str(row[0]) for row in roster_rows})
    draft_meta = _get_player_draft_meta(conn, season_id, player_ids)
    placeholders = ','.join(['?'] * len(player_ids))
    history_rows = conn.execute(
        f"""SELECT player_id, date, war, war_diff
            FROM war_daily
            WHERE season_id = ?
              AND player_id IN ({placeholders})
              AND date <= ?
            ORDER BY player_id, date""",
        (season_id, *player_ids, target_date),
    ).fetchall()

    player_history: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for player_id, date, war, war_diff in history_rows:
        player_history[str(player_id)].append(
            {'date': date, 'war': war, 'war_diff': war_diff}
        )

    player_stints: dict[str, list[dict[str, Any]]] = defaultdict(list)
    player_meta: dict[str, dict[str, Any]] = {}
    for player_id, name, joined_date, left_date in roster_rows:
        player_id_str = str(player_id)
        player_stints[player_id_str].append(
            {
                'joined_date': joined_date,
                'left_date': left_date,
            }
        )
        draft_order, draft_round = draft_meta.get(player_id_str, (None, None))
        meta = player_meta.setdefault(
            player_id_str,
            {
                'player_id': player_id_str,
                'name': name,
                'draft_order': draft_order,
                'draft_round': draft_round,
            },
        )
        if meta['draft_order'] is None and draft_order is not None:
            meta['draft_order'] = draft_order
        if meta['draft_round'] is None and draft_round is not None:
            meta['draft_round'] = draft_round

    previous_snapshot_date = get_latest_prior_war_snapshot_date(conn, season_id, target_date)
    snapshot: list[dict[str, Any]] = []
    for player_id in player_ids:
        history = player_history.get(player_id, [])
        stints = player_stints[player_id]
        raw_player_daily_war_as_of_target_date = round(
            get_player_raw_daily_war_as_of_date(history, target_date),
            2,
        )
        raw_player_daily_war_diff_on_target_date = round(
            get_player_raw_daily_war_diff_on_date(history, target_date),
            2,
        )
        team_contributed_war_as_of_target_date = calculate_team_contributed_war_as_of_date(
            conn,
            player_id,
            team_id,
            season_id,
            target_date,
            player_history=history,
            player_stints=stints,
        )
        team_contributed_war_diff_on_target_date = calculate_team_contributed_war_diff_on_date(
            conn,
            player_id,
            team_id,
            season_id,
            target_date,
            player_history=history,
            player_stints=stints,
            previous_snapshot_date=previous_snapshot_date,
        )

        meta = player_meta[player_id]
        team_contributed_war_as_of_target_date = round(team_contributed_war_as_of_target_date, 2)
        snapshot.append(
            {
                'player_id': player_id,
                'Name': meta['name'],
                'draft_order': meta['draft_order'],
                'draft_round': meta['draft_round'] or '',
                'raw_player_daily_war_as_of_target_date': raw_player_daily_war_as_of_target_date,
                'raw_player_daily_war_diff_on_target_date': raw_player_daily_war_diff_on_target_date,
                'team_contributed_war_as_of_target_date': team_contributed_war_as_of_target_date,
                'team_contributed_war_diff_on_target_date': team_contributed_war_diff_on_target_date,
                'WAR': team_contributed_war_as_of_target_date,
                '변화량': team_contributed_war_diff_on_target_date,
            }
        )

    return snapshot


def _get_player_draft_meta(
    conn: sqlite3.Connection,
    season_id: int,
    player_ids: list[str],
) -> dict[str, tuple[int | None, str | None]]:
    if not player_ids:
        return {}

    placeholders = ','.join(['?'] * len(player_ids))
    rows = conn.execute(
        f"""SELECT player_id, pick_order, round
            FROM draft
            WHERE season_id = ?
              AND player_id IN ({placeholders})
            ORDER BY CASE WHEN pick_order IS NULL THEN 1 ELSE 0 END,
                     pick_order,
                     application_date,
                     id""",
        (season_id, *player_ids),
    ).fetchall()

    meta: dict[str, tuple[int | None, str | None]] = {}
    for player_id, pick_order, round_name in rows:
        player_id_str = str(player_id)
        if player_id_str not in meta:
            meta[player_id_str] = (pick_order, round_name)

    return meta


def list_regular_team_ids(
    conn: sqlite3.Connection,
) -> list[str]:
    query = "SELECT id FROM fantasy_teams WHERE id != '퐈' ORDER BY id"
    return [row[0] for row in conn.execute(query).fetchall()]


def calculate_team_total_contribution(
    conn: sqlite3.Connection,
    team_id: str,
    season_id: int,
    target_date: str,
) -> float:
    snapshot = build_team_contribution_snapshot(conn, team_id, season_id, target_date)
    return round(sum(float(row['team_contributed_war_as_of_target_date']) for row in snapshot), 2)


def calculate_team_contributed_war_as_of_date(
    conn: sqlite3.Connection,
    player_id: str,
    team_id: str,
    season_id: int,
    target_date: str,
    player_history: list[dict[str, Any]] | None = None,
    player_stints: list[dict[str, Any]] | None = None,
) -> float:
    history = player_history or _load_player_history(conn, player_id, season_id, target_date)
    stints = player_stints or _load_player_stints(conn, player_id, team_id, season_id, target_date)
    team_contributed_war_as_of_target_date = 0.0

    for stint in stints:
        joined_date = stint['joined_date']
        left_date = stint['left_date']

        if joined_date > target_date:
            continue

        join_war = get_transaction_war(conn, player_id, season_id, joined_date, team_id, 'in')
        if join_war is None:
            join_war = get_player_raw_daily_war_before_date(history, joined_date)

        if left_date and left_date <= target_date:
            leave_war = get_transaction_war(
                conn,
                player_id,
                season_id,
                left_date,
                team_id,
                'out',
            )
            if leave_war is None:
                leave_war = get_player_raw_daily_war_before_date(history, left_date)
            team_contributed_war_as_of_target_date += leave_war - join_war
            continue

        raw_player_daily_war_as_of_date = get_player_raw_daily_war_as_of_date(history, target_date)
        team_contributed_war_as_of_target_date += raw_player_daily_war_as_of_date - join_war

    return round(team_contributed_war_as_of_target_date, 2)


def calculate_team_contributed_war_diff_on_date(
    conn: sqlite3.Connection,
    player_id: str,
    team_id: str,
    season_id: int,
    target_date: str,
    player_history: list[dict[str, Any]] | None = None,
    player_stints: list[dict[str, Any]] | None = None,
    previous_snapshot_date: str | None = None,
) -> float:
    current_contribution = calculate_team_contributed_war_as_of_date(
        conn,
        player_id,
        team_id,
        season_id,
        target_date,
        player_history=player_history,
        player_stints=player_stints,
    )

    if previous_snapshot_date is None:
        previous_snapshot_date = get_latest_prior_war_snapshot_date(conn, season_id, target_date)

    if previous_snapshot_date is None:
        return round(current_contribution, 2)

    previous_contribution = calculate_team_contributed_war_as_of_date(
        conn,
        player_id,
        team_id,
        season_id,
        previous_snapshot_date,
        player_history=player_history,
        player_stints=player_stints,
    )
    return round(current_contribution - previous_contribution, 2)


def get_player_raw_daily_war_as_of_date(
    player_history: list[dict[str, Any]],
    target_date: str,
) -> float:
    return lookup_latest_war_on_or_before(player_history, target_date)


def get_player_raw_daily_war_before_date(
    player_history: list[dict[str, Any]],
    target_date: str,
) -> float:
    return lookup_latest_war_before(player_history, target_date)


def get_player_raw_daily_war_diff_on_date(
    player_history: list[dict[str, Any]],
    target_date: str,
) -> float:
    return lookup_war_diff_on_date(player_history, target_date)


def get_latest_prior_war_snapshot_date(
    conn: sqlite3.Connection,
    season_id: int,
    target_date: str,
) -> str | None:
    row = conn.execute(
        "SELECT MAX(date) FROM war_daily WHERE season_id = ? AND date < ?",
        (season_id, target_date),
    ).fetchone()
    return row[0] if row and row[0] else None


def _load_player_history(
    conn: sqlite3.Connection,
    player_id: str,
    season_id: int,
    target_date: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT date, war, war_diff
           FROM war_daily
           WHERE player_id = ?
             AND season_id = ?
             AND date <= ?
           ORDER BY date""",
        (player_id, season_id, target_date),
    ).fetchall()
    return [
        {'date': date, 'war': war, 'war_diff': war_diff}
        for date, war, war_diff in rows
    ]


def _load_player_stints(
    conn: sqlite3.Connection,
    player_id: str,
    team_id: str,
    season_id: int,
    target_date: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT joined_date, left_date
           FROM roster
           WHERE player_id = ?
             AND team_id = ?
             AND season_id = ?
             AND joined_date <= ?
           ORDER BY joined_date, id""",
        (player_id, team_id, season_id, target_date),
    ).fetchall()
    return [
        {'joined_date': joined_date, 'left_date': left_date}
        for joined_date, left_date in rows
    ]


def lookup_latest_war_before(player_history: list[dict[str, Any]], cutoff_date: str) -> float:
    latest_war = 0.0
    for history in player_history:
        if history['date'] >= cutoff_date:
            break
        latest_war = float(history['war'])
    return latest_war


def lookup_latest_war_on_or_before(
    player_history: list[dict[str, Any]],
    cutoff_date: str,
) -> float:
    latest_war = 0.0
    for history in player_history:
        if history['date'] > cutoff_date:
            break
        latest_war = float(history['war'])
    return latest_war


def lookup_war_diff_on_date(player_history: list[dict[str, Any]], target_date: str) -> float:
    for history in player_history:
        if history['date'] == target_date:
            return float(history['war_diff']) if history['war_diff'] is not None else 0.0
    return 0.0


def get_transaction_war(
    conn: sqlite3.Connection,
    player_id: str,
    season_id: int | None,
    transaction_date: str,
    team_id: str,
    direction: str,
) -> float | None:
    clauses = ["player_id = ?", "transaction_date = ?"]
    params: list[Any] = [player_id, transaction_date]

    if season_id is not None:
        clauses.append("season_id = ?")
        params.append(season_id)

    if direction == 'in':
        clauses.append("to_team_id = ?")
    else:
        clauses.append("from_team_id = ?")
    params.append(team_id)

    row = conn.execute(
        f"""SELECT id, war_at_transaction
            FROM transactions
            WHERE {' AND '.join(clauses)}
            ORDER BY id DESC
            LIMIT 1""",
        tuple(params),
    ).fetchone()
    if row is None:
        return None

    derived_row = conn.execute(
        """SELECT war
           FROM war_daily
           WHERE player_id = ?
             AND (? IS NULL OR season_id = ?)
             AND date < ?
           ORDER BY date DESC, id DESC
           LIMIT 1""",
        (player_id, season_id, season_id, transaction_date),
    ).fetchone()
    if derived_row and derived_row[0] is not None:
        return float(derived_row[0])

    return 0.0


__all__ = [
    'build_team_contribution_snapshot',
    'calculate_team_contributed_war_as_of_date',
    'calculate_team_contributed_war_diff_on_date',
    'calculate_team_total_contribution',
    'get_latest_prior_war_snapshot_date',
    'get_player_raw_daily_war_as_of_date',
    'get_player_raw_daily_war_before_date',
    'get_player_raw_daily_war_diff_on_date',
    'list_regular_team_ids',
    'get_transaction_war',
    'lookup_latest_war_before',
    'lookup_latest_war_on_or_before',
    'lookup_war_diff_on_date',
]
