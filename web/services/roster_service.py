"""Roster service - data loading for roster pages.

Uses new Long format schema (draft + roster + transactions tables).
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize

from web.utils import get_team_order


def get_draft_slots_from_db(db, season_id: int | None = None) -> List[str]:
    """Get ordered list of draft slots from database."""
    cursor = db.cursor()
    if season_id is None:
        cursor.execute(
            """
            SELECT round FROM draft
            GROUP BY round
            ORDER BY MIN(pick_order)
            """
        )
    else:
        cursor.execute(
            """
            SELECT round FROM draft
            WHERE season_id = ?
            GROUP BY round
            ORDER BY MIN(pick_order)
            """,
            (season_id,),
        )
    rows = cursor.fetchall()
    return [r[0] for r in rows] if rows else []


def get_draft_data(db, season_id: int | None = None) -> Tuple[List[Dict], List[str]]:
    """Get draft roster data.

    Returns:
        Tuple of (rows, column_order)
    """
    team_order = get_team_order(season_id)

    # Get draft data from new schema
    date_subquery = "SELECT MAX(date) FROM war_daily"
    params: tuple[Any, ...] = ()
    if season_id is not None:
        date_subquery = "SELECT MAX(date) FROM war_daily WHERE season_id = ?"
        params = (season_id, season_id, season_id)

    season_filter = "WHERE d.season_id = ?" if season_id is not None else ""

    query = f"""
        SELECT
            d.team_id,
            d.round,
            p.name as player_name,
            w.war
        FROM draft d
        JOIN players p ON d.player_id = p.id
        LEFT JOIN (
            SELECT player_id, war
            FROM war_daily
            WHERE date = ({date_subquery})
            {"AND season_id = ?" if season_id is not None else ""}
        ) w ON w.player_id = d.player_id
        {season_filter}
        ORDER BY d.team_id, d.pick_order
    """

    df = pd.read_sql_query(query, db, params=params)

    if df.empty:
        return [], []

    # Pivot to wide format
    df_names = df.pivot(index='team_id', columns='round', values='player_name')
    df_war = df.pivot(index='team_id', columns='round', values='war')

    # Get dynamic column order from database
    column_order = get_draft_slots_from_db(db, season_id)

    if not column_order:
        column_order = ['용투1', '용투2', '용타', '아쿼'] + [f'{i}R' for i in range(1, 26)]

    # Ensure columns exist
    for col in column_order:
        if col not in df_names.columns:
            df_names[col] = None
            df_war[col] = None

    df_names = df_names[column_order]
    df_war = df_war[column_order]

    # Z-score calculation for colors
    norm = Normalize(vmin=-3, vmax=3)
    sm = ScalarMappable(cmap='coolwarm', norm=norm)

    z_scores = pd.DataFrame(index=df_war.index, columns=df_war.columns)
    for col in df_war.columns:
        values = pd.to_numeric(df_war[col], errors='coerce')
        values = values.where(df_names[col].notna(), other=np.nan)
        values = values.fillna(0.0).values
        mean = np.nanmean(values)
        std = np.nanstd(values)
        z_scores[col] = (values - mean) / std if std != 0.0 else np.zeros_like(values)

    # Build rows (in team order)
    rows = []
    for team in team_order:
        if team in df_names.index:
            row_dict: Dict[str, Any] = {'팀': team}
            for col in column_order:
                player_name = df_names.loc[team, col] if pd.notnull(df_names.loc[team, col]) else ''
                war_value = df_war.loc[team, col] if team in df_war.index else None

                try:
                    if player_name:
                        if war_value is not None and not pd.isna(war_value):
                            war_numeric = float(war_value)
                        else:
                            war_numeric = 0.0
                        war_str = f"{war_numeric:.2f}"
                        z_score = z_scores.loc[team, col]
                        rgb = sm.to_rgba(z_score, bytes=True)[:3]
                        bg_color = f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"
                    else:
                        war_str = ''
                        bg_color = "rgb(255, 255, 255)"
                except (ValueError, TypeError):
                    war_str = ''
                    bg_color = "rgb(255, 255, 255)"

                row_dict[col] = {
                    'name': player_name,
                    'war': war_str,
                    'bg_color': bg_color
                }
            rows.append(row_dict)

    return rows, column_order


def get_transaction_data(
    db,
    season_id: int | None = None,
    season_year: int | None = None,
) -> List[Dict]:
    """Get transaction data.

    Returns:
        List of transaction rows
    """
    season_date_filter = ""
    params: list[Any] = []

    if season_id is not None:
        season_date_filter = "WHERE t.season_id = ?"
        params.append(season_id)
    elif season_year is not None:
        season_date_filter = "WHERE t.transaction_date LIKE ?"
        params.append(f"{season_year}-%")

    query = f"""
        SELECT
            t.id as rowid,
            t.transaction_date as 날짜,
            p.name as 선수명,
            t.player_id as 선수ID,
            t.from_team_id as 이전팀,
            t.to_team_id as 새팀,
            COALESCE(w.war, 0.0) as WAR
        FROM transactions t
        JOIN players p ON t.player_id = p.id
        LEFT JOIN war_daily w ON w.player_id = t.player_id
            AND w.date = (
                SELECT MAX(date) FROM war_daily
                WHERE player_id = t.player_id
                AND date < REPLACE(t.transaction_date, '/', '-')
                {"AND season_id = ?" if season_id is not None else ""}
            )
        {season_date_filter}
        ORDER BY t.transaction_date ASC
    """

    if season_id is not None:
        params.insert(0, season_id)

    df = pd.read_sql_query(query, db, params=params)

    rows = []
    for _, row in df.iterrows():
        from_team = row['이전팀'] if row['이전팀'] else '퐈'
        to_team = row['새팀'] if row['새팀'] else '퐈'
        변동내용 = f"팀 {from_team} → 팀 {to_team}"

        # Convert date format if needed
        date_str = row['날짜']
        if '-' in str(date_str):
            date_str = date_str.replace('-', '/')

        rows.append({
            'rowid': row['rowid'],
            '날짜': date_str,
            '선수명': row['선수명'],
            '선수ID': row['선수ID'],
            '변동내용': 변동내용,
            'WAR': f"{row['WAR']:.2f}" if pd.notnull(row['WAR']) else "-"
        })

    return rows


def get_roster_seed(db, player_id: str, season_id: int) -> tuple[str, str] | None:
    draft_row = db.execute(
        """SELECT team_id, application_date
           FROM draft
           WHERE player_id = ? AND season_id = ?
           ORDER BY application_date, pick_order, id
           LIMIT 1""",
        (player_id, season_id),
    ).fetchone()
    if draft_row and draft_row['application_date']:
        return draft_row['team_id'], draft_row['application_date']

    roster_row = db.execute(
        """SELECT team_id, joined_date
           FROM roster
           WHERE player_id = ? AND season_id = ?
           ORDER BY joined_date, id
           LIMIT 1""",
        (player_id, season_id),
    ).fetchone()
    earliest_transaction = db.execute(
        """SELECT transaction_date
           FROM transactions
           WHERE player_id = ? AND season_id = ?
           ORDER BY transaction_date, id
           LIMIT 1""",
        (player_id, season_id),
    ).fetchone()
    if roster_row and roster_row['team_id'] != '퐈' and (
        earliest_transaction is None
        or roster_row['joined_date'] < earliest_transaction['transaction_date']
    ):
        return roster_row['team_id'], roster_row['joined_date']

    return None


def validate_transaction_save(
    db,
    *,
    player_id: str,
    season_id: int,
    transaction_date: str,
    from_team_id: str | None,
    to_team_id: str | None,
    transaction_id: int | None = None,
    previous_player_id: str | None = None,
) -> str | None:
    if from_team_id == to_team_id:
        return "같은 팀으로의 로스터 변동은 저장할 수 없습니다."

    if db.execute("SELECT 1 FROM players WHERE id = ?", (player_id,)).fetchone() is None:
        return "해당 선수를 찾을 수 없습니다."

    for team_id in (from_team_id, to_team_id):
        if team_id is None:
            continue
        team_exists = db.execute(
            "SELECT 1 FROM fantasy_teams WHERE id = ?",
            (team_id,),
        ).fetchone()
        if team_exists is None:
            return "해당 팀을 찾을 수 없습니다."

    same_day_row = db.execute(
        """SELECT 1
           FROM transactions
           WHERE player_id = ? AND season_id = ? AND transaction_date = ?
             AND (? IS NULL OR id != ?)
           LIMIT 1""",
        (player_id, season_id, transaction_date, transaction_id, transaction_id),
    ).fetchone()
    if same_day_row is not None:
        return "같은 날짜에 같은 선수의 로스터 변동은 1건만 저장할 수 있습니다."

    interval_error = _validate_existing_roster_intervals(db, player_id, season_id)
    if interval_error:
        return interval_error

    candidate_error = _validate_player_timeline(
        db,
        player_id=player_id,
        season_id=season_id,
        transaction_date=transaction_date,
        from_team_id=from_team_id,
        to_team_id=to_team_id,
        transaction_id=transaction_id,
        include_candidate=True,
    )
    if candidate_error:
        return candidate_error

    if previous_player_id and previous_player_id != player_id:
        previous_interval_error = _validate_existing_roster_intervals(
            db,
            previous_player_id,
            season_id,
        )
        if previous_interval_error:
            return previous_interval_error

        old_player_error = _validate_player_timeline(
            db,
            player_id=previous_player_id,
            season_id=season_id,
            transaction_date=transaction_date,
            from_team_id=from_team_id,
            to_team_id=to_team_id,
            transaction_id=transaction_id,
            include_candidate=False,
        )
        if old_player_error:
            return old_player_error

    return None


def _validate_player_timeline(
    db,
    *,
    player_id: str,
    season_id: int,
    transaction_date: str,
    from_team_id: str | None,
    to_team_id: str | None,
    transaction_id: int | None,
    include_candidate: bool,
) -> str | None:
    seed = get_roster_seed(db, player_id, season_id)
    current_team = seed[0] if seed else None

    rows = db.execute(
        """SELECT id, transaction_date, from_team_id, to_team_id
           FROM transactions
           WHERE player_id = ? AND season_id = ?
             AND (? IS NULL OR id != ?)
           ORDER BY transaction_date, id""",
        (player_id, season_id, transaction_id, transaction_id),
    ).fetchall()

    timeline = [
        {
            'id': row['id'],
            'transaction_date': row['transaction_date'],
            'from_team_id': row['from_team_id'],
            'to_team_id': row['to_team_id'],
        }
        for row in rows
    ]

    if include_candidate:
        timeline.append(
            {
                'id': transaction_id if transaction_id is not None else 10**18,
                'transaction_date': transaction_date,
                'from_team_id': from_team_id,
                'to_team_id': to_team_id,
            }
        )

    timeline.sort(key=lambda item: (item['transaction_date'], item['id']))

    active_joined_date = seed[1] if seed else None

    for item in timeline:
        expected_from = item['from_team_id']
        next_team = item['to_team_id']

        if expected_from != current_team:
            current_label = f"팀 {current_team}" if current_team else "팀 퐈"
            expected_label = f"팀 {expected_from}" if expected_from else "팀 퐈"
            return (
                f"유효하지 않은 로스터 변동입니다. {item['transaction_date']} 시점 선수 소속은 "
                f"{current_label}인데 {expected_label}에서 이동시키려 했습니다."
            )

        if expected_from and active_joined_date is not None and item['transaction_date'] <= active_joined_date:
            return (
                f"유효하지 않은 로스터 변동입니다. {item['transaction_date']}에 종료되는 소속 기간이 "
                f"시작일 {active_joined_date}보다 빠르거나 같습니다."
            )

        current_team = next_team
        active_joined_date = item['transaction_date'] if next_team else None

    return None


def _validate_existing_roster_intervals(db, player_id: str, season_id: int) -> str | None:
    rows = db.execute(
        """SELECT team_id, joined_date, left_date
           FROM roster
           WHERE player_id = ? AND season_id = ?
           ORDER BY joined_date, COALESCE(left_date, '9999-12-31'), id""",
        (player_id, season_id),
    ).fetchall()

    previous_left_date = None
    active_count = 0
    for row in rows:
        joined_date = row['joined_date']
        left_date = row['left_date']

        if left_date is None:
            active_count += 1
        elif left_date <= joined_date:
            return (
                f"유효하지 않은 기존 로스터 이력이 있습니다. {joined_date}에 시작한 소속 기간이 "
                f"{left_date}에 종료될 수 없습니다."
            )

        if previous_left_date is not None and previous_left_date > joined_date:
            return (
                f"유효하지 않은 기존 로스터 이력이 있습니다. {joined_date}부터 시작하는 소속 기간이 "
                "이전 소속 기간과 겹칩니다."
            )

        previous_left_date = left_date or '9999-12-31'

    if active_count > 1:
        return "유효하지 않은 기존 로스터 이력이 있습니다. 현재 소속이 2개 이상입니다."

    return None
