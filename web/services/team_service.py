"""Team service - data loading for team pages.

Uses new Long format schema (roster + war_daily + draft tables).
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from app.core.cache import TTL_MEDIUM, cached_query
from app.services.team_contribution import build_team_contribution_snapshot
from web.utils import get_team_color_scale, get_team_order


def get_team_names(db, season_id: int | None = None) -> List[str]:
    """Get list of team names from database."""
    def _query():
        return get_team_order(season_id)
    cache_key = f"team_names_{season_id}" if season_id is not None else "team_names"
    return cached_query(cache_key, _query, ttl=TTL_MEDIUM, namespace="team")


def _get_team_roster_war(
    db,
    team: str,
    season_id: int | None = None,
    as_of_date: str | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """Get team roster with WAR data from new schema.

    Args:
        db: Database connection
        team: Team ID
        season_id: Optional season ID filter
    """

    if season_id is None:
        return pd.DataFrame(), pd.DataFrame(), []

    date_columns = _get_team_table_dates(db, season_id)
    if not date_columns:
        return pd.DataFrame(), pd.DataFrame(), []
    if as_of_date:
        date_columns = [date for date in date_columns if date <= as_of_date]

    war_rows: Dict[str, Dict[str, Any]] = {}
    diff_rows: Dict[str, Dict[str, Any]] = {}
    for date in date_columns:
        snapshot = build_team_contribution_snapshot(db, team, season_id, date)
        for row in snapshot:
            player_id = row['player_id']
            war_row = war_rows.setdefault(
                player_id,
                {
                    'draft_order': row['draft_order'],
                    'draft_round': row['draft_round'],
                    'ID': player_id,
                    'Name': row['Name'],
                },
            )
            diff_row = diff_rows.setdefault(
                player_id,
                {
                    'draft_order': row['draft_order'],
                    'draft_round': row['draft_round'],
                    'ID': player_id,
                    'Name': row['Name'],
                },
            )
            war_row[date] = row['team_contributed_war_as_of_target_date']
            diff_row[date] = row['team_contributed_war_diff_on_target_date']

    if not war_rows:
        return pd.DataFrame(), pd.DataFrame(), date_columns

    for row in war_rows.values():
        for date in date_columns:
            row.setdefault(date, None)
    for row in diff_rows.values():
        for date in date_columns:
            row.setdefault(date, None)

    return pd.DataFrame(war_rows.values()), pd.DataFrame(diff_rows.values()), date_columns


def _get_team_table_dates(db, season_id: int | None = None) -> List[str]:
    if season_id is not None:
        query = "SELECT DISTINCT date FROM war_daily WHERE season_id = ? ORDER BY date"
        params: tuple[Any, ...] = (season_id,)
    else:
        query = "SELECT DISTINCT date FROM war_daily ORDER BY date"
        params = ()

    df = pd.read_sql_query(query, db, params=params)
    if df.empty:
        return []
    return [date for date in df['date'].dropna().tolist() if date]


def _normalize_team_as_of_date(requested_date: str, date_columns: List[str]) -> str:
    if not date_columns:
        return ''

    if not requested_date:
        return date_columns[-1]

    if requested_date <= date_columns[0]:
        return date_columns[0]

    if requested_date >= date_columns[-1]:
        return date_columns[-1]

    if requested_date in date_columns:
        return requested_date

    earlier_dates = [date for date in date_columns if date <= requested_date]
    return earlier_dates[-1] if earlier_dates else date_columns[0]


def _build_team_contribution_rows(
    db,
    team: str,
    selected_date: str,
    season_id: int | None = None,
) -> List[Dict[str, Any]]:
    if season_id is None:
        return []
    snapshot = build_team_contribution_snapshot(db, team, season_id, selected_date)
    return [
        {
            'player_id': row['player_id'],
            'Name': row['Name'],
            'draft_order': row['draft_order'],
            'index': '' if pd.isna(row['draft_round']) else row['draft_round'],
            'WAR': row['team_contributed_war_as_of_target_date'],
            '변화량': row['team_contributed_war_diff_on_target_date'],
        }
        for row in snapshot
    ]


def get_team_table_data(
    db,
    team: str,
    selected_date: str,
    sort_by: str = 'WAR',
    sort_order: str = 'desc',
    season_id: int | None = None
) -> Tuple[List[Dict], List[str], str]:
    """Get team data for period=1 mode.

    Returns:
        Tuple of (rows, date_columns, selected_date)
    """
    df_war, df_diff, date_columns = _get_team_roster_war(db, team, season_id)
    if df_war.empty:
        return [], [], selected_date

    if not selected_date or selected_date not in date_columns:
        selected_date = date_columns[-1] if date_columns else ''

    df_war, df_diff, _ = _get_team_roster_war(db, team, season_id, as_of_date=selected_date)
    if df_war.empty:
        return [], date_columns, selected_date

    result_df = pd.DataFrame()
    result_df['draft_order'] = df_war['draft_order'].values
    result_df['index'] = df_war['draft_round'].fillna('').values
    result_df['ID'] = df_war['ID'].values
    result_df['Name'] = df_war['Name'].values
    result_df['WAR'] = df_war[selected_date].values if selected_date in df_war.columns else 0
    result_df['변화량'] = df_diff[selected_date].values if selected_date in df_diff.columns else 0
    available_diff_dates = [date for date in date_columns if date in df_diff.columns]
    get_rgba = get_team_color_scale(df_diff[available_diff_dates])

    # Calculate ranks
    war_order = result_df[result_df['WAR'].notnull()].sort_values(
        ['WAR', 'draft_order', 'Name'],
        ascending=[False, True, True],
        kind='mergesort',
    )
    rank_dict = {idx: i+1 for i, idx in enumerate(war_order.index)}
    result_df['rank_sort'] = result_df.index.map(rank_dict)

    # Sort
    ascending = (sort_order == 'asc')
    if sort_by == 'Name':
        result_df = result_df.sort_values('Name', ascending=ascending)
    elif sort_by == 'index':
        result_df = result_df.sort_values(
            ['draft_order', 'Name'],
            ascending=ascending,
            na_position='last',
        )
    elif sort_by in ['WAR', selected_date, '변화량']:
        sort_col = 'WAR' if sort_by in ['WAR', selected_date] else '변화량'
        if sort_col == 'WAR':
            result_df = result_df.sort_values(
                ['WAR', 'rank_sort'],
                ascending=[ascending, True],
                kind='mergesort',
            )
        else:
            result_df = result_df.sort_values(sort_col, ascending=ascending)

    # Build rows
    rows = []
    for idx, row in result_df.iterrows():
        if pd.notnull(row['WAR']):
            rows.append({
                '순위': rank_dict.get(idx, ''),
                'index': row['index'],
                'Name': row['Name'],
                'WAR': f"{row['WAR']:.2f}",
                '변화량': f"{row['변화량']:+.2f}" if pd.notnull(row['변화량']) else "0.00",
                '변화량_색상': get_rgba(row['변화량'])
            })

    return rows, date_columns, selected_date


def get_team_weekly_data(
    db,
    team: str,
    selected_date: str,
    sort_by: str = '',
    sort_order: str = 'desc',
    season_id: int | None = None
) -> Tuple[List[Dict], List[str], List[str]]:
    """Get team data for period=7 mode.

    Returns:
        Tuple of (rows, date_columns, selected_dates)
    """
    available_dates = _get_team_table_dates(db, season_id)
    if not available_dates:
        return [], [], []

    selected_date = _normalize_team_as_of_date(selected_date, available_dates)

    df_war, _, date_columns = _get_team_roster_war(
        db,
        team,
        season_id,
        as_of_date=selected_date,
    )

    if df_war.empty:
        return [], available_dates, []

    # Calculate date range
    if selected_date in date_columns:
        end_idx = date_columns.index(selected_date)
    else:
        end_idx = len(date_columns) - 1
    start_idx = max(0, end_idx - 6)
    selected_dates = date_columns[start_idx:end_idx + 1]

    if not sort_by and selected_dates:
        sort_by = selected_dates[-1]

    # Prepare result
    result_df = df_war[['draft_order', 'draft_round', 'Name'] + selected_dates].copy()
    result_df['index'] = result_df['draft_round'].fillna('')

    # Sort
    ascending = (sort_order == 'asc')
    if sort_by == 'Name':
        result_df = result_df.sort_values('Name', ascending=ascending)
    elif sort_by == 'index':
        result_df = result_df.sort_values(
            ['draft_order', 'Name'],
            ascending=ascending,
            na_position='last',
        )
    elif sort_by in selected_dates:
        result_df = result_df.sort_values(sort_by, ascending=ascending)

    # Calculate ranks (based on last date)
    last_date = selected_dates[-1] if selected_dates else ''
    if last_date:
        war_order = result_df[last_date].sort_values(ascending=False)
        rank_dict = {idx: i+1 for i, idx in enumerate(war_order.index)}
    else:
        rank_dict = {}

    # Build rows
    rows = []
    for idx, row in result_df.iterrows():
        row_dict = {
            '순위': rank_dict.get(idx, ''),
            'index': row['index'],
            'Name': row['Name']
        }
        for date in selected_dates:
            row_dict[date] = f"{row[date]:.2f}" if pd.notnull(row[date]) else ""
        rows.append(row_dict)

    return rows, available_dates, selected_dates


def get_team_graph_data(
    db,
    team: str,
    end_date: str = '',
    period: str = '30',
    season_id: int | None = None
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    """Get team data for graph mode.

    Returns:
        Tuple of (graph_data, date_columns, selected_dates)
    """
    available_dates = _get_team_table_dates(db, season_id)
    if not available_dates:
        return {'dates': [], 'players': [], 'data': []}, [], []

    end_date = _normalize_team_as_of_date(end_date, available_dates)

    df_war, _, date_columns = _get_team_roster_war(
        db,
        team,
        season_id,
        as_of_date=end_date or None,
    )

    if df_war.empty:
        return {'dates': [], 'players': [], 'data': []}, available_dates, []

    # Calculate date range
    end_idx = date_columns.index(end_date) if end_date in date_columns else len(date_columns) - 1
    if period == '7':
        start_idx = max(0, end_idx - 6)
    elif period == '30':
        start_idx = max(0, end_idx - 29)
    else:
        start_idx = 0

    selected_dates = date_columns[start_idx:end_idx + 1]

    # Prepare data
    data_list = []
    for _, row in df_war.iterrows():
        clean_row = []
        for date in selected_dates:
            val = row.get(date)
            clean_row.append(None if pd.isna(val) else val)
        data_list.append(clean_row)

    graph_data = {
        'dates': selected_dates,
        'players': df_war['Name'].tolist(),
        'data': data_list
    }

    return graph_data, available_dates, selected_dates
