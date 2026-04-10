"""League service - data loading for league pages.

Uses new Long format schema (team_war_daily table).
"""

from __future__ import annotations

from typing import Dict, List, Any, Tuple

import numpy as np
import pandas as pd

from web.utils import get_db, get_team_order, get_color_scale
from app.core.cache import cache, TTL_MEDIUM


def _get_team_war_data(db, season_id: int | None = None) -> pd.DataFrame:
    """Load team WAR data from team_war_daily table.
    
    Args:
        db: Database connection
        season_id: Optional season ID filter (None = all seasons)
    """
    if season_id:
        query = """
            SELECT team_id, date, total_war, war_diff, rank
            FROM team_war_daily
            WHERE season_id = ?
            ORDER BY date, rank, team_id
        """
        return pd.read_sql_query(query, db, params=(season_id,))

    query = """
        SELECT team_id, date, total_war, war_diff, rank
        FROM team_war_daily
        ORDER BY date, rank, team_id
    """
    return pd.read_sql_query(query, db)


def _pivot_team_war(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    """Convert long format to wide format for display compatibility."""
    if df.empty:
        return pd.DataFrame(), pd.DataFrame(), []
    
    # Pivot WAR values
    df_war = df.pivot(index='team_id', columns='date', values='total_war')
    
    # Pivot diff values
    df_diff = df.pivot(index='team_id', columns='date', values='war_diff')
    
    # Get date columns sorted
    date_columns = sorted(df_war.columns.tolist())
    
    return df_war, df_diff, date_columns


def _normalize_league_date(requested_date: str, date_columns: List[str]) -> str:
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


def get_league_table_data(
    db,
    selected_date: str,
    sort_by: str = 'WAR',
    sort_order: str = 'desc',
    season_id: int | None = None
) -> Tuple[List[Dict], List[str], str]:
    """Get league table data for period=1 mode.
    
    Args:
        db: Database connection
        selected_date: Selected date for display
        sort_by: Column to sort by
        sort_order: 'asc' or 'desc'
        season_id: Season ID filter
    
    Returns:
        Tuple of (rows, date_columns, selected_date)
    """
    df_raw = _get_team_war_data(db, season_id)
    df_teams, df_diff, date_columns = _pivot_team_war(df_raw)
    
    if df_teams.empty:
        return [], [], selected_date
    
    selected_date = _normalize_league_date(selected_date, date_columns)
    
    if not date_columns:
        return [], [], selected_date
    
    # Prepare result DataFrame
    result_df = pd.DataFrame()
    result_df['WAR'] = df_teams[selected_date]
    result_df['변화량'] = df_diff[selected_date]
    
    # Sort
    ascending = (sort_order == 'asc')
    if sort_by in ['WAR', '변화량']:
        result_df = result_df.sort_values(sort_by, ascending=ascending)
    
    # Calculate color scale
    get_rgba = get_color_scale(df_diff[selected_date].values, df_all_diff=df_diff)
    
    rank_rows = df_raw[df_raw['date'] == selected_date][['team_id', 'rank']].drop_duplicates('team_id')
    rank_dict = {
        row['team_id']: '' if pd.isna(row['rank']) else int(row['rank'])
        for _, row in rank_rows.iterrows()
        if row['team_id'] != '퐈'
    }
    
    # Calculate max WAR for bar width
    war_values = [float(row['WAR']) for _, row in result_df.iterrows() if float(row['WAR']) > 0]
    max_war = max(war_values) if war_values else 1.0
    
    # Team order
    team_order = get_team_order(season_id)
    team_order_dict = {team: i for i, team in enumerate(team_order)}
    
    # Build rows
    rows = []
    for idx, row in result_df.iterrows():
        if pd.notnull(row['WAR']):
            war_value = float(row['WAR'])
            rows.append({
                '순위': rank_dict.get(idx, ''),
                '팀': idx,
                'WAR': f"{row['WAR']:.2f}",
                '변화량': f"{row['변화량']:+.2f}" if pd.notnull(row['변화량']) else "0.00",
                '변화량_색상': get_rgba(row['변화량']),
                'WAR_비율': (war_value / max_war * 80) if war_value > 0 else 0
            })
    
    # Sort by team if requested
    if sort_by == '팀':
        if sort_order == 'asc':
            rows.sort(key=lambda x: team_order_dict.get(x['팀'], len(team_order)))
        else:
            rows.sort(key=lambda x: team_order_dict.get(x['팀'], len(team_order)), reverse=True)
    
    return rows, date_columns, selected_date


def get_league_weekly_data(
    db,
    selected_date: str,
    sort_by: str = '',
    sort_order: str = 'desc',
    season_id: int | None = None
) -> Tuple[List[Dict], List[str], List[str]]:
    """Get league data for period=7 mode.
    
    Args:
        db: Database connection
        selected_date: Selected end date
        sort_by: Column to sort by
        sort_order: 'asc' or 'desc'
        season_id: Season ID filter
    
    Returns:
        Tuple of (rows, date_columns, selected_dates)
    """
    df_raw = _get_team_war_data(db, season_id)
    df_teams, _, date_columns = _pivot_team_war(df_raw)
    
    if df_teams.empty:
        return [], [], []
    
    if not selected_date or selected_date not in date_columns:
        selected_date = date_columns[-1] if date_columns else ''
    
    # Calculate date range (7 days ending at selected_date)
    end_idx = date_columns.index(selected_date) if selected_date in date_columns else len(date_columns) - 1
    start_idx = max(0, end_idx - 6)
    selected_dates = date_columns[start_idx:end_idx + 1]
    
    if not sort_by and selected_dates:
        sort_by = selected_dates[-1]
    
    # Prepare result (exclude FA)
    result_df = df_teams[selected_dates].copy()
    result_df = result_df.loc[result_df.index != '퐈']
    
    # Team order
    team_order = get_team_order(season_id)
    team_order_dict = {team: i for i, team in enumerate(team_order)}
    
    # Sort
    ascending = (sort_order == 'asc')
    if sort_by == '팀':
        result_df = result_df.sort_index(
            key=lambda x: x.map(lambda t: team_order_dict.get(t, len(team_order))),
            ascending=ascending
        )
    elif sort_by in selected_dates:
        result_df = result_df.sort_values(sort_by, ascending=ascending)
    
    last_date = selected_dates[-1] if selected_dates else ''
    rank_rows = df_raw[df_raw['date'] == last_date][['team_id', 'rank']].drop_duplicates('team_id')
    rank_dict = {
        row['team_id']: '' if pd.isna(row['rank']) else int(row['rank'])
        for _, row in rank_rows.iterrows()
    } if last_date else {}
    
    # Build rows
    rows = []
    for idx, row in result_df.iterrows():
        row_dict = {
            '순위': rank_dict.get(idx, ''),
            '팀': idx
        }
        for date in selected_dates:
            row_dict[date] = f"{row[date]:.2f}" if pd.notnull(row[date]) else ""
        rows.append(row_dict)
    
    return rows, date_columns, selected_dates


def get_league_graph_data(
    db,
    end_date: str = '',
    period: str = '30',
    season_id: int | None = None
) -> Tuple[Dict[str, Any], List[str], List[str]]:
    """Get league data for graph mode.
    
    Args:
        db: Database connection
        end_date: End date for graph
        period: '7', '30', or 'all'
        season_id: Season ID filter
    
    Returns:
        Tuple of (graph_data, date_columns, selected_dates)
    """
    df_raw = _get_team_war_data(db, season_id)
    df_teams, _, date_columns = _pivot_team_war(df_raw)
    
    if df_teams.empty:
        return {'dates': [], 'teams': [], 'data': []}, [], []
    
    # Exclude FA
    df_teams = df_teams.loc[df_teams.index != '퐈']
    
    end_date = _normalize_league_date(end_date, date_columns)
    
    # Calculate date range
    end_idx = date_columns.index(end_date) if end_date in date_columns else len(date_columns) - 1
    if period == '7':
        start_idx = max(0, end_idx - 6)
    elif period == '30':
        start_idx = max(0, end_idx - 29)
    else:  # 'all'
        start_idx = 0
    
    selected_dates = date_columns[start_idx:end_idx + 1]
    
    # Prepare data for chart (NaN -> None)
    data_list = []
    for row in df_teams[selected_dates].values:
        clean_row = [None if pd.isna(val) else val for val in row]
        data_list.append(clean_row)
    
    graph_data = {
        'dates': selected_dates,
        'teams': df_teams.index.tolist(),
        'data': data_list
    }
    
    return graph_data, date_columns, selected_dates
