"""Player service - data loading for player pages.

Uses new Long format schema (players + war_daily + roster tables).
"""

from __future__ import annotations

from typing import Dict, List, Any, Tuple, Set

import pandas as pd

from web.utils import get_db, get_team_order
from app.core.cache import cached_query, TTL_SHORT, TTL_MEDIUM


def get_player_data(
    db,
    selected_types: List[str],
    selected_teams: List[str],
    selected_date: str,
    search_query: str,
    sort_by: str,
    sort_order: str,
    season_id: int | None = None
) -> Tuple[List[Dict], List[str], List[str], Dict[str, str]]:
    """Get player data with filters and sorting.
    
    Returns:
        Tuple of (rows, display_columns, date_columns, column_names)
    """
    team_order = get_team_order(season_id)
    team_order_with_fa = team_order + ['퐈']
    team_order_dict = {team: i for i, team in enumerate(team_order_with_fa)}
    
    # Get date columns from war_daily
    def _get_dates():
        if season_id is None:
            df = pd.read_sql_query("SELECT DISTINCT date FROM war_daily ORDER BY date", db)
        else:
            df = pd.read_sql_query(
                "SELECT DISTINCT date FROM war_daily WHERE season_id = ? ORDER BY date",
                db,
                params=(season_id,),
            )
        return df['date'].tolist()

    cache_key = f"war_dates_{season_id}" if season_id is not None else "war_dates"
    date_columns = cached_query(cache_key, _get_dates, ttl=TTL_MEDIUM, namespace="player")
    
    if not selected_date and date_columns:
        selected_date = date_columns[-1]
    
    # Map display date to ISO date
    date_columns_display = []
    for d in date_columns:
        if '-' in d:
            parts = d.split('-')
            date_columns_display.append(f"{parts[1]}/{parts[2]}")
        else:
            date_columns_display.append(d)
    date_mapping = dict(zip(date_columns_display, date_columns))
    selected_date_iso = date_mapping.get(selected_date, selected_date)
    
    # Build player query
    type_filter = ""
    if selected_types and set(selected_types) != {'bat', 'pit'}:
        types_str = "','".join(selected_types)
        type_filter = f"AND p.player_type IN ('{types_str}')"
    
    roster_join = "LEFT JOIN roster r ON r.player_id = p.id AND r.left_date IS NULL"
    war_join = "LEFT JOIN war_daily w ON w.player_id = p.id AND w.date = ?"
    params: list[Any] = [selected_date_iso]

    if season_id is not None:
        roster_join = "LEFT JOIN roster r ON r.player_id = p.id AND r.left_date IS NULL AND r.season_id = ?"
        war_join = "LEFT JOIN war_daily w ON w.player_id = p.id AND w.date = ? AND w.season_id = ?"
        params = [season_id, selected_date_iso, season_id]

    query = f"""
        SELECT 
            p.id,
            p.name,
            p.player_type as type,
            r.team_id as team,
            w.war,
            w.war_diff
        FROM players p
        {roster_join}
        {war_join}
        WHERE 1=1 {type_filter}
        ORDER BY w.war DESC NULLS LAST
    """

    df = pd.read_sql_query(query, db, params=params)
    
    # Fill missing team with FA
    df['team'] = df['team'].fillna('퐈')
    df = df[df['war'].notnull()]

    display_columns = ['Name', 'type', 'team', 'WAR', '변화량']
    column_names = {
        'Name': '선수명',
        'type': '포지션',
        'team': '소속팀',
        'WAR': 'WAR',
        '변화량': '변화량'
    }
    
    # Apply team filter
    if not selected_teams:
        return [], display_columns, date_columns, column_names

    df = df[df['team'].isin(selected_teams)]
    
    # Apply search filter
    if search_query:
        df = df[df['name'].str.contains(search_query, case=False, na=False)]
    
    war_rank_source = df[df['war'].notnull()].copy()
    war_rank_source = war_rank_source.sort_values(['war', 'name', 'team'], ascending=[False, True, True], na_position='last', kind='mergesort')
    war_rank_dict = {row['id']: i + 1 for i, (_, row) in enumerate(war_rank_source.iterrows())}
    df['war_rank_sort'] = df['id'].map(war_rank_dict)

    # Apply sorting
    effective_sort_by = sort_by or 'WAR'
    ascending = (sort_order == 'asc')
    if effective_sort_by == 'Name':
        df = df.sort_values('name', ascending=ascending)
    elif effective_sort_by == 'type':
        df = df.sort_values('type', ascending=ascending)
    elif effective_sort_by == 'team':
        df['team_order'] = df['team'].map(lambda t: team_order_dict.get(t, len(team_order)))
        df = df.sort_values('team_order', ascending=ascending)
        df = df.drop('team_order', axis=1)
    elif effective_sort_by in ['WAR', selected_date, selected_date_iso]:
        df = df.sort_values(['war', 'war_rank_sort'], ascending=[ascending, True], na_position='last', kind='mergesort')
    elif effective_sort_by == '변화량':
        df = df.sort_values('war_diff', ascending=ascending, na_position='last')
    
    # Build rows
    rows = []
    for _, row in df.iterrows():
        player_type_display = '타자' if row['type'] == 'bat' else '투수'
        rows.append({
            'ID': row['id'],
            'Name': row['name'],
            'type': player_type_display,
            'team': row['team'],
            'war_rank': war_rank_dict.get(row['id'], ''),
            'WAR': f"{row['war']:.2f}" if pd.notnull(row['war']) else '-',
            selected_date_iso: f"{row['war']:.2f}" if pd.notnull(row['war']) else '-',
            '변화량': f"{row['war_diff']:+.2f}" if pd.notnull(row['war_diff']) else '-',
        })
    
    return rows, display_columns, date_columns, column_names
