"""GBOAT service - data loading for GOAT/BOAT pages.

Uses new Long format schema (daily_records table).
"""

from __future__ import annotations

from typing import Dict, List, Any, Tuple

import pandas as pd

from web.utils import get_db, get_team_order
from app.core.cache import cached_query, TTL_MEDIUM


def get_available_dates(db, season_year: int | None = None) -> List[str]:
    """Get list of available dates for GOAT/BOAT."""
    def _query():
        try:
            if season_year is None:
                df = pd.read_sql_query("SELECT DISTINCT date FROM daily_records ORDER BY date", db)
            else:
                df = pd.read_sql_query(
                    "SELECT DISTINCT date FROM daily_records WHERE date LIKE ? ORDER BY date",
                    db,
                    params=(f"{season_year}-%",),
                )
            # Convert to MM/DD format for display
            dates = []
            for d in df['date'].tolist():
                if '-' in d:
                    parts = d.split('-')
                    dates.append(f"{parts[1]}/{parts[2]}")
                else:
                    dates.append(d)
            return dates
        except Exception:
            return []
    
    cache_key = f"gboat_dates_{season_year}" if season_year is not None else "gboat_dates"
    return cached_query(cache_key, _query, ttl=TTL_MEDIUM, namespace="gboat")


def _convert_date_to_iso(date_mmdd: str, year: int) -> str:
    """Convert MM/DD to YYYY-MM-DD format."""
    if '/' in date_mmdd:
        month, day = date_mmdd.split('/')
        return f"{year}-{month}-{day}"
    return date_mmdd


def _convert_date_to_display(date_iso: str) -> str:
    """Convert YYYY-MM-DD to MM/DD format."""
    if '-' in date_iso:
        parts = date_iso.split('-')
        return f"{parts[1]}/{parts[2]}"
    return date_iso


def get_gboat_data(
    db,
    start_date: str,
    end_date: str,
    selected_teams: List[str],
    season_year: int,
    season_id: int | None = None,
) -> Tuple[List[Dict], List[Dict]]:
    """Get GOAT and BOAT data.
    
    Returns:
        Tuple of (goat_rows, boat_rows)
    """
    team_order = get_team_order(season_id) + ['퐈']
    
    # Convert dates to ISO format
    start_iso = _convert_date_to_iso(start_date, season_year)
    end_iso = _convert_date_to_iso(end_date, season_year)
    
    # Build team filter
    params = [start_iso, end_iso]
    if not selected_teams:
        team_filter = " AND 0=1"
    elif set(selected_teams) != set(team_order):
        real_teams = [team for team in selected_teams if team != '퐈']
        include_fa = '퐈' in selected_teams
        clauses = []
        if real_teams:
            placeholders = ','.join(['?'] * len(real_teams))
            clauses.append(f"dr.team_id IN ({placeholders})")
            params.extend(real_teams)
        if include_fa:
            clauses.append("dr.team_id IS NULL")
        team_filter = f" AND ({' OR '.join(clauses)})" if clauses else " AND 0=1"
    else:
        team_filter = ""
    
    # GOAT query
    try:
        goat_query = f"""
            SELECT 
                dr.team_id as 소속팀,
                p.name as 이름,
                dr.date as 날짜,
                dr.war_diff as WAR변동
            FROM daily_records dr
            JOIN players p ON dr.player_id = p.id
            WHERE dr.record_type = 'GOAT'
              AND dr.date BETWEEN ? AND ?
              {team_filter}
            ORDER BY dr.war_diff DESC
        """
        goat_df = pd.read_sql_query(goat_query, db, params=params.copy())
        
        goat_rows = []
        for i, (_, row) in enumerate(goat_df.iterrows()):
            goat_rows.append({
                '순위': i + 1,
                '소속팀': row['소속팀'] or '퐈',
                '이름': row['이름'],
                '날짜': _convert_date_to_display(row['날짜']),
                'WAR 변동': f"+{row['WAR변동']:.2f}"
            })
    except Exception:
        goat_rows = []
    
    # BOAT query
    try:
        # Reset params
        params = [start_iso, end_iso]
        if selected_teams and set(selected_teams) != set(team_order):
            params.extend([team for team in selected_teams if team != '퐈'])
        
        boat_query = f"""
            SELECT 
                dr.team_id as 소속팀,
                p.name as 이름,
                dr.date as 날짜,
                dr.war_diff as WAR변동
            FROM daily_records dr
            JOIN players p ON dr.player_id = p.id
            WHERE dr.record_type = 'BOAT'
              AND dr.date BETWEEN ? AND ?
              {team_filter}
            ORDER BY dr.war_diff ASC
        """
        boat_df = pd.read_sql_query(boat_query, db, params=params)
        
        boat_rows = []
        for i, (_, row) in enumerate(boat_df.iterrows()):
            boat_rows.append({
                '순위': i + 1,
                '소속팀': row['소속팀'] or '퐈',
                '이름': row['이름'],
                '날짜': _convert_date_to_display(row['날짜']),
                'WAR 변동': f"{row['WAR변동']:.2f}"
            })
    except Exception:
        boat_rows = []
    
    return goat_rows, boat_rows
