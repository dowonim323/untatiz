"""Utility functions for web application."""

from __future__ import annotations

import sqlite3
from typing import Callable, List

import numpy as np
import pandas as pd
from flask import current_app, g
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize

from app.core.utils import get_business_year


def get_db():
    """Get database connection for current request context.
    
    Returns:
        sqlite3.Connection: Database connection with Row factory and FK enforcement
    """
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(current_app.config['DATABASE'])
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
    return db


def get_team_order(season_id: int | None = None) -> List[str]:
    db = get_db()
    target_season_id = season_id or get_current_season_id()

    cur = db.execute(
        """
        SELECT team_id
        FROM draft
        WHERE season_id = ?
        GROUP BY team_id
        ORDER BY MIN(pick_order)
        """,
        (target_season_id,),
    )
    teams = [row['team_id'] for row in cur.fetchall()]
    if teams:
        return teams

    cur = db.execute(
        """
        SELECT team_id
        FROM roster
        WHERE season_id = ?
        GROUP BY team_id
        ORDER BY MIN(joined_date), MIN(id)
        """,
        (target_season_id,),
    )
    teams = [row['team_id'] for row in cur.fetchall()]
    if teams:
        return teams

    cur = db.execute("SELECT id FROM fantasy_teams WHERE id != '퐈' ORDER BY rowid")
    return [row['id'] for row in cur.fetchall()]


def get_all_seasons() -> List[dict]:
    """Get all seasons from database.
    
    Returns:
        List[dict]: List of season records with id, year, is_active
    """
    db = get_db()
    cur = db.execute("""
        SELECT id, year, start_date, end_date, is_active 
        FROM seasons 
        ORDER BY year DESC
    """)
    seasons = []
    for row in cur.fetchall():
        seasons.append({
            'id': row['id'],
            'year': row['year'],
            'start_date': row['start_date'],
            'end_date': row['end_date'],
            'is_active': bool(row['is_active'])
        })
    return seasons


def get_current_season_id() -> int:
    """Get the current active season ID.
    
    Returns:
        int: Active season ID (defaults to 1 if none found)
    """
    db = get_db()
    cur = db.execute("SELECT id FROM seasons WHERE is_active = 1 LIMIT 1")
    row = cur.fetchone()
    return row['id'] if row else 1


def get_season_by_year(year: int) -> dict | None:
    """Get season record by year.
    
    Args:
        year: Season year (e.g., 2025)
        
    Returns:
        Season dict or None if not found
    """
    db = get_db()
    cur = db.execute("SELECT id, year, is_active FROM seasons WHERE year = ?", (year,))
    row = cur.fetchone()
    if row:
        return {'id': row['id'], 'year': row['year'], 'is_active': bool(row['is_active'])}
    return None


def get_selected_season(request_args) -> tuple[int, int]:
    """Get selected season from request args or default to current.
    
    Args:
        request_args: Flask request.args
        
    Returns:
        Tuple of (season_id, season_year)
    """
    season_year = request_args.get('season', type=int)
    
    if season_year:
        season = get_season_by_year(season_year)
        if season:
            return season['id'], season['year']
    
    # Default to active season
    seasons = get_all_seasons()
    for s in seasons:
        if s['is_active']:
            return s['id'], s['year']
    
    # Fallback to first season
    if seasons:
        return seasons[0]['id'], seasons[0]['year']
    
    return 1, get_business_year()


def get_color_scale(values, df_all_diff=None) -> Callable:
    """Calculate background color based on change values.
    
    Uses z-score normalization and coolwarm colormap.
    
    Args:
        values: Array of values for fallback calculation
        df_all_diff: Optional DataFrame with all diff values for global normalization
        
    Returns:
        Callable: Function that maps value to RGB color string
    """
    if df_all_diff is not None:
        all_values = pd.to_numeric(pd.Series(df_all_diff.values.flatten()), errors='coerce').dropna().to_numpy()
        if len(all_values) == 0:
            return lambda x: "rgb(255, 255, 255)"
        mean = np.nanmean(all_values)
        std = np.nanstd(all_values)
    else:
        values = np.array([float(v) for v in values if pd.notnull(v)])
        if len(values) == 0:
            return lambda x: "rgb(255, 255, 255)"
        mean = np.nanmean(values)
        std = np.nanstd(values)

    norm = Normalize(vmin=-3, vmax=3)
    sm = ScalarMappable(cmap='coolwarm', norm=norm)

    def get_rgba(value):
        if pd.isnull(value):
            return "rgb(255, 255, 255)"
        if std == 0:
            return "rgb(255, 255, 255)"
        z_score = (float(value) - mean) / std
        rgb = sm.to_rgba(z_score, bytes=True)[:3]
        return f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"

    return get_rgba


def get_team_color_scale(df_team_diff) -> Callable:
    """Calculate background color for team info page.
    
    Args:
        df_team_diff: DataFrame with team diff values
        
    Returns:
        Callable: Function that maps value to RGB color string
    """
    values = df_team_diff.select_dtypes(include=[np.number]).values.flatten()
    values = values[~np.isnan(values)]

    mean = np.nanmean(values)
    std = np.nanstd(values)

    norm = Normalize(vmin=-3, vmax=3)
    sm = ScalarMappable(cmap='coolwarm', norm=norm)

    def get_rgba(value):
        if pd.isnull(value):
            return "rgb(255, 255, 255)"
        if std == 0:
            return "rgb(255, 255, 255)"
        z_score = (float(value) - mean) / std
        rgb = sm.to_rgba(z_score, bytes=True)[:3]
        return f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"

    return get_rgba


def get_zscore_color_mapper(df: pd.DataFrame, columns: List[str]) -> Callable:
    """Create z-score based color mapper for DataFrame columns.
    
    Args:
        df: DataFrame with numeric values
        columns: List of column names to calculate z-scores for
        
    Returns:
        Callable: Function that takes (team, column) and returns (z_score, color)
    """
    # Calculate z-scores for each column
    z_scores = pd.DataFrame(index=df.index, columns=columns)
    for col in columns:
        values = pd.to_numeric(df[col], errors='coerce').values
        mean = np.nanmean(values)
        std = np.nanstd(values)
        z_scores[col] = (values - mean) / std if std != 0.0 else np.zeros_like(values)

    norm = Normalize(vmin=-3, vmax=3)
    sm = ScalarMappable(cmap='coolwarm', norm=norm)

    def get_color(team: str, column: str) -> str:
        if team not in z_scores.index or column not in z_scores.columns:
            return "rgb(255, 255, 255)"
        z_score = z_scores.loc[team, column]
        if pd.isna(z_score):
            return "rgb(255, 255, 255)"
        rgb = sm.to_rgba(z_score, bytes=True)[:3]
        return f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"

    return get_color
