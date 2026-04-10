"""Index service - data loading for home page."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

import pandas as pd

from app.config.settings import load_config
from web.utils import get_color_scale


def get_update_info(db_path: str) -> Dict[str, Any]:
    """Get update information from scraper_status and daily_games tables.
    
    Args:
        db_path: Path to database
        
    Returns:
        Dict with update_time, war_update_status, games_date, games, no_games
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get scraper status
    cursor.execute("SELECT last_updated_at, target_date, total_games, updated_games, war_status FROM scraper_status WHERE id = 1")
    status_row = cursor.fetchone()
    
    if not status_row:
        conn.close()
        return {
            'update_time': "정보 없음",
            'war_update_status': None,
            'games_date': "정보 없음",
            'games': [],
            'no_games': None
        }
    
    update_time = status_row[0]
    target_date = status_row[1]
    total_games = status_row[2]
    updated_games = status_row[3]
    war_status = status_row[4]
    
    # Format games_date like "경기 날짜 : MM/DD"
    if target_date:
        try:
            from datetime import datetime
            dt = datetime.strptime(target_date, "%Y-%m-%d")
            games_date = f"경기 날짜 : {dt.strftime('%m/%d')}"
        except ValueError:
            games_date = f"경기 날짜 : {target_date}"
    else:
        games_date = "정보 없음"
    
    # Get games for the target date
    games = []
    no_games = None
    
    if war_status == 'no_games':
        no_games = '오늘은 경기가 없습니다.'
    elif target_date:
        cursor.execute("""
            SELECT away_team, home_team, away_score, home_score, game_status
            FROM daily_games
            WHERE game_date = ?
            ORDER BY game_order
        """, (target_date,))
        
        for row in cursor.fetchall():
            away_team, home_team, away_score, home_score, game_status = row
            
            is_score = away_score is not None and home_score is not None
            if is_score:
                status = f"{away_score} : {home_score}"
            elif game_status == 'cancelled':
                status = "우천취소"
            elif game_status == 'postponed':
                status = "연기"
            elif game_status == 'in_progress':
                status = "진행 중"
            else:
                status = "시작 전"
            
            games.append({
                'is_score': is_score,
                'status': status,
                'home_team': home_team,
                'home_score': home_score or 0,
                'away_team': away_team,
                'away_score': away_score or 0,
            })

    if war_status == 'completed':
        war_update_status = "업데이트 완료"
    elif war_status == 'no_games':
        war_update_status = "경기 없음"
    else:
        game_statuses = {game['status'] for game in games}
        if games and game_statuses <= {'시작 전'}:
            war_update_status = "경기 시작 전"
        elif games and all(game['is_score'] for game in games):
            war_update_status = "업데이트 대기 중"
        else:
            war_update_status = "경기 진행 중"
    
    conn.close()
    
    return {
        'update_time': update_time,
        'war_update_status': war_update_status,
        'games_date': games_date,
        'games': games,
        'no_games': no_games
    }


def get_league_standings(db_path: str, season_id: int | None = None) -> List[Dict[str, Any]]:
    """Get league standings for home page using new Long format schema.
    
    Args:
        db_path: Path to database
        
    Returns:
        List of team standings with WAR, rank, and change info
    """
    conn = sqlite3.connect(db_path)

    # Get latest date from team_war_daily
    cursor = conn.cursor()
    if season_id is None:
        cursor.execute("SELECT MAX(date) FROM team_war_daily")
        result = cursor.fetchone()
    else:
        cursor.execute("SELECT MAX(date) FROM team_war_daily WHERE season_id = ?", (season_id,))
        result = cursor.fetchone()
    if not result or not result[0]:
        conn.close()
        return []

    latest_date = result[0]

    # Get team standings for latest date
    if season_id is None:
        df = pd.read_sql_query(
            """
            SELECT team_id, total_war, war_diff, rank
            FROM team_war_daily
            WHERE date = ?
            ORDER BY total_war DESC, team_id
            """,
            conn,
            params=(latest_date,),
        )
    else:
        df = pd.read_sql_query(
            """
            SELECT team_id, total_war, war_diff, rank
            FROM team_war_daily
            WHERE date = ? AND season_id = ?
            ORDER BY total_war DESC, team_id
            """,
            conn,
            params=(latest_date, season_id),
        )
    conn.close()

    if df.empty:
        return []

    max_war = df['total_war'].max() if not df.empty else 0

    # 변화량 색상 계산을 위한 함수 가져오기
    all_diffs = df['war_diff'].values
    color_scale = get_color_scale(all_diffs)

    league_standings = []

    for _, row in df.iterrows():
        team = row['team_id']
        war = row['total_war']
        change = 0.0 if pd.isna(row['war_diff']) else float(row['war_diff'])
        war_rank = '' if team == '퐈' or pd.isna(row['rank']) else int(row['rank'])
        war_ratio = (war / max_war * 100) if max_war > 0 else 0
        change_color = color_scale(change)

        league_standings.append({
            'team': team,
            'WAR': f"{war:.2f}",
            'WAR_순위': war_rank,
            'WAR_비율': war_ratio,
            '변화량': f"{change:+.2f}" if change != 0 else "0.00",
            '변화량_색상': change_color
        })

    return league_standings


def get_news_for_date(news_date: str) -> Optional[Dict[str, Any]]:
    """Get news for a specific date.
    
    Args:
        news_date: Date string (format: MM/DD)
        
    Returns:
        News data dict or None
    """
    try:
        news_file_path = load_config().news_dir / 'news.json'
        with open(news_file_path, 'r', encoding='utf-8') as f:
            news_data = json.load(f)
        return news_data.get(news_date)
    except Exception:
        return None
