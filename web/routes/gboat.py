"""GBOAT blueprint - GOAT/BOAT routes."""

from __future__ import annotations

from flask import Blueprint, render_template, request

from web.utils import get_db, get_selected_season, get_team_order
from web.services.gboat_service import get_available_dates, get_gboat_data

bp = Blueprint('gboat', __name__)


@bp.route('/category/gboat')
def gboat_view():
    """GOAT/BOAT page."""
    db = get_db()
    season_id, season_year = get_selected_season(request.args)

    all_dates = get_available_dates(db, season_year)
    team_order = get_team_order(season_id) + ['퐈']
    
    # 끝 날짜 (기본값: 가장 최근 날짜)
    end_date = request.args.get('end_date', all_dates[-1] if all_dates else '')
    
    if not end_date or end_date not in all_dates:
        end_date = all_dates[-1] if all_dates else ''
    
    # 끝 날짜의 인덱스 찾기
    end_idx = all_dates.index(end_date) if end_date in all_dates else len(all_dates) - 1
    
    # 시작 날짜 (기본값: 끝 날짜로부터 일주일 전)
    default_start_idx = max(0, end_idx - 6)
    default_start_date = all_dates[default_start_idx] if all_dates and default_start_idx < len(all_dates) else ''
    
    start_date = request.args.get('start_date', default_start_date)
    
    if not start_date or start_date not in all_dates:
        start_date = default_start_date
    
    # 시작 날짜의 인덱스 찾기
    start_idx = all_dates.index(start_date) if start_date in all_dates else 0
    
    # 시작 날짜가 끝 날짜보다 뒤에 있는 경우 조정
    if start_idx > end_idx:
        start_idx, end_idx = end_idx, start_idx
        start_date, end_date = end_date, start_date
    
    # 선택된 팀
    teams_param = request.args.get('teams', None)
    if teams_param is None:
        selected_teams = team_order
    else:
        selected_teams = teams_param.split(',') if teams_param else []
    
    # 데이터 가져오기
    goat_rows, boat_rows = get_gboat_data(db, start_date, end_date, selected_teams, season_year, season_id=season_id)
    
    return render_template('gboat.html',
                         category='gboat',
                         start_date=start_date,
                         end_date=end_date,
                         available_dates=all_dates,
                         team_order=team_order,
                         selected_teams=selected_teams,
                         goat_rows=goat_rows,
                         boat_rows=boat_rows)
