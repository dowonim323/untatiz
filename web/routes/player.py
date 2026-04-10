"""Player blueprint - player list routes."""

from __future__ import annotations

from flask import Blueprint, render_template, request

from web.utils import get_db, get_selected_season, get_team_order
from web.services.player_service import get_player_data

bp = Blueprint('player', __name__)


@bp.route('/category/player')
def player_view():
    """Player list page."""
    db = get_db()
    season_id, _ = get_selected_season(request.args)
    
    # 팀 목록 가져오기
    team_order = get_team_order(season_id)
    team_order_with_fa = team_order + ['퐈']
    
    # 선택된 유형
    types_param = request.args.get('types', 'bat,pit')
    selected_types = types_param.split(',') if types_param else ['bat', 'pit']
    
    # 선택된 팀
    teams_param = request.args.get('teams', None)
    if teams_param is None:
        selected_teams = team_order_with_fa
    else:
        selected_teams = teams_param.split(',') if teams_param else []
    
    # 검색어
    search_query = request.args.get('search', '')
    
    # 날짜
    selected_date = request.args.get('date', '')
    
    # 정렬
    sort_by = request.args.get('sort', '')
    sort_order = request.args.get('order', 'desc')
    
    # 데이터 가져오기
    rows, display_columns, date_columns, column_names = get_player_data(
        db, selected_types, selected_teams, selected_date,
        search_query, sort_by or selected_date, sort_order, season_id=season_id
    )
    
    # selected_date 업데이트
    if not selected_date and date_columns:
        selected_date = date_columns[-1]
    
    if not sort_by:
        sort_by = selected_date
    
    return render_template('player.html',
                         category='player',
                         selected_types=selected_types,
                         team_order=team_order_with_fa,
                         selected_teams=selected_teams,
                         columns=display_columns,
                         column_names=column_names,
                         rows=rows,
                         sort_by=sort_by,
                         sort_order=sort_order,
                         search_query=search_query,
                         date=selected_date,
                         selected_date=selected_date,
                         available_dates=date_columns)
