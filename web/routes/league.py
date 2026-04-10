"""League blueprint - league standings routes."""

from __future__ import annotations

from flask import Blueprint, render_template, request

from web.utils import get_db, get_selected_season
from web.services.league_service import (
    get_league_table_data,
    get_league_weekly_data,
    get_league_graph_data,
)

bp = Blueprint('league', __name__)


@bp.route('/category/league')
def league_view():
    """League standings page."""
    db = get_db()
    season_id, _ = get_selected_season(request.args)
    
    sub_category = request.args.get('sub', 'table')
    period = request.args.get('period', '1')
    selected_date = request.args.get('date', '')
    sort_by = request.args.get('sort', 'WAR')
    sort_order = request.args.get('order', 'desc')
    
    if sub_category == 'graph':
        period = request.args.get('period', '30')
        graph_data, date_columns, selected_dates = get_league_graph_data(
            db, end_date=selected_date, period=period, season_id=season_id
        )
        normalized_end_date = selected_dates[-1] if selected_dates else (date_columns[-1] if date_columns else '')
        
        return render_template('league.html',
                             category='league',
                             sub_category=sub_category,
                             period=period,
                             end_date=normalized_end_date,
                             start_date=selected_dates[0] if selected_dates else '',
                             available_dates=date_columns,
                             graph_data=graph_data,
                             selected_dates=selected_dates)
    
    elif period == '1':
        # WAR가 기본 정렬일 때는 order 파라미터가 없으면 desc로 설정
        default_order = 'desc' if sort_by != 'WAR' else request.args.get('order', 'desc')
        sort_order = request.args.get('order', default_order)
        
        rows, date_columns, selected_date = get_league_table_data(
            db, selected_date, sort_by, sort_order, season_id=season_id
        )
        
        # selected_dates 계산
        start_idx = date_columns.index(selected_date) if selected_date in date_columns else len(date_columns) - 1
        if start_idx + 7 > len(date_columns):
            start_idx = len(date_columns) - 7
        if start_idx < 0:
            start_idx = 0
        selected_dates = date_columns[start_idx:start_idx + 7]
        
        return render_template('league.html',
                             category='league',
                             sub_category=sub_category,
                             rows=rows,
                             period=period,
                             date=selected_date,
                             available_dates=date_columns,
                             sort_by=sort_by,
                             sort_order=sort_order,
                             selected_dates=selected_dates)
    
    elif period == '7':
        sort_by = request.args.get('sort', '')
        rows, date_columns, selected_dates = get_league_weekly_data(
            db, selected_date, sort_by, sort_order, season_id=season_id
        )
        
        if not sort_by and selected_dates:
            sort_by = selected_dates[-1]
        
        return render_template('league.html',
                             category='league',
                             sub_category=sub_category,
                             rows=rows,
                             period=period,
                             selected_dates=selected_dates,
                             start_date=selected_dates[0] if selected_dates else '',
                             end_date=selected_dates[-1] if selected_dates else '',
                             available_dates=date_columns,
                             sort_by=sort_by,
                             sort_order=sort_order)
    
    return render_template('league.html', category='league')
