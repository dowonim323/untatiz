"""Team blueprint - team info routes."""

from __future__ import annotations

from flask import Blueprint, render_template, request

from web.utils import get_db, get_selected_season
from web.services.team_service import (
    get_team_names,
    get_team_table_data,
    get_team_weekly_data,
    get_team_graph_data,
)

bp = Blueprint('team', __name__)


@bp.route('/category/team')
def team_view():
    """Team info page."""
    db = get_db()
    season_id, _ = get_selected_season(request.args)
    
    team_names = get_team_names(db, season_id=season_id)
    selected_team = request.args.get('team', team_names[0] if team_names else '')
    period = request.args.get('period', '1')
    sub_category = request.args.get('sub', 'table')
    selected_date = request.args.get('date', '')
    sort_by = request.args.get('sort', '')
    sort_order = request.args.get('order', 'desc')
    
    if sub_category == 'graph':
        period = request.args.get('period', '30')
        graph_data, date_columns, selected_dates = get_team_graph_data(
            db, selected_team, end_date=selected_date, period=period, season_id=season_id
        )
        normalized_end_date = selected_dates[-1] if selected_dates else (date_columns[-1] if date_columns else '')
        
        return render_template('team_info.html',
                             category='team',
                             sub_category=sub_category,
                             team_names=team_names,
                             selected_team=selected_team,
                             period=period,
                             end_date=normalized_end_date,
                             start_date=selected_dates[0] if selected_dates else '',
                             available_dates=date_columns,
                             graph_data=graph_data)
    
    elif period == '1':
        if not sort_by:
            sort_by = 'WAR'
            sort_order = 'desc'
        
        rows, date_columns, selected_date = get_team_table_data(
            db, selected_team, selected_date, sort_by, sort_order, season_id=season_id
        )
        
        return render_template('team_info.html',
                             category='team',
                             team_names=team_names,
                             selected_team=selected_team,
                             rows=rows,
                             period=period,
                             date=selected_date,
                             available_dates=date_columns,
                             sort_by=sort_by,
                             sort_order=sort_order)
    
    else:  # period == '7'
        rows, date_columns, selected_dates = get_team_weekly_data(
            db, selected_team, selected_date, sort_by, sort_order, season_id=season_id
        )
        
        if not sort_by and selected_dates:
            sort_by = selected_dates[-1]
        
        return render_template('team_info.html',
                             category='team',
                             team_names=team_names,
                             selected_team=selected_team,
                             rows=rows,
                             period=period,
                             selected_dates=selected_dates,
                             start_date=selected_dates[0] if selected_dates else '',
                             end_date=selected_dates[-1] if selected_dates else '',
                             available_dates=date_columns,
                             sort_by=sort_by,
                             sort_order=sort_order)
