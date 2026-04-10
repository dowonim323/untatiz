"""API blueprint - JSON API endpoints for graphs and data."""

from __future__ import annotations

from datetime import datetime

import pytz
from flask import Blueprint, request, jsonify, render_template

import pandas as pd

from web.utils import get_db, get_selected_season
from web.services.league_service import get_league_graph_data
from web.services.team_service import get_team_graph_data

bp = Blueprint('api', __name__)

KST = pytz.timezone('Asia/Seoul')


@bp.route('/api/status')
def get_status():
    """Return current scraper status for real-time polling."""
    db = get_db()
    cur = db.execute("""
        SELECT last_updated_at, target_date, total_games, 
               updated_games, war_status
        FROM scraper_status WHERE id = 1
    """)
    row = cur.fetchone()
    
    if row:
        last_updated_str = row['last_updated_at']
        minutes_ago = 0
        
        if last_updated_str:
            try:
                last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
                if last_updated.tzinfo is None:
                    last_updated = KST.localize(last_updated)
                now = datetime.now(KST)
                minutes_ago = int((now - last_updated).total_seconds() / 60)
            except (ValueError, TypeError):
                pass
        
        return jsonify({
            'last_updated': last_updated_str,
            'minutes_ago': minutes_ago,
            'target_date': row['target_date'],
            'games': f"{row['updated_games'] or 0}/{row['total_games'] or 0}",
            'war_status': row['war_status'] or 'unknown'
        })
    
    return jsonify({'error': 'No status available'}), 404


@bp.route('/api/players/search')
def search_players():
    """Search players by name for autocomplete."""
    q = request.args.get('q', '').strip()
    # Allow single Korean characters (each is a complete syllable)
    # For ASCII, require at least 2 characters
    min_len = 1 if any('\uac00' <= c <= '\ud7a3' for c in q) else 2
    if len(q) < min_len:
        return jsonify({'players': []})
    
    db = get_db()
    season_id, _ = get_selected_season(request.args)
    cur = db.execute("""
        SELECT p.id, p.name, p.player_type, p.position,
               COALESCE(w.war, 0) as current_war
        FROM players p
        LEFT JOIN (
            SELECT player_id, war FROM war_daily 
            WHERE date = (SELECT MAX(date) FROM war_daily WHERE season_id = ?)
              AND season_id = ?
        ) w ON p.id = w.player_id
        WHERE p.name LIKE ?
        LIMIT 20
    """, (season_id, season_id, f'%{q}%',))
    
    players = []
    for row in cur.fetchall():
        players.append({
            'id': row['id'],
            'name': row['name'],
            'type': row['player_type'],
            'position': row['position'],
            'war': round(row['current_war'] or 0, 2)
        })
    
    return jsonify({'players': players})


@bp.route('/graph_data')
def graph_data():
    """Get league graph data as JSON."""
    period = request.args.get('period', '30')
    end_date = request.args.get('date', '')
    db = get_db()
    season_id, _ = get_selected_season(request.args)
    
    graph_data, _, _ = get_league_graph_data(db, end_date, period, season_id=season_id)
    return jsonify(graph_data)


@bp.route('/team_graph_data')
def team_graph_data():
    """Get team graph data as JSON."""
    team = request.args.get('team')
    period = request.args.get('period', '30')
    end_date = request.args.get('date', '')
    
    if not team:
        return jsonify({"error": "Team parameter is required"}), 400
    
    db = get_db()
    season_id, _ = get_selected_season(request.args)
    graph_data, _, _ = get_team_graph_data(db, team, end_date, period, season_id=season_id)
    return jsonify(graph_data)


@bp.route('/table/<table_name>')
def table_view(table_name):
    """Generic table view."""
    db = get_db()
    try:
        cur = db.execute(f"SELECT * FROM [{table_name}]")
    except Exception as e:
        return f"Error: {e}"
    rows = cur.fetchall()
    colnames = rows[0].keys() if rows else []
    return render_template('table.html', table_name=table_name, rows=rows, colnames=colnames)
