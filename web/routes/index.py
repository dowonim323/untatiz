"""Index blueprint - home page routes."""

from __future__ import annotations

from flask import Blueprint, current_app, render_template, request

from web.services.index_service import (
    get_league_standings,
    get_news_for_date,
    get_update_info,
)
from web.utils import get_selected_season

bp = Blueprint('index', __name__)


@bp.route('/')
def index():
    """Home page with league standings and update info."""
    app_config = current_app.config.get('APP_CONFIG')

    try:
        db_path = current_app.config['DATABASE']
        season_id, _ = get_selected_season(request.args)

        # Get update info
        update_info = get_update_info(db_path)

        # Get league standings
        league_standings = get_league_standings(db_path, season_id=season_id)

        # Get news if available
        news_date = None
        current_news = None

        if update_info['games_date'] and ' : ' in update_info['games_date']:
            news_date = update_info['games_date'].split(' : ')[1]
            if update_info['war_update_status'] == "업데이트 완료":
                current_news = get_news_for_date(news_date)

        return render_template('index.html',
                              update_time=update_info['update_time'],
                              war_update_status=update_info['war_update_status'],
                              games_date=update_info['games_date'],
                              games=update_info['games'],
                              no_games=update_info['no_games'],
                              league_standings=league_standings,
                              current_news=current_news,
                              news_date=news_date,
                              league_name=app_config.league_name if app_config else '')
    except Exception:
        return render_template(
            'index.html',
            error="데이터를 불러오는 중 오류가 발생했습니다.",
            league_name=app_config.league_name if app_config else '',
        )


@bp.route('/health', methods=['GET'])
def health_check():
    """Docker healthcheck endpoint with scraper monitoring."""
    import sqlite3
    from datetime import datetime

    import pytz
    from flask import jsonify

    try:
        db_path = current_app.config['DATABASE']
        db = sqlite3.connect(db_path)
        cursor = db.cursor()
        cursor.execute("SELECT 1")

        # Get scraper status
        cursor.execute(
            "SELECT last_updated_at, war_status FROM scraper_status WHERE id = 1"
        )
        scraper_result = cursor.fetchone()
        db.close()

        # Determine database status
        database_status = "connected"

        # Initialize scraper info
        scraper_info = {
            "last_updated": None,
            "minutes_ago": None,
            "war_status": "no_data",
            "is_stale": False
        }

        # Parse scraper status if available
        if scraper_result:
            last_updated_str = scraper_result[0]
            war_status = scraper_result[1]

            # Parse ISO timestamp
            try:
                kst = pytz.timezone('Asia/Seoul')
                last_updated = datetime.fromisoformat(last_updated_str.replace('Z', '+00:00'))
                if last_updated.tzinfo is None:
                    last_updated_kst = kst.localize(last_updated)
                else:
                    last_updated_kst = last_updated.astimezone(kst)

                scraper_info["last_updated"] = last_updated_str
                scraper_info["war_status"] = war_status or "pending"

                # Calculate minutes ago
                now_kst = datetime.now(kst)
                minutes_ago = int(
                    (now_kst - last_updated_kst).total_seconds() / 60
                )
                scraper_info["minutes_ago"] = minutes_ago

                # Check staleness: > 120 minutes AND between 14:00-23:00 KST
                if minutes_ago > 120 and 14 <= now_kst.hour < 23:
                    scraper_info["is_stale"] = True
            except (ValueError, TypeError):
                pass

        # Determine overall status
        overall_status = "healthy"
        if scraper_info["is_stale"]:
            overall_status = "warning"
        elif database_status != "connected":
            overall_status = "unhealthy"

        return jsonify({
            "status": overall_status,
            "database": database_status,
            "scraper": scraper_info,
            "timestamp": datetime.now(pytz.timezone('Asia/Seoul')).isoformat()
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "database": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 503
