"""Flask application factory for Untatiz web application."""

from __future__ import annotations

import secrets
from flask import Flask, g, request
import sqlite3

from app.config.settings import load_config
from app.core.schema import ensure_runtime_db
from app.core.utils import get_business_year


def create_app(config=None):
    """Create and configure the Flask application.
    
    Args:
        config: Optional configuration object (uses load_config() if None)
        
    Returns:
        Flask: Configured Flask application
    """
    app = Flask(__name__, 
                static_folder='static',
                template_folder='templates')
    
    # Load configuration
    if config is None:
        config = load_config()

    ensure_runtime_db(config.db_path, get_business_year())
    
    # Store config in app
    app.config['APP_CONFIG'] = config
    app.config['DATABASE'] = str(config.db_path)
    app.secret_key = config.flask_secret_key or secrets.token_hex(16)
    
    # Register database teardown
    @app.teardown_appcontext
    def close_db(error):
        db = getattr(g, '_database', None)
        if db is not None:
            db.close()
    
    # Inject season context into all templates
    @app.context_processor
    def inject_season_context():
        """Inject season-related variables into all templates."""
        from web.utils import get_all_seasons, get_selected_season
        
        seasons = get_all_seasons()
        season_id, season_year = get_selected_season(request.args)
        
        return {
            'all_seasons': seasons,
            'current_season_id': season_id,
            'current_season_year': season_year,
        }
    
    # Register blueprints
    from web.routes.index import bp as index_bp
    from web.routes.admin import bp as admin_bp
    from web.routes.league import bp as league_bp
    from web.routes.team import bp as team_bp
    from web.routes.roster import bp as roster_bp
    from web.routes.player import bp as player_bp
    from web.routes.gboat import bp as gboat_bp
    from web.routes.news import bp as news_bp
    from web.routes.api import bp as api_bp
    
    app.register_blueprint(index_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(league_bp)
    app.register_blueprint(team_bp)
    app.register_blueprint(roster_bp)
    app.register_blueprint(player_bp)
    app.register_blueprint(gboat_bp)
    app.register_blueprint(news_bp)
    app.register_blueprint(api_bp)
    
    return app
