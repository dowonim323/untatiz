"""Scraper modules for Untatiz."""

from app.scraper.client import StatizClient
from app.scraper.parsers import (
    load_statiz_bat,
    load_statiz_pit,
    update_games,
    BAT_COLUMNS,
    PIT_COLUMNS,
)
from app.scraper.jobs import (
    update_db,
    check_update,
    backup_db,
)
from app.scraper.scheduler import (
    load_state,
    get_war_status,
    get_team_status,
    check_should_update,
    run_update,
)

__all__ = [
    # Client
    "StatizClient",
    # Parsers
    "load_statiz_bat",
    "load_statiz_pit",
    "update_games",
    "BAT_COLUMNS",
    "PIT_COLUMNS",
    # Jobs
    "update_db",
    "check_update",
    "backup_db",
    # Scheduler
    "load_state",
    "get_war_status",
    "get_team_status",
    "check_should_update",
    "run_update",
]
