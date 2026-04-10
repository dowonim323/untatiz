"""Core utilities for Untatiz."""

from app.core.db import (
    DatabaseManager,
    get_connection,
    list_tables,
    load_table,
    save_table,
    backup_database,
)
from app.core.logging import setup_logging, StreamToLogger
from app.core.utils import (
    get_date,
    get_business_date,
    get_business_year,
    get_kst_now,
    get_kst_timestamp,
    get_time_status,
    col_to_letter,
    parse_date_string,
    get_team_from_svg,
    TEAM_SVG_MAP_2025,
    TEAM_SVG_MAP_2024,
)
from app.core.cache import (
    cache,
    cached_query,
    invalidate,
    invalidate_all,
    invalidate_after_update,
    TTL_SHORT,
    TTL_MEDIUM,
    TTL_LONG,
    TTL_STATIC,
)
from app.core.schema import (
    SCHEMA_SQL,
    DRAFT_ROUNDS,
    FANTASY_TEAMS,
    init_schema,
    init_fantasy_teams,
    init_season,
)

__all__ = [
    # Database
    "DatabaseManager",
    "get_connection",
    "list_tables",
    "load_table",
    "save_table",
    "backup_database",
    # Logging
    "setup_logging",
    "StreamToLogger",
    # Utils
    "get_date",
    "get_business_date",
    "get_business_year",
    "get_kst_now",
    "get_kst_timestamp",
    "get_time_status",
    "col_to_letter",
    "parse_date_string",
    "get_team_from_svg",
    "TEAM_SVG_MAP_2025",
    "TEAM_SVG_MAP_2024",
    # Cache
    "cache",
    "cached_query",
    "invalidate",
    "invalidate_all",
    "invalidate_after_update",
    "TTL_SHORT",
    "TTL_MEDIUM",
    "TTL_LONG",
    "TTL_STATIC",
    # Schema
    "SCHEMA_SQL",
    "DRAFT_ROUNDS",
    "FANTASY_TEAMS",
    "init_schema",
    "init_fantasy_teams",
    "init_season",
]
