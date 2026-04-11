"""Core utilities for Untatiz."""

from app.core.cache import (
    TTL_LONG,
    TTL_MEDIUM,
    TTL_SHORT,
    TTL_STATIC,
    cache,
    cached_query,
    invalidate,
    invalidate_after_update,
    invalidate_all,
)
from app.core.db import DatabaseManager
from app.core.logging import setup_logging
from app.core.schema import (
    DRAFT_ROUNDS,
    FANTASY_TEAMS,
    SCHEMA_SQL,
    init_fantasy_teams,
    init_schema,
    init_season,
)
from app.core.utils import (
    TEAM_SVG_MAP_2024,
    TEAM_SVG_MAP_2025,
    col_to_letter,
    get_business_date,
    get_business_year,
    get_date,
    get_kst_now,
    get_kst_timestamp,
    get_team_from_svg,
    get_time_status,
    parse_date_string,
)

__all__ = [
    # Database
    "DatabaseManager",
    # Logging
    "setup_logging",
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
