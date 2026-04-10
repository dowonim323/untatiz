"""Core utility functions for Untatiz."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Tuple

import pytz


def get_business_date() -> date:
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    if now.hour < 14:
        return (now - timedelta(days=1)).date()
    return now.date()


def get_date() -> str:
    return get_business_date().strftime('%m/%d')


def get_business_year() -> int:
    return get_business_date().year


def get_kst_now() -> datetime:
    """Get current KST datetime.
    
    Returns:
        datetime: Current datetime in KST
    """
    kst = pytz.timezone('Asia/Seoul')
    return datetime.now(kst)


def get_kst_timestamp() -> str:
    """Get current KST timestamp string.
    
    Returns:
        str: Timestamp in 'YYYY-MM-DD HH:MM:SS' format
    """
    return get_kst_now().strftime('%Y-%m-%d %H:%M:%S')


def get_time_status() -> int:
    """Get time status for update scheduling.
    
    Returns 0 if in first half of hour (0-29 minutes).
    Returns 1 if in second half of hour (30-59 minutes).
    
    Returns:
        int: 0 or 1 based on current minute
    """
    now = datetime.now()
    minute = now.minute
    return 0 if minute < 30 else 1


def col_to_letter(col: int) -> str:
    """Convert column number to Excel-style letter.
    
    Args:
        col: Column number (1-based)
        
    Returns:
        str: Column letter (A, B, ..., Z, AA, AB, ...)
    """
    letter = ''
    while col > 0:
        col, remainder = divmod(col - 1, 26)
        letter = chr(65 + remainder) + letter
    return letter


def parse_date_string(date_str: str) -> Tuple[int, int]:
    """Parse date string 'MM/DD' to month and day.
    
    Args:
        date_str: Date string in 'MM/DD' format
        
    Returns:
        Tuple[int, int]: (month, day)
    """
    parts = date_str.split('/')
    return int(parts[0]), int(parts[1])


TEAM_ID_MAP = {
    "2002": "KIA",
    "12001": "KT",
    "10001": "키움",
    "11001": "NC",
    "5002": "LG",
    "1001": "삼성",
    "6002": "두산",
    "7002": "한화",
    "9002": "SSG",
    "3001": "롯데",
}

TEAM_SVG_MAP_2025 = {f"/data/team/ci/2025/{team_id}.svg": name for team_id, name in TEAM_ID_MAP.items()}
TEAM_SVG_MAP_2024 = {f"/data/team/ci/2024/{team_id}.svg": name for team_id, name in TEAM_ID_MAP.items()}


def get_team_from_svg(svg_path: str, year: int | None = None) -> str:
    return TEAM_ID_MAP.get(Path(svg_path).stem, "")


__all__ = [
    "get_date",
    "get_business_date",
    "get_business_year",
    "get_kst_now",
    "get_kst_timestamp",
    "get_time_status",
    "col_to_letter",
    "parse_date_string",
    "get_team_from_svg",
    "TEAM_ID_MAP",
    "TEAM_SVG_MAP_2025",
    "TEAM_SVG_MAP_2024",
]
