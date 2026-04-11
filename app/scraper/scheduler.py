"""Scraper scheduler - manages update timing and state persistence."""

from __future__ import annotations

import json
import re
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Set, Tuple

import pandas as pd
import pytz
from requests import RequestException

from app.core.db import DatabaseManager
from app.core.utils import get_business_year, get_date
from app.scraper.parsers import update_games

if TYPE_CHECKING:
    from app.scraper.client import RequestsRateLimiter


WebDriver = Any


class UpdateMode(Enum):
    """Update check frequency modes.
    
    State transitions:
        EVERY_30MIN -> EVERY_5MIN: When games start (started > 0)
        EVERY_5MIN -> HOURLY: After successful completion for the day
    """
    EVERY_30MIN = "every_30min"
    HOURLY = "hourly"
    EVERY_5MIN = "every_5min"


def should_check_now(mode: UpdateMode) -> bool:
    """Determine if we should run a check at the current minute.
    
    Args:
        mode: Current update mode
        
    Returns:
        bool: True if check should run now
    """
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    minute = now.minute
    
    if mode == UpdateMode.EVERY_30MIN:
        return minute in (0, 30)
    elif mode == UpdateMode.HOURLY:
        return minute == 0
    else:
        return minute % 5 == 0


def get_next_check_minute(mode: UpdateMode) -> str:
    """Get description of next check time.
    
    Args:
        mode: Current update mode
        
    Returns:
        str: Description like ":00", ":10", "every minute"
    """
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    minute = now.minute
    
    if mode == UpdateMode.EVERY_30MIN:
        next_min = 30 if minute < 30 else 0
        suffix = " (next hour)" if next_min == 0 else ""
        return f":{next_min:02d}{suffix}"
    elif mode == UpdateMode.HOURLY:
        return ":00 (next hour)"
    else:
        next_min = ((minute // 5) + 1) * 5 % 60
        return f":{next_min:02d}"


def load_state(state_file: Path) -> Dict:
    """Load previous execution state from file.
    
    Args:
        state_file: Path to state JSON file
        
    Returns:
        dict: State with keys including mode, team_previous, request_count
    """
    default_state = {
        "mode": UpdateMode.EVERY_30MIN.value,
        "team_previous": set(),
        "request_count": 0,
        "startup_marker": None,
        "cancelled_update_business_date": None,
        "postgame_update_completed": False,
        "postgame_update_business_date": None,
    }
    
    try:
        if state_file.exists():
            with open(state_file, "r") as f:
                state = json.load(f)
                # Convert list back to set
                state["team_previous"] = set(state.get("team_previous", []))
                state.pop("time_previous", None)
                state.pop("update_status", None)
                state.setdefault("startup_marker", None)
                state.setdefault("cancelled_update_business_date", None)
                # Ensure mode exists (migration from old format)
                if "mode" not in state:
                    state["mode"] = UpdateMode.EVERY_30MIN.value
                state.setdefault("postgame_update_completed", False)
                state.setdefault("postgame_update_business_date", None)
                return state
    except Exception as e:
        print(f"[State] Failed to load state file: {str(e)}")
    
    return default_state


def save_state_dict(state_file: Path, state: Dict) -> None:
    """Save state dictionary to file.
    
    Args:
        state_file: Path to state JSON file
        state: State dictionary to save
    """
    # Prepare state for JSON serialization
    state_to_save = {
        **state,
        "team_previous": list(state.get("team_previous", [])),
    }
    
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(state_file, "w") as f:
            json.dump(state_to_save, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[State] Failed to save state file: {str(e)}")


def get_war_status(db_path: Path) -> int:
    """Get WAR update status from scraper_status table.
    
    Args:
        db_path: Path to database
        
    Returns:
        int: 1 if WAR is updated, 0 if pending
    """
    db = DatabaseManager(db_path)
    try:
        with db.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT war_status FROM scraper_status WHERE id = 1")
            row = cursor.fetchone()
            if row and row[0] == 'completed':
                return 1
            elif row and row[0] == 'pending':
                return 0
    except Exception:
        pass
    return 0


def get_team_status(
    driver: "WebDriver",
    rate_limiter: "RequestsRateLimiter | None" = None,
    year: int | None = None
) -> Tuple[int, int, int]:
    """Get game status for today.
    
    Args:
        driver: Scraper driver/session wrapper
        rate_limiter: Optional rate limiter
        year: Season year
        
    Returns:
        Tuple of (started_games, updated_games, total_games)
    """
    year = year or get_business_year()
    games_df, started = update_games(driver, rate_limiter, "started", year)
    games = games_df.values.tolist()
    
    if len(games) > 1 and games[1][0] == '오늘은 경기가 없습니다.':
        return 0, 0, 0
    
    # Parse "업데이트 : X/Y경기" format
    if len(games) > 1:
        match = re.search(r'(\d+)/(\d+)', games[1][0])
        if match:
            updated = int(match.group(1))
            total = int(match.group(2))
            return started, updated, total
    
    return 0, 0, 0


def get_playing_teams(
    driver: "WebDriver",
    rate_limiter: "RequestsRateLimiter | None" = None,
    year: int | None = None
) -> Set[str]:
    """Get the set of teams playing in today's games.
    
    Extracts team names from the game schedule for the current business date.
    
    Args:
        driver: Scraper driver/session wrapper
        rate_limiter: Optional rate limiter
        year: Season year
        
    Returns:
        Set[str]: Set of team names (e.g., {'KIA', 'LG', 'SSG', '두산'})
    """
    year = year or get_business_year()
    games_result = update_games(driver, rate_limiter, "df", year)
    if isinstance(games_result, tuple):
        games_df = games_result[0]
    else:
        games_df = games_result
    games = games_df.values.tolist()
    
    if len(games) > 1 and games[1][0] == '오늘은 경기가 없습니다.':
        return set()
    
    playing_teams = set()
    # Skip header rows (date and update count)
    for row in games[2:]:
        if len(row) >= 3:
            away_team = row[0]
            status = row[1] if len(row) > 1 else ''
            home_team = row[2]
            if status == '우천취소':
                continue
            if away_team and away_team not in ('', '우천취소'):
                playing_teams.add(away_team)
            if home_team and home_team not in ('', '우천취소'):
                playing_teams.add(home_team)
    
    return playing_teams


def has_cancelled_games(
    driver: "WebDriver",
    rate_limiter: "RequestsRateLimiter | None" = None,
    year: int | None = None
) -> bool:
    year = year or get_business_year()
    games_result = update_games(driver, rate_limiter, "df", year)
    games_df = games_result[0] if isinstance(games_result, tuple) else games_result
    games = games_df.values.tolist()

    if len(games) <= 2:
        return False

    for row in games[2:]:
        if len(row) < 3:
            continue
        away_team = str(row[0]).strip()
        status = str(row[1]).strip()
        home_team = str(row[2]).strip()
        if not away_team or not home_team:
            continue
        if status == '우천취소':
            return True
    return False


def persist_schedule_snapshot(
    driver: "WebDriver",
    db_path: Path,
    rate_limiter: "RequestsRateLimiter | None" = None,
    year: int | None = None,
) -> str:
    year = year or get_business_year()
    games_result = update_games(driver, rate_limiter, "df", year)
    games_df = games_result[0] if isinstance(games_result, tuple) else games_result

    from app.core.db import DatabaseManager
    from app.scraper.jobs import _update_info_table

    db = DatabaseManager(db_path)
    _update_info_table(db, pd.DataFrame(), pd.DataFrame(), games_df)
    status_row = db.fetch_one("SELECT war_status FROM scraper_status WHERE id = 1")
    return status_row[0] if status_row else 'pending'


def check_should_update(
    driver: "WebDriver",
    state: Dict,
    db_path: Path,
    rate_limiter: "RequestsRateLimiter | None" = None,
    year: int | None = None,
    force_check: bool = False,
) -> Tuple[bool, Dict, str]:
    """State machine based update detection algorithm.
    
    State Machine:
        EVERY_30MIN -> EVERY_5MIN: When games start (started > 0)
        EVERY_5MIN -> full update: When all active games are final (updated == total)
        EVERY_5MIN -> HOURLY: After the full update marks WAR completion in the DB
    
    Check Frequencies:
        EVERY_30MIN: At :00 and :30
        EVERY_5MIN: At :00, :05, :10, :15, :20, :25, :30, :35, :40, :45, :50, :55
    
    Args:
        driver: Scraper driver/session wrapper
        state: Previous execution state
        db_path: Path to database
        rate_limiter: Optional rate limiter
        year: Season year
        
    Returns:
        Tuple of (should_update, new_state, reason)
    """
    # Parse current mode from state
    mode_str = state.get("mode", UpdateMode.EVERY_30MIN.value)
    try:
        current_mode = UpdateMode(mode_str)
    except ValueError:
        current_mode = UpdateMode.EVERY_30MIN
    
    request_count = state.get("request_count", 0) + 1
    current_business_date = get_date()
    year = year or get_business_year()
    base_state = {
        key: value
        for key, value in state.items()
        if key not in {"time_previous", "update_status"}
    }
    base_state.update({
        "request_count": request_count,
    })
    if base_state.get("cancelled_update_business_date") != current_business_date:
        base_state["cancelled_update_business_date"] = None

    # Check if we should even run at this minute
    if not force_check and not should_check_now(current_mode):
        next_check = get_next_check_minute(current_mode)
        return False, base_state, f"skip (not check time, next: {next_check})"

    if current_mode == UpdateMode.HOURLY:
        if state.get("postgame_update_completed", False):
            if state.get("postgame_update_business_date") == current_business_date:
                return True, base_state, "hourly: postgame follow-up full update"
            base_state["postgame_update_completed"] = False
            base_state["postgame_update_business_date"] = None
            current_mode = UpdateMode.EVERY_30MIN
            base_state["mode"] = current_mode.value
        else:
            current_mode = UpdateMode.EVERY_30MIN
            base_state["mode"] = current_mode.value
    
    started, updated, total = get_team_status(driver, rate_limiter, year)
    
    # Build new state template
    new_state = {
        **base_state,
        "mode": current_mode.value,
    }
    
    # =================================================================
    # State Machine Logic
    # =================================================================
    
    if current_mode == UpdateMode.EVERY_30MIN:
        if total == 0:
            if state.get("cancelled_update_business_date") != current_business_date:
                return True, new_state, "every_30min: no games full update"
            new_state["postgame_update_completed"] = False
            new_state["postgame_update_business_date"] = None
            new_state["cancelled_update_business_date"] = current_business_date
            return False, new_state, "every_30min: no games today"
        
        if started > 0:
            new_state["mode"] = UpdateMode.EVERY_5MIN.value
            new_state["postgame_update_completed"] = False
            new_state["postgame_update_business_date"] = None
            new_state["cancelled_update_business_date"] = None
            print("[Scheduler] Mode: EVERY_30MIN -> EVERY_5MIN (games started)")
            return False, new_state, "every_30min->every_5min: games started"
        
        new_state["postgame_update_completed"] = False
        new_state["postgame_update_business_date"] = None
        new_state["cancelled_update_business_date"] = None
        return False, new_state, "every_30min: games not started yet"
    
    else:
        if total == 0:
            new_state["mode"] = UpdateMode.EVERY_30MIN.value
            new_state["cancelled_update_business_date"] = current_business_date
            return False, new_state, "every_5min: no active games today"

        if updated < total:
            return False, new_state, f"every_5min: waiting for game final ({updated}/{total})"

        print("[Scheduler] EVERY_5MIN ready for full update")
        return True, new_state, "every_5min: ready for full update"


def run_update(
    driver: "WebDriver",
    db_path: Path,
    backup_dir: Path,
    rate_limiter: "RequestsRateLimiter | None" = None,
    year: int | None = None,
    webhook_url: str | None = None,
) -> tuple[bool, str, bool]:
    """Execute the update process.
    
    Loads data, scrapes stats, calculates WAR, and updates database.
    
    Args:
        driver: Scraper driver/session wrapper
        db_path: Path to database
        backup_dir: Path for backup directory
        rate_limiter: Optional rate limiter
        year: Season year
        webhook_url: Optional Discord webhook URL for notifications
        
    Returns:
        Tuple of (success, final_war_status, transitioned_to_completed)
    """
    from app.scraper.jobs import backup_db, update_db
    from app.scraper.parsers import load_statiz_bat, load_statiz_pit, update_games
    from app.services.data_loader import load_data
    from app.services.war_calculator import get_war

    year = year or get_business_year()
    db = DatabaseManager(db_path)
    
    print(f"update started at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
    
    try:
        previous_status_row = db.fetch_one("SELECT war_status FROM scraper_status WHERE id = 1")
        previous_status = previous_status_row[0] if previous_status_row else 'pending'
        season_id = db.fetch_one("SELECT id FROM seasons WHERE is_active = 1")
        active_season_id = int(season_id[0]) if season_id else 1
        player_name, player_id, player_activation, war_basis, transaction = load_data(
            db_path,
            season_id=active_season_id,
        )
        
        games_result = update_games(driver, rate_limiter, "df", year)
        games = games_result[0] if isinstance(games_result, tuple) else games_result
        bat = load_statiz_bat(driver, rate_limiter, year)
        pit = load_statiz_pit(driver, rate_limiter, year)
        
        live_war, current_war = get_war(bat, pit, player_id, player_activation, war_basis)
        
        update_db(
            player_name, player_id, player_activation,
            live_war, current_war, bat, pit, games, db_path
        )
        
        backup_db(db_path, backup_dir)
        
        print(f"update finished at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
        
        db = DatabaseManager(db_path)
        final_status_row = db.fetch_one("SELECT war_status FROM scraper_status WHERE id = 1")
        final_status = final_status_row[0] if final_status_row else 'pending'
        transitioned_to_completed = previous_status == 'pending' and final_status == 'completed'
        if webhook_url and transitioned_to_completed:
            from app.config.settings import load_config
            from app.services.notification import notify_update_complete

            config = load_config()
            notify_update_complete(webhook_url, config)
        
        return True, final_status, transitioned_to_completed
        
    except RequestException:
        raise

    except Exception as e:
        print(f"[Update] Error during update: {str(e)}")
        import traceback
        traceback.print_exc()
        current_status_row = db.fetch_one("SELECT war_status FROM scraper_status WHERE id = 1")
        current_status = current_status_row[0] if current_status_row else 'pending'
        return False, current_status, False


__all__ = [
    # State management
    "load_state",
    "save_state_dict",
    # Status functions
    "get_war_status",
    "get_team_status",
    "get_playing_teams",
    # Update mode
    "UpdateMode",
    "should_check_now",
    "get_next_check_minute",
    # Update detection
    "check_should_update",
    # Update execution
    "run_update",
]
