#!/usr/bin/env python3
"""Untatiz scraper - Korean Baseball Fantasy League data collection.

This is the main entrypoint for the batch scraper.
Designed to run as a 1-minute batch job via cron or supervisor.

Usage:
    python untatiz.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError

from app.config.settings import load_config
from app.core.logging import setup_logging
from app.core.schema import ensure_runtime_db
from app.scraper.client import StatizClient
from app.scraper.parsers import StatizLoginRequiredError
from app.scraper.scheduler import (
    load_state,
    save_state_dict,
    check_should_update,
    persist_schedule_snapshot,
    run_update,
)
from app.core.utils import get_business_year, get_date

CONTAINER_START_MARKER_FILE = Path('/app/log/container_start_marker')
MAX_BROWSER_RECOVERY_ATTEMPTS = 3

def main() -> int:
    """Main entrypoint for batch scraper.
    
    Single execution model:
    1. Load state from previous run
    2. Prepare driver (rotate if needed)
    3. Check if update is needed
    4. Execute update if needed
    5. Save state
    6. Cleanup and exit
    
    Returns:
        int: Exit code (0 = success, 1 = error)
    """
    # Load configuration
    config = load_config()
    season_year = get_business_year()
    ensure_runtime_db(config.db_path, season_year)
    
    # Setup logging
    log_path = config.log_dir / "untatiz.log"
    setup_logging(log_path)
    
    print(f"[Main] Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load previous state
    state = load_state(config.state_file)
    runtime_start_marker = None
    if CONTAINER_START_MARKER_FILE.exists():
        runtime_start_marker = CONTAINER_START_MARKER_FILE.read_text(encoding='utf-8').strip() or None
    startup_force_check = runtime_start_marker is not None and state.get('startup_marker') != runtime_start_marker
    consumed_startup_marker = False
    # Create client
    try:
        client = StatizClient.from_config(
            config,
            initial_index=state.get("rotation_index", 0),
            account_usage=state.get("account_usage"),
        )
    except Exception as e:
        print(f"[Main] Failed to create client: {str(e)}")
        return 1
    
    try:
        if not client.rotate():
            state["rotation_index"] = client.get_next_rotation_index()
            state["account_usage"] = client.get_account_usage_state()
            print("[Error] Failed to prepare driver")
            save_state_dict(config.state_file, state)
            return 1

        state["rotation_index"] = client.get_next_rotation_index()
        
        driver = client.driver
        
        browser_attempt = 0
        while True:
            try:
                # Check if update is needed
                should_update, new_state, reason = check_should_update(
                    driver=driver,
                    state=state,
                    db_path=config.db_path,
                    rate_limiter=client.rate_limiter,
                    year=season_year,
                    force_check=startup_force_check,
                )
                print(f"[Main] Decision: {reason}")
                if startup_force_check:
                    state['startup_marker'] = runtime_start_marker
                    consumed_startup_marker = True

                # Execute update if needed
                if should_update:
                    backup_dir = config.base_dir / "backup"
                    success, final_status, transitioned_to_completed = run_update(
                        driver=driver,
                        db_path=config.db_path,
                        backup_dir=backup_dir,
                        rate_limiter=client.rate_limiter,
                        year=season_year,
                        webhook_url=config.discord_webhook_url
                    )

                    if not success:
                        failure_state = {
                            **state,
                            "request_count": 0,
                            "rotation_index": state["rotation_index"],
                            "account_usage": client.get_account_usage_state(),
                        }
                        save_state_dict(config.state_file, failure_state)
                        print("[Main] Full update failed")
                        return 1
                    elif final_status == 'no_games':
                        new_state['cancelled_update_business_date'] = get_date()
                    elif final_status == 'completed' and reason == "every_5min: ready for full update":
                        new_state["postgame_update_completed"] = True
                        new_state["postgame_update_business_date"] = get_date()
                        new_state["mode"] = "hourly"
                    elif final_status == 'completed' and reason == "hourly: postgame follow-up full update":
                        new_state["postgame_update_completed"] = True
                        new_state["postgame_update_business_date"] = get_date()
                        new_state["mode"] = "hourly"
                    elif reason == "every_5min: ready for full update":
                        new_state["mode"] = "every_5min"
                        new_state["postgame_update_completed"] = False
                        new_state["postgame_update_business_date"] = None

                new_state["rotation_index"] = state["rotation_index"]
                new_state["startup_marker"] = state.get("startup_marker")
                new_state["account_usage"] = client.get_account_usage_state()
                if not should_update and not reason.startswith("skip") and reason not in {
                    "every_30min: no games today",
                    "every_30min: no games already handled",
                }:
                    snapshot_status = persist_schedule_snapshot(
                        driver=driver,
                        db_path=config.db_path,
                        rate_limiter=client.rate_limiter,
                        year=season_year,
                    )
                    print(f"[Main] Snapshot persisted with status: {snapshot_status}")
                break
            except StatizLoginRequiredError as e:
                browser_attempt += 1
                print(f"[Main] Session expired {browser_attempt}/{MAX_BROWSER_RECOVERY_ATTEMPTS}: {e}")
                if browser_attempt >= MAX_BROWSER_RECOVERY_ATTEMPTS:
                    raise
                if client.refresh_current_pair():
                    driver = client.driver
                    continue
                if not client.rotate():
                    raise
                state["rotation_index"] = client.get_next_rotation_index()
                driver = client.driver
                continue
            except (PlaywrightError, PlaywrightTimeoutError) as e:
                browser_attempt += 1
                print(f"[Main] Browser error {browser_attempt}/{MAX_BROWSER_RECOVERY_ATTEMPTS}: {e}")
                if browser_attempt >= MAX_BROWSER_RECOVERY_ATTEMPTS:
                    raise
                if client.retry_current_pair():
                    driver = client.driver
                    continue
                if not client.rotate():
                    raise
                state["rotation_index"] = client.get_next_rotation_index()
                driver = client.driver
                continue
        
        # Save state
        save_state_dict(config.state_file, new_state)
        
        print(f"[Main] Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return 0
        
    except Exception as e:
        print(f"[Main] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        
        state["request_count"] = 0
        state["rotation_index"] = client.get_next_rotation_index()
        state["account_usage"] = client.get_account_usage_state()
        if not consumed_startup_marker:
            state["startup_marker"] = runtime_start_marker
        save_state_dict(config.state_file, state)
        return 1
        
    finally:
        # Always cleanup
        client.cleanup()


if __name__ == "__main__":
    sys.exit(main())
