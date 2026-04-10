from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.scraper.parsers import StatizLoginRequiredError
from app.scraper.scheduler import UpdateMode, check_should_update, load_state, save_state_dict
import untatiz


def test_check_should_update_skips_without_driver_when_not_check_time(temp_db):
    state = {
        "mode": UpdateMode.EVERY_30MIN.value,
        "request_count": 2,
    }

    with patch("app.scraper.scheduler.should_check_now", return_value=False), patch(
        "app.scraper.scheduler.get_next_check_minute", return_value=":30"
    ), patch(
        "app.scraper.scheduler.get_team_status",
        side_effect=AssertionError("driver path should not run when check time is skipped"),
    ), patch("app.scraper.scheduler.get_date", return_value="2026-04-10"):
        should_update, new_state, reason = check_should_update(
            driver=object(),
            state=state,
            db_path=temp_db,
        )

    assert should_update is False
    assert new_state["request_count"] == 3
    assert reason == "skip (not check time, next: :30)"


def test_check_should_update_touches_driver_once_check_time_arrives(temp_db):
    state = {
        "mode": UpdateMode.EVERY_30MIN.value,
        "request_count": 0,
    }
    driver = object()
    seen: dict[str, object] = {}

    def fake_get_team_status(actual_driver, rate_limiter=None, year=None):
        seen["driver"] = actual_driver
        return 0, 0, 1

    with patch("app.scraper.scheduler.should_check_now", return_value=True), patch(
        "app.scraper.scheduler.get_team_status", side_effect=fake_get_team_status
    ), patch("app.scraper.scheduler.get_date", return_value="2026-04-10"):
        should_update, new_state, reason = check_should_update(
            driver=driver,
            state=state,
            db_path=temp_db,
        )

    assert seen["driver"] is driver
    assert should_update is False
    assert new_state["mode"] == UpdateMode.EVERY_30MIN.value
    assert reason == "every_30min: games not started yet"


def test_check_should_update_can_short_circuit_postgame_followup_without_driver(temp_db):
    state = {
        "mode": UpdateMode.HOURLY.value,
        "request_count": 0,
        "postgame_update_completed": True,
        "postgame_update_business_date": "2026-04-10",
    }

    with patch("app.scraper.scheduler.should_check_now", return_value=True), patch(
        "app.scraper.scheduler.get_team_status",
        side_effect=AssertionError("driver path should not run for same-day postgame follow-up"),
    ), patch("app.scraper.scheduler.get_date", return_value="2026-04-10"):
        should_update, new_state, reason = check_should_update(
            driver=object(),
            state=state,
            db_path=temp_db,
        )

    assert should_update is True
    assert new_state["request_count"] == 1
    assert reason == "hourly: postgame follow-up full update"


def test_check_should_update_runs_full_update_every_5min_once_games_end(temp_db):
    state = {
        "mode": UpdateMode.EVERY_5MIN.value,
        "request_count": 0,
    }

    with patch("app.scraper.scheduler.should_check_now", return_value=True), patch(
        "app.scraper.scheduler.get_team_status", return_value=(2, 2, 2)
    ), patch("app.scraper.scheduler.get_date", return_value="2026-04-10"):
        should_update, new_state, reason = check_should_update(
            driver=object(),
            state=state,
            db_path=temp_db,
        )

    assert should_update is True
    assert new_state["mode"] == UpdateMode.EVERY_5MIN.value
    assert reason == "every_5min: ready for full update"


def test_main_rotates_client_before_scheduler_skip_decision(temp_db, tmp_path):
    state_file = tmp_path / "scraper_state.json"
    config = SimpleNamespace(
        db_path=temp_db,
        log_dir=tmp_path,
        state_file=state_file,
        base_dir=tmp_path,
        discord_webhook_url=None,
    )

    class DummyClient:
        def __init__(self):
            self.driver = object()
            self.rate_limiter = None
            self.rotate_calls = 0
            self.cleanup_calls = 0

        def rotate(self) -> bool:
            self.rotate_calls += 1
            return True

        def get_next_rotation_index(self) -> int:
            return 1

        def get_account_usage_state(self):
            return {}

        def cleanup(self) -> None:
            self.cleanup_calls += 1

    client = DummyClient()
    loaded_state = {
        "mode": UpdateMode.EVERY_30MIN.value,
        "rotation_index": 0,
        "request_count": 0,
    }
    skipped_state = {
        **loaded_state,
        "request_count": 1,
    }

    with patch("untatiz.load_config", return_value=config), patch(
        "untatiz.ensure_runtime_db"
    ), patch("untatiz.setup_logging"), patch(
        "untatiz.get_business_year", return_value=2026
    ), patch("untatiz.load_state", return_value=loaded_state), patch(
        "untatiz.save_state_dict"
    ), patch(
        "untatiz.check_should_update",
        return_value=(False, skipped_state, "skip (not check time, next: :30)"),
    ), patch.object(untatiz.StatizClient, "from_config", return_value=client):
        exit_code = untatiz.main()

    assert exit_code == 0
    assert client.rotate_calls == 1
    assert client.cleanup_calls == 1


def test_scheduler_state_round_trips_account_usage(tmp_path):
    state_file = Path(tmp_path) / "scraper_state.json"
    original_state = {
        "mode": UpdateMode.EVERY_30MIN.value,
        "team_previous": {"LG"},
        "request_count": 3,
        "rotation_index": 1,
        "account_usage": {
            "reuse@example.com|": {
                "login_count": 2,
                "last_login_at": "2026-04-10T12:00:00+09:00",
            }
        },
    }

    save_state_dict(state_file, original_state)
    loaded_state = load_state(state_file)

    assert loaded_state["team_previous"] == {"LG"}
    assert loaded_state["account_usage"] == original_state["account_usage"]


def test_main_refreshes_expired_session_and_retries(temp_db, tmp_path):
    state_file = tmp_path / "scraper_state.json"
    config = SimpleNamespace(
        db_path=temp_db,
        log_dir=tmp_path,
        state_file=state_file,
        base_dir=tmp_path,
        discord_webhook_url=None,
    )

    class DummyClient:
        def __init__(self):
            self.driver = object()
            self.rate_limiter = None
            self.rotate_calls = 0
            self.refresh_calls = 0
            self.cleanup_calls = 0

        def rotate(self) -> bool:
            self.rotate_calls += 1
            self.driver = object()
            return True

        def refresh_current_pair(self) -> bool:
            self.refresh_calls += 1
            self.driver = object()
            return True

        def get_next_rotation_index(self) -> int:
            return 1

        def get_account_usage_state(self):
            return {
                "refresh@example.com|": {
                    "login_count": 1,
                    "last_login_at": "2026-04-10T12:00:00+09:00",
                }
            }

        def cleanup(self) -> None:
            self.cleanup_calls += 1

    client = DummyClient()
    loaded_state = {
        "mode": UpdateMode.EVERY_30MIN.value,
        "rotation_index": 0,
        "request_count": 0,
        "account_usage": {},
    }
    final_state = {
        **loaded_state,
        "request_count": 1,
        "account_usage": client.get_account_usage_state(),
    }
    call_count = {"check_should_update": 0}

    def fake_check_should_update(*args, **kwargs):
        call_count["check_should_update"] += 1
        if call_count["check_should_update"] == 1:
            raise StatizLoginRequiredError("expired session")
        return False, final_state, "skip (not check time, next: :30)"

    with patch("untatiz.load_config", return_value=config), patch(
        "untatiz.ensure_runtime_db"
    ), patch("untatiz.setup_logging"), patch(
        "untatiz.get_business_year", return_value=2026
    ), patch("untatiz.load_state", return_value=loaded_state), patch(
        "untatiz.save_state_dict"
    ) as save_state_mock, patch(
        "untatiz.check_should_update", side_effect=fake_check_should_update
    ), patch.object(untatiz.StatizClient, "from_config", return_value=client):
        exit_code = untatiz.main()

    assert exit_code == 0
    assert client.rotate_calls == 1
    assert client.refresh_calls == 1
    assert client.cleanup_calls == 1
    assert call_count["check_should_update"] == 2
    assert save_state_mock.call_args.args[1]["account_usage"] == client.get_account_usage_state()
