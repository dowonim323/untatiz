from __future__ import annotations

from pathlib import Path

from app.scraper.client import StatizClient


def test_statiz_client_rotate_initializes_requests_driver(tmp_path: Path):
    client = StatizClient(state_dir=tmp_path)

    assert client.rotate() is True
    assert client.driver is not None
    assert client.session is client.driver.session
    assert client.get_next_rotation_index() == 0
    assert client.get_account_usage_state() == {}

    client.cleanup()


def test_statiz_client_refresh_current_pair_recreates_session(tmp_path: Path):
    client = StatizClient(state_dir=tmp_path)
    assert client.rotate() is True
    first_session = client.session

    assert client.refresh_current_pair() is True
    assert client.session is not None
    assert client.session is not first_session

    client.cleanup()


def test_statiz_client_retry_current_pair_recreates_driver_when_cleaned_up(tmp_path: Path):
    client = StatizClient(state_dir=tmp_path)
    assert client.rotate() is True
    first_driver = client.driver

    client.cleanup()
    assert client.driver is None

    assert client.retry_current_pair() is True
    assert client.driver is not None
    assert client.driver is not first_driver

    client.cleanup()


def test_statiz_client_persists_player_info_cache(tmp_path: Path):
    client = StatizClient(state_dir=tmp_path)
    assert client.rotate() is True
    assert client.driver is not None
    client.driver._player_info_cache["12922"] = {"position": "SS"}
    client.cleanup()

    reloaded_client = StatizClient(state_dir=tmp_path)
    assert reloaded_client.rotate() is True
    assert reloaded_client.driver is not None
    assert reloaded_client.driver._player_info_cache == {}
    reloaded_client.cleanup()
