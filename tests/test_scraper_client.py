from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import patch

import pytest
from playwright.sync_api import sync_playwright

from app.scraper.client import StatizClient
from statiz_utils import AccountProxyPairManager


class _FakeDriver:
    def __init__(self, state_label: str):
        self.state_label = state_label
        self.quit_calls = 0

    def get_cookies(self):
        return [{"name": "auth", "value": self.state_label}]

    def save_storage_state(self, path: str) -> None:
        Path(path).write_text(self.state_label, encoding="utf-8")

    def quit(self) -> None:
        self.quit_calls += 1


def _launch_test_browser(playwright):
    try:
        return playwright.chromium.launch(headless=True)
    except Exception:
        for browser_path in ("/usr/bin/chromium", "/usr/bin/chromium-browser"):
            if Path(browser_path).exists():
                return playwright.chromium.launch(headless=True, executable_path=browser_path)
        raise


@pytest.fixture
def auth_cookie_server():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/login":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Set-Cookie", "auth=ok; Path=/; Max-Age=3600; HttpOnly")
                self.end_headers()
                self.wfile.write(b"<html><body>logged in</body></html>")
                return

            if self.path == "/protected":
                cookie_header = self.headers.get("Cookie", "")
                if "auth=ok" in cookie_header:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(b"<html><body>protected ok</body></html>")
                    return

                self.send_response(401)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"<html><body>login required</body></html>")
                return

            self.send_response(404)
            self.end_headers()

        def log_message(self, format, *args):  # noqa: A003
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_playwright_storage_state_reuses_auth_cookie_across_fresh_contexts(
    tmp_path, auth_cookie_server
):
    state_file = tmp_path / "auth-state.json"
    protected_url = f"{auth_cookie_server}/protected"
    login_url = f"{auth_cookie_server}/login"

    with sync_playwright() as playwright:
        browser = _launch_test_browser(playwright)

        fresh_context = browser.new_context()
        fresh_page = fresh_context.new_page()
        fresh_page.goto(protected_url)
        assert fresh_page.text_content("body") == "login required"
        fresh_context.close()

        first_context = browser.new_context()
        first_page = first_context.new_page()
        first_page.goto(login_url)
        assert first_page.text_content("body") == "logged in"

        first_page.goto(protected_url)
        assert first_page.text_content("body") == "protected ok"
        assert any(
            cookie.get("name") == "auth" and cookie.get("value") == "ok"
            for cookie in first_context.cookies([protected_url])
        )

        first_context.storage_state(path=str(state_file))
        first_context.close()

        reused_context = browser.new_context(storage_state=str(state_file))
        reused_page = reused_context.new_page()
        reused_page.goto(protected_url)
        assert reused_page.text_content("body") == "protected ok"
        assert any(
            cookie.get("name") == "auth" and cookie.get("value") == "ok"
            for cookie in reused_context.cookies([protected_url])
        )

        reused_context.close()
        browser.close()


def test_account_proxy_pair_manager_uses_round_robin_order_and_updates_usage():
    manager = AccountProxyPairManager(
        accounts=[
            {"user_id": "first@example.com", "user_pw": "pw"},
            {"user_id": "second@example.com", "user_pw": "pw"},
        ],
        proxies=[],
        initial_index=1,
        account_usage={
            "first@example.com|": {
                "login_count": 0,
                "last_login_at": "2026-04-09T10:00:00+09:00",
            },
            "second@example.com|": {
                "login_count": 9,
                "last_login_at": "2026-04-10T10:00:00+09:00",
            },
        },
    )

    selected_pair = manager.get_next()
    next_pair = manager.get_next()

    assert selected_pair is not None
    assert selected_pair.user_id == "second@example.com"
    assert next_pair is not None
    assert next_pair.user_id == "first@example.com"

    manager.record_login_success(selected_pair, logged_in_at="2026-04-10T12:00:00+09:00")
    usage_state = manager.get_account_usage_state()

    assert usage_state["second@example.com|"] == {
        "login_count": 10,
        "last_login_at": "2026-04-10T12:00:00+09:00",
    }


def test_statiz_client_reuses_saved_storage_state_without_fresh_login(tmp_path):
    client = StatizClient(
        accounts=[{"user_id": "reuse@example.com", "user_pw": "pw"}],
        proxies=[],
        state_dir=tmp_path,
    )
    current_pair = client.pair_manager.get_next()
    assert current_pair is not None
    client._current_pair = current_pair

    storage_state_path = client._get_storage_state_path(current_pair)
    assert storage_state_path is not None
    storage_state_path.parent.mkdir(parents=True, exist_ok=True)
    storage_state_path.write_text("saved-session", encoding="utf-8")
    reused_driver = _FakeDriver("reused-session")

    with patch.object(StatizClient, "create_driver", return_value=reused_driver) as create_driver, patch(
        "app.scraper.client.login_statiz",
        side_effect=AssertionError("fresh login should not run when saved state exists"),
    ), patch("app.scraper.client.get_session_from_driver", return_value=object()) as get_session:
        assert client.retry_current_pair() is True

    assert client.driver is reused_driver
    assert client.session is get_session.return_value
    assert create_driver.call_args.kwargs["storage_state_path"] == storage_state_path


def test_statiz_client_refresh_current_pair_recreates_saved_state(tmp_path):
    client = StatizClient(
        accounts=[{"user_id": "refresh@example.com", "user_pw": "pw"}],
        proxies=[],
        state_dir=tmp_path,
    )
    current_pair = client.pair_manager.get_next()
    assert current_pair is not None
    client._current_pair = current_pair

    storage_state_path = client._get_storage_state_path(current_pair)
    assert storage_state_path is not None
    storage_state_path.parent.mkdir(parents=True, exist_ok=True)
    storage_state_path.write_text("stale-session", encoding="utf-8")
    refreshed_driver = _FakeDriver("fresh-session")

    with patch.object(StatizClient, "create_driver", return_value=refreshed_driver) as create_driver, patch(
        "app.scraper.client.login_statiz", return_value=True
    ) as login_statiz_mock, patch(
        "app.scraper.client.get_session_from_driver", return_value=object()
    ):
        assert client.refresh_current_pair() is True

    assert login_statiz_mock.call_count == 1
    assert create_driver.call_args.kwargs.get("storage_state_path") is None
    assert storage_state_path.read_text(encoding="utf-8") == "fresh-session"
    assert client.get_account_usage_state()["refresh@example.com|"]["login_count"] == 1
