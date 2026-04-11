from __future__ import annotations

import random
import time
from typing import Any

import requests


class RequestsRateLimiter:
    def __init__(
        self,
        min_delay: float = 0.05,
        max_delay: float = 0.15,
        forbidden_penalty: float = 0.5,
    ) -> None:
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.forbidden_penalty = forbidden_penalty
        self._last_request_monotonic = 0.0
        self._extra_penalty = 0.0

    def wait(self) -> float:
        delay = random.uniform(self.min_delay, self.max_delay) + self._extra_penalty
        now = time.monotonic()
        elapsed = now - self._last_request_monotonic
        sleep_for = max(0.0, delay - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last_request_monotonic = time.monotonic()
        return sleep_for

    def on_success(self) -> None:
        self._extra_penalty = 0.0

    def on_forbidden(self) -> None:
        self._extra_penalty = max(self._extra_penalty, self.forbidden_penalty)


class _MLBParkDriver:
    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    def __init__(self, session: requests.Session):
        self.session = session
        self._season_stats_cache: dict[str, dict[str, Any]] = {}
        self._player_info_cache: dict[str, dict[str, Any]] = {}

    def build_headers(self, referer: str) -> dict[str, str]:
        return {
            "User-Agent": self.DEFAULT_USER_AGENT,
            "Referer": referer,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Origin": "https://mlbpark.donga.com",
        }

    def post_json(
        self,
        url: str,
        data: dict[str, Any],
        referer: str,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        response = self.session.post(
            url,
            headers=self.build_headers(referer),
            data=data,
            timeout=timeout,
        )
        response.raise_for_status()
        if not response.text:
            raise ValueError(f"Empty response from MLBPARK endpoint: {url}")
        return response.json()

    def quit(self) -> None:
        self.session.close()


class StatizClient:
    def __init__(self, rate_limiter: RequestsRateLimiter | None = None):
        self.rate_limiter = rate_limiter or RequestsRateLimiter()
        self._driver: _MLBParkDriver | None = None
        self._session: requests.Session | None = None

    @classmethod
    def from_config(cls, config) -> "StatizClient":
        return cls()

    @property
    def driver(self) -> _MLBParkDriver | None:
        return self._driver

    @property
    def session(self) -> requests.Session | None:
        return self._session

    @property
    def is_ready(self) -> bool:
        return self._driver is not None and self._session is not None

    def create_driver(self) -> _MLBParkDriver:
        session = requests.Session()
        session.headers.update({
            "User-Agent": _MLBParkDriver.DEFAULT_USER_AGENT,
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        })
        return _MLBParkDriver(session=session)

    def _initialize_driver(self) -> bool:
        self._cleanup_driver()
        self._driver = self.create_driver()
        self._session = self._driver.session
        return True

    def initialize_session(self) -> bool:
        return self._initialize_driver()

    def retry_current_pair(self) -> bool:
        return self._initialize_driver()

    def refresh_current_pair(self) -> bool:
        return self._initialize_driver()

    def ensure_ready(self) -> bool:
        if not self.is_ready:
            return self.initialize_session()
        return True

    def _cleanup_driver(self) -> None:
        if self._driver is not None:
            self._driver.quit()
        self._driver = None
        self._session = None

    def cleanup(self) -> None:
        self._cleanup_driver()

    def __enter__(self) -> "StatizClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cleanup()


__all__ = ["RequestsRateLimiter", "StatizClient"]
