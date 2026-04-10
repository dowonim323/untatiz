from __future__ import annotations

import random
import time
from pathlib import Path
from typing import Any, Dict, Optional

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
    def __init__(
        self,
        accounts: list[dict] | None = None,
        proxies: list[str] | None = None,
        initial_index: int = 0,
        rate_limiter: Optional[RequestsRateLimiter] = None,
        rotation_count: int = 10,
        account_usage: Optional[Dict[str, Dict[str, Any]]] = None,
        state_dir: Optional[Path] = None,
    ):
        self.accounts = accounts or []
        self.proxies = proxies or []
        self.initial_index = initial_index
        self.rotation_count = rotation_count
        self.rate_limiter = rate_limiter or RequestsRateLimiter()
        self.state_dir = state_dir
        self._account_usage = account_usage or {}
        self._driver: Optional[_MLBParkDriver] = None
        self._session: Optional[requests.Session] = None
        self._request_count = 0

    @classmethod
    def from_config(
        cls,
        config,
        initial_index: int = 0,
        account_usage: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> "StatizClient":
        return cls(
            accounts=[],
            proxies=[],
            initial_index=initial_index,
            account_usage=account_usage,
            state_dir=config.log_dir / "mlbpark_cache",
        )

    @classmethod
    def from_credentials_file(
        cls,
        credentials_path: str,
        initial_index: int = 0,
        account_usage: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> "StatizClient":
        return cls(
            accounts=[],
            proxies=[],
            initial_index=initial_index,
            account_usage=account_usage,
            state_dir=Path(credentials_path).resolve().parent / "mlbpark_cache",
        )

    @property
    def driver(self) -> Optional[_MLBParkDriver]:
        return self._driver

    @property
    def session(self) -> Optional[requests.Session]:
        return self._session

    @property
    def current_pair(self) -> None:
        return None

    @property
    def request_count(self) -> int:
        return self._request_count

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
        self._request_count = 0
        return True

    def rotate(self) -> bool:
        return self._initialize_driver()

    def retry_current_pair(self) -> bool:
        return self._initialize_driver()

    def refresh_current_pair(self) -> bool:
        return self._initialize_driver()

    def ensure_ready(self) -> bool:
        if not self.is_ready:
            return self.rotate()
        return True

    def should_rotate(self) -> bool:
        return self._request_count >= self.rotation_count

    def increment_request_count(self) -> None:
        self._request_count += 1

    def reset_request_count(self) -> None:
        self._request_count = 0

    def wait(self) -> float:
        return self.rate_limiter.wait()

    def on_success(self) -> None:
        self.rate_limiter.on_success()

    def on_error(self) -> None:
        self.rate_limiter.on_forbidden()

    def _cleanup_driver(self) -> None:
        if self._driver is not None:
            self._driver.quit()
        self._driver = None
        self._session = None

    def cleanup(self) -> None:
        self._cleanup_driver()

    def get_next_rotation_index(self) -> int:
        return self.initial_index

    def get_account_usage_state(self) -> Dict[str, Dict[str, Any]]:
        return self._account_usage

    def __enter__(self) -> "StatizClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cleanup()


__all__ = ["RequestsRateLimiter", "StatizClient"]
