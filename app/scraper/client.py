"""Statiz scraper client - manages driver, sessions, and account rotation."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from playwright.sync_api import sync_playwright

from statiz_utils import (
    AccountProxyPairManager,
    AccountProxyPair,
    RateLimiter,
    load_multiple_accounts,
    load_proxies,
    login_statiz,
    get_session_from_driver,
)


class StatizClient:
    """Statiz scraper client with account-proxy rotation.
    
    Manages browser instances, login sessions, and automatic
    account rotation to avoid rate limiting.
    
    Usage:
        client = StatizClient.from_config(config)
        
        # Rotate to next account and get logged-in driver
        if client.rotate():
            driver = client.driver
            # Use driver for scraping
        
        # Cleanup when done
        client.cleanup()
    """
    
    class PlaywrightPageWrapper:
        def __init__(self, playwright, browser, context, page):
            self._playwright = playwright
            self._browser = browser
            self._context = context
            self.page = page

        def get(self, url: str) -> None:
            self.page.goto(url, wait_until='load')

        @property
        def page_source(self) -> str:
            return self.page.content()

        @property
        def current_url(self) -> str:
            return self.page.url

        def get_cookies(self):
            return self._context.cookies()

        def save_storage_state(self, path: str) -> None:
            self._context.storage_state(path=path)

        def quit(self) -> None:
            try:
                self.page.close()
            except Exception:
                pass
            try:
                self._context.close()
            except Exception:
                pass
            try:
                self._browser.close()
            except Exception:
                pass
            try:
                self._playwright.stop()
            except Exception:
                pass

    STABLE_USER_AGENT = (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    )
    DRIVER_RETRY_ATTEMPTS = 3

    def __init__(
        self,
        accounts: List[dict],
        proxies: List[str],
        initial_index: int = 0,
        rate_limiter: Optional[RateLimiter] = None,
        rotation_count: int = 10,
        account_usage: Optional[Dict[str, Dict[str, Any]]] = None,
        state_dir: Optional[Path] = None,
    ):
        """Initialize StatizClient.
        
        Args:
            accounts: List of account dicts with user_id and user_pw
            proxies: List of proxy URLs
            rate_limiter: Optional RateLimiter instance
            rotation_count: Rotate account after this many requests
        """
        self.rotation_count = rotation_count
        self.state_dir = state_dir
        
        # Rate limiter (create default if not provided)
        self.rate_limiter = rate_limiter or RateLimiter(min_delay=1.0, max_delay=2.0)
        
        # Account-proxy pair manager
        self.pair_manager = AccountProxyPairManager(
            accounts=accounts,
            proxies=proxies,
            initial_index=initial_index,
            account_usage=account_usage or {},
        )
        
        # Current state
        self._current_pair: Optional[AccountProxyPair] = None
        self._driver: Optional[Any] = None
        self._session: Optional[requests.Session] = None
        self._request_count: int = 0
    
    @classmethod
    def from_config(
        cls,
        config,
        initial_index: int = 0,
        account_usage: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> "StatizClient":
        """Create StatizClient from AppConfig.
        
        Args:
            config: AppConfig instance
            
        Returns:
            StatizClient: Configured client instance
        """
        credentials_path = str(config.credentials_path)
        accounts = load_multiple_accounts(credentials_path)
        proxies = load_proxies(credentials_path)
        
        return cls(
            accounts=accounts,
            proxies=proxies,
            initial_index=initial_index,
            account_usage=account_usage,
            state_dir=config.state_file.parent / "browser_states",
        )
    
    @classmethod
    def from_credentials_file(
        cls,
        credentials_path: str,
        initial_index: int = 0,
        account_usage: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> "StatizClient":
        """Create StatizClient from credentials file.
        
        Args:
            credentials_path: Path to credentials.json
            
        Returns:
            StatizClient: Configured client instance
        """
        accounts = load_multiple_accounts(credentials_path)
        proxies = load_proxies(credentials_path)
        
        return cls(
            accounts=accounts,
            proxies=proxies,
            initial_index=initial_index,
            account_usage=account_usage,
            state_dir=Path(credentials_path).resolve().parent / "browser_states",
        )
    
    @property
    def driver(self) -> Optional[Any]:
        """Current browser wrapper instance."""
        return self._driver
    
    @property
    def session(self) -> Optional[requests.Session]:
        """Current requests Session (with cookies from driver)."""
        return self._session
    
    @property
    def current_pair(self) -> Optional[AccountProxyPair]:
        """Current account-proxy pair."""
        return self._current_pair
    
    @property
    def request_count(self) -> int:
        """Number of requests since last rotation."""
        return self._request_count
    
    @property
    def is_ready(self) -> bool:
        """Check if client is ready (has active driver and session)."""
        return self._driver is not None and self._current_pair is not None
    
    def create_driver(self, proxy: Optional[str] = None, storage_state_path: Optional[Path] = None) -> Any:
        """Create a Playwright browser page with optional proxy.
        
        Args:
            proxy: Optional proxy URL
            
        Returns:
            Browser wrapper with get/page_source/current_url/get_cookies/quit
        """
        playwright = sync_playwright().start()
        launch_args = [
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-blink-features=AutomationControlled',
        ]
        launch_kwargs = {
            'headless': True,
            'args': launch_args,
            'chromium_sandbox': False,
        }
        if proxy:
            launch_kwargs['proxy'] = {'server': proxy}
        try:
            browser = playwright.chromium.launch(**launch_kwargs)
            context_kwargs = {
                'user_agent': self.STABLE_USER_AGENT,
                'viewport': {'width': 1920, 'height': 1080},
            }
            if storage_state_path is not None:
                context_kwargs['storage_state'] = str(storage_state_path)
            context = browser.new_context(
                **context_kwargs,
            )
            page = context.new_page()
            return self.PlaywrightPageWrapper(playwright, browser, context, page)
        except Exception:
            playwright.stop()
            raise
    
    def rotate(self) -> bool:
        """Rotate to next account-proxy pair and login.
        
        Creates a new browser session, logs in to Statiz, and sets up session.
        
        Returns:
            bool: True if rotation and login succeeded
        """
        if self.pair_manager is None or self.pair_manager.get_pair_count() == 0:
            print("[StatizClient] No account-proxy pairs available")
            return False
        
        # Cleanup existing driver
        self._cleanup_driver()
        
        # Get next pair
        self._current_pair = self.pair_manager.get_next()
        if self._current_pair is None:
            print("[StatizClient] Failed to get next account-proxy pair")
            return False
        return self._login_current_pair()

    def retry_current_pair(self) -> bool:
        if self._current_pair is None:
            return False
        return self._login_current_pair()

    def refresh_current_pair(self) -> bool:
        if self._current_pair is None:
            return False
        return self._login_current_pair(force_fresh_login=True)

    def _get_storage_state_path(self, pair: Optional[AccountProxyPair]) -> Optional[Path]:
        if self.state_dir is None or pair is None:
            return None
        pair_key = self.pair_manager.get_pair_key(pair)
        digest = hashlib.sha256(pair_key.encode("utf-8")).hexdigest()
        return self.state_dir / f"{digest}.json"

    def _delete_saved_storage_state(self, pair: Optional[AccountProxyPair]) -> None:
        storage_state_path = self._get_storage_state_path(pair)
        if storage_state_path is None:
            return
        try:
            storage_state_path.unlink()
        except FileNotFoundError:
            return

    def _save_current_storage_state(self) -> None:
        if self._driver is None or self._current_pair is None:
            return

        storage_state_path = self._get_storage_state_path(self._current_pair)
        if storage_state_path is None:
            return

        storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        self._driver.save_storage_state(str(storage_state_path))

    def _restore_current_pair(self) -> bool:
        if self._current_pair is None:
            return False

        storage_state_path = self._get_storage_state_path(self._current_pair)
        if storage_state_path is None or not storage_state_path.exists():
            return False

        user_id = self._current_pair.user_id
        proxy = self._current_pair.proxy

        try:
            self._cleanup_driver()
            self._driver = self.create_driver(proxy, storage_state_path=storage_state_path)
            self._session = get_session_from_driver(self._driver)
            self._current_pair.is_logged_in = True
            self.pair_manager.mark_success(self._current_pair)
            self._request_count = 0
            print(f"[StatizClient] Reused saved session: {user_id[:5]}***")
            return True
        except Exception as e:
            print(f"[StatizClient] Saved session restore failed: {user_id[:5]}*** ({e})")
            self._cleanup_driver()
            self._delete_saved_storage_state(self._current_pair)
            return False

    def _login_current_pair(self, force_fresh_login: bool = False) -> bool:
        if self._current_pair is None:
            return False

        user_id = self._current_pair.user_id
        proxy = self._current_pair.proxy

        if not force_fresh_login and self._restore_current_pair():
            return True

        if force_fresh_login:
            self._delete_saved_storage_state(self._current_pair)
        
        print(f"[StatizClient] Rotating to: {user_id[:5]}*** (proxy: {proxy[:30] if proxy else 'None'})")

        last_error = None
        for attempt in range(1, self.DRIVER_RETRY_ATTEMPTS + 1):
            try:
                self._cleanup_driver()
                self._driver = self.create_driver(proxy)

                if login_statiz(self._driver, self._current_pair.user_id, self._current_pair.user_pw):
                    self._session = get_session_from_driver(self._driver)
                    self._current_pair.is_logged_in = True
                    self.pair_manager.mark_success(self._current_pair)
                    self.pair_manager.record_login_success(self._current_pair)
                    self._save_current_storage_state()
                    self._request_count = 0
                    print(f"[StatizClient] Login success: {user_id[:5]}***")
                    return True

                last_error = "login failed"
                print(f"[StatizClient] Login attempt {attempt}/{self.DRIVER_RETRY_ATTEMPTS} failed: {user_id[:5]}***")
            except Exception as e:
                last_error = str(e)
                print(f"[StatizClient] Rotation attempt {attempt}/{self.DRIVER_RETRY_ATTEMPTS} error: {last_error}")

            self._cleanup_driver()

        self.pair_manager.mark_failed(self._current_pair)
        print(f"[StatizClient] Rotation failed after {self.DRIVER_RETRY_ATTEMPTS} attempts: {user_id[:5]}*** ({last_error})")
        return False
    
    def ensure_ready(self) -> bool:
        """Ensure client is ready, rotating if necessary.
        
        Returns:
            bool: True if client is ready
        """
        if not self.is_ready:
            return self.rotate()
        return True
    
    def should_rotate(self) -> bool:
        """Check if rotation is needed based on request count.
        
        Returns:
            bool: True if rotation is recommended
        """
        return self._request_count >= self.rotation_count
    
    def increment_request_count(self) -> None:
        """Increment the request counter."""
        self._request_count += 1
    
    def reset_request_count(self) -> None:
        """Reset the request counter."""
        self._request_count = 0
    
    def wait(self) -> float:
        """Apply rate limiting delay.
        
        Returns:
            float: Actual wait time in seconds
        """
        return self.rate_limiter.wait()
    
    def on_success(self) -> None:
        """Mark request as successful (updates rate limiter)."""
        self.rate_limiter.on_success()
    
    def on_error(self) -> None:
        """Mark request as failed (updates rate limiter)."""
        self.rate_limiter.on_forbidden()
    
    def _cleanup_driver(self) -> None:
        """Cleanup current browser session."""
        if self._driver:
            try:
                self._driver.quit()
            except Exception:
                pass
            self._driver = None
            self._session = None
    
    def cleanup(self) -> None:
        """Full cleanup - close driver and all resources."""
        self._cleanup_driver()
        if self.pair_manager:
            self.pair_manager.close_all()

    def get_next_rotation_index(self) -> int:
        return self.pair_manager.get_next_index() if self.pair_manager else 0

    def get_account_usage_state(self) -> Dict[str, Dict[str, Any]]:
        return self.pair_manager.get_account_usage_state() if self.pair_manager else {}
    
    def __enter__(self) -> "StatizClient":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - cleanup resources."""
        self.cleanup()


__all__ = ["StatizClient"]
