"""Statiz.co.kr 유틸리티 모듈 - 자격 증명 관리, 로그인, 데이터 스크래핑"""

import json
import random
import threading
import time
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Tuple, Union, cast

import requests
import pandas as pd
from bs4 import BeautifulSoup


class SupportsQuit(Protocol):
    def quit(self) -> None:
        ...


def _to_numeric_series(values: Any) -> pd.Series:
    return cast(pd.Series, pd.to_numeric(values, errors='coerce'))


def _sort_by_year(df: Any) -> pd.DataFrame:
    frame = cast(pd.DataFrame, df)
    return cast(pd.DataFrame, frame.sort_values(by='Year').reset_index(drop=True))


# =============================================================================
# Custom Exceptions
# =============================================================================

class StatizError(Exception):
    """Statiz 스크래핑 관련 기본 예외"""
    pass


class ForbiddenError(StatizError):
    """403 Forbidden 에러"""
    def __init__(self, url: str, proxy: Optional[str] = None):
        self.url = url
        self.proxy = proxy
        super().__init__(f"403 Forbidden: {url}" + (f" (proxy: {proxy})" if proxy else ""))


class RateLimitError(StatizError):
    """Rate limit 초과 (429 또는 연속 403)"""
    def __init__(self, message: str = "Rate limit exceeded"):
        super().__init__(message)


class MaxRetriesExceededError(StatizError):
    """최대 재시도 횟수 초과"""
    def __init__(self, url: str, attempts: int):
        self.url = url
        self.attempts = attempts
        super().__init__(f"Max retries ({attempts}) exceeded for: {url}")


# =============================================================================
# Rate Limiter
# =============================================================================

@dataclass
class RateLimiter:
    """
    요청 간 딜레이를 관리하는 Rate Limiter.
    
    Args:
        min_delay: 최소 딜레이 (초)
        max_delay: 최대 딜레이 (초)
        backoff_factor: 403 발생 시 딜레이 증가 배수
        max_delay_cap: 백오프 후 최대 딜레이 상한
    
    Usage:
        limiter = RateLimiter(min_delay=1.0, max_delay=2.0)
        limiter.wait()  # 요청 전 호출
        limiter.on_success()  # 성공 시
        limiter.on_forbidden()  # 403 발생 시 (딜레이 증가)
    """
    min_delay: float = 1.0
    max_delay: float = 2.0
    backoff_factor: float = 2.0
    max_delay_cap: float = 30.0
    
    _current_min: float = field(default=1.0, init=False, repr=False)
    _current_max: float = field(default=2.0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _consecutive_403: int = field(default=0, init=False, repr=False)
    
    def __post_init__(self):
        self._current_min = self.min_delay
        self._current_max = self.max_delay
    
    def wait(self) -> float:
        """요청 전 딜레이 적용. 실제 대기 시간 반환."""
        with self._lock:
            delay = random.uniform(self._current_min, self._current_max)
        time.sleep(delay)
        return delay
    
    def on_success(self):
        """성공 시 딜레이를 점진적으로 원래대로 복구."""
        with self._lock:
            self._consecutive_403 = 0
            # 점진적 복구 (한 번에 원래대로 돌리지 않음)
            self._current_min = max(self.min_delay, self._current_min / 1.5)
            self._current_max = max(self.max_delay, self._current_max / 1.5)
    
    def on_forbidden(self):
        """403 발생 시 딜레이 증가 (exponential backoff)."""
        with self._lock:
            self._consecutive_403 += 1
            self._current_min = min(self._current_min * self.backoff_factor, self.max_delay_cap)
            self._current_max = min(self._current_max * self.backoff_factor, self.max_delay_cap * 1.5)
            
            if self._consecutive_403 >= 5:
                raise RateLimitError(f"연속 403 발생 {self._consecutive_403}회 - Rate limit 의심")
    
    def get_current_delay(self) -> Tuple[float, float]:
        """현재 딜레이 범위 반환."""
        with self._lock:
            return (self._current_min, self._current_max)
    
    def reset(self):
        """딜레이를 초기값으로 리셋."""
        with self._lock:
            self._current_min = self.min_delay
            self._current_max = self.max_delay
            self._consecutive_403 = 0


# =============================================================================
# Account-Proxy Pair Manager (Round-Robin)
# =============================================================================

@dataclass
class AccountProxyPair:
    """계정-프록시 쌍을 나타내는 데이터 클래스"""
    user_id: str
    user_pw: str
    proxy: Optional[str] = None
    order: int = 0
    session: Optional[requests.Session] = field(default=None, repr=False)
    driver: Optional[SupportsQuit] = field(default=None, repr=False)
    is_logged_in: bool = field(default=False, repr=False)
    failure_count: int = field(default=0, repr=False)


@dataclass
class AccountProxyPairManager:
    """
    계정-프록시 쌍을 Round-Robin 방식으로 관리.
    
    계정 수와 프록시 수 중 더 적은 수로 쌍을 생성하고, 남는 것은 버림.
    각 계정은 고유한 프록시 IP와 1:1로 대응됨.
    
    Args:
        accounts: 계정 리스트 [{"user_id": "...", "user_pw": "..."}, ...]
        proxies: 프록시 URL 리스트 ["http://...", "socks5://...", ...]
    
    Usage:
        manager = AccountProxyPairManager(accounts, proxies)
        pair = manager.get_next()  # 다음 계정-프록시 쌍 반환
        manager.mark_failed(pair)  # 실패한 쌍 표시
        manager.mark_success(pair)  # 성공한 쌍 표시
    """
    accounts: List[dict] = field(default_factory=list)
    proxies: List[str] = field(default_factory=list)
    initial_index: int = 0
    account_usage: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    _pairs: List[AccountProxyPair] = field(default_factory=list, init=False, repr=False)
    _index: int = field(default=0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _failed_pairs: set = field(default_factory=set, init=False, repr=False)
    _max_failures: int = field(default=5, init=False, repr=False)
    
    def __post_init__(self):
        """계정과 프록시를 쌍으로 묶음. 더 적은 수에 맞춤."""
        self.account_usage = {
            key: self._normalize_usage_entry(value)
            for key, value in self.account_usage.items()
        }
        uses_embedded_proxy = any(account.get("proxy") for account in self.accounts)
        pair_count = len(self.accounts) if uses_embedded_proxy else (min(len(self.accounts), len(self.proxies)) if self.proxies else len(self.accounts))

        for i in range(pair_count):
            account = self.accounts[i]
            proxy = account.get("proxy") or (self.proxies[i] if i < len(self.proxies) else None)
            
            pair = AccountProxyPair(
                user_id=account.get("user_id", ""),
                user_pw=account.get("user_pw", ""),
                proxy=proxy,
                order=i,
            )
            self._pairs.append(pair)

        if not uses_embedded_proxy and len(self.accounts) > pair_count:
            dropped = len(self.accounts) - pair_count
            print(f"[AccountProxyPairManager] 계정 {dropped}개 버림 (프록시 부족)")
        if not uses_embedded_proxy and len(self.proxies) > pair_count:
            dropped = len(self.proxies) - pair_count
            print(f"[AccountProxyPairManager] 프록시 {dropped}개 버림 (계정 부족)")

        if self._pairs:
            self._index = self.initial_index % len(self._pairs)
        
        print(f"[AccountProxyPairManager] {len(self._pairs)}개 계정-프록시 쌍 생성됨")
    
    @staticmethod
    def _normalize_usage_entry(entry: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        entry = dict(entry or {})
        try:
            login_count = max(int(entry.get("login_count", 0)), 0)
        except (TypeError, ValueError):
            login_count = 0

        last_login_at = entry.get("last_login_at")
        if not isinstance(last_login_at, str):
            last_login_at = ""

        return {
            "login_count": login_count,
            "last_login_at": last_login_at,
        }

    def get_pair_key(self, pair: AccountProxyPair) -> str:
        return f"{pair.user_id}|{pair.proxy or ''}"

    def get_pair_usage(self, pair: AccountProxyPair) -> Dict[str, Any]:
        key = self.get_pair_key(pair)
        usage = self._normalize_usage_entry(self.account_usage.get(key))
        self.account_usage[key] = usage
        return usage

    def get_account_usage_state(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {
                key: self._normalize_usage_entry(value)
                for key, value in self.account_usage.items()
            }

    def record_login_success(self, pair: AccountProxyPair, logged_in_at: Optional[str] = None) -> None:
        if not pair:
            return

        with self._lock:
            key = self.get_pair_key(pair)
            usage = self._normalize_usage_entry(self.account_usage.get(key))
            usage["login_count"] += 1
            usage["last_login_at"] = logged_in_at or datetime.now().astimezone().isoformat()
            self.account_usage[key] = usage

    def get_next(self) -> Optional[AccountProxyPair]:
        if not self._pairs:
            return None
        
        with self._lock:
            available = [i for i, p in enumerate(self._pairs) if i not in self._failed_pairs]
            if not available:
                print("[AccountProxyPairManager] 모든 쌍 실패 - 리셋")
                self._failed_pairs.clear()
                for p in self._pairs:
                    p.failure_count = 0
                available = list(range(len(self._pairs)))

            self._index = self._index % len(available)
            selected_idx = available[self._index]
            self._index += 1
            return self._pairs[selected_idx]
    
    def mark_failed(self, pair: AccountProxyPair):
        """쌍 실패 표시. max_failures 초과 시 비활성화."""
        if not pair:
            return
        
        with self._lock:
            pair.failure_count += 1
            if pair.failure_count >= self._max_failures:
                idx = self._pairs.index(pair) if pair in self._pairs else -1
                if idx >= 0:
                    self._failed_pairs.add(idx)
                    print(f"[AccountProxyPairManager] 쌍 비활성화: {pair.user_id[:5]}*** ({self._max_failures}회 실패)")
    
    def mark_success(self, pair: AccountProxyPair):
        """쌍 성공 표시. 실패 카운트 리셋."""
        if not pair:
            return
        
        with self._lock:
            pair.failure_count = 0
    
    def get_pair_count(self) -> int:
        """전체 쌍 수 반환."""
        return len(self._pairs)
    
    def get_active_count(self) -> int:
        """활성 쌍 수 반환."""
        with self._lock:
            return len(self._pairs) - len(self._failed_pairs)

    def get_next_index(self) -> int:
        with self._lock:
            if not self._pairs:
                return 0
            return self._index % len(self._pairs)
    
    def reset(self):
        """모든 쌍 상태 리셋."""
        with self._lock:
            self._index = 0
            self._failed_pairs.clear()
            for p in self._pairs:
                p.failure_count = 0
                p.is_logged_in = False
    
    def close_all(self):
        """모든 드라이버/세션 정리."""
        for pair in self._pairs:
            if pair.driver:
                try:
                    pair.driver.quit()
                except:
                    pass
                pair.driver = None
            pair.session = None
            pair.is_logged_in = False


# =============================================================================
# Proxy Rotator
# =============================================================================

@dataclass
class ProxyRotator:
    """
    Round-robin 방식으로 프록시를 로테이션.
    
    Args:
        proxies: 프록시 URL 리스트 (예: ["http://user:pass@host:port", ...])
                 빈 리스트면 프록시 없이 직접 연결
    
    Usage:
        rotator = ProxyRotator(["http://proxy1:8080", "http://proxy2:8080"])
        proxy = rotator.get_next()  # 다음 프록시 반환
        rotator.mark_failed("http://proxy1:8080")  # 실패한 프록시 표시
    """
    proxies: List[str] = field(default_factory=list)
    
    _index: int = field(default=0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _failed_proxies: set = field(default_factory=set, init=False, repr=False)
    _failure_counts: dict = field(default_factory=dict, init=False, repr=False)
    _max_failures: int = field(default=3, init=False, repr=False)
    
    def get_next(self) -> Optional[str]:
        """
        다음 프록시 반환 (round-robin).
        프록시가 없거나 모두 실패하면 None 반환 (직접 연결).
        """
        if not self.proxies:
            return None
        
        with self._lock:
            available = [p for p in self.proxies if p not in self._failed_proxies]
            if not available:
                # 모든 프록시 실패 시 리셋하고 직접 연결 시도
                print("[ProxyRotator] 모든 프록시 실패 - 직접 연결 시도")
                self._failed_proxies.clear()
                self._failure_counts.clear()
                return None
            
            # Round-robin
            self._index = self._index % len(available)
            proxy = available[self._index]
            self._index += 1
            return proxy
    
    def mark_failed(self, proxy: str):
        """프록시 실패 표시. max_failures 초과 시 비활성화."""
        if not proxy:
            return
        
        with self._lock:
            self._failure_counts[proxy] = self._failure_counts.get(proxy, 0) + 1
            if self._failure_counts[proxy] >= self._max_failures:
                self._failed_proxies.add(proxy)
                print(f"[ProxyRotator] 프록시 비활성화: {proxy[:30]}... ({self._max_failures}회 실패)")
    
    def mark_success(self, proxy: str):
        """프록시 성공 표시. 실패 카운트 리셋."""
        if not proxy:
            return
        
        with self._lock:
            self._failure_counts[proxy] = 0
    
    def get_proxies_dict(self, proxy: Optional[str]) -> Optional[dict]:
        """requests용 proxies 딕셔너리 반환."""
        if not proxy:
            return None
        return {"http": proxy, "https": proxy}
    
    def reset(self):
        """모든 프록시 상태 리셋."""
        with self._lock:
            self._index = 0
            self._failed_proxies.clear()
            self._failure_counts.clear()
    
    @property
    def active_count(self) -> int:
        """활성 프록시 수."""
        with self._lock:
            return len(self.proxies) - len(self._failed_proxies)
    
    @property
    def total_count(self) -> int:
        """전체 프록시 수."""
        return len(self.proxies)
from tqdm import tqdm


def load_credentials(credentials_path: str = ".credentials.json") -> Tuple[str, str]:
    """자격 증명 파일에서 ID/PW 로드. FileNotFoundError 또는 KeyError 발생 가능."""
    path = Path(credentials_path)
    if not path.exists():
        raise FileNotFoundError(
            f"자격 증명 파일을 찾을 수 없습니다: {credentials_path}\n"
            f"다음 형식으로 파일을 생성하세요:\n"
            f'{{"statiz": {{"user_id": "your_email", "user_pw": "your_password"}}}}'
        )
    
    with open(path, 'r', encoding='utf-8') as f:
        creds = json.load(f)
    
    statiz = creds.get("statiz", {})
    user_id = statiz.get("user_id")
    user_pw = statiz.get("user_pw")
    
    if not user_id or not user_pw:
        raise KeyError("자격 증명 파일에 'statiz.user_id' 또는 'statiz.user_pw'가 없습니다.")
    
    return user_id, user_pw


def load_multiple_accounts(credentials_path: str = ".credentials.json") -> List[dict]:
    """
    자격 증명 파일에서 여러 계정 로드.
    
    Expected format in .credentials.json:
    {
        "statiz_accounts": [
            {"user_id": "email1@example.com", "user_pw": "password1"},
            {"user_id": "email2@example.com", "user_pw": "password2"}
        ],
        "proxies": [
            "http://user:pass@host1:port",
            "socks5://user:pass@host2:port"
        ]
    }
    
    단일 계정 형식도 지원 (하위 호환성):
    {
        "statiz": {"user_id": "...", "user_pw": "..."}
    }
    
    Returns:
        계정 리스트 [{"user_id": "...", "user_pw": "..."}, ...]
    """
    path = Path(credentials_path)
    if not path.exists():
        raise FileNotFoundError(
            f"자격 증명 파일을 찾을 수 없습니다: {credentials_path}\n"
            f"다음 형식으로 파일을 생성하세요:\n"
            f'{{"statiz_accounts": [{{"user_id": "email", "user_pw": "pass"}}], "proxies": ["http://..."]}}'
        )
    
    with open(path, 'r', encoding='utf-8') as f:
        creds = json.load(f)
    
    # 다중 계정 형식 우선
    if "statiz_accounts" in creds:
        accounts = creds.get("statiz_accounts", [])
        if accounts:
            print(f"[load_multiple_accounts] {len(accounts)}개 계정 로드됨")
            return accounts
    
    # 단일 계정 형식 (하위 호환성)
    statiz = creds.get("statiz", {})
    if statiz.get("user_id") and statiz.get("user_pw"):
        print("[load_multiple_accounts] 단일 계정 형식 사용")
        return [{"user_id": statiz["user_id"], "user_pw": statiz["user_pw"]}]
    
    raise KeyError("자격 증명 파일에 'statiz_accounts' 또는 'statiz' 키가 없습니다.")


def load_proxies(credentials_path: str = ".credentials.json") -> List[str]:
    """
    자격 증명 파일에서 프록시 목록 로드.
    
    Expected format in .credentials.json:
    {
        "statiz": {"user_id": "...", "user_pw": "..."},
        "proxies": [
            "http://user:pass@host1:port",
            "http://user:pass@host2:port",
            "socks5://user:pass@host3:port"
        ]
    }
    
    Returns:
        프록시 URL 리스트 (없으면 빈 리스트)
    """
    path = Path(credentials_path)
    if not path.exists():
        return []
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            creds = json.load(f)
        return creds.get("proxies", [])
    except Exception:
        return []


# =============================================================================
# HTTP Request with 403 Detection
# =============================================================================

def _make_request(
    session: requests.Session,
    url: str,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    max_retries: int = 3,
    timeout: int = 30
) -> requests.Response:
    """
    403 감지 및 재시도를 포함한 HTTP GET 요청.
    
    Args:
        session: requests.Session
        url: 요청 URL
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        max_retries: 최대 재시도 횟수
        timeout: 요청 타임아웃 (초)
    
    Returns:
        requests.Response
    
    Raises:
        ForbiddenError: 403 발생 시 (재시도 후에도 실패)
        MaxRetriesExceededError: 최대 재시도 초과
        RateLimitError: Rate limit 감지
    """
    last_error = None
    
    for attempt in range(max_retries):
        # Rate limiting
        if rate_limiter:
            rate_limiter.wait()
        
        # Get proxy
        proxy = proxy_rotator.get_next() if proxy_rotator else None
        proxies = proxy_rotator.get_proxies_dict(proxy) if proxy_rotator else None
        
        try:
            response = session.get(url, proxies=proxies, timeout=timeout)
            
            # 403 Forbidden 감지
            if response.status_code == 403:
                if rate_limiter:
                    rate_limiter.on_forbidden()
                if proxy_rotator and proxy:
                    proxy_rotator.mark_failed(proxy)
                
                last_error = ForbiddenError(url, proxy)
                print(f"[403 Forbidden] {url[:50]}... (시도 {attempt + 1}/{max_retries})")
                
                # 백오프 대기
                backoff_time = min(2 ** attempt, 30)
                time.sleep(backoff_time)
                continue
            
            # 429 Too Many Requests
            if response.status_code == 429:
                if rate_limiter:
                    rate_limiter.on_forbidden()
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"[429 Rate Limited] {retry_after}초 대기...")
                time.sleep(retry_after)
                continue
            
            # 다른 에러
            if response.status_code >= 400:
                last_error = StatizError(f"HTTP {response.status_code}: {url}")
                continue
            
            # 성공
            if rate_limiter:
                rate_limiter.on_success()
            if proxy_rotator and proxy:
                proxy_rotator.mark_success(proxy)
            
            return response
            
        except requests.exceptions.RequestException as e:
            last_error = e
            if proxy_rotator and proxy:
                proxy_rotator.mark_failed(proxy)
            print(f"[요청 오류] {str(e)[:50]}... (시도 {attempt + 1}/{max_retries})")
            time.sleep(2 ** attempt)
            continue
    
    # 모든 재시도 실패
    if isinstance(last_error, ForbiddenError):
        raise last_error
    raise MaxRetriesExceededError(url, max_retries)


def login_statiz(driver, user_id: str, user_pw: str, max_retries: int = 3) -> bool:
    """Statiz.co.kr 로그인. 성공 시 True 반환."""
    login_url = "https://statiz.co.kr/member/?m=login"

    if hasattr(driver, 'page'):
        page = driver.page
        for attempt in range(max_retries):
            try:
                print(f"[로그인] 시도 {attempt + 1}/{max_retries}...")
                page.goto(login_url, wait_until='load')
                id_locator = page.locator("#userID, input[name='userID'], input[type='text']").first
                pw_locator = page.locator("#userPassword, input[name='userPassword'], input[type='password']").first
                id_locator.fill(user_id)
                pw_locator.fill(user_pw)
                form_locator = page.locator('form').first
                pw_locator.press('Enter')
                page.wait_for_timeout(1000)
                if 'login' in page.url.lower() and form_locator.count() > 0:
                    form_locator.evaluate("form => form.submit()")
                page.wait_for_timeout(3000)
                current_url = page.url
                page_text = page.text_content('body') or ''
                if 'login' not in current_url.lower() or '로그아웃' in page_text or '마이페이지' in page_text:
                    print(f"[로그인] 성공! 현재 URL: {current_url}")
                    return True
                print(f"[로그인] 시도 {attempt + 1} 실패 - 재시도 중...")
            except Exception as e:
                print(f"[로그인] 브라우저 오류 발생: {e}")
                return False

        print("[로그인] 모든 시도 실패")
        return False
    print("[로그인] 모든 시도 실패")
    return False


def get_session_from_driver(driver) -> requests.Session:
    """드라이버의 쿠키를 사용하여 requests Session 생성"""
    session = requests.Session()
    for cookie in driver.get_cookies():
        session.cookies.set(cookie['name'], cookie['value'])
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    return session


TEAM_DICT = {
    '<span style="background:#000000;color:#fff;">K</span>': "KT",
    '<span style="background:#002b69;color:#fff;">N</span>': "NC",
    '<span style="background:#005284;color:#fff;">청</span>': "청보",
    '<span style="background:#0061AA;color:#fff;">삼</span>': "삼성",
    '<span style="background:#007f55;color:#fff;">현</span>': "현대",
    '<span style="background:#042071;color:#fff;">두</span>': "두산",
    '<span style="background:#15326f;color:#fff;">O</span>': "OB",
    '<span style="background:#1552f8;color:#fff;">M</span>': "MBC",
    '<span style="background:#224433;color:#fff;">태</span>': "태평양",
    '<span style="background:#86001f;color:#fff;">키</span>': "키움",
    '<span style="background:#888888;color:#fff;">롯</span>': "롯데",
    '<span style="background:#88b7e1;color:#fff;">삼</span>': "삼성",
    '<span style="background:#cf152d;color:#fff;">S</span>': "SSG",
    '<span style="background:#ed1c24;color:#fff;">K</span>': "KIA",
    '<span style="background:#ed1c24;color:#fff;">빙</span>': "빙그레",
    '<span style="background:#f37321;color:#fff;">한</span>': "한화",
    '<span style="background:#fc1cad;color:#fff;">L</span>': "LG",
    '<span style="background:#ff0000;color:#fff;">S</span>': "SK",
    '<span style="background:#ff0c00;color:#fff;">해</span>': "해태",
    '<span style="background:#ffc81e;color:#fff;">쌍</span>': "쌍방울",
}


def load_statiz_bat(
    session: requests.Session, 
    limit: int = 3000,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None
) -> pd.DataFrame:
    """
    타자 목록 스크래핑 (requests 사용).
    
    Args:
        session: 로그인된 requests.Session
        limit: 가져올 최대 선수 수 (pr 파라미터). 3000이면 전체 타자 수집.
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
    
    Returns:
        pd.DataFrame: Name, ID 컬럼을 가진 타자 목록
    
    Raises:
        ForbiddenError: 403 발생 시
        MaxRetriesExceededError: 최대 재시도 초과
    """
    url = f"https://statiz.co.kr/stats/?m=total&m2=batting&m3=default&so=PA&ob=DESC&sy=1982&ey=2025&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr={limit}&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2="
    
    response = _make_request(
        session=session,
        url=url,
        proxy_rotator=proxy_rotator,
        rate_limiter=rate_limiter,
        max_retries=5
    )
    
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        raise StatizError("테이블을 찾을 수 없습니다")
    
    table = tables[0]
    rows = table.find_all("tr")
    
    records = []
    for i in range(2, len(rows)):
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        
        name = tds[1].text.strip()
        a_tag = tds[1].find('a')
        if not a_tag:
            continue
        href = a_tag.get('href', '')
        if 'p_no=' not in str(href):
            continue
        player_id = str(href).split('p_no=')[-1]
        
        records.append({"Name": name, "ID": player_id})
    
    return pd.DataFrame(records)


def load_bat_list_improved(
    session: requests.Session, 
    bat_id_df: pd.DataFrame,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    save_interval: int = 100,
    save_path: Optional[str] = None
) -> list:
    """
    타자 기록 스크래핑 (requests 사용).
    
    팀 이적 처리: raw 데이터 수집 후 연도별 합산.
    비율 컬럼(AVG, OBP, SLG, OPS, wRC+)은 합산 후 재계산.
    
    Args:
        session: 로그인된 requests.Session
        bat_id_df: 타자 ID DataFrame
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        save_interval: 중간 저장 간격 (선수 수)
        save_path: 중간 저장 경로 (None이면 저장 안함)
    
    Returns:
        list: 선수별 DataFrame 리스트
    """
    import pickle
    from datetime import datetime
    
    bat_list = []
    failed_players = []
    consecutive_403 = 0
    
    pbar = tqdm(list(bat_id_df.index), desc="선수 데이터 로딩 중")
    
    for idx, ind in enumerate(pbar):
        name = bat_id_df.loc[ind, "Name"]
        player_id = bat_id_df.loc[ind, "ID"]
        pbar.set_postfix({"선수": name, "성공": len(bat_list), "실패": len(failed_players)})
        
        try:
            player_df = _scrape_single_batter(
                session=session, 
                player_id=player_id, 
                name=name,
                proxy_rotator=proxy_rotator,
                rate_limiter=rate_limiter
            )
            if player_df is not None and not player_df.empty:
                bat_list.append(player_df)
                consecutive_403 = 0
                
        except ForbiddenError as e:
            consecutive_403 += 1
            failed_players.append({"name": name, "id": player_id, "error": "403"})
            print(f"\n[403] {name} - 연속 403: {consecutive_403}")
            
            # 연속 403이 많으면 긴 대기
            if consecutive_403 >= 3:
                wait_time = min(60 * consecutive_403, 300)  # 최대 5분
                print(f"[경고] 연속 403 {consecutive_403}회 - {wait_time}초 대기...")
                time.sleep(wait_time)
            
            if consecutive_403 >= 10:
                print("[중단] 연속 403이 10회 초과 - 스크래핑 중단")
                break
                
        except RateLimitError as e:
            print(f"\n[Rate Limit] {e}")
            print("[중단] Rate limit 감지 - 스크래핑 중단")
            break
            
        except MaxRetriesExceededError as e:
            failed_players.append({"name": name, "id": player_id, "error": "max_retries"})
            print(f"\n[재시도 초과] {name}")
            
        except Exception as e:
            failed_players.append({"name": name, "id": player_id, "error": str(e)})
            print(f"\n[오류] {name}: {str(e)[:50]}")
            continue
        
        # 중간 저장
        if save_path and (idx + 1) % save_interval == 0:
            checkpoint_path = f"{save_path}_checkpoint_{idx + 1}.pkl"
            with open(checkpoint_path, 'wb') as f:
                pickle.dump(bat_list, f)
            print(f"\n[체크포인트] {checkpoint_path} 저장 ({len(bat_list)}명)")
    
    # 결과 출력
    print(f"\n스크래핑 완료: 성공 {len(bat_list)}명, 실패 {len(failed_players)}명")
    
    if failed_players:
        failed_df = pd.DataFrame(failed_players)
        today = datetime.now().strftime('%y%m%d_%H%M')
        failed_path = f"failed_players_{today}.csv"
        failed_df.to_csv(failed_path, index=False, encoding='utf-8-sig')
        print(f"실패 목록 저장: {failed_path}")
    
    return bat_list


def _extract_table_headers(rows) -> list:
    """
    테이블 헤더 동적 추출.
    
    Row 0: 메인 헤더 (Year, Team, Age, Pos., G, oWAR, ..., 비율, WAR)
    Row 1: 비율 서브헤더 (AVG, OBP, SLG, OPS, R/ePA, wRC+)
    
    '비율' 컬럼 위치에 Row 1의 서브헤더를 삽입하여 33개 컬럼 생성.
    """
    header_row0 = rows[0].find_all(['th', 'td'])
    header_row1 = rows[1].find_all(['th', 'td'])
    
    columns = []
    for cell in header_row0:
        col_name = cell.text.strip()
        if col_name == '비율':
            # '비율' 대신 Row 1의 서브헤더들 삽입
            for sub_cell in header_row1:
                columns.append(sub_cell.text.strip())
        else:
            # 'Pos.' -> 'Pos' 정규화
            if col_name == 'Pos.':
                col_name = 'Pos'
            columns.append(col_name)
    
    return columns


def _parse_row_to_dict(tds, columns: list, is_rowspan_row: bool = False) -> dict:
    """
    테이블 행을 딕셔너리로 변환.
    
    Args:
        tds: td 요소 리스트
        columns: 컬럼명 리스트 (33개)
        is_rowspan_row: rowspan 연속 행 여부 (32개 셀, Year 없음)
    """
    record = {}
    
    if is_rowspan_row:
        # rowspan 연속 행: Year 컬럼 제외하고 파싱
        cols_without_year = columns[1:]  # Year 제외
        for i, col in enumerate(cols_without_year):
            if i < len(tds):
                record[col] = tds[i].text.strip()
    else:
        # 일반 행: 모든 컬럼 파싱
        for i, col in enumerate(columns):
            if i < len(tds):
                record[col] = tds[i].text.strip()
    
    return record


def _scrape_single_batter(
    session: requests.Session, 
    player_id: str, 
    name: str,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None
) -> Optional[pd.DataFrame]:
    """
    단일 타자 기록 스크래핑 (requests 사용).
    
    URL: https://statiz.co.kr/player/?m=year&p_no={player_id}
    
    헤더를 웹페이지에서 동적으로 추출하여 사용.
    시즌 중 팀 이적 시 rowspan으로 표현되며, 연속 행은 Year 컬럼이 없음.
    
    Args:
        session: requests.Session
        player_id: 선수 ID
        name: 선수 이름
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
    
    Returns:
        선수 DataFrame 또는 None
    
    Raises:
        ForbiddenError: 403 발생 시
        MaxRetriesExceededError: 최대 재시도 초과
    """
    url = f"https://statiz.co.kr/player/?m=year&p_no={player_id}"
    
    response = _make_request(
        session=session,
        url=url,
        proxy_rotator=proxy_rotator,
        rate_limiter=rate_limiter,
        max_retries=3
    )
    
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        return None
    
    table = tables[0]
    rows = table.find_all("tr")
    
    # 헤더 동적 추출
    columns = _extract_table_headers(rows)
    
    # 데이터 행 파싱 (Row 2부터 시작, 마지막 3개 행은 통산/헤더)
    records = []
    current_year = None
    
    for i in range(2, len(rows) - 3):
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        
        tds = tr.find_all("td")
        num_cells = len(tds)
        
        if num_cells == len(columns):
            # 일반 행 (Year 포함)
            year_text = tds[0].text.strip()
            if not year_text.isdigit():
                continue
            
            current_year = year_text
            record = _parse_row_to_dict(tds, columns, is_rowspan_row=False)
            records.append(record)
            
        elif num_cells == len(columns) - 1 and current_year is not None:
            # rowspan 연속 행 (Year 없음, 시즌 중 이적)
            record = _parse_row_to_dict(tds, columns, is_rowspan_row=True)
            record['Year'] = current_year  # 이전 행의 Year 사용
            records.append(record)
    
    if not records:
        return None
    
    df = pd.DataFrame(records)
    
    # 데이터 타입 변환
    int_cols = ['Year', 'Age', 'G', 'PA', 'ePA', 'AB', 'R', 'H', '2B', '3B', 'HR', 
                'TB', 'RBI', 'SB', 'CS', 'BB', 'HP', 'IB', 'SO', 'GDP', 'SH', 'SF']
    float_cols = ['oWAR', 'dWAR', 'WAR', 'AVG', 'OBP', 'SLG', 'OPS', 'R/ePA', 'wRC+']
    
    for col in int_cols:
        if col in df.columns:
            df[col] = _to_numeric_series(df[col]).fillna(0).astype(int)
    for col in float_cols:
        if col in df.columns:
            df[col] = _to_numeric_series(df[col]).fillna(0.0)
    
    # 팀 이적 처리: 같은 연도 기록 합산
    # 합산 가능한 컬럼은 sum, 비율 컬럼은 재계산
    sum_cols = ['G', 'PA', 'ePA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'TB', 'RBI', 
                'SB', 'CS', 'BB', 'HP', 'IB', 'SO', 'GDP', 'SH', 'SF']
    war_cols = ['oWAR', 'dWAR', 'WAR']
    
    agg_dict = {col: 'sum' for col in sum_cols if col in df.columns}
    agg_dict.update({col: 'sum' for col in war_cols if col in df.columns})
    agg_dict['Age'] = 'first'
    agg_dict['Team'] = 'first'  # 첫 번째 팀만 사용
    agg_dict['Pos'] = 'first'
    
    df_agg = df.groupby('Year').agg(agg_dict).reset_index()
    
    # 비율 컬럼 재계산
    if 'AB' in df_agg.columns and df_agg['AB'].sum() > 0:
        df_agg['AVG'] = (df_agg['H'] / df_agg['AB']).round(3).fillna(0)
        df_agg['SLG'] = (df_agg['TB'] / df_agg['AB']).round(3).fillna(0)
    if all(c in df_agg.columns for c in ['H', 'BB', 'HP', 'AB', 'SF']):
        df_agg['OBP'] = ((df_agg['H'] + df_agg['BB'] + df_agg['HP']) / 
                         (df_agg['AB'] + df_agg['BB'] + df_agg['HP'] + df_agg['SF'])).round(3).fillna(0)
    if 'OBP' in df_agg.columns and 'SLG' in df_agg.columns:
        df_agg['OPS'] = (df_agg['OBP'] + df_agg['SLG']).round(3)
    
    # wRC+는 PA 가중 평균으로 재계산
    if 'wRC+' in df.columns and 'PA' in df.columns:
        df['wRC+_weighted'] = df['wRC+'] * df['PA']
        wrc_agg = df.groupby('Year').agg({'wRC+_weighted': 'sum', 'PA': 'sum'}).reset_index()
        wrc_agg['wRC+'] = (wrc_agg['wRC+_weighted'] / wrc_agg['PA']).round(1)
        df_agg = df_agg.drop('wRC+', axis=1, errors='ignore')
        df_agg = df_agg.merge(wrc_agg[['Year', 'wRC+']], on='Year', how='left')
    
    # R/ePA는 원본 값 사용 (PA 가중 평균) - Statiz 자체 계산 방식이므로 재계산하지 않음
    if 'R/ePA' in df.columns and 'PA' in df.columns:
        df['R/ePA_weighted'] = df['R/ePA'] * df['PA']
        repa_agg = df.groupby('Year').agg({'R/ePA_weighted': 'sum', 'PA': 'sum'}).reset_index()
        repa_agg['R/ePA'] = (repa_agg['R/ePA_weighted'] / repa_agg['PA']).round(3)
        df_agg = df_agg.drop('R/ePA', axis=1, errors='ignore')
        df_agg = df_agg.merge(repa_agg[['Year', 'R/ePA']], on='Year', how='left')
    
    # Name 컬럼 추가
    df_agg.insert(0, 'Name', name)
    
    # 컬럼 순서를 웹페이지 헤더 순서대로 정렬 (Name 추가)
    column_order = ['Name'] + columns
    # 실제 존재하는 컬럼만 선택
    column_order = [c for c in column_order if c in df_agg.columns]
    df_agg = df_agg[column_order]
    
    return _sort_by_year(df_agg)


# =============================================================================
# NEW APPROACH: Yearly Stats Scraping (44 requests instead of 2800+)
# =============================================================================

def scrape_yearly_batting_stats(
    session: requests.Session,
    year: int,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    limit: int = 5000
) -> pd.DataFrame:
    """
    특정 연도의 타자 기록 스크래핑.
    
    URL: https://statiz.co.kr/stats/?m=main&m2=batting&m3=default&year={year}&pr={limit}
    
    Args:
        session: 로그인된 requests.Session
        year: 스크래핑할 연도 (1982-2025)
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        limit: 가져올 최대 선수 수 (기본 1000)
    
    Returns:
        pd.DataFrame: 해당 연도 타자 기록 (player_id 포함)
    
    Raises:
        ForbiddenError: 403 발생 시
        StatizError: 테이블 파싱 실패 시
    """
    url = (
        f"https://statiz.co.kr/stats/?m=main&m2=batting&m3=default"
        f"&so=PA&ob=DESC&year={year}"
        f"&sy=&ey=&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha="
        f"&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba="
        f"&li=&as=&ae=&pl=&gc=&lr=&pr={limit}&ph=&hs=&us=&na=&ls="
        f"&sf1=&sk1=&sv1=&sf2=&sk2=&sv2="
    )
    
    response = _make_request(
        session=session,
        url=url,
        proxy_rotator=proxy_rotator,
        rate_limiter=rate_limiter,
        max_retries=5
    )
    
    # 로그인 필요 페이지로 리다이렉트 되었는지 확인
    if "alert('로그인" in response.text or "location.href='/member/?m=login" in response.text:
        raise StatizError(f"{year}년 - 세션 만료 (로그인 필요)")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        raise StatizError(f"{year}년 테이블을 찾을 수 없습니다")
    
    table = tables[0]
    rows = table.find_all("tr")
    
    if len(rows) < 3:
        # 데이터 없는 연도 (예: 1982년에 일부 팀만 존재)
        return pd.DataFrame()
    
    # 첫 번째 데이터 행에 실제 데이터가 있는지 확인
    first_data_row = rows[2] if len(rows) > 2 else None
    if first_data_row:
        tds = first_data_row.find_all("td")
        if len(tds) < 5:
            raise StatizError(f"{year}년 - 데이터 행 형식 오류 (td 개수: {len(tds)})")
    
    # 헤더 추출 (Row 0: 메인 헤더, Row 1: 비율 서브헤더)
    columns = _extract_yearly_headers(rows)
    
    # 데이터 파싱
    records = []
    for i in range(2, len(rows)):
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        
        # 선수 ID 추출 (이름 컬럼의 링크에서)
        name_td = tds[1]  # 두 번째 컬럼이 이름
        a_tag = name_td.find('a')
        if not a_tag:
            continue
        
        href = a_tag.get('href', '')
        if 'p_no=' not in str(href):
            continue
        
        player_id = str(href).split('p_no=')[-1].split('&')[0]
        name = name_td.text.strip()
        
        # 행 데이터 파싱
        record = {'player_id': player_id, 'Name': name, 'Year': year}
        
        # Pos 셀 파싱 (tds[2]) - "YY  POS" 형식 (예: "25  3B")
        # 주의: 이 페이지에는 Team 정보가 없음! 연도 코드 + 포지션만 있음
        pos_td = tds[2]
        pos_spans = pos_td.find_all('span')
        if len(pos_spans) >= 3:
            # 첫 번째 span: 연도 코드 (예: "25") - 무시
            # 두 번째 span: 팀 아이콘 - 무시
            # 세 번째 span: 포지션 (예: "3B", "CF", "SS")
            record['Pos'] = pos_spans[-1].text.strip()
        elif len(pos_spans) >= 1:
            # fallback: 마지막 span이 포지션
            record['Pos'] = pos_spans[-1].text.strip()
        else:
            # span이 없으면 전체 텍스트에서 포지션 추출 시도
            full_text = pos_td.text.strip()
            # "25  3B" 형식에서 마지막 부분 추출
            parts = full_text.split()
            record['Pos'] = parts[-1] if parts else ''
        
        # 나머지 컬럼 파싱 (tds[3]부터)
        # columns에서 Team, Pos, PA_sort 다음부터 시작
        stat_columns = [
            'G', 'oWAR', 'dWAR', 'PA', 'ePA', 'AB', 'R', 'H', 
            '2B', '3B', 'HR', 'TB', 'RBI', 'SB', 'CS', 'BB', 'HP', 'IB', 
            'SO', 'GDP', 'SH', 'SF',
            'AVG', 'OBP', 'SLG', 'OPS', 'R/ePA', 'wRC+', 'WAR'
        ]
        
        # tds[3]은 PA_sort (정렬 기준, 스킵)
        # tds[4]부터 실제 스탯
        stat_start_idx = 4
        for col_idx, col_name in enumerate(stat_columns):
            td_idx = stat_start_idx + col_idx
            if td_idx < len(tds):
                record[col_name] = tds[td_idx].text.strip()
        
        records.append(record)
    
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    
    # 데이터 타입 변환
    df = _convert_batting_dtypes(df)
    
    return df


def _extract_yearly_headers(rows) -> list:
    """
    연도별 통계 테이블에서 헤더 추출.
    
    연도별 페이지 실제 헤더 구조:
    Row 0: Rank, Name, YY+Pos, Sort▼, G, oWAR, dWAR, PA▼, ePA, AB, R, H, 2B, 3B, HR, TB, RBI, SB, CS, BB, HP, IB, SO, GDP, SH, SF, 비율(colspan=6), WAR
    Row 1: PA, AVG, OBP, SLG, OPS, R/ePA, wRC+
    
    주의: 이 페이지에는 Team 정보가 없음! tds[2]는 "연도코드 + 포지션" 형식.
    
    실제 데이터 컬럼 (33개):
    tds[0]: Rank (스킵)
    tds[1]: Name (+ player_id 링크)
    tds[2]: YY + Pos (연도코드 + 포지션, Team 없음)
    tds[3]: PA (Sort값, 스킵 - 중복)
    tds[4]: G
    tds[5]: oWAR
    ... (이후 순서대로)
    
    Returns:
        컬럼명 리스트 (Pos만 포함, Team 없음)
    """
    # 실제 데이터 컬럼 순서 (Rank, Name 제외)
    # tds[2]는 "YY + Pos" 형식 - Team 정보 없음!
    columns = [
        'Pos',      # tds[2]에서 추출 (Team 없음)
        'PA_sort',  # tds[3]: Sort 컬럼 (PA 값, 스킵)
        'G', 'oWAR', 'dWAR', 'PA', 'ePA', 'AB', 'R', 'H', 
        '2B', '3B', 'HR', 'TB', 'RBI', 'SB', 'CS', 'BB', 'HP', 'IB', 
        'SO', 'GDP', 'SH', 'SF',
        'AVG', 'OBP', 'SLG', 'OPS', 'R/ePA', 'wRC+', 'WAR'
    ]
    return columns


def _convert_batting_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """타자 DataFrame 데이터 타입 변환."""
    int_cols = ['Year', 'Age', 'G', 'PA', 'ePA', 'AB', 'R', 'H', '2B', '3B', 'HR', 
                'TB', 'RBI', 'SB', 'CS', 'BB', 'HP', 'IB', 'SO', 'GDP', 'SH', 'SF']
    float_cols = ['oWAR', 'dWAR', 'WAR', 'AVG', 'OBP', 'SLG', 'OPS', 'R/ePA', 'wRC+']
    
    for col in int_cols:
        if col in df.columns:
            df[col] = _to_numeric_series(df[col]).fillna(0).astype(int)
    
    for col in float_cols:
        if col in df.columns:
            df[col] = _to_numeric_series(df[col]).fillna(0.0)
    
    return df


def scrape_all_years_batting(
    session: requests.Session,
    start_year: int = 1982,
    end_year: int = 2025,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    limit_per_year: int = 1000
) -> pd.DataFrame:
    """
    1982년부터 현재까지 모든 연도의 타자 기록 스크래핑.
    
    Args:
        session: 로그인된 requests.Session
        start_year: 시작 연도 (기본 1982)
        end_year: 종료 연도 (기본 2025)
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        limit_per_year: 연도별 최대 선수 수 (기본 1000)
    
    Returns:
        pd.DataFrame: 모든 연도 타자 기록 통합 (player_id, Year 포함)
    """
    all_data = []
    failed_years = []
    total_records = 0
    max_retries_per_year = 3
    
    years = list(range(start_year, end_year + 1))
    pbar = tqdm(years, desc="연도별 스크래핑")
    
    for year in pbar:
        success = False
        last_error = None
        
        for attempt in range(max_retries_per_year):
            try:
                df = scrape_yearly_batting_stats(
                    session=session,
                    year=year,
                    proxy_rotator=proxy_rotator,
                    rate_limiter=rate_limiter,
                    limit=limit_per_year
                )
                
                if not df.empty:
                    all_data.append(df)
                    total_records += len(df)
                    pbar.set_postfix({"연도": year, "선수": len(df), "누적": total_records})
                else:
                    pbar.set_postfix({"연도": year, "선수": 0, "누적": total_records})
                
                success = True
                break  # 성공하면 재시도 루프 탈출
                    
            except ForbiddenError as e:
                last_error = e
                pbar.set_postfix({"연도": year, "상태": f"403 ({attempt+1}/{max_retries_per_year})"})
                if attempt < max_retries_per_year - 1:
                    time.sleep(5 * (attempt + 1))  # 점진적 대기
                
            except Exception as e:
                last_error = e
                pbar.set_postfix({"연도": year, "상태": f"재시도 ({attempt+1}/{max_retries_per_year})"})
                if attempt < max_retries_per_year - 1:
                    time.sleep(2 * (attempt + 1))  # 점진적 대기
        
        if not success:
            failed_years.append(year)
            error_msg = str(last_error)[:50] if last_error else "알 수 없는 오류"
            print(f"\n[실패] {year}년: {error_msg}")
    
    if failed_years:
        print(f"\n실패한 연도: {failed_years}")
    
    if not all_data:
        return pd.DataFrame()
    
    # 모든 연도 데이터 통합
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n총 {len(combined_df)}개 레코드 수집 (연도: {start_year}-{end_year})")
    
    return combined_df


# =============================================================================
# Age-based Batting Data Scraping (연령별 스크래핑)
# =============================================================================


def scrape_batting_by_age(
    session: requests.Session,
    age: int,
    start_year: int = 1982,
    end_year: int = 2025,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    limit: int = 10000
) -> pd.DataFrame:
    """
    특정 연령의 모든 시즌 타자 기록 스크래핑.
    
    URL 예시 (19세):
    https://statiz.co.kr/stats/?m=main&m2=batting&m3=default&so=WAR&ob=DESC
    &year=2025&sy=1982&ey=2025&as=19&ae=19&pr=10000&lt=10100&reg=A
    
    Args:
        session: 로그인된 requests.Session
        age: 조회할 연령 (17~51)
        start_year: 시작 연도 (기본 1982)
        end_year: 종료 연도 (기본 2025)
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        limit: 최대 결과 수 (기본 10000)
    
    Returns:
        pd.DataFrame: 해당 연령의 모든 시즌 타자 기록 (player_id, Year, Age 포함)
    """
    # URL 구성
    url = (
        f"https://statiz.co.kr/stats/?"
        f"m=main&m2=batting&m3=default&so=WAR&ob=DESC"
        f"&year={end_year}&sy={start_year}&ey={end_year}"
        f"&te=&po=&lt=10100&reg=A&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp="
        f"&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li="
        f"&as={age}&ae={age}"  # 연령 필터
        f"&pl=&gc=&lr=&pr={limit}&ph=&hs=&us=&na=&ls=0"
        f"&sf1=G&sk1=&sv1=&sf2=G&sk2=&sv2="
    )
    
    response = _make_request(
        session=session,
        url=url,
        proxy_rotator=proxy_rotator,
        rate_limiter=rate_limiter,
        max_retries=5
    )
    
    # 로그인 필요 페이지로 리다이렉트 되었는지 확인
    if "alert('로그인" in response.text or "location.href='/member/?m=login" in response.text:
        raise StatizError(f"Age {age} - 세션 만료 (로그인 필요)")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        raise StatizError(f"Age {age} - 테이블을 찾을 수 없습니다")
    
    table = tables[0]
    rows = table.find_all("tr")
    
    if len(rows) < 3:
        # 해당 연령에 데이터 없음
        return pd.DataFrame()
    
    # 데이터 파싱
    records = []
    for i in range(2, len(rows)):
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue
        
        # 선수 ID 추출 (이름 컬럼의 링크에서)
        name_td = tds[1]
        a_tag = name_td.find('a')
        if not a_tag:
            continue
        
        href = a_tag.get('href', '')
        if 'p_no=' not in str(href):
            continue
        
        player_id = str(href).split('p_no=')[-1].split('&')[0]
        name = name_td.text.strip()
        
        # Team/Year/Pos 셀 파싱 (tds[2]) - "YY  POS" 형식 (예: "94  RF")
        # 연도 코드(YY)는 2자리, 82~99면 1900년대, 00~25면 2000년대
        team_pos_td = tds[2]
        team_pos_text = team_pos_td.text.strip()
        parts = team_pos_text.split()
        
        year_code = None
        pos = ''
        
        if len(parts) >= 2:
            # 첫 번째가 연도 코드, 마지막이 포지션
            year_code_str = parts[0]
            pos = parts[-1]
            
            try:
                year_code = int(year_code_str)
            except ValueError:
                pass
        elif len(parts) == 1:
            # 포지션만 있는 경우
            pos = parts[0]
        
        # 연도 코드를 실제 연도로 변환
        year = None
        if year_code is not None:
            if 82 <= year_code <= 99:
                year = 1900 + year_code
            elif 0 <= year_code <= 30:  # 2000~2030
                year = 2000 + year_code
        
        record = {
            'player_id': player_id,
            'Name': name,
            'Year': year,
            'Age': age,
            'Pos': pos
        }
        
        # 나머지 스탯 컬럼 파싱 (tds[3]은 Sort값 스킵, tds[4]부터)
        stat_columns = [
            'G', 'oWAR', 'dWAR', 'PA', 'ePA', 'AB', 'R', 'H', 
            '2B', '3B', 'HR', 'TB', 'RBI', 'SB', 'CS', 'BB', 'HP', 'IB', 
            'SO', 'GDP', 'SH', 'SF',
            'AVG', 'OBP', 'SLG', 'OPS', 'R/ePA', 'wRC+', 'WAR'
        ]
        
        stat_start_idx = 4
        for col_idx, col_name in enumerate(stat_columns):
            td_idx = stat_start_idx + col_idx
            if td_idx < len(tds):
                record[col_name] = tds[td_idx].text.strip()
        
        records.append(record)
    
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    
    # 데이터 타입 변환
    df = _convert_batting_dtypes(df)
    
    return df


def scrape_all_ages_batting(
    session: requests.Session,
    start_age: int = 17,
    end_age: int = 51,
    start_year: int = 1982,
    end_year: int = 2025,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    limit_per_age: int = 10000
) -> pd.DataFrame:
    """
    17세부터 51세까지 모든 연령의 타자 기록 스크래핑.
    
    각 연령별로 1982~2025년 전체 시즌 데이터를 조회.
    결과에 Year와 Age 컬럼이 모두 포함됨.
    
    Args:
        session: 로그인된 requests.Session
        start_age: 시작 연령 (기본 17)
        end_age: 종료 연령 (기본 51)
        start_year: 시작 연도 (기본 1982)
        end_year: 종료 연도 (기본 2025)
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        limit_per_age: 연령별 최대 선수 수 (기본 10000)
    
    Returns:
        pd.DataFrame: 모든 연령의 타자 기록 통합 (player_id, Year, Age 포함)
    """
    all_data = []
    failed_ages = []
    total_records = 0
    max_retries_per_age = 3
    
    ages = list(range(start_age, end_age + 1))
    pbar = tqdm(ages, desc="연령별 스크래핑")
    
    for age in pbar:
        success = False
        last_error = None
        
        for attempt in range(max_retries_per_age):
            try:
                df = scrape_batting_by_age(
                    session=session,
                    age=age,
                    start_year=start_year,
                    end_year=end_year,
                    proxy_rotator=proxy_rotator,
                    rate_limiter=rate_limiter,
                    limit=limit_per_age
                )
                
                if not df.empty:
                    all_data.append(df)
                    total_records += len(df)
                    pbar.set_postfix({"연령": age, "레코드": len(df), "누적": total_records})
                else:
                    pbar.set_postfix({"연령": age, "레코드": 0, "누적": total_records})
                
                success = True
                break  # 성공하면 재시도 루프 탈출
                    
            except ForbiddenError as e:
                last_error = e
                pbar.set_postfix({"연령": age, "상태": f"403 ({attempt+1}/{max_retries_per_age})"})
                if attempt < max_retries_per_age - 1:
                    time.sleep(5 * (attempt + 1))  # 점진적 대기
                
            except Exception as e:
                last_error = e
                pbar.set_postfix({"연령": age, "상태": f"재시도 ({attempt+1}/{max_retries_per_age})"})
                if attempt < max_retries_per_age - 1:
                    time.sleep(2 * (attempt + 1))  # 점진적 대기
        
        if not success:
            failed_ages.append(age)
            error_msg = str(last_error)[:50] if last_error else "알 수 없는 오류"
            print(f"\n[실패] {age}세: {error_msg}")
    
    if failed_ages:
        print(f"\n실패한 연령: {failed_ages}")
    
    if not all_data:
        return pd.DataFrame()
    
    # 모든 연령 데이터 통합
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n총 {len(combined_df)}개 레코드 수집 (연령: {start_age}-{end_age}세, 연도: {start_year}-{end_year})")
    
    return combined_df


def aggregate_by_player(
    df: pd.DataFrame,
    fill_missing_years: bool = True
) -> dict:
    """
    연도별 통합 데이터를 선수 ID별로 그룹화하고 누락 연도 채우기.
    
    Args:
        df: scrape_all_years_batting()의 결과 DataFrame
        fill_missing_years: 누락 연도를 0으로 채울지 여부
    
    Returns:
        dict: {player_id: DataFrame} 형태의 딕셔너리
    """
    if df.empty:
        return {}
    
    players = {}
    grouped = df.groupby('player_id')
    
    for player_id, group in tqdm(grouped, desc="선수별 데이터 정리"):
        player_df = group.copy().sort_values('Year').reset_index(drop=True)
        name = player_df['Name'].iloc[0]
        
        if fill_missing_years and len(player_df) > 1:
            player_df = _fill_missing_years_for_player(player_df)
        
        players[player_id] = player_df
    
    print(f"\n총 {len(players)}명의 선수 데이터 정리 완료")
    return players


def _fill_missing_years_for_player(df: pd.DataFrame) -> pd.DataFrame:
    """
    선수의 누락된 연도를 0으로 채우기.
    
    첫 시즌부터 마지막 시즌까지 빈 연도가 있으면 0으로 채움.
    """
    if df.empty or len(df) < 2:
        return df
    
    year_series = _to_numeric_series(df['Year']).dropna()
    if year_series.empty:
        return df

    min_year = int(year_series.min())
    max_year = int(year_series.max())
    existing_years = {int(year) for year in year_series.tolist()}
    
    # 기본 정보 (첫 행에서 가져오기)
    name = df['Name'].iloc[0]
    player_id = df['player_id'].iloc[0]
    
    # Age 계산을 위한 참조
    ref_row = df.iloc[0]
    ref_year = int(ref_row['Year'])
    ref_age = int(ref_row['Age']) if 'Age' in df.columns else 0
    
    # 누락 연도 찾기
    missing_years = []
    for year in range(min_year, max_year + 1):
        if year not in existing_years:
            missing_years.append(year)
    
    if not missing_years:
        return df
    
    # 누락 연도에 대한 빈 레코드 생성
    new_rows = []
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    
    for year in missing_years:
        new_row = {
            'player_id': player_id,
            'Name': name,
            'Year': year,
            'Age': ref_age + (year - ref_year) if ref_age > 0 else 0,
        }
        
        # 나머지 숫자 컬럼은 0으로
        for col in numeric_cols:
            if col not in new_row:
                new_row[col] = 0
        
        # 문자열 컬럼
        if 'Team' in df.columns:
            new_row['Team'] = ''
        if 'Pos' in df.columns:
            new_row['Pos'] = ''
        
        new_rows.append(new_row)
    
    if new_rows:
        new_df = pd.DataFrame(new_rows)
        df = pd.concat([df, new_df], ignore_index=True)
        df = df.sort_values('Year').reset_index(drop=True)
    
    return df


def convert_to_player_list(players: dict) -> list:
    """
    {player_id: DataFrame} 딕셔너리를 DataFrame 리스트로 변환.
    
    기존 bat_list 형식과 호환되도록 변환.
    
    Args:
        players: aggregate_by_player()의 결과
    
    Returns:
        list: [DataFrame, DataFrame, ...] 형태
    """
    return list(players.values())


def scrape_batting_data_fast(
    session: requests.Session,
    start_year: int = 1982,
    end_year: int = 2025,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    fill_missing_years: bool = True
) -> list:
    """
    빠른 타자 데이터 스크래핑 (연도별 접근 방식).
    
    기존 방식: ~2800 요청 (선수별)
    새 방식: ~44 요청 (연도별) -> 98% 요청 감소!
    
    Args:
        session: 로그인된 requests.Session
        start_year: 시작 연도 (기본 1982)
        end_year: 종료 연도 (기본 2025)
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        fill_missing_years: 누락 연도를 0으로 채울지 여부
    
    Returns:
        list: [DataFrame, DataFrame, ...] 형태 (기존 bat_list와 동일한 형식)
    
    Usage:
        session = get_session_from_driver(driver)
        rate_limiter = RateLimiter(min_delay=1.0, max_delay=2.0)
        bat_list = scrape_batting_data_fast(session, rate_limiter=rate_limiter)
    """
    print("=" * 50)
    print("빠른 타자 데이터 스크래핑 (연도별 접근)")
    print(f"연도 범위: {start_year} - {end_year} ({end_year - start_year + 1}년)")
    print("=" * 50)
    
    # Step 1: 모든 연도 스크래핑
    all_data = scrape_all_years_batting(
        session=session,
        start_year=start_year,
        end_year=end_year,
        proxy_rotator=proxy_rotator,
        rate_limiter=rate_limiter
    )
    
    if all_data.empty:
        print("데이터를 가져오지 못했습니다.")
        return []
    
    # Step 2: 선수별로 그룹화 및 누락 연도 채우기
    players = aggregate_by_player(all_data, fill_missing_years=fill_missing_years)
    
    # Step 3: 리스트로 변환
    bat_list = convert_to_player_list(players)
    
    print(f"\n최종 결과: {len(bat_list)}명의 선수 데이터")
    return bat_list


def scrape_batting_data_by_age(
    session: requests.Session,
    start_age: int = 17,
    end_age: int = 51,
    start_year: int = 1982,
    end_year: int = 2025,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    fill_missing_years: bool = True
) -> list:
    """
    연령별 타자 데이터 스크래핑 (Age 컬럼 포함).
    
    장점:
    - 생년월일 검색 불필요 (Age가 데이터에 직접 포함됨)
    - 정확한 생물학적 나이 제공 (Statiz에서 제공하는 값 사용)
    
    요청 수: 약 35개 (17~51세 = 35개 연령)
    
    Args:
        session: 로그인된 requests.Session
        start_age: 시작 연령 (기본 17)
        end_age: 종료 연령 (기본 51)
        start_year: 시작 연도 (기본 1982)
        end_year: 종료 연도 (기본 2025)
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        fill_missing_years: 누락 연도를 0으로 채울지 여부
    
    Returns:
        list: [DataFrame, DataFrame, ...] 형태 (기존 bat_list와 동일한 형식)
              각 DataFrame에는 Year, Age 컬럼이 포함됨
    
    Usage:
        session = get_session_from_driver(driver)
        rate_limiter = RateLimiter(min_delay=0.5, max_delay=1.5)
        bat_list = scrape_batting_data_by_age(session, rate_limiter=rate_limiter)
        
        # 각 선수 DataFrame에 Age 컬럼이 이미 포함됨
        for df in bat_list:
            print(df[['Name', 'Year', 'Age', 'WAR']].head())
    """
    print("=" * 50)
    print("연령별 타자 데이터 스크래핑")
    print(f"연령 범위: {start_age} - {end_age}세 ({end_age - start_age + 1}개 요청)")
    print(f"연도 범위: {start_year} - {end_year}")
    print("=" * 50)
    
    # Step 1: 모든 연령 스크래핑
    all_data = scrape_all_ages_batting(
        session=session,
        start_age=start_age,
        end_age=end_age,
        start_year=start_year,
        end_year=end_year,
        proxy_rotator=proxy_rotator,
        rate_limiter=rate_limiter
    )
    
    if all_data.empty:
        print("데이터를 가져오지 못했습니다.")
        return []
    
    # Step 2: 선수별로 그룹화 및 누락 연도 채우기
    players = aggregate_by_player(all_data, fill_missing_years=fill_missing_years)
    
    # Step 3: 리스트로 변환
    bat_list = convert_to_player_list(players)
    
    print(f"\n최종 결과: {len(bat_list)}명의 선수 데이터 (Age 컬럼 포함)")
    return bat_list


# =============================================================================
# Age Estimation from Debut Year (Simple Fallback)
# =============================================================================

import re
from datetime import date
from typing import Dict


def estimate_age_from_debut_year(
    first_year: int,
    target_year: int,
    default_debut_age: int = 20
) -> int:
    """
    데뷔 연도로부터 나이 추정.
    
    KBO 선수 평균 데뷔 나이:
    - 고졸 직행: 18-19세
    - 대졸 입단: 22-23세
    - 평균: 약 20세
    
    Args:
        first_year: 첫 KBO 시즌
        target_year: 나이 계산 대상 연도
        default_debut_age: 기본 데뷔 나이 (기본값 20)
    
    Returns:
        추정 나이 (오차 ±2세)
    """
    years_played = target_year - first_year
    return default_debut_age + years_played


def add_estimated_age_to_batting_data(
    bat_list: list,
    target_year: int = 2026,
    default_debut_age: int = 20
) -> list:
    """
    bat_list의 각 선수 DataFrame에 추정 Age 컬럼 추가.
    
    Statiz 선수 페이지가 JS 렌더링이라 requests로 접근 불가하여,
    데뷔 연도 기반으로 나이를 추정함.
    
    Args:
        bat_list: 선수별 DataFrame 리스트
        target_year: 나이 계산 대상 연도
        default_debut_age: 기본 데뷔 나이 (기본값 20)
    
    Returns:
        Age 컬럼이 추가된 bat_list
    """
    if not bat_list:
        return bat_list
    
    print("=" * 50)
    print(f"Age 컬럼 추가 (데뷔 연도 기반 추정, 오차 ±2세)")
    print(f"대상 연도: {target_year}, 기본 데뷔 나이: {default_debut_age}세")
    print("=" * 50)
    
    updated_list = []
    
    for df in bat_list:
        df = df.copy()
        
        if not df.empty and 'Year' in df.columns:
            first_year = df['Year'].min()
            
            # 각 연도별 추정 나이 계산
            df['Age'] = df['Year'].apply(
                lambda y: estimate_age_from_debut_year(first_year, y, default_debut_age)
            )
        else:
            df['Age'] = None
        
        updated_list.append(df)
    
    print(f"Age 추가 완료: {len(updated_list)}명")
    return updated_list


# =============================================================================
# Age Scraping from Statiz Player Pages (Most Reliable - requires JS rendering)
# NOTE: Statiz player pages use JavaScript rendering, cannot be scraped via requests.
# Use Selenium for full page rendering if needed.
# =============================================================================


def scrape_age_from_statiz_player_page(
    session: requests.Session,
    player_id: str,
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None
) -> Optional[Dict[int, int]]:
    """
    Statiz 선수 페이지에서 연도별 Age 정보 스크래핑.
    
    URL: https://statiz.co.kr/player/?m=year&p_no={player_id}
    
    선수 페이지의 Year 테이블에는 Age 컬럼이 있음.
    연도별 접근에서는 Age가 없지만, 개별 선수 페이지에는 있음.
    
    Args:
        session: 로그인된 requests.Session
        player_id: Statiz player ID
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
    
    Returns:
        {year: age} 딕셔너리 또는 None (실패 시)
    
    Example:
        >>> ages = scrape_age_from_statiz_player_page(session, "12345")
        >>> ages
        {2020: 28, 2021: 29, 2022: 30, ...}
    """
    url = f"https://statiz.co.kr/player/?m=year&p_no={player_id}"
    
    try:
        response = _make_request(
            session=session,
            url=url,
            proxy_rotator=proxy_rotator,
            rate_limiter=rate_limiter,
            max_retries=3
        )
    except (ForbiddenError, MaxRetriesExceededError) as e:
        print(f"[Age 스크래핑 실패] player_id={player_id}: {e}")
        return None
    
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all("table")
    if not tables:
        return None
    
    table = tables[0]
    rows = table.find_all("tr")
    
    if len(rows) < 3:
        return None
    
    # 헤더에서 Age 컬럼 인덱스 찾기
    header_row = rows[0]
    headers = [th.text.strip() for th in header_row.find_all(['th', 'td'])]
    
    age_col_idx = None
    year_col_idx = None
    
    for idx, col_name in enumerate(headers):
        if col_name == 'Age':
            age_col_idx = idx
        elif col_name == 'Year':
            year_col_idx = idx
    
    if age_col_idx is None or year_col_idx is None:
        return None
    
    # 데이터 행 파싱
    year_age_map = {}
    current_year = None
    
    for i in range(2, len(rows) - 3):  # 마지막 3개 행은 통산/헤더
        tr = rows[i]
        if tr.find("th") is not None:
            continue
        
        tds = tr.find_all("td")
        num_cells = len(tds)
        
        if num_cells == len(headers):
            # 일반 행 (Year 포함)
            year_text = tds[year_col_idx].text.strip()
            if not year_text.isdigit():
                continue
            
            current_year = int(year_text)
            try:
                age = int(tds[age_col_idx].text.strip())
                year_age_map[current_year] = age
            except ValueError:
                continue
                
        elif num_cells == len(headers) - 1 and current_year is not None:
            # rowspan 연속 행 (Year 없음, 시즌 중 이적)
            # Age는 같은 연도이므로 이미 저장됨
            pass
    
    return year_age_map if year_age_map else None


def scrape_ages_for_recent_players(
    session: requests.Session,
    bat_list: list,
    recent_years: list = [2022, 2023, 2024, 2025],
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None,
    cache_path: str = "age_cache.json"
) -> Dict[str, Dict[int, int]]:
    """
    최근 활동한 선수들의 Age 정보만 스크래핑 (효율적).
    
    2022-2024년 중 한 해라도 PA > 0인 선수만 대상.
    캐시를 활용하여 중복 스크래핑 방지.
    
    Args:
        session: 로그인된 requests.Session
        bat_list: 선수별 DataFrame 리스트
        recent_years: 최근 활동 기준 연도 리스트
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
        cache_path: Age 캐시 파일 경로
    
    Returns:
        {player_id: {year: age}} 딕셔너리
    """
    # Age 캐시 로드
    cache = _load_age_cache(cache_path)
    
    # 최근 활동 선수 필터링
    recent_player_ids = []
    player_names = {}
    
    for df in bat_list:
        if df.empty or 'player_id' not in df.columns:
            continue
        
        pid = str(df['player_id'].iloc[0])
        
        # 이미 캐시에 있으면 스킵
        if pid in cache:
            continue
        
        # 최근 활동 여부 확인
        recent_data = df[df['Year'].isin(recent_years)]
        if recent_data.empty:
            continue
        
        if 'PA' in df.columns and (recent_data['PA'] > 0).any():
            recent_player_ids.append(pid)
            player_names[pid] = df['Name'].iloc[0] if 'Name' in df.columns else ""
    
    print(f"Age 스크래핑 대상: {len(recent_player_ids)}명 (캐시: {len(cache)}명)")
    
    if not recent_player_ids:
        return cache
    
    # Rate limiter 기본값
    if rate_limiter is None:
        rate_limiter = RateLimiter(min_delay=1.0, max_delay=2.0)
    
    # 스크래핑
    new_found = 0
    not_found = 0
    
    pbar = tqdm(recent_player_ids, desc="Age 스크래핑 중")
    
    for pid in pbar:
        name = player_names.get(pid, "")
        pbar.set_postfix({"선수": name[:6], "찾음": new_found, "실패": not_found})
        
        year_age_map = scrape_age_from_statiz_player_page(
            session=session,
            player_id=pid,
            proxy_rotator=proxy_rotator,
            rate_limiter=rate_limiter
        )
        
        if year_age_map:
            cache[pid] = year_age_map
            new_found += 1
        else:
            cache[pid] = {}  # 빈 딕셔너리로 표시 (재시도 방지)
            not_found += 1
        
        # 주기적으로 캐시 저장 (20명마다)
        if (new_found + not_found) % 20 == 0:
            _save_age_cache(cache, cache_path)
    
    # 최종 캐시 저장
    _save_age_cache(cache, cache_path)
    
    print(f"\nAge 스크래핑 완료:")
    print(f"  - 새로 찾음: {new_found}명")
    print(f"  - 찾지 못함: {not_found}명")
    print(f"  - 캐시 총: {len(cache)}명")
    
    return cache


def _load_age_cache(cache_path: str) -> Dict[str, Dict[int, int]]:
    """Age 캐시 로드."""
    path = Path(cache_path)
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                raw_cache = json.load(f)
            # JSON은 키가 문자열이므로 int로 변환
            cache = {}
            for pid, year_age in raw_cache.items():
                cache[pid] = {int(y): a for y, a in year_age.items()} if year_age else {}
            return cache
        except Exception:
            return {}
    return {}


def _save_age_cache(cache: Dict[str, Dict[int, int]], cache_path: str):
    """Age 캐시 저장."""
    # int 키를 문자열로 변환 (JSON 호환)
    json_cache = {}
    for pid, year_age in cache.items():
        json_cache[pid] = {str(y): a for y, a in year_age.items()} if year_age else {}
    
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(json_cache, f, ensure_ascii=False, indent=2)


def add_age_to_batting_data_v2(
    bat_list: list,
    session: requests.Session,
    age_cache_path: str = "age_cache.json",
    proxy_rotator: Optional[ProxyRotator] = None,
    rate_limiter: Optional[RateLimiter] = None
) -> list:
    """
    bat_list의 각 선수 DataFrame에 Age 컬럼 추가 (Statiz 페이지 직접 스크래핑).
    
    기존 add_age_to_batting_data()보다 훨씬 안정적:
    - Statiz 자체 데이터 사용 (정확도 100%)
    - 최근 활동 선수만 대상 (~300명)
    - 캐시 활용으로 재실행 시 빠름
    
    Args:
        bat_list: 선수별 DataFrame 리스트
        session: 로그인된 requests.Session
        age_cache_path: Age 캐시 파일 경로
        proxy_rotator: ProxyRotator 인스턴스 (선택)
        rate_limiter: RateLimiter 인스턴스 (선택)
    
    Returns:
        Age 컬럼이 추가된 bat_list
    """
    if not bat_list:
        return bat_list
    
    print("=" * 50)
    print("Age 컬럼 추가 (Statiz 선수 페이지 직접 스크래핑)")
    print("=" * 50)
    
    # Age 정보 스크래핑 (최근 활동 선수만)
    age_cache = scrape_ages_for_recent_players(
        session=session,
        bat_list=bat_list,
        recent_years=[2022, 2023, 2024, 2025],
        proxy_rotator=proxy_rotator,
        rate_limiter=rate_limiter,
        cache_path=age_cache_path
    )
    
    # Age 컬럼 추가
    updated_list = []
    with_age = 0
    
    for df in bat_list:
        df = df.copy()
        
        if 'player_id' in df.columns and not df.empty:
            pid = str(df['player_id'].iloc[0])
            
            if pid in age_cache and age_cache[pid]:
                year_age_map = age_cache[pid]
                # 연도별 Age 매핑
                df['Age'] = df['Year'].apply(lambda y: year_age_map.get(y, None))
                if df['Age'].notna().any():
                    with_age += 1
            else:
                df['Age'] = None
        else:
            df['Age'] = None
        
        updated_list.append(df)
    
    print(f"\nAge 추가 완료: {with_age}/{len(updated_list)}명")
    
    return updated_list


# =============================================================================
# Birthdate Scraping via Google Search (Legacy - less reliable)
# =============================================================================


def search_birthdate_namuwiki(
    name: str,
    rate_limiter: Optional[RateLimiter] = None
) -> Optional[date]:
    """
    나무위키에서 선수 생년월일 찾기.
    
    Args:
        name: 선수 이름
        rate_limiter: RateLimiter
    
    Returns:
        date 객체 또는 None
    """
    import urllib.parse
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    
    # 다양한 URL 패턴 시도
    url_patterns = [
        f"https://namu.wiki/w/{urllib.parse.quote(name)}(야구선수)",
        f"https://namu.wiki/w/{urllib.parse.quote(name)}(야구)",
        f"https://namu.wiki/w/{urllib.parse.quote(name)}",
    ]
    
    for url in url_patterns:
        if rate_limiter:
            rate_limiter.wait()
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                # "야구" 또는 "KBO" 키워드가 있는지 확인 (야구선수 페이지인지)
                if '야구' in response.text or 'KBO' in response.text or '프로야구' in response.text:
                    birthdate = _extract_birthdate_from_html(response.text)
                    if birthdate:
                        return birthdate
                        
        except Exception as e:
            continue
    
    return None


def search_birthdate_google(
    name: str, 
    player_id: str,
    rate_limiter: Optional[RateLimiter] = None,
    max_retries: int = 2
) -> Optional[date]:
    """
    생년월일 찾기 - 나무위키 우선, 실패 시 구글.
    
    Args:
        name: 선수 이름
        player_id: Statiz player ID
        rate_limiter: RateLimiter
        max_retries: 최대 재시도 횟수
    
    Returns:
        date 객체 또는 None
    """
    # 1. 나무위키 시도 (더 안정적)
    birthdate = search_birthdate_namuwiki(name, rate_limiter)
    if birthdate:
        return birthdate
    
    # 2. 구글 검색 fallback
    import urllib.parse
    
    queries = [
        f"{name} KBO 야구 생년월일",
        f"{name} 야구선수 생년",
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    for query in queries:
        if rate_limiter:
            rate_limiter.wait()
        
        try:
            encoded_query = urllib.parse.quote(query)
            url = f"https://www.google.com/search?q={encoded_query}&hl=ko"
            
            response = session.get(url, timeout=10)
            
            if response.status_code == 429:
                time.sleep(30)
                continue
            
            if response.status_code != 200:
                continue
            
            birthdate = _extract_birthdate_from_html(response.text)
            if birthdate:
                return birthdate
                
        except Exception as e:
            continue
    
    return None


def _extract_birthdate_from_html(html: str) -> Optional[date]:
    """
    HTML에서 생년월일 패턴 추출.
    
    지원 형식:
    - YYYY년 MM월 DD일
    - YYYY.MM.DD
    - YYYY-MM-DD
    - YYYY/MM/DD
    """
    # 패턴들 (한국식 생년월일 형식)
    patterns = [
        # YYYY년 MM월 DD일
        r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
        # YYYY.MM.DD 또는 YYYY-MM-DD 또는 YYYY/MM/DD
        r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})',
        # 생년월일: YYYY.MM.DD
        r'생년월일[:\s]*(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})',
        # 출생: YYYY년
        r'출생[:\s]*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html)
        for match in matches:
            try:
                year, month, day = int(match[0]), int(match[1]), int(match[2])
                
                # 유효한 연도 범위 (1960-2010, KBO 선수 기준)
                if 1960 <= year <= 2010 and 1 <= month <= 12 and 1 <= day <= 31:
                    return date(year, month, day)
            except (ValueError, IndexError):
                continue
    
    return None


def calculate_age(birthdate: date, target_year: int) -> int:
    """
    생년월일로부터 특정 연도의 나이 계산 (만 나이 기준).
    
    Args:
        birthdate: 생년월일
        target_year: 대상 연도
    
    Returns:
        나이 (int)
    """
    # 해당 연도 시즌 중반(7월 1일) 기준으로 계산
    reference_date = date(target_year, 7, 1)
    age = reference_date.year - birthdate.year
    
    # 생일이 지나지 않았으면 -1
    if (reference_date.month, reference_date.day) < (birthdate.month, birthdate.day):
        age -= 1
    
    return age


def load_birthdate_cache(cache_path: str = "birthdate_cache.json") -> Dict[str, str]:
    """생년월일 캐시 로드."""
    path = Path(cache_path)
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_birthdate_cache(cache: Dict[str, str], cache_path: str = "birthdate_cache.json"):
    """생년월일 캐시 저장."""
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def scrape_birthdates_for_players(
    player_ids: list,
    player_names: dict,
    rate_limiter: Optional[RateLimiter] = None,
    cache_path: str = "birthdate_cache.json",
    progress_bar: bool = True
) -> Dict[str, date]:
    """
    여러 선수의 생년월일 스크래핑 (캐시 활용).
    
    Args:
        player_ids: player_id 리스트
        player_names: {player_id: name} 딕셔너리
        rate_limiter: RateLimiter
        cache_path: 캐시 파일 경로
        progress_bar: tqdm 사용 여부
    
    Returns:
        {player_id: date} 딕셔너리
    """
    # 캐시 로드
    cache = load_birthdate_cache(cache_path)
    results = {}
    new_found = 0
    cache_hits = 0
    not_found = 0
    
    # Rate limiter 기본값 (구글용 - 더 느리게)
    if rate_limiter is None:
        rate_limiter = RateLimiter(min_delay=2.0, max_delay=4.0)
    
    iterator = tqdm(player_ids, desc="생년월일 검색 중") if progress_bar else player_ids
    
    for player_id in iterator:
        player_id_str = str(player_id)
        name = player_names.get(player_id_str, "")
        
        # 캐시 확인
        if player_id_str in cache:
            cached_date = cache[player_id_str]
            if cached_date:
                try:
                    year, month, day = map(int, cached_date.split('-'))
                    results[player_id_str] = date(year, month, day)
                    cache_hits += 1
                except:
                    pass
            continue
        
        # 구글 검색
        birthdate = search_birthdate_google(name, player_id_str, rate_limiter)
        
        if birthdate:
            results[player_id_str] = birthdate
            cache[player_id_str] = birthdate.isoformat()
            new_found += 1
        else:
            cache[player_id_str] = ""  # Not found 표시
            not_found += 1
        
        # 주기적으로 캐시 저장 (10명마다)
        if (new_found + not_found) % 10 == 0:
            save_birthdate_cache(cache, cache_path)
    
    # 최종 캐시 저장
    save_birthdate_cache(cache, cache_path)
    
    print(f"\n생년월일 검색 완료:")
    print(f"  - 캐시 히트: {cache_hits}명")
    print(f"  - 새로 찾음: {new_found}명")
    print(f"  - 찾지 못함: {not_found}명")
    
    return results


def add_age_to_batting_data(
    bat_list: list,
    birthdate_cache_path: str = "birthdate_cache.json",
    rate_limiter: Optional[RateLimiter] = None
) -> list:
    """
    bat_list의 각 선수 DataFrame에 Age 컬럼 추가.
    
    Args:
        bat_list: 선수별 DataFrame 리스트
        birthdate_cache_path: 생년월일 캐시 파일 경로
        rate_limiter: RateLimiter (구글 검색용)
    
    Returns:
        Age 컬럼이 추가된 bat_list
    """
    if not bat_list:
        return bat_list
    
    # 모든 player_id와 이름 수집
    player_ids = []
    player_names = {}
    
    for df in bat_list:
        if 'player_id' in df.columns and not df.empty:
            pid = str(df['player_id'].iloc[0])
            player_ids.append(pid)
            player_names[pid] = df['Name'].iloc[0] if 'Name' in df.columns else ""
    
    player_ids = list(set(player_ids))
    print(f"총 {len(player_ids)}명의 선수 생년월일 검색...")
    
    # 생년월일 스크래핑
    birthdates = scrape_birthdates_for_players(
        player_ids=player_ids,
        player_names=player_names,
        rate_limiter=rate_limiter,
        cache_path=birthdate_cache_path
    )
    
    # Age 컬럼 추가
    updated_list = []
    for df in bat_list:
        df = df.copy()
        
        if 'player_id' in df.columns and not df.empty:
            pid = str(df['player_id'].iloc[0])
            
            if pid in birthdates:
                birthdate = birthdates[pid]
                # 각 연도별 나이 계산
                df['Age'] = df['Year'].apply(lambda y: calculate_age(birthdate, y))
            else:
                df['Age'] = None
        else:
            df['Age'] = None
        
        updated_list.append(df)
    
    # Age가 있는 선수 수 계산
    with_age = sum(1 for df in updated_list if df['Age'].notna().any())
    print(f"\nAge 추가 완료: {with_age}/{len(updated_list)}명")
    
    return updated_list
