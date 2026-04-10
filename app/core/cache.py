"""TTL-based caching layer for Untatiz.

Provides thread-safe, TTL-based caching without external dependencies (Redis, etc).
Uses Python's functools and a simple dict-based cache.

Usage:
    from app.core.cache import cache, cached_query, invalidate_all
    
    # Decorator-based
    @cache(ttl=60)
    def get_team_standings():
        return db.query("SELECT * FROM v_team_standings")
    
    # Function-based
    result = cached_query("standings", lambda: db.query(...), ttl=60)
    
    # Invalidation
    invalidate_all()  # Clear everything
    invalidate("standings")  # Clear specific key
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar

logger = logging.getLogger(__name__)

# Type variable for generic cache
T = TypeVar('T')

# Global cache store
_cache: Dict[str, CacheEntry] = {}
_cache_lock = threading.RLock()

# Default TTLs (seconds)
TTL_SHORT = 30       # Standings, frequently changing data
TTL_MEDIUM = 300     # Rosters, player lists
TTL_LONG = 3600      # GOAT/BOAT, historical data
TTL_STATIC = 86400   # Static config, draft data


class CacheEntry:
    """Single cache entry with TTL."""
    
    __slots__ = ('value', 'expires_at', 'created_at')
    
    def __init__(self, value: Any, ttl: int):
        self.value = value
        self.created_at = time.time()
        self.expires_at = self.created_at + ttl
    
    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at
    
    @property
    def age(self) -> float:
        return time.time() - self.created_at


def _make_key(*args, **kwargs) -> str:
    """Generate cache key from function arguments."""
    key_parts = [str(arg) for arg in args]
    key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
    key_str = ":".join(key_parts)
    return hashlib.md5(key_str.encode()).hexdigest()[:16]


def cache(
    ttl: int = TTL_MEDIUM,
    namespace: Optional[str] = None,
    key_func: Optional[Callable[..., str]] = None
):
    """Decorator to cache function results with TTL.
    
    Args:
        ttl: Time-to-live in seconds
        namespace: Cache namespace (defaults to function name)
        key_func: Custom function to generate cache key from args
        
    Usage:
        @cache(ttl=60)
        def get_standings():
            return expensive_query()
        
        @cache(ttl=300, namespace="teams")
        def get_team(team_id):
            return query_team(team_id)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        ns = namespace or func.__name__
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = _make_key(*args, **kwargs)
            
            full_key = f"{ns}:{cache_key}"
            
            with _cache_lock:
                # Check cache
                if full_key in _cache:
                    entry = _cache[full_key]
                    if not entry.is_expired:
                        logger.debug(f"Cache hit: {full_key} (age: {entry.age:.1f}s)")
                        return entry.value
                    else:
                        # Expired, remove it
                        del _cache[full_key]
                        logger.debug(f"Cache expired: {full_key}")
            
            # Cache miss - call function
            logger.debug(f"Cache miss: {full_key}")
            result = func(*args, **kwargs)
            
            with _cache_lock:
                _cache[full_key] = CacheEntry(result, ttl)
            
            return result
        
        # Add cache management methods to wrapper
        wrapper_any = wrapper  # type: Any
        wrapper_any.invalidate = lambda: invalidate(ns)
        wrapper_any.cache_info = lambda: get_namespace_info(ns)
        
        return wrapper
    
    return decorator


def cached_query(
    key: str,
    query_func: Callable[[], T],
    ttl: int = TTL_MEDIUM,
    namespace: str = "query"
) -> T:
    """Execute query with caching.
    
    Functional alternative to the decorator approach.
    
    Args:
        key: Unique key for this query
        query_func: Function to call on cache miss
        ttl: Time-to-live in seconds
        namespace: Cache namespace
        
    Returns:
        Query result (cached or fresh)
        
    Usage:
        result = cached_query(
            "team_standings",
            lambda: db.query("SELECT * FROM v_team_standings"),
            ttl=60
        )
    """
    full_key = f"{namespace}:{key}"
    
    with _cache_lock:
        if full_key in _cache:
            entry = _cache[full_key]
            if not entry.is_expired:
                logger.debug(f"Cache hit: {full_key}")
                return entry.value
            del _cache[full_key]
    
    # Cache miss
    logger.debug(f"Cache miss: {full_key}")
    result = query_func()
    
    with _cache_lock:
        _cache[full_key] = CacheEntry(result, ttl)
    
    return result


def get(key: str, namespace: str = "default") -> Optional[Any]:
    """Get value from cache.
    
    Args:
        key: Cache key
        namespace: Cache namespace
        
    Returns:
        Cached value or None if not found/expired
    """
    full_key = f"{namespace}:{key}"
    
    with _cache_lock:
        if full_key in _cache:
            entry = _cache[full_key]
            if not entry.is_expired:
                return entry.value
            del _cache[full_key]
    
    return None


def set(key: str, value: Any, ttl: int = TTL_MEDIUM, namespace: str = "default") -> None:
    """Set value in cache.
    
    Args:
        key: Cache key
        value: Value to cache
        ttl: Time-to-live in seconds
        namespace: Cache namespace
    """
    full_key = f"{namespace}:{key}"
    
    with _cache_lock:
        _cache[full_key] = CacheEntry(value, ttl)


def invalidate(namespace: Optional[str] = None, key: Optional[str] = None) -> int:
    """Invalidate cache entries.
    
    Args:
        namespace: Invalidate all entries in namespace (if key is None)
                   or specific entry (if key is provided)
        key: Specific key to invalidate
        
    Returns:
        Number of entries invalidated
    """
    count = 0
    
    with _cache_lock:
        if namespace is None:
            # Invalidate everything
            count = len(_cache)
            _cache.clear()
            logger.info(f"Cache cleared: {count} entries")
        elif key is None:
            # Invalidate namespace
            prefix = f"{namespace}:"
            keys_to_delete = [k for k in _cache if k.startswith(prefix)]
            for k in keys_to_delete:
                del _cache[k]
                count += 1
            logger.debug(f"Cache invalidated namespace '{namespace}': {count} entries")
        else:
            # Invalidate specific key
            full_key = f"{namespace}:{key}"
            if full_key in _cache:
                del _cache[full_key]
                count = 1
                logger.debug(f"Cache invalidated: {full_key}")
    
    return count


def invalidate_all() -> int:
    """Invalidate all cache entries.
    
    Returns:
        Number of entries invalidated
    """
    return invalidate(namespace=None)


def get_stats() -> Dict[str, Any]:
    """Get cache statistics.
    
    Returns:
        dict: Cache statistics
    """
    with _cache_lock:
        total = len(_cache)
        expired = sum(1 for entry in _cache.values() if entry.is_expired)
        
        # Group by namespace
        namespaces: Dict[str, int] = {}
        for key in _cache:
            ns = key.split(':')[0]
            namespaces[ns] = namespaces.get(ns, 0) + 1
        
        return {
            'total_entries': total,
            'expired_entries': expired,
            'active_entries': total - expired,
            'namespaces': namespaces,
        }


def get_namespace_info(namespace: str) -> Dict[str, Any]:
    """Get info about a specific namespace.
    
    Args:
        namespace: Namespace to inspect
        
    Returns:
        dict: Namespace info
    """
    prefix = f"{namespace}:"
    
    with _cache_lock:
        entries = [(k, v) for k, v in _cache.items() if k.startswith(prefix)]
        
        if not entries:
            return {'count': 0, 'keys': []}
        
        return {
            'count': len(entries),
            'keys': [k.replace(prefix, '') for k, _ in entries],
            'oldest_age': max(e.age for _, e in entries),
            'expired': sum(1 for _, e in entries if e.is_expired),
        }


def cleanup_expired() -> int:
    """Remove expired entries from cache.
    
    Returns:
        Number of entries removed
    """
    with _cache_lock:
        expired_keys = [k for k, v in _cache.items() if v.is_expired]
        for k in expired_keys:
            del _cache[k]
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")
        
        return len(expired_keys)


# Namespace-specific invalidation helpers
def invalidate_standings() -> int:
    """Invalidate team standings cache."""
    return invalidate("standings") + invalidate("league")


def invalidate_players() -> int:
    """Invalidate player-related cache."""
    return invalidate("players") + invalidate("player") + invalidate("war")


def invalidate_rosters() -> int:
    """Invalidate roster cache."""
    return invalidate("roster") + invalidate("team")


def invalidate_after_update() -> int:
    """Invalidate all caches that might be affected by data update.
    
    Call this after scraper updates the database.
    """
    count = 0
    count += invalidate("standings")
    count += invalidate("league")
    count += invalidate("players")
    count += invalidate("player")
    count += invalidate("war")
    count += invalidate("roster")
    count += invalidate("team")
    count += invalidate("gboat")
    count += invalidate("query")
    logger.info(f"Post-update cache invalidation: {count} entries cleared")
    return count


__all__ = [
    # Constants
    'TTL_SHORT',
    'TTL_MEDIUM',
    'TTL_LONG',
    'TTL_STATIC',
    # Core functions
    'cache',
    'cached_query',
    'get',
    'set',
    'invalidate',
    'invalidate_all',
    'get_stats',
    'get_namespace_info',
    'cleanup_expired',
    # Convenience invalidators
    'invalidate_standings',
    'invalidate_players',
    'invalidate_rosters',
    'invalidate_after_update',
]
