from __future__ import annotations

import json
import logging
import time
from functools import wraps
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_timed_call(*, logger: logging.Logger, enabled: bool, log_all: bool, slow_ms: float):
    def timed_call(label: str):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                if not enabled:
                    return func(*args, **kwargs)
                started_at = time.perf_counter()
                try:
                    return func(*args, **kwargs)
                finally:
                    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
                    if log_all or elapsed_ms >= slow_ms:
                        logger.info("TIMING %s took %.1f ms", label, elapsed_ms)

            return wrapper

        return decorator

    return timed_call


def _cache_key_to_jsonable(key: Any) -> Any:
    if isinstance(key, tuple):
        return [_cache_key_to_jsonable(part) for part in key]
    return key


def _cache_key_from_jsonable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_cache_key_from_jsonable(part) for part in value)
    return value


def _trim_cache_dict(cache: dict[Any, dict[str, Any]], max_items: int) -> dict[Any, dict[str, Any]]:
    if len(cache) <= max_items:
        return dict(cache)
    items = sorted(cache.items(), key=lambda item: float((item[1] or {}).get("timestamp") or 0.0), reverse=True)[:max_items]
    return dict(items)


def save_persistent_caches(
    *,
    enabled: bool,
    cache_dir: Path,
    cache_path: Path,
    cache_lock: Lock,
    named_caches: dict[str, tuple[dict[Any, dict[str, Any]], int]],
    logger: logging.Logger,
) -> None:
    if not enabled:
        return
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        with cache_lock:
            payload = {
                "version": 1,
                "saved_at": time.time(),
                **{
                    cache_name: [
                        {"key": _cache_key_to_jsonable(key), "value": value}
                        for key, value in _trim_cache_dict(cache, max_items).items()
                    ]
                    for cache_name, (cache, max_items) in named_caches.items()
                },
            }
            temp_path = cache_path.with_suffix(".tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            temp_path.replace(cache_path)
    except Exception as exc:
        logger.warning("Persistent cache save failed: %s", exc)


def load_persistent_caches(
    *,
    enabled: bool,
    cache_path: Path,
    cache_lock: Lock,
    named_targets: dict[str, dict[Any, dict[str, Any]]],
    logger: logging.Logger,
) -> None:
    if not enabled or not cache_path.exists():
        return
    try:
        with cache_lock:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        for cache_name, target in named_targets.items():
            for entry in payload.get(cache_name) or []:
                try:
                    key = _cache_key_from_jsonable(entry.get("key"))
                    value = entry.get("value")
                    if key is None or not isinstance(value, dict):
                        continue
                    target[key] = value
                except Exception:
                    continue
    except Exception as exc:
        logger.warning("Persistent cache load failed: %s", exc)


def create_retry_http_session(
    *,
    user_agent: str,
    retry_total: int,
    backoff_factor: float,
    retry_status_codes: tuple[int, ...],
    pool_connections: int = 20,
    pool_maxsize: int = 20,
    trust_env: bool = False,
) -> requests.Session:
    session = requests.Session()
    session.trust_env = bool(trust_env)
    retry = Retry(
        total=retry_total,
        connect=retry_total,
        read=retry_total,
        status=retry_total,
        backoff_factor=backoff_factor,
        status_forcelist=retry_status_codes,
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nba.com/",
            "Origin": "https://www.nba.com",
            "Connection": "keep-alive",
        }
    )
    return session


def is_transient_request_error(exc: Exception) -> bool:
    if isinstance(
        exc,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True

    text = str(exc).lower()
    transient_tokens = (
        "remote end closed connection without response",
        "connection aborted",
        "max retries exceeded",
        "temporarily unavailable",
        "read timed out",
        "connect timeout",
        "too many requests",
        "429",
        "502",
        "503",
        "504",
    )
    return any(token in text for token in transient_tokens)


def call_with_retries(
    factory: Callable[[], Any],
    *,
    label: str,
    attempts: int,
    base_delay: float,
    retry_predicate: Callable[[Exception], bool],
    before_attempt: Callable[[], None] | None = None,
) -> Any:
    last_exc: Exception | None = None
    for attempt in range(attempts):
        if before_attempt is not None:
            before_attempt()
        try:
            return factory()
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts - 1 or not retry_predicate(exc):
                raise
            time.sleep(base_delay * (2 ** attempt))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{label} failed without a captured exception.")
