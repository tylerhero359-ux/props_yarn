from __future__ import annotations

import copy
import json
import logging
import math
import os
import queue

import re
import datetime as dt
import time
import threading
import unicodedata
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import BytesIO
from pathlib import Path
from functools import wraps
from threading import Lock
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pypdf import PdfReader

try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from nba_api.stats.endpoints import (
    CommonPlayerInfo,
    CommonTeamRoster,
    LeagueDashPlayerStats,
    PlayerGameLog,
    PlayerNextNGames,
    ScoreboardV2,
    TeamGameLog,
)
from nba_api.stats.static import players as static_players
from nba_api.stats.static import teams as static_teams
from concurrent.futures import ThreadPoolExecutor, as_completed

LOGGER = logging.getLogger("nba_props_app")
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

NBA_TIMING_ENABLED = os.getenv("NBA_TIMING_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
NBA_TIMING_LOG_ALL = os.getenv("NBA_TIMING_LOG_ALL", "0").strip().lower() in {"1", "true", "yes", "on"}
NBA_TIMING_SLOW_MS = float(os.getenv("NBA_TIMING_SLOW_MS", "800"))

ANALYSIS_PARALLEL_ENABLED = os.getenv("NBA_ANALYSIS_PARALLEL_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
ANALYSIS_PARALLEL_MAX_WORKERS = max(1, int(os.getenv("NBA_ANALYSIS_PARALLEL_MAX_WORKERS", "4")))


def timed_call(label: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not NBA_TIMING_ENABLED:
                return func(*args, **kwargs)
            started_at = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed_ms = (time.perf_counter() - started_at) * 1000.0
                if NBA_TIMING_LOG_ALL or elapsed_ms >= NBA_TIMING_SLOW_MS:
                    LOGGER.info("TIMING %s took %.1f ms", label, elapsed_ms)

        return wrapper

    return decorator

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

WARM_CACHE_ON_STARTUP_ENABLED = os.getenv("NBA_WARM_CACHE_ON_STARTUP_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_STARTUP_MAX_WORKERS = max(1, int(os.getenv("NBA_WARM_CACHE_STARTUP_MAX_WORKERS", "4")))
WARM_CACHE_PRELOAD_TODAYS_GAMES = os.getenv("NBA_WARM_CACHE_PRELOAD_TODAYS_GAMES", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_PRELOAD_INJURIES = os.getenv("NBA_WARM_CACHE_PRELOAD_INJURIES", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_PRELOAD_PLAYERS = os.getenv("NBA_WARM_CACHE_PRELOAD_PLAYERS", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_PRELOAD_TEAMS = os.getenv("NBA_WARM_CACHE_PRELOAD_TEAMS", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_PRELOAD_TEAM_ROSTERS = os.getenv("NBA_WARM_CACHE_PRELOAD_TEAM_ROSTERS", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_TEAM_ROSTERS_MAX_WORKERS = max(1, int(os.getenv("NBA_WARM_CACHE_TEAM_ROSTERS_MAX_WORKERS", "2")))

PERSISTENT_CACHE_DIR = BASE_DIR / ".runtime_cache"
PERSISTENT_CACHE_PATH = PERSISTENT_CACHE_DIR / "persistent_cache.json"
PERSISTENT_CACHE_ENABLED = os.getenv("NBA_PERSISTENT_CACHE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
PERSISTENT_CACHE_MAX_ROSTERS = max(30, int(os.getenv("NBA_PERSISTENT_CACHE_MAX_ROSTERS", "120")))
PERSISTENT_CACHE_MAX_PLAYER_INFO = max(100, int(os.getenv("NBA_PERSISTENT_CACHE_MAX_PLAYER_INFO", "600")))
PERSISTENT_CACHE_MAX_NEXT_GAMES = max(30, int(os.getenv("NBA_PERSISTENT_CACHE_MAX_NEXT_GAMES", "240")))
PERSISTENT_CACHE_LOCK = Lock()

WARM_CACHE_START_LOCK = threading.Lock()
WARM_CACHE_STARTED = False

def _warm_cache_task(name: str, func):
    try:
        start = time.perf_counter()
        func()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        if NBA_TIMING_LOG_ALL or elapsed_ms >= NBA_TIMING_SLOW_MS:
            LOGGER.info("TIMING warm_cache:%s took %.1f ms", name, elapsed_ms)
    except Exception as exc:
        LOGGER.warning("Warm-cache task %s failed: %s", name, exc)

def _today_scoreboard_date() -> str:
    return dt.datetime.now().strftime("%m/%d/%Y")


def _ensure_persistent_cache_dir() -> None:
    if not PERSISTENT_CACHE_ENABLED:
        return
    PERSISTENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_key_to_jsonable(key: Any) -> Any:
    if isinstance(key, tuple):
        return [ _cache_key_to_jsonable(part) for part in key ]
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


def save_persistent_caches() -> None:
    """Write the persistent cache to disk synchronously. Prefer save_persistent_caches_async()
    for call sites inside request handlers so the disk write never blocks a response."""
    if not PERSISTENT_CACHE_ENABLED:
        return
    try:
        _ensure_persistent_cache_dir()
        with PERSISTENT_CACHE_LOCK:
            payload = {
                "version": 1,
                "saved_at": time.time(),
                "roster_cache": [
                    {"key": _cache_key_to_jsonable(key), "value": value}
                    for key, value in _trim_cache_dict(ROSTER_CACHE, PERSISTENT_CACHE_MAX_ROSTERS).items()
                ],
                "player_info_cache": [
                    {"key": _cache_key_to_jsonable(key), "value": value}
                    for key, value in _trim_cache_dict(PLAYER_INFO_CACHE, PERSISTENT_CACHE_MAX_PLAYER_INFO).items()
                ],
                "next_game_cache": [
                    {"key": _cache_key_to_jsonable(key), "value": value}
                    for key, value in _trim_cache_dict(NEXT_GAME_CACHE, PERSISTENT_CACHE_MAX_NEXT_GAMES).items()
                ],
                "team_next_game_cache": [
                    {"key": _cache_key_to_jsonable(key), "value": value}
                    for key, value in _trim_cache_dict(TEAM_NEXT_GAME_CACHE, PERSISTENT_CACHE_MAX_NEXT_GAMES).items()
                ],
            }
            temp_path = PERSISTENT_CACHE_PATH.with_suffix(".tmp")
            temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            temp_path.replace(PERSISTENT_CACHE_PATH)
    except Exception as exc:
        LOGGER.warning("Persistent cache save failed: %s", exc)


# Debounced background save: collapses rapid successive save requests into a
# single disk write after PERSISTENT_CACHE_SAVE_DEBOUNCE_SECONDS of quiet time.
# This prevents each roster/player-info fetch from blocking its caller with a
# synchronous 300 KB JSON write.
PERSISTENT_CACHE_SAVE_DEBOUNCE_SECONDS = float(
    os.getenv("NBA_PERSISTENT_CACHE_SAVE_DEBOUNCE_SECONDS", "5")
)
_PERSISTENT_CACHE_SAVE_PENDING = threading.Event()
_PERSISTENT_CACHE_SAVE_WORKER_STARTED = False
_PERSISTENT_CACHE_SAVE_WORKER_LOCK = Lock()


def _persistent_cache_save_worker() -> None:
    """Background thread that flushes the persistent cache after a debounce window."""
    while True:
        _PERSISTENT_CACHE_SAVE_PENDING.wait()
        _PERSISTENT_CACHE_SAVE_PENDING.clear()
        time.sleep(PERSISTENT_CACHE_SAVE_DEBOUNCE_SECONDS)
        # If another save was requested while we slept, keep draining.
        if _PERSISTENT_CACHE_SAVE_PENDING.is_set():
            continue
        save_persistent_caches()


def _ensure_persistent_cache_save_worker() -> None:
    global _PERSISTENT_CACHE_SAVE_WORKER_STARTED
    with _PERSISTENT_CACHE_SAVE_WORKER_LOCK:
        if _PERSISTENT_CACHE_SAVE_WORKER_STARTED:
            return
        _PERSISTENT_CACHE_SAVE_WORKER_STARTED = True
    threading.Thread(
        target=_persistent_cache_save_worker,
        name="persistent-cache-save",
        daemon=True,
    ).start()


def save_persistent_caches_async() -> None:
    """Schedule a debounced background save. Returns immediately; does not block."""
    if not PERSISTENT_CACHE_ENABLED:
        return
    _ensure_persistent_cache_save_worker()
    _PERSISTENT_CACHE_SAVE_PENDING.set()


def _load_named_persistent_cache(entries: list[dict[str, Any]] | None, target: dict[Any, dict[str, Any]]) -> None:
    for entry in entries or []:
        try:
            key = _cache_key_from_jsonable(entry.get("key"))
            value = entry.get("value")
            if key is None or not isinstance(value, dict):
                continue
            target[key] = value
        except Exception:
            continue


def load_persistent_caches() -> None:
    if not PERSISTENT_CACHE_ENABLED or not PERSISTENT_CACHE_PATH.exists():
        return
    try:
        with PERSISTENT_CACHE_LOCK:
            payload = json.loads(PERSISTENT_CACHE_PATH.read_text(encoding="utf-8"))
        _load_named_persistent_cache(payload.get("roster_cache"), ROSTER_CACHE)
        _load_named_persistent_cache(payload.get("player_info_cache"), PLAYER_INFO_CACHE)
        _load_named_persistent_cache(payload.get("next_game_cache"), NEXT_GAME_CACHE)
        _load_named_persistent_cache(payload.get("team_next_game_cache"), TEAM_NEXT_GAME_CACHE)
        LOGGER.info(
            "Loaded persistent cache snapshot: %s rosters, %s player info, %s next games, %s team next games",
            len(ROSTER_CACHE),
            len(PLAYER_INFO_CACHE),
            len(NEXT_GAME_CACHE),
            len(TEAM_NEXT_GAME_CACHE),
        )
    except Exception as exc:
        LOGGER.warning("Persistent cache load failed: %s", exc)


def preload_team_rosters_for_current_season() -> None:
    current_season = current_nba_season()
    max_workers = min(WARM_CACHE_TEAM_ROSTERS_MAX_WORKERS, max(1, len(TEAM_POOL)))
    team_ids = [int(team["id"]) for team in TEAM_POOL]
    LOGGER.info("Preloading %s team roster(s) for %s with %s worker(s)", len(team_ids), current_season, max_workers)

    def _fetch(team_id: int) -> None:
        try:
            fetch_team_roster(team_id=team_id, season=current_season)
        except Exception as exc:
            LOGGER.warning("Warm-cache roster preload failed for team %s: %s", team_id, exc)

    if max_workers <= 1:
        for team_id in team_ids:
            _fetch(team_id)
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch, team_id) for team_id in team_ids]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                pass


def warm_cache_on_startup() -> None:
    global WARM_CACHE_STARTED
    if not WARM_CACHE_ON_STARTUP_ENABLED:
        return
    with WARM_CACHE_START_LOCK:
        if WARM_CACHE_STARTED:
            return
        WARM_CACHE_STARTED = True

    load_persistent_caches()

    tasks: list[tuple[str, Any]] = []

    if WARM_CACHE_PRELOAD_PLAYERS:
        tasks.append(("players", lambda: PLAYER_POOL))

    if WARM_CACHE_PRELOAD_TEAMS:
        tasks.append(("teams", lambda: list(TEAM_LOOKUP.values())))

    if WARM_CACHE_PRELOAD_TODAYS_GAMES:
        tasks.append(("todays_games", lambda: fetch_scoreboard_games(_today_scoreboard_date())))

    if WARM_CACHE_PRELOAD_INJURIES:
        tasks.append(("injuries", lambda: fetch_latest_injury_report_payload()))

    if WARM_CACHE_PRELOAD_TEAM_ROSTERS:
        tasks.append(("team_rosters", preload_team_rosters_for_current_season))

    if not tasks:
        return

    max_workers = min(WARM_CACHE_STARTUP_MAX_WORKERS, max(1, len(tasks)))
    LOGGER.info("Starting warm-cache preload with %s task(s) and %s worker(s)", len(tasks), max_workers)

    if max_workers <= 1:
        for task_name, task_func in tasks:
            _warm_cache_task(task_name, task_func)
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_warm_cache_task, task_name, task_func): task_name for task_name, task_func in tasks}
        for future in as_completed(future_map):
            try:
                future.result()
            except Exception as exc:
                LOGGER.warning("Warm-cache future %s failed: %s", future_map[future], exc)

app = FastAPI(title="NBA Props Bar Chart App")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

STAT_MAP = {
    "PTS": "PTS",
    "REB": "REB",
    "AST": "AST",
    "3PM": "FG3M",
    "STL": "STL",
    "BLK": "BLK",
    "PRA": None,
    "PR": None,
    "PA": None,
    "RA": None,
}
VALIDATED_BOX_SCORE_COLUMNS = ("PTS", "REB", "AST", "FG3M", "STL", "BLK", "FGA", "FG3A", "FTA")
GAME_LOG_CACHE_SCHEMA_VERSION = "validated_boxscore_v1"
ANALYSIS_CACHE_SCHEMA_VERSION = "validated_analysis_v2"  # v2: last_n removed from cache key; filtered_pool stored alongside payload
POSITION_LABELS = {
    "G": "Guards",
    "F": "Forwards",
    "C": "Centers",
}


def current_nba_game_date() -> str:
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

PLAYER_POOL = static_players.get_players()
TEAM_POOL = sorted(static_teams.get_teams(), key=lambda team: team["full_name"])
TEAM_LOOKUP = {team["id"]: team for team in TEAM_POOL}
PLAYER_LOOKUP = {int(player["id"]): player for player in PLAYER_POOL}


def build_team_alias_lookup() -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for team in TEAM_POOL:
        keys = {
            str(team.get("abbreviation") or "").strip().lower(),
            str(team.get("nickname") or "").strip().lower(),
            str(team.get("city") or "").strip().lower(),
            str(team.get("full_name") or "").strip().lower(),
        }
        for key in keys:
            if key:
                lookup[key] = team
    return lookup


TEAM_ALIAS_LOOKUP = build_team_alias_lookup()
CACHE_TTL_SECONDS = 21600
PROFILE_TTL_SECONDS = 86400
POSITION_TTL_SECONDS = 43200
NEXT_GAME_TTL_SECONDS = 3600
GAME_LOG_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
RECENT_GAME_LOG_CACHE: dict[tuple[int, str, str, int], dict[str, Any]] = {}
GAME_LOG_FAILURE_META: dict[tuple[int, str, str], dict[str, float]] = {}
ROSTER_CACHE: dict[tuple[int, str], dict[str, Any]] = {}
PLAYER_INFO_CACHE: dict[int, dict[str, Any]] = {}
NEXT_GAME_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
TEAM_NEXT_GAME_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
POSITION_DASH_CACHE: dict[tuple[str, str, str, int], dict[str, Any]] = {}
POSITION_SUMMARY_CACHE: dict[tuple[str, int, str], dict[str, Any]] = {}
POSITION_MATCHUP_CACHE: dict[tuple[int, str, str, str, str], dict[str, Any]] = {}
POSITION_DASH_FAILURE_META: dict[tuple[str, str, str, int], dict[str, float]] = {}
POSITION_DASH_FAILURE_COOLDOWN_SECONDS = 300
POSITION_DASH_MAX_STALE_SECONDS = 24 * 60 * 60
POSITION_DASH_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS = 6
POSITION_DASH_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS = 25
POSITION_DASH_FETCH_ATTEMPTS_WITH_RELIABLE_STALE = 1
POSITION_DASH_FETCH_ATTEMPTS_NO_RELIABLE_STALE = 2
POSITION_DASH_FETCH_TIMEOUT_CAP_SECONDS = 12
POSITION_DASH_FETCH_TIMEOUT_FLOOR_SECONDS = 3
TEAM_GAME_LOG_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
TEAM_RECENT_CONTEXT_CACHE: dict[tuple[int, str, str, int], dict[str, Any]] = {}
TEAM_CONTEXT_TTL_SECONDS = 3600
SCOREBOARD_CACHE: dict[str, dict[str, Any]] = {}
ENRICHED_LOG_CACHE: dict[tuple[int, str, str, int | None], dict[str, Any]] = {}
ANALYSIS_CACHE_TTL_SECONDS = 7200
ANALYSIS_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
AVAILABILITY_CACHE: dict[tuple[str, str, str], dict[str, Any]] = {}
TEAM_OPPORTUNITY_CACHE: dict[tuple[str, str, str, str], dict[str, Any]] = {}
INJURY_REPORT_PAGE_URL = "https://official.nba.com/nba-injury-report-2025-26-season/"
INJURY_REPORT_TTL_SECONDS = 600
INJURY_REPORT_CACHE: dict[str, Any] = {"timestamp": 0.0, "payload": None}
INJURY_REPORT_LINKS_CACHE: dict[str, Any] = {"timestamp": 0.0, "links": []}
INJURY_REPORT_URL_CACHE: dict[str, dict[str, Any]] = {}
INJURY_REPORT_META: dict[str, float] = {"last_attempt": 0.0, "last_failure": 0.0}
INJURY_MATCH_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
INJURY_STATUS_ORDER = {"Out": 0, "Ineligible": 0, "Suspended": 0, "Doubtful": 1, "Questionable": 2, "Pending report": 2, "Not listed": 3, "Available": 4, "Probable": 4}
UNAVAILABLE_STATUSES = {"Out", "Ineligible", "Suspended"}
RISKY_STATUSES = {"Doubtful", "Questionable", "Pending report"}
GOOD_STATUSES = {"Available", "Probable"}
REPORT_STATUSES = ["Questionable", "Ineligible", "Suspended", "Doubtful", "Probable", "Available", "Out"]
STATUS_PATTERN = "|".join(re.escape(status) for status in sorted(REPORT_STATUSES, key=len, reverse=True))
REQUEST_LOCK = Lock()
LAST_REQUEST_TIME = 0.0

NBA_REQUEST_TIMEOUT = (5, 20)
NBA_RETRY_TOTAL = 4
NBA_BACKOFF_FACTOR = 0.8
NBA_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

TEAM_ROSTER_TIMEOUT_SECONDS = 16
TEAM_ROSTER_RETRY_ATTEMPTS = 3
TEAM_ROSTER_RETRY_BASE_DELAY = 0.75
TEAM_ROSTER_FAILURE_META: dict[tuple[int, str], dict[str, float]] = {}
TEAM_ROSTER_CACHE_TTL_SECONDS = 12 * 60 * 60
TEAM_ROSTER_MAX_STALE_SECONDS = 45 * 24 * 60 * 60
TEAM_ROSTER_FAILURE_COOLDOWN_SECONDS = 300
TEAM_ROSTER_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS = 6
TEAM_ROSTER_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS = 24
TEAM_ROSTER_FETCH_ATTEMPTS_WITH_RELIABLE_STALE = 1
TEAM_ROSTER_FETCH_ATTEMPTS_NO_RELIABLE_STALE = 3
TEAM_ROSTER_FETCH_TIMEOUT_CAP_SECONDS = 18
TEAM_ROSTER_FETCH_TIMEOUT_FLOOR_SECONDS = 4

PLAYER_INFO_TIMEOUT_SECONDS = 12
PLAYER_INFO_RETRY_ATTEMPTS = 2
PLAYER_INFO_RETRY_BASE_DELAY = 0.5

NEXT_GAME_TIMEOUT_SECONDS = 10
NEXT_GAME_RETRY_ATTEMPTS = 2
NEXT_GAME_RETRY_BASE_DELAY = 0.5

SCOREBOARD_TIMEOUT_SECONDS = 10
SCOREBOARD_RETRY_ATTEMPTS = 2
SCOREBOARD_RETRY_BASE_DELAY = 0.5
SCOREBOARD_CACHE_TTL_SECONDS = 60
SCOREBOARD_MAX_STALE_SECONDS = 12 * 60 * 60
SCOREBOARD_FAILURE_COOLDOWN_SECONDS = 180
SCOREBOARD_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS = 6
SCOREBOARD_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS = 20
SCOREBOARD_FETCH_ATTEMPTS_WITH_RELIABLE_STALE = 1
SCOREBOARD_FETCH_ATTEMPTS_NO_RELIABLE_STALE = 2
SCOREBOARD_FETCH_TIMEOUT_CAP_SECONDS = 10
SCOREBOARD_FETCH_TIMEOUT_FLOOR_SECONDS = 3

INJURY_REPORT_PAGE_TIMEOUT = (3, 6)
INJURY_REPORT_PDF_TIMEOUT = (3, 8)
INJURY_REPORT_LINKS_TTL_SECONDS = 300
INJURY_REPORT_FAILURE_COOLDOWN_SECONDS = 180
INJURY_REPORT_MAX_STALE_SECONDS = 6 * 60 * 60
GAME_LOG_FAILURE_COOLDOWN_SECONDS = 60
GAME_LOG_MAX_STALE_SECONDS = 6 * 60 * 60
GAME_LOG_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS = 5
GAME_LOG_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS = 18
GAME_LOG_FETCH_ATTEMPTS_WITH_RELIABLE_STALE = 1
GAME_LOG_FETCH_ATTEMPTS_NO_RELIABLE_STALE = 2
GAME_LOG_FETCH_TIMEOUT_CAP_SECONDS = 12
GAME_LOG_FETCH_TIMEOUT_FLOOR_SECONDS = 3
PLAYER_INFO_FAILURE_META: dict[int, dict[str, float]] = {}
PLAYER_INFO_MAX_STALE_SECONDS = 7 * 24 * 60 * 60
PLAYER_INFO_FAILURE_COOLDOWN_SECONDS = 300
PLAYER_INFO_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS = 6
PLAYER_INFO_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS = 20
PLAYER_INFO_FETCH_ATTEMPTS_WITH_RELIABLE_STALE = 1
PLAYER_INFO_FETCH_ATTEMPTS_NO_RELIABLE_STALE = 2
PLAYER_INFO_FETCH_TIMEOUT_CAP_SECONDS = 12
PLAYER_INFO_FETCH_TIMEOUT_FLOOR_SECONDS = 3

NEXT_GAME_FAILURE_META: dict[tuple[int, str, str], dict[str, float]] = {}
NEXT_GAME_MAX_STALE_SECONDS = 12 * 60 * 60
NEXT_GAME_FAILURE_COOLDOWN_SECONDS = 300
NEXT_GAME_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS = 6
NEXT_GAME_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS = 20
NEXT_GAME_FETCH_ATTEMPTS_WITH_RELIABLE_STALE = 1
NEXT_GAME_FETCH_ATTEMPTS_NO_RELIABLE_STALE = 2
NEXT_GAME_FETCH_TIMEOUT_CAP_SECONDS = 10
NEXT_GAME_FETCH_TIMEOUT_FLOOR_SECONDS = 3

FILTERED_POOL_CACHE_TTL_SECONDS = 300
FILTERED_POOL_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
STAT_SUMMARY_CACHE_TTL_SECONDS = 300
STAT_SUMMARY_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}

DEBUG_METADATA_ENABLED = os.getenv("NBA_DEBUG_METADATA_ENABLED", "0").strip() == "1"

HYBRID_ANALYZER_ENABLED = os.getenv("NBA_HYBRID_ANALYZER_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
ANALYSIS_CACHE_TTL_SECONDS = min(ANALYSIS_CACHE_TTL_SECONDS, 300)
HYBRID_ANALYSIS_MAX_STALE_SECONDS = max(ANALYSIS_CACHE_TTL_SECONDS, int(os.getenv("NBA_HYBRID_ANALYSIS_MAX_STALE_SECONDS", "1800")))
HYBRID_GAME_LOG_SOFT_TTL_SECONDS = max(300, int(os.getenv("NBA_HYBRID_GAME_LOG_SOFT_TTL_SECONDS", "3600")))
HYBRID_GAME_LOG_MAX_STALE_SECONDS = max(HYBRID_GAME_LOG_SOFT_TTL_SECONDS, int(os.getenv("NBA_HYBRID_GAME_LOG_MAX_STALE_SECONDS", "43200")))
HYBRID_PLAYER_INFO_SOFT_TTL_SECONDS = max(1800, int(os.getenv("NBA_HYBRID_PLAYER_INFO_SOFT_TTL_SECONDS", "21600")))
HYBRID_PLAYER_INFO_MAX_STALE_SECONDS = max(HYBRID_PLAYER_INFO_SOFT_TTL_SECONDS, int(os.getenv("NBA_HYBRID_PLAYER_INFO_MAX_STALE_SECONDS", "604800")))
HYBRID_NEXT_GAME_SOFT_TTL_SECONDS = max(60, int(os.getenv("NBA_HYBRID_NEXT_GAME_SOFT_TTL_SECONDS", "900")))
HYBRID_NEXT_GAME_MAX_STALE_SECONDS = max(HYBRID_NEXT_GAME_SOFT_TTL_SECONDS, int(os.getenv("NBA_HYBRID_NEXT_GAME_MAX_STALE_SECONDS", "21600")))
HYBRID_REFRESH_QUEUE_MAXSIZE = max(32, int(os.getenv("NBA_HYBRID_REFRESH_QUEUE_MAXSIZE", "512")))
HYBRID_REFRESH_WORKERS = max(1, int(os.getenv("NBA_HYBRID_REFRESH_WORKERS", "1")))
HYBRID_REFRESH_LOG_COOLDOWN_SECONDS = max(30, int(os.getenv("NBA_HYBRID_REFRESH_LOG_COOLDOWN_SECONDS", "120")))

HYBRID_REFRESH_QUEUE: queue.Queue[tuple[str, tuple[Any, ...], dict[str, Any]]] = queue.Queue(maxsize=HYBRID_REFRESH_QUEUE_MAXSIZE)
HYBRID_REFRESH_PENDING: set[tuple[str, tuple[Any, ...]]] = set()
HYBRID_REFRESH_LOCK = Lock()
HYBRID_REFRESH_WORKER_STARTED = False
HYBRID_REFRESH_LAST_LOG: dict[tuple[str, tuple[Any, ...]], float] = {}

NBA_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _cache_age_seconds(entry: dict[str, Any] | None) -> float | None:
    if not entry:
        return None
    ts = float(entry.get("timestamp") or 0.0)
    if ts <= 0:
        return None
    return max(0.0, time.time() - ts)


def _throttled_refresh_log(job_key: tuple[str, tuple[Any, ...]], message: str, *args: Any) -> None:
    now_ts = time.time()
    last_ts = float(HYBRID_REFRESH_LAST_LOG.get(job_key) or 0.0)
    if now_ts - last_ts >= HYBRID_REFRESH_LOG_COOLDOWN_SECONDS:
        LOGGER.info(message, *args)
        HYBRID_REFRESH_LAST_LOG[job_key] = now_ts


def enqueue_hybrid_refresh(job_type: str, key: tuple[Any, ...], **payload: Any) -> bool:
    if not HYBRID_ANALYZER_ENABLED:
        return False
    job_key = (str(job_type), tuple(key))
    with HYBRID_REFRESH_LOCK:
        if job_key in HYBRID_REFRESH_PENDING:
            return False
        if HYBRID_REFRESH_QUEUE.full():
            _throttled_refresh_log(job_key, "Hybrid refresh queue full; skipping %s %s", job_type, key)
            return False
        HYBRID_REFRESH_PENDING.add(job_key)
    try:
        HYBRID_REFRESH_QUEUE.put_nowait((str(job_type), tuple(key), dict(payload)))
        return True
    except Exception:
        with HYBRID_REFRESH_LOCK:
            HYBRID_REFRESH_PENDING.discard(job_key)
        return False


def _run_hybrid_refresh_job(job_type: str, key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    if job_type == "game_log":
        player_id, season, season_type = key
        fetch_player_game_log(player_id=int(player_id), season=str(season), season_type=str(season_type))
        return
    if job_type == "player_info":
        (player_id,) = key
        fetch_common_player_info(player_id=int(player_id))
        return
    if job_type == "team_next_game":
        team_id, primary_player_id, season, season_type = key
        resolve_team_next_game(team_id=int(team_id), primary_player_id=int(primary_player_id), season=str(season), season_type=str(season_type))
        return
    if job_type == "team_roster":
        team_id, season = key
        fetch_team_roster(team_id=int(team_id), season=str(season))
        return


def _hybrid_refresh_worker() -> None:
    while True:
        job_type, key, payload = HYBRID_REFRESH_QUEUE.get()
        job_key = (job_type, key)
        try:
            _run_hybrid_refresh_job(job_type, key, payload)
        except Exception as exc:
            _throttled_refresh_log(job_key, "Hybrid refresh failed for %s %s: %s", job_type, key, exc)
        finally:
            with HYBRID_REFRESH_LOCK:
                HYBRID_REFRESH_PENDING.discard(job_key)
            HYBRID_REFRESH_QUEUE.task_done()


def start_hybrid_refresh_workers() -> None:
    global HYBRID_REFRESH_WORKER_STARTED
    if not HYBRID_ANALYZER_ENABLED:
        return
    with HYBRID_REFRESH_LOCK:
        if HYBRID_REFRESH_WORKER_STARTED:
            return
        HYBRID_REFRESH_WORKER_STARTED = True
    for worker_index in range(HYBRID_REFRESH_WORKERS):
        threading.Thread(target=_hybrid_refresh_worker, name=f"hybrid-refresh-{worker_index+1}", daemon=True).start()


def get_player_game_log_hybrid(player_id: int, season: str, season_type: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cache_key = (player_id, season, season_type)
    cached = GAME_LOG_CACHE.get(cache_key)
    age_seconds = _cache_age_seconds(cached)
    if cached and isinstance(cached.get("rows"), list):
        if age_seconds is not None and age_seconds < HYBRID_GAME_LOG_SOFT_TTL_SECONDS:
            return cached["rows"], {"source": "cache-fresh", "seconds_ago": round(age_seconds, 2), "refresh_queued": False}
        if age_seconds is not None and age_seconds < HYBRID_GAME_LOG_MAX_STALE_SECONDS:
            queued = enqueue_hybrid_refresh("game_log", cache_key)
            return cached["rows"], {"source": "cache-stale", "seconds_ago": round(age_seconds, 2), "refresh_queued": bool(queued)}
    rows = fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
    fresh_age = _cache_age_seconds(GAME_LOG_CACHE.get(cache_key))
    return rows, {"source": "live", "seconds_ago": round(fresh_age, 2) if fresh_age is not None else None, "refresh_queued": False}


def get_player_info_hybrid(player_id: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    cached = PLAYER_INFO_CACHE.get(player_id)
    age_seconds = _cache_age_seconds(cached)
    if cached and isinstance(cached.get("row"), dict):
        if age_seconds is not None and age_seconds < HYBRID_PLAYER_INFO_SOFT_TTL_SECONDS:
            return cached["row"], {"source": "cache-fresh", "seconds_ago": round(age_seconds, 2), "refresh_queued": False}
        if age_seconds is not None and age_seconds < HYBRID_PLAYER_INFO_MAX_STALE_SECONDS:
            queued = enqueue_hybrid_refresh("player_info", (player_id,))
            return cached["row"], {"source": "cache-stale", "seconds_ago": round(age_seconds, 2), "refresh_queued": bool(queued)}
    try:
        row = fetch_common_player_info(player_id)
    except HTTPException:
        return None, {"source": "unavailable", "seconds_ago": round(age_seconds, 2) if age_seconds is not None else None, "refresh_queued": False}
    fresh_age = _cache_age_seconds(PLAYER_INFO_CACHE.get(player_id))
    return row, {"source": "live", "seconds_ago": round(fresh_age, 2) if fresh_age is not None else None, "refresh_queued": False}


def get_team_next_game_hybrid(team_id: int | None, primary_player_id: int, season: str, season_type: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not team_id:
        return None, {"source": "no-team", "seconds_ago": None, "refresh_queued": False}
    cache_key = (team_id, season, season_type)
    cached = TEAM_NEXT_GAME_CACHE.get(cache_key)
    age_seconds = _cache_age_seconds(cached)
    cached_row = cached.get("row") if cached else None
    if cached and age_seconds is not None and age_seconds < HYBRID_NEXT_GAME_SOFT_TTL_SECONDS:
        return cached_row, {"source": "cache-fresh", "seconds_ago": round(age_seconds, 2), "refresh_queued": False}
    if cached and age_seconds is not None and age_seconds < HYBRID_NEXT_GAME_MAX_STALE_SECONDS:
        queued = enqueue_hybrid_refresh("team_next_game", (team_id, primary_player_id, season, season_type))
        return cached_row, {"source": "cache-stale", "seconds_ago": round(age_seconds, 2), "refresh_queued": bool(queued)}
    row = resolve_team_next_game(team_id=team_id, primary_player_id=primary_player_id, season=season, season_type=season_type)
    fresh_age = _cache_age_seconds(TEAM_NEXT_GAME_CACHE.get(cache_key))
    return row, {"source": "live", "seconds_ago": round(fresh_age, 2) if fresh_age is not None else None, "refresh_queued": False}


def build_h2h_payload_from_rows(rows_source: list[dict[str, Any]], next_game: dict[str, Any] | None, stat: str, line: float) -> dict[str, Any]:
    payload = {
        "opponent_name": next_game.get("opponent_name") if next_game else "",
        "opponent_abbreviation": next_game.get("opponent_abbreviation") if next_game else "",
        "games_count": 0,
        "hit_count": 0,
        "hit_rate": 0.0,
        "average": 0.0,
        "games": [],
    }
    if not next_game or not next_game.get("opponent_abbreviation"):
        return payload
    opponent_abbreviation = str(next_game.get("opponent_abbreviation") or "").upper().strip()
    h2h_rows = [row for row in (rows_source or []) if opponent_abbreviation and opponent_abbreviation in str(row.get("MATCHUP") or "").upper()]
    if not h2h_rows:
        return payload
    h2h_games: list[dict[str, Any]] = []
    h2h_values: list[float] = []
    h2h_hits = 0
    for row in h2h_rows:
        game_entry = build_game_log_entry(row, stat, line)
        if game_entry["hit"]:
            h2h_hits += 1
        h2h_values.append(game_entry["value"])
        h2h_games.append(game_entry)
    payload.update({
        "opponent_name": next_game.get("opponent_name") or "",
        "opponent_abbreviation": opponent_abbreviation,
        "games_count": len(h2h_games),
        "hit_count": h2h_hits,
        "hit_rate": round((h2h_hits / len(h2h_games)) * 100, 1),
        "average": round(sum(h2h_values) / len(h2h_values), 2),
        "games": list(reversed(h2h_games)),
    })
    return payload


def overlay_sensitive_analysis_sections(payload: dict[str, Any], player_name: str, team_name: str | None, analysis_source_rows: list[dict[str, Any]], resolved_team_id: int | None, player_id: int, season: str, season_type: str, stat: str, line: float) -> tuple[dict[str, Any], dict[str, Any]]:
    refreshed = copy.deepcopy(payload)
    freshness_overlay: dict[str, Any] = {}
    availability = build_availability_payload(player_name=player_name, team_name=team_name)
    refreshed["availability"] = availability
    freshness_overlay["availability_source"] = "live-overlay"
    next_game, next_game_meta = get_team_next_game_hybrid(team_id=resolved_team_id, primary_player_id=player_id, season=season, season_type=season_type)
    refreshed.setdefault("matchup", {})["next_game"] = next_game
    refreshed["h2h"] = build_h2h_payload_from_rows(analysis_source_rows, next_game, stat, line)
    try:
        refreshed["environment"] = build_game_environment_context(analysis_source_rows, next_game, team_id=resolved_team_id, season=season, season_type=season_type)
    except Exception:
        pass
    freshness_overlay["next_game_source"] = next_game_meta.get("source")
    freshness_overlay["next_game_seconds_ago"] = next_game_meta.get("seconds_ago")
    freshness_overlay["next_game_refresh_queued"] = next_game_meta.get("refresh_queued")
    return refreshed, freshness_overlay


def create_nba_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=NBA_RETRY_TOTAL,
        connect=NBA_RETRY_TOTAL,
        read=NBA_RETRY_TOTAL,
        status=NBA_RETRY_TOTAL,
        backoff_factor=NBA_BACKOFF_FACTOR,
        status_forcelist=NBA_RETRY_STATUS_CODES,
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=20,
        pool_maxsize=20,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": NBA_USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nba.com/",
            "Origin": "https://www.nba.com",
            "Connection": "keep-alive",
        }
    )
    return session


NBA_HTTP = create_nba_http_session()


def is_transient_nba_error(exc: Exception) -> bool:
    if isinstance(exc, (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
    )):
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


def nba_http_get(url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None, timeout: tuple[int, int] | int | None = None) -> requests.Response:
    response = NBA_HTTP.get(url, params=params, headers=headers, timeout=timeout or NBA_REQUEST_TIMEOUT)
    response.raise_for_status()
    return response


def call_nba_with_retries(factory, *, label: str, attempts: int = NBA_RETRY_TOTAL, base_delay: float = NBA_BACKOFF_FACTOR):
    last_exc: Exception | None = None
    for attempt in range(attempts):
        throttle_request()
        try:
            return factory()
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts - 1 or not is_transient_nba_error(exc):
                raise
            time.sleep(base_delay * (2 ** attempt))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{label} failed without a captured exception.")


def current_nba_season() -> str:
    now = datetime.now(ZoneInfo("America/New_York"))
    year = now.year
    if now.month >= 10:
        start_year = year
    else:
        start_year = year - 1
    end_year_short = str(start_year + 1)[-2:]
    return f"{start_year}-{end_year_short}"


def normalize_name(name: str) -> str:
    return " ".join(name.lower().strip().split())


def normalize_compact_text(value: str) -> str:
    raw = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    raw = re.sub(r"[^A-Za-z0-9]", "", raw)
    return raw.lower()


def normalize_report_person_name(name: str) -> str:
    raw = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode("ascii")
    raw = raw.replace("-", " ")
    raw = raw.replace("'", "")
    raw = raw.replace(".", " ")
    raw = raw.strip()

    if "," in raw:
        last, first = [part.strip() for part in raw.split(",", 1)]
        # Handle CamelCase fused last names from PDF text extraction
        # e.g. "JonesGarcia" -> "Jones Garcia", "MooreJr" -> "Moore Jr"
        last = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", last)
        raw = f"{first} {last}".strip()
    else:
        # Handle CamelCase in names without comma (rare but defensive)
        raw = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", raw)

    raw = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", " ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"[^A-Za-z\s]", " ", raw)
    return " ".join(raw.lower().split())


def build_player_name_variants(name: str) -> set[str]:
    variants: set[str] = set()
    raw = str(name or "").strip()
    if not raw:
        return variants

    canonical = normalize_report_person_name(raw)
    if canonical:
        variants.add(canonical)

    ascii_raw = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z,\s'-]", " ", ascii_raw)
    cleaned = " ".join(cleaned.split())
    if cleaned:
        variants.add(normalize_report_person_name(cleaned))

    if "," in ascii_raw:
        parts = [part.strip() for part in ascii_raw.split(",", 1)]
        if len(parts) == 2:
            reversed_name = f"{parts[1]} {parts[0]}".strip()
            if reversed_name:
                variants.add(normalize_report_person_name(reversed_name))
    else:
        pieces = [piece for piece in re.split(r"\s+", ascii_raw.strip()) if piece]
        if len(pieces) >= 2:
            reversed_name = f"{pieces[-1]}, {' '.join(pieces[:-1])}".strip()
            variants.add(normalize_report_person_name(reversed_name))

    return {variant for variant in variants if variant}


def build_player_variant_lookup() -> dict[int, set[str]]:
    lookup: dict[int, set[str]] = {}
    for player in PLAYER_POOL:
        player_id = int(player["id"])
        lookup[player_id] = build_player_name_variants(str(player.get("full_name", "")))
    return lookup


PLAYER_VARIANTS_LOOKUP = build_player_variant_lookup()

ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"
ODDS_DEFAULT_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_blocks",
    "player_steals",
    "player_turnovers",
    "player_points_rebounds_assists",
    "player_points_rebounds",
    "player_points_assists",
    "player_rebounds_assists",
]
ODDS_MARKET_TO_STAT = {
    "player_points": "PTS",
    "player_rebounds": "REB",
    "player_assists": "AST",
    "player_threes": "3PM",
    "player_blocks": "BLK",
    "player_steals": "STL",
    "player_points_rebounds_assists": "PRA",
    "player_points_rebounds": "PR",
    "player_points_assists": "PA",
    "player_rebounds_assists": "RA",
}

def report_name_variants(name: str) -> set[str]:
    """Backward-compatible alias for player name variant generation."""
    return build_player_name_variants(name)


def parse_injury_report_timestamp(url: str) -> datetime:
    match = re.search(r"Injury-Report_(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})(AM|PM)\.pdf", url)
    if not match:
        return datetime.min
    date_part, hour, minute, meridiem = match.groups()
    return datetime.strptime(f"{date_part} {hour}:{minute}{meridiem}", "%Y-%m-%d %I:%M%p")


def format_injury_report_timestamp(report_dt: datetime | None) -> str:
    if not report_dt or report_dt == datetime.min:
        return ""
    return report_dt.strftime("%b %d, %Y %I:%M %p ET")


def extract_team_prefix(text_line: str) -> tuple[str | None, str]:
    compact_line = normalize_compact_text(text_line)
    for team in sorted(TEAM_POOL, key=lambda item: len(item["full_name"]), reverse=True):
        team_name = str(team["full_name"])
        if text_line.startswith(team_name):
            return team_name, text_line[len(team_name):].strip()

        compact_team = normalize_compact_text(team_name)
        if compact_line.startswith(compact_team):
            consumed = len(compact_team)
            remainder_start = 0
            alnum_seen = 0
            for idx, ch in enumerate(text_line):
                if ch.isalnum():
                    alnum_seen += 1
                if alnum_seen >= consumed:
                    remainder_start = idx + 1
                    break
            return team_name, text_line[remainder_start:].strip()

    return None, text_line


def parse_injury_report_rows(report_text: str) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    pending_teams: set[str] = set()
    current_team: str | None = None
    last_row: dict[str, Any] | None = None
    for raw_line in report_text.splitlines():
        line = " ".join(str(raw_line or "").split())
        if not line:
            continue
        compact_line = normalize_compact_text(line)
        if line.startswith("Injury Report:") or line.startswith("Page ") or compact_line.startswith("page") or re.match(r"^page\d+of\d+$", compact_line):
            continue
        if line.startswith("Game Date ") or line.startswith("Current Status") or compact_line.startswith("gamedate") or compact_line.startswith("gamedategametimematchup"):
            continue

        full_row_match = re.match(r"^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}\s*\(ET\)\s+[A-Z]{2,4}@[A-Z]{2,4}\s+(.*)$", line)
        if full_row_match:
            line = full_row_match.group(1).strip()
        else:
            # Strip "HH:MM(ET) AAA@BBB " prefix (time + matchup, no date)
            time_matchup = re.match(r"^\d{2}:\d{2}\s*\(ET\)\s+[A-Z]{2,4}@[A-Z]{2,4}\s+(.*)$", line)
            if time_matchup:
                line = time_matchup.group(1).strip()
            else:
                # Strip bare "AAA@BBB " matchup prefix (no date, no time)
                bare_matchup = re.match(r"^[A-Z]{2,4}@[A-Z]{2,4}\s+(.*)$", line)
                if bare_matchup:
                    line = bare_matchup.group(1).strip()

        team_name, remainder = extract_team_prefix(line)
        if team_name:
            current_team = team_name
        else:
            remainder = line

        if remainder == "NOT YET SUBMITTED":
            if current_team:
                pending_teams.add(current_team)
            last_row = None
            continue

        row_match = re.match(
            rf"^(?P<player>.+?)\s+(?P<status>{STATUS_PATTERN})\b(?:\s+(?P<reason>.*))?$",
            remainder,
        )
        if row_match and current_team:
            player_display = row_match.group("player").strip()
            # Normalise display name: PDF text-extract can fuse "Last,First" with no space.
            # Insert space after comma if missing so keys and display are consistent.
            player_display = re.sub(r",(?!\s)", ", ", player_display)
            status = row_match.group("status").strip()
            reason = (row_match.group("reason") or "").strip()
            row_payload = {
                "team_name": current_team,
                "player_display": player_display,
                "player_key": normalize_report_person_name(player_display),
                "status": status,
                "reason": reason,
            }
            rows.append(row_payload)
            last_row = row_payload
            continue

        if last_row and not extract_team_prefix(line)[0] and not re.match(r"^\d{2}/\d{2}/\d{4}", line):
            continuation = line.strip()
            if continuation and not continuation.startswith("Game Date"):
                last_row["reason"] = f"{last_row.get('reason', '').strip()} {continuation}".strip()

    return {"rows": rows, "pending_teams": sorted(pending_teams)}


def extract_injury_report_rows_from_table(pdf_bytes: bytes) -> list[dict[str, Any]] | None:
    """Use pdfplumber's table extractor to read the NBA injury report PDF as a
    structured table, giving us exact (team, player, status, reason) columns
    without the column-interleaving bugs that plague free-text extraction."""
    if pdfplumber is None:
        return None
    try:
        rows: list[dict[str, Any]] = []
        # Track the last valid team across rows — the PDF only names the team on
        # the first row for that team; subsequent rows leave the team cell blank.
        last_team_name: str | None = None
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:  # type: ignore[arg-type]
            for page in pdf.pages:
                tables = page.extract_tables() or []
                for table in tables:
                    for row in table:
                        if not row:
                            continue
                        # NBA injury report table columns (0-indexed):
                        # 0: Game Date  1: Game Time  2: Matchup  3: Team
                        # 4: Player     5: Current Status  6: Reason
                        cells = [str(c or "").strip() for c in row]
                        if len(cells) < 5:
                            continue
                        # Skip header rows
                        if cells[0].lower() in ("game date", "date") or cells[3].lower() in ("team",):
                            continue
                        # Skip totally empty rows
                        if not any(cells):
                            continue

                        # Search for a team name in columns 2-5
                        team_col_idx = None
                        for ci in range(2, min(6, len(cells))):
                            cell_compact = normalize_compact_text(cells[ci])
                            for team in TEAM_POOL:
                                if normalize_compact_text(team["full_name"]) == cell_compact:
                                    team_col_idx = ci
                                    break
                            if team_col_idx is not None:
                                break

                        if team_col_idx is not None:
                            # New team found in this row — resolve and carry forward
                            tc = normalize_compact_text(cells[team_col_idx])
                            for team in TEAM_POOL:
                                if normalize_compact_text(team["full_name"]) == tc:
                                    last_team_name = str(team["full_name"])
                                    break
                            player_col_idx = team_col_idx + 1
                            status_col_idx = team_col_idx + 2
                            reason_col_idx = team_col_idx + 3
                        else:
                            # No team cell — use carry-forward team (same team, next player)
                            if not last_team_name:
                                continue
                            # Canonical column layout: col 4=player, 5=status, 6=reason
                            player_col_idx = 4
                            status_col_idx = 5
                            reason_col_idx = 6

                        team_full_name = last_team_name
                        if not team_full_name:
                            continue

                        player_cell = cells[player_col_idx] if player_col_idx < len(cells) else ""
                        status_cell = cells[status_col_idx] if status_col_idx < len(cells) else ""
                        reason_cell = cells[reason_col_idx] if reason_col_idx < len(cells) else ""

                        # Validate status
                        status_cell = status_cell.strip()
                        if status_cell not in REPORT_STATUSES:
                            found = None
                            for s in sorted(REPORT_STATUSES, key=len, reverse=True):
                                if s.lower() in status_cell.lower():
                                    found = s
                                    break
                            if not found:
                                continue
                            status_cell = found

                        player_display = player_cell.strip()
                        # Normalise: PDF text-extract can produce "Last,First" with no space.
                        player_display = re.sub(r",(?!\s)", ", ", player_display)
                        if not player_display:
                            continue

                        rows.append({
                            "team_name": team_full_name,
                            "player_display": player_display,
                            "player_key": normalize_report_person_name(player_display),
                            "status": status_cell,
                            "reason": reason_cell.strip(),
                        })
        return rows if rows else None
    except Exception:
        return None


def extract_injury_report_text_candidates(pdf_bytes: bytes) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []

    if pdfplumber is not None:
        try:
            plumber_pages: list[str] = []
            with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:  # type: ignore[arg-type]
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    plumber_pages.append(page_text)
            plumber_text = "\n".join(plumber_pages).strip()
            if plumber_text:
                candidates.append({"method": "pdfplumber", "text": plumber_text})
        except Exception:
            pass

    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        pypdf_text = "\n".join((page.extract_text() or "") for page in reader.pages).strip()
        if pypdf_text:
            candidates.append({"method": "pypdf", "text": pypdf_text})
    except Exception:
        pass

    unique: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        text_value = candidate["text"]
        if text_value not in seen:
            seen.add(text_value)
            unique.append(candidate)
    return unique


def choose_best_injury_report_parse(pdf_bytes: bytes) -> dict[str, Any]:
    # Text-only parsing path. Table extraction is intentionally skipped because
    # the official injury report PDFs have been unreliable for structured table
    # extraction in this app and only add noisy warnings to the logs.
    candidates = extract_injury_report_text_candidates(pdf_bytes)
    best_payload: dict[str, Any] = {"rows": [], "pending_teams": [], "raw_text": "", "method": "none"}

    for candidate in candidates:
        parsed = parse_injury_report_rows(candidate["text"])
        score = len(parsed.get("rows") or [])
        best_score = len(best_payload.get("rows") or [])
        if score > best_score:
            best_payload = {
                "rows": parsed.get("rows") or [],
                "pending_teams": parsed.get("pending_teams") or [],
                "raw_text": candidate["text"],
                "method": candidate["method"],
            }

    if not best_payload["rows"] and candidates:
        fallback = candidates[0]
        parsed = parse_injury_report_rows(fallback["text"])
        best_payload = {
            "rows": parsed.get("rows") or [],
            "pending_teams": parsed.get("pending_teams") or [],
            "raw_text": fallback["text"],
            "method": fallback["method"],
        }

    return best_payload


def list_recent_injury_report_links(limit: int = 12) -> list[str]:
    now_ts = time.time()
    cached_links = INJURY_REPORT_LINKS_CACHE.get("links") or []
    cached_ts = float(INJURY_REPORT_LINKS_CACHE.get("timestamp") or 0.0)
    if cached_links and now_ts - cached_ts < INJURY_REPORT_TTL_SECONDS:
        return list(cached_links)[:limit]

    headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
    page_response = nba_http_get(INJURY_REPORT_PAGE_URL, headers=headers, timeout=INJURY_REPORT_PAGE_TIMEOUT)
    page_response.raise_for_status()
    html = page_response.text
    links = set(re.findall(r"https://ak-static\.cms\.nba\.com/referee/injury/Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html))
    if not links:
        relative_links = re.findall(r"/wp-content/uploads/.+?Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html)
        links = {f"https://official.nba.com{match}" for match in relative_links}
    if not links:
        raise RuntimeError("No injury report PDF links found on the official page.")

    sorted_links = sorted(links, key=parse_injury_report_timestamp, reverse=True)
    INJURY_REPORT_LINKS_CACHE["timestamp"] = now_ts
    INJURY_REPORT_LINKS_CACHE["links"] = sorted_links
    return sorted_links[:limit]


def fetch_injury_report_payload_for_url(report_url: str) -> dict[str, Any]:
    cached = INJURY_REPORT_URL_CACHE.get(report_url)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < INJURY_REPORT_TTL_SECONDS:
        return dict(cached.get("payload") or {})

    headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
    latest_dt = parse_injury_report_timestamp(report_url)
    pdf_response = nba_http_get(report_url, headers=headers, timeout=(5, 30))
    pdf_response.raise_for_status()
    parsed_choice = choose_best_injury_report_parse(pdf_response.content)
    payload = {
        "ok": True,
        "report_url": report_url,
        "report_timestamp": latest_dt.isoformat() if latest_dt != datetime.min else "",
        "report_label": format_injury_report_timestamp(latest_dt),
        "rows": parsed_choice.get("rows") or [],
        "pending_teams": parsed_choice.get("pending_teams") or [],
        "raw_text": parsed_choice.get("raw_text") or "",
        "parse_method": parsed_choice.get("method") or "unknown",
        "error": None,
    }
    INJURY_REPORT_URL_CACHE[report_url] = {"timestamp": now_ts, "payload": payload}
    return payload


def search_report_payload_for_player(report_payload: dict[str, Any], player_name: str, team_name: str | None = None) -> dict[str, Any] | None:
    player_key = normalize_report_person_name(player_name)
    wanted_team = str(team_name or "").strip()
    rows = report_payload.get("rows") or []
    candidates = [row for row in rows if row.get("player_key") == player_key]

    if wanted_team:
        team_filtered = [row for row in candidates if row.get("team_name") == wanted_team]
        if team_filtered:
            candidates = team_filtered

    if not candidates:
        token_set = set(player_key.split())
        fuzzy_matches = []
        for row in rows:
            row_tokens = set(str(row.get("player_key") or "").split())
            if token_set and token_set.issubset(row_tokens):
                fuzzy_matches.append(row)
        if wanted_team:
            team_fuzzy = [row for row in fuzzy_matches if row.get("team_name") == wanted_team]
            if team_fuzzy:
                fuzzy_matches = team_fuzzy
        if fuzzy_matches:
            candidates = fuzzy_matches

    if not candidates:
        direct_row = try_direct_report_match(
            report_text=str(report_payload.get("raw_text") or ""),
            player_name=player_name,
            team_name=team_name or None,
        )
        if direct_row:
            candidates = [direct_row]

    if not candidates:
        raw_text = str(report_payload.get("raw_text") or "")
        name_variants = [
            player_name.strip(),
            " ".join(reversed(player_name.strip().split(" ", 1))) if " " in player_name.strip() else player_name.strip(),
        ]
        last_first = ""
        parts = [part for part in player_name.strip().split() if part]
        if len(parts) >= 2:
            last_first = f"{parts[-1]}, {' '.join(parts[:-1])}"
            name_variants.append(last_first)
        lowered_lines = [" ".join(line.split()) for line in raw_text.splitlines() if str(line or "").strip()]
        current_team: str | None = None
        for line in lowered_lines:
            team_hit, remainder = extract_team_prefix(line)
            if team_hit:
                current_team = team_hit
            else:
                remainder = line
            if wanted_team and current_team != wanted_team:
                continue
            for variant in name_variants:
                if variant and variant.lower() in remainder.lower():
                    row_match = re.match(rf"^(?P<player>.+?)\s+(?P<status>{STATUS_PATTERN})\b(?:\s+(?P<reason>.*))?$", remainder)
                    if row_match:
                        raw_disp = re.sub(r",(?!\s)", ", ", row_match.group("player").strip())
                        return {
                            "team_name": current_team or wanted_team,
                            "player_display": raw_disp,
                            "player_key": normalize_report_person_name(raw_disp),
                            "status": row_match.group("status").strip(),
                            "reason": (row_match.group("reason") or "").strip(),
                        }

    return candidates[0] if candidates else None


def find_player_in_recent_reports(player_name: str, team_name: str | None = None, max_reports: int = 8) -> dict[str, Any] | None:
    cache_key = (normalize_report_person_name(player_name), str(team_name or "").strip())
    cached = INJURY_MATCH_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < INJURY_REPORT_TTL_SECONDS:
        result = cached.get("result")
        return dict(result) if isinstance(result, dict) else None

    try:
        links = list_recent_injury_report_links(limit=max_reports)
    except Exception:
        return None

    for link in links:
        try:
            payload = fetch_injury_report_payload_for_url(link)
        except Exception:
            continue
        matched_row = search_report_payload_for_player(payload, player_name=player_name, team_name=team_name)
        if matched_row:
            result = {
                "row": matched_row,
                "report_label": payload.get("report_label") or "",
                "report_url": payload.get("report_url") or link,
                "pending_teams": payload.get("pending_teams") or [],
            }
            INJURY_MATCH_CACHE[cache_key] = {"timestamp": now_ts, "result": result}
            return result

    INJURY_MATCH_CACHE[cache_key] = {"timestamp": now_ts, "result": None}
    return None


def try_direct_report_match(report_text: str, player_name: str, team_name: str | None = None) -> dict[str, Any] | None:
    if not report_text.strip():
        return None

    parsed = parse_injury_report_rows(report_text)
    rows = parsed.get("rows") or []
    player_key = normalize_report_person_name(player_name)
    wanted_team = str(team_name or "").strip()

    exact = [row for row in rows if row.get("player_key") == player_key]
    if wanted_team:
        team_exact = [row for row in exact if row.get("team_name") == wanted_team]
        if team_exact:
            exact = team_exact
    if exact:
        return exact[0]

    token_set = set(player_key.split())
    fuzzy: list[dict[str, Any]] = []
    for row in rows:
        row_tokens = set(str(row.get("player_key") or "").split())
        if token_set and token_set.issubset(row_tokens):
            fuzzy.append(row)
    if wanted_team:
        team_fuzzy = [row for row in fuzzy if row.get("team_name") == wanted_team]
        if team_fuzzy:
            fuzzy = team_fuzzy
    if fuzzy:
        return fuzzy[0]

    return None


@timed_call("fetch_latest_injury_report_payload")
def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def _injury_payload_is_reliable_stale(payload: dict[str, Any] | None, cache_timestamp: float | None = None) -> bool:
    if not payload or not payload.get("ok"):
        return False

    now_et = datetime.now(ZoneInfo("America/New_York"))
    report_dt = _parse_iso_datetime(payload.get("report_timestamp"))
    if report_dt is not None:
        try:
            report_dt = report_dt.astimezone(ZoneInfo("America/New_York")) if report_dt.tzinfo else report_dt.replace(tzinfo=ZoneInfo("America/New_York"))
        except Exception:
            pass
        if report_dt.date() != now_et.date():
            return False

    fetched_at = payload.get("fetched_at")
    fetched_dt = _parse_iso_datetime(fetched_at)
    age_seconds = None
    if fetched_dt is not None:
        age_seconds = max(0.0, (datetime.now(fetched_dt.tzinfo or ZoneInfo("America/New_York")) - fetched_dt).total_seconds())
    elif cache_timestamp:
        age_seconds = max(0.0, time.time() - float(cache_timestamp))

    if age_seconds is None:
        return False
    return age_seconds <= INJURY_REPORT_MAX_STALE_SECONDS


def _build_injury_error_payload(exc: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "report_url": "",
        "report_timestamp": "",
        "report_label": "",
        "rows": [],
        "pending_teams": [],
        "raw_text": "",
        "parse_method": "error",
        "error": str(exc),
        "source": "error",
        "fetched_at": datetime.now(ZoneInfo("America/New_York")).isoformat(),
    }


def _fetch_injury_report_links(headers: dict[str, str]) -> list[str]:
    now_ts = time.time()
    cached_links = INJURY_REPORT_LINKS_CACHE.get("links") or []
    cached_links_ts = float(INJURY_REPORT_LINKS_CACHE.get("timestamp") or 0.0)
    if cached_links and now_ts - cached_links_ts < INJURY_REPORT_LINKS_TTL_SECONDS:
        return list(cached_links)

    page_response = requests.get(INJURY_REPORT_PAGE_URL, headers=headers, timeout=INJURY_REPORT_PAGE_TIMEOUT)
    page_response.raise_for_status()
    html = page_response.text
    links = set(re.findall(r"https://ak-static\.cms\.nba\.com/referee/injury/Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html))
    if not links:
        relative_links = re.findall(r"/wp-content/uploads/.+?Injury-Report_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}(?:AM|PM)\.pdf", html)
        links = {f"https://official.nba.com{match}" for match in relative_links}
    if not links:
        raise RuntimeError("No injury report PDF links found on the official page.")

    result = sorted(links)
    INJURY_REPORT_LINKS_CACHE["timestamp"] = now_ts
    INJURY_REPORT_LINKS_CACHE["links"] = result
    return result


def _fetch_injury_report_pdf_payload(report_url: str, report_dt: datetime, headers: dict[str, str]) -> dict[str, Any]:
    cached_url_payload = INJURY_REPORT_URL_CACHE.get(report_url)
    if cached_url_payload and cached_url_payload.get("payload"):
        return copy.deepcopy(cached_url_payload["payload"])

    pdf_response = requests.get(report_url, headers=headers, timeout=INJURY_REPORT_PDF_TIMEOUT)
    pdf_response.raise_for_status()
    parsed_choice = choose_best_injury_report_parse(pdf_response.content)
    payload = {
        "ok": True,
        "report_url": report_url,
        "report_timestamp": report_dt.isoformat() if report_dt != datetime.min else "",
        "report_label": format_injury_report_timestamp(report_dt),
        "rows": parsed_choice.get("rows") or [],
        "pending_teams": parsed_choice.get("pending_teams") or [],
        "raw_text": parsed_choice.get("raw_text") or "",
        "parse_method": parsed_choice.get("method") or "unknown",
        "error": None,
        "source": "fresh",
        "fetched_at": datetime.now(ZoneInfo("America/New_York")).isoformat(),
    }
    INJURY_REPORT_URL_CACHE[report_url] = {"timestamp": time.time(), "payload": copy.deepcopy(payload)}
    return payload


def fetch_latest_injury_report_payload() -> dict[str, Any]:
    now_ts = time.time()
    cached = INJURY_REPORT_CACHE.get("payload")
    cached_ts = float(INJURY_REPORT_CACHE.get("timestamp") or 0.0)
    if cached and now_ts - cached_ts < INJURY_REPORT_TTL_SECONDS:
        return cached

    if (
        cached
        and _injury_payload_is_reliable_stale(cached, cached_ts)
        and now_ts - float(INJURY_REPORT_META.get("last_failure") or 0.0) < INJURY_REPORT_FAILURE_COOLDOWN_SECONDS
    ):
        stale_payload = dict(cached)
        stale_payload["source"] = stale_payload.get("source") or "stale"
        return stale_payload

    headers = {"User-Agent": "Mozilla/5.0 (compatible; NBAPropsTracker/1.0)"}
    INJURY_REPORT_META["last_attempt"] = now_ts
    try:
        links = _fetch_injury_report_links(headers)
        latest_url = max(links, key=parse_injury_report_timestamp)
        latest_dt = parse_injury_report_timestamp(latest_url)

        if cached and cached.get("ok") and cached.get("report_url") == latest_url:
            verified_payload = dict(cached)
            verified_payload["source"] = "verified-cache"
            INJURY_REPORT_CACHE["timestamp"] = now_ts
            INJURY_REPORT_CACHE["payload"] = verified_payload
            return verified_payload

        payload = _fetch_injury_report_pdf_payload(latest_url, latest_dt, headers)
        INJURY_REPORT_CACHE["timestamp"] = now_ts
        INJURY_REPORT_CACHE["payload"] = payload
        return payload
    except Exception as exc:
        INJURY_REPORT_META["last_failure"] = now_ts
        if cached and _injury_payload_is_reliable_stale(cached, cached_ts):
            stale_payload = dict(cached)
            stale_payload["source"] = "stale-fallback"
            stale_payload["error"] = str(exc)
            INJURY_REPORT_CACHE["timestamp"] = now_ts
            INJURY_REPORT_CACHE["payload"] = stale_payload
            return stale_payload

        payload = _build_injury_error_payload(exc)
        INJURY_REPORT_CACHE["timestamp"] = now_ts
        INJURY_REPORT_CACHE["payload"] = payload
        return payload

def build_availability_payload(player_name: str, team_name: str | None = None) -> dict[str, Any]:
    report_payload = fetch_latest_injury_report_payload()
    report_label = str(report_payload.get("report_label") or "")
    team_name = str(team_name or "").strip()
    cache_key = (normalize_report_person_name(player_name), team_name, report_label)
    cached = AVAILABILITY_CACHE.get(cache_key)
    if cached:
        return dict(cached)

    if not report_payload.get("ok"):
        result = {
            "status": "Unknown",
            "tone": "neutral",
            "reason": "Official injury report unavailable right now.",
            "note": "Could not fetch the latest official injury report.",
            "source": "Official NBA injury report",
            "report_label": "",
            "report_url": "",
            "is_unavailable": False,
            "is_risky": False,
            "sort_rank": 3,
        }
        AVAILABILITY_CACHE[cache_key] = dict(result)
        return result

    player_key = normalize_report_person_name(player_name)
    rows = report_payload.get("rows") or []
    candidates = [row for row in rows if row.get("player_key") == player_key]

    if team_name:
        team_filtered = [row for row in candidates if row.get("team_name") == team_name]
        if team_filtered:
            candidates = team_filtered

    if not candidates:
        token_set = set(player_key.split())
        fuzzy_matches = []
        for row in rows:
            row_tokens = set(str(row.get("player_key") or "").split())
            if token_set and token_set.issubset(row_tokens):
                fuzzy_matches.append(row)
        if team_name:
            team_fuzzy = [row for row in fuzzy_matches if row.get("team_name") == team_name]
            if team_fuzzy:
                fuzzy_matches = team_fuzzy
        if fuzzy_matches:
            candidates = fuzzy_matches

    if not candidates:
        direct_row = try_direct_report_match(
            report_text=str(report_payload.get("raw_text") or ""),
            player_name=player_name,
            team_name=team_name or None,
        )
        if direct_row:
            candidates = [direct_row]

    if candidates:
        row = candidates[0]
        status = str(row.get("status") or "Unknown").strip()
        if status in GOOD_STATUSES:
            tone = "good"
        elif status in UNAVAILABLE_STATUSES:
            tone = "bad"
        elif status in RISKY_STATUSES:
            tone = "warning"
        else:
            tone = "neutral"

        reason = str(row.get("reason") or "").strip()
        note = reason or "Official status found on the latest NBA injury report."
        result = {
            "status": status,
            "tone": tone,
            "reason": reason,
            "note": note,
            "source": "Official NBA injury report",
            "report_label": report_payload.get("report_label") or "",
            "report_url": report_payload.get("report_url") or "",
            "is_unavailable": status in UNAVAILABLE_STATUSES,
            "is_risky": status in RISKY_STATUSES,
            "sort_rank": INJURY_STATUS_ORDER.get(status, 3),
        }
        AVAILABILITY_CACHE[cache_key] = dict(result)
        return result

    if team_name and team_name in set(report_payload.get("pending_teams") or []):
        result = {
            "status": "Pending report",
            "tone": "warning",
            "reason": "Team report not yet submitted on the latest official injury report.",
            "note": "The team has not yet submitted its latest official report.",
            "source": "Official NBA injury report",
            "report_label": report_payload.get("report_label") or "",
            "report_url": report_payload.get("report_url") or "",
            "is_unavailable": False,
            "is_risky": True,
            "sort_rank": INJURY_STATUS_ORDER.get("Pending report", 2),
        }
        AVAILABILITY_CACHE[cache_key] = dict(result)
        return result

    recent_match = find_player_in_recent_reports(player_name=player_name, team_name=team_name or None, max_reports=12)
    if recent_match and recent_match.get("row"):
        row = dict(recent_match["row"])
        status = str(row.get("status") or "Unknown").strip()
        if status in GOOD_STATUSES:
            tone = "good"
        elif status in UNAVAILABLE_STATUSES:
            tone = "bad"
        elif status in RISKY_STATUSES:
            tone = "warning"
        else:
            tone = "neutral"

        reason = str(row.get("reason") or "").strip()
        note = reason or "Official status found on a recent NBA injury report."
        result = {
            "status": status,
            "tone": tone,
            "reason": reason,
            "note": note,
            "source": "Official NBA injury report",
            "report_label": recent_match.get("report_label") or report_payload.get("report_label") or "",
            "report_url": recent_match.get("report_url") or report_payload.get("report_url") or "",
            "is_unavailable": status in UNAVAILABLE_STATUSES,
            "is_risky": status in RISKY_STATUSES,
            "sort_rank": INJURY_STATUS_ORDER.get(status, 3),
        }
        AVAILABILITY_CACHE[cache_key] = dict(result)
        return result

    result = {
        "status": "Not listed",
        "tone": "good",
        "reason": "Player is not listed on the latest official injury report.",
        "note": "No official injury designation found on the latest report.",
        "source": "Official NBA injury report",
        "report_label": report_payload.get("report_label") or "",
        "report_url": report_payload.get("report_url") or "",
        "is_unavailable": False,
        "is_risky": False,
        "sort_rank": INJURY_STATUS_ORDER.get("Not listed", 3),
    }
    AVAILABILITY_CACHE[cache_key] = dict(result)
    return result

def build_team_availability_summary(team_name: str | None, report_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    team_name = str(team_name or "").strip()
    payload = report_payload or fetch_latest_injury_report_payload()
    if not payload.get("ok"):
        return {
            "team_name": team_name,
            "headline": "Report unavailable",
            "tone": "neutral",
            "listed_count": 0,
            "out_count": 0,
            "questionable_count": 0,
            "probable_count": 0,
            "players": [],
        }

    pending_teams = set(payload.get("pending_teams") or [])
    if team_name and team_name in pending_teams:
        return {
            "team_name": team_name,
            "headline": "Pending report",
            "tone": "warning",
            "listed_count": 0,
            "out_count": 0,
            "questionable_count": 0,
            "probable_count": 0,
            "players": [],
        }

    rows = [row for row in (payload.get("rows") or []) if row.get("team_name") == team_name]
    out_count = sum(1 for row in rows if str(row.get("status") or "") in UNAVAILABLE_STATUSES)
    questionable_count = sum(1 for row in rows if str(row.get("status") or "") in RISKY_STATUSES)
    probable_count = sum(1 for row in rows if str(row.get("status") or "") in GOOD_STATUSES)

    if out_count:
        headline = f"{out_count} out"
        if questionable_count:
            headline += f" • {questionable_count} questionable"
        tone = "bad"
    elif questionable_count:
        headline = f"{questionable_count} questionable"
        tone = "warning"
    elif rows:
        headline = f"{len(rows)} listed"
        tone = "neutral"
    else:
        headline = "Clean report"
        tone = "good"

    return {
        "team_name": team_name,
        "headline": headline,
        "tone": tone,
        "listed_count": len(rows),
        "out_count": out_count,
        "questionable_count": questionable_count,
        "probable_count": probable_count,
        "players": [
            {
                "name": re.sub(r",(?!\s)", ", ", str(row.get("player_display") or "").strip()),
                "status": str(row.get("status") or "").strip(),
            }
            for row in rows[:4]
        ],
    }


def build_confidence_engine(
    *,
    side: str,
    hit_rate: float,
    games_count: int,
    edge: float | None,
    ev: float,
    matchup_delta_pct: float | None,
    availability: dict[str, Any],
    opportunity: dict[str, Any] | None = None,
    team_context: dict[str, Any] | None = None,
    environment: dict[str, Any] | None = None,
    stat: str | None = None,
    player_position: str | None = None,
    line: float | None = None,
    average: float | None = None,
) -> dict[str, Any]:
    opportunity = opportunity or {}
    team_context = team_context or {}
    environment = environment or {}

    if availability.get("is_unavailable"):
        return {
            "grade": "X",
            "score": 0,
            "tone": "out",
            "summary": "Officially unavailable on the latest report.",
            "tier": "Unavailable",
            "components": {"availability": -100.0},
            "tags": ["Unavailable"],
        }

    edge_value = float(edge or 0.0)
    matchup_value = float(matchup_delta_pct or 0.0)
    if side == "UNDER":
        matchup_value *= -1

    avg_value = float(average or 0.0)
    line_value = float(line or 0.0)
    raw_line_delta = round(avg_value - line_value, 2) if line is not None and average is not None else 0.0
    line_delta = raw_line_delta if side == 'OVER' else -raw_line_delta

    form_component = max(-12.0, min(16.0, (hit_rate - 50.0) * 0.46))
    sample_component = max(-4.0, min(6.0, (games_count - 5) * 0.55))
    ev_component = max(-14.0, min(16.0, ev * 100.0))
    edge_component = max(-12.0, min(14.0, edge_value * 0.9))
    line_value_component = max(-9.0, min(10.0, line_delta * 2.2))

    matchup_component = 0.0
    tags: list[str] = []
    if matchup_delta_pct is not None:
        matchup_component = max(-18.0, min(12.0, matchup_value * 0.52))
        if matchup_value >= 10:
            tags.append('Soft position matchup')
        elif matchup_value >= 4:
            tags.append('Opponent allows this stat')
        elif matchup_value <= -10:
            tags.append('Tough position matchup')
        elif matchup_value <= -4:
            tags.append('Opponent denies this stat')

    injury_component = max(-2.0, min(12.0, float(team_context.get('net_adjustment') or team_context.get('injury_adjustment') or 0.0)))
    if injury_component >= 5:
        tags.append('Injury boost')
    elif injury_component > 0:
        tags.append('Usage bump')

    minutes_trend = str(opportunity.get("minutes_trend") or "steady")
    volume_trend = str(opportunity.get("volume_trend") or "steady")
    minutes_component = 0.0
    if minutes_trend == 'up':
        minutes_component += 4.0
        tags.append('Minutes rising')
    elif minutes_trend == 'down':
        minutes_component -= 5.0
        tags.append('Minutes falling')
    if volume_trend == 'up':
        minutes_component += 3.5 if side == 'OVER' else -1.5
        if 'Usage bump' not in tags:
            tags.append('Volume up')
    elif volume_trend == 'down':
        minutes_component -= 3.5 if side == 'OVER' else 1.5

    schedule_component = 0.0
    rest_days = environment.get("rest_days")
    if isinstance(rest_days, int):
        if rest_days >= 2:
            schedule_component += 2.5
            tags.append('Extra rest')
        elif rest_days == 0:
            schedule_component -= 4.0
            tags.append('No rest edge')
    if environment.get("is_back_to_back"):
        schedule_component -= 6.0
        if 'Back-to-back' not in tags:
            tags.append('Back-to-back')
    elif environment.get("schedule_density") == "light":
        schedule_component += 1.5

    availability_component = 0.0
    if availability.get("is_risky"):
        availability_component -= 12.0
        tags.append('Player status risk')

    score = 46.0
    components = {
        'form': round(form_component, 1),
        'sample': round(sample_component, 1),
        'ev': round(ev_component, 1),
        'edge': round(edge_component, 1),
        'line_value': round(line_value_component, 1),
        'matchup': round(matchup_component, 1),
        'injury_context': round(injury_component, 1),
        'minutes_role': round(minutes_component, 1),
        'schedule': round(schedule_component, 1),
        'availability': round(availability_component, 1),
    }
    score += sum(components.values())
    score = int(max(0, min(99, round(score))))

    if score >= 85:
        grade = 'A'
        tone = 'elite'
        tier = 'Elite'
    elif score >= 72:
        grade = 'B'
        tone = 'good'
        tier = 'High'
    elif score >= 60:
        grade = 'C'
        tone = 'warm'
        tier = 'Medium'
    elif score >= 48:
        grade = 'D'
        tone = 'neutral'
        tier = 'Low'
    else:
        grade = 'F'
        tone = 'bad'
        tier = 'Very low'

    summary_parts: list[str] = []
    if injury_component >= 5:
        summary_parts.append('injury-driven usage boost')
    elif injury_component > 0:
        summary_parts.append('mild lineup boost')

    if matchup_component >= 5:
        summary_parts.append('favorable position environment')
    elif matchup_component <= -5:
        summary_parts.append('tough opponent environment')

    if minutes_component >= 4:
        summary_parts.append('role trending up')
    elif minutes_component <= -4:
        summary_parts.append('role cooling off')

    if line_value_component >= 4:
        summary_parts.append('line sits below recent output')
    elif line_value_component <= -4:
        summary_parts.append('line looks inflated')

    if availability.get('is_risky'):
        summary_parts.append('watch final status')
    elif schedule_component <= -5:
        summary_parts.append('schedule drag applied')

    if not summary_parts:
        if edge_value >= 4 or ev >= 0.05:
            summary_parts.append('solid baseline edge')
        else:
            summary_parts.append('mixed signals')

    unique_tags: list[str] = []
    for tag in tags + list(team_context.get('impact_tags') or []):
        if tag and tag not in unique_tags:
            unique_tags.append(tag)

    return {
        'grade': grade,
        'score': score,
        'tone': tone,
        'tier': tier,
        'summary': ' • '.join(summary_parts).capitalize(),
        'components': components,
        'tags': unique_tags[:6],
        'line_delta': round(line_delta, 2),
        'raw_line_delta': round(raw_line_delta, 2),
        'player_position': player_position or '',
        'stat': stat or '',
    }

def throttle_request() -> None:
    global LAST_REQUEST_TIME

    with REQUEST_LOCK:
        elapsed = time.time() - LAST_REQUEST_TIME
        if elapsed < 0.20:
            time.sleep(0.20 - elapsed)
        LAST_REQUEST_TIME = time.time()


def compute_stat_value(row: dict[str, Any], stat: str) -> float:
    normalized_row = sanitize_game_log_row(row)
    if stat == "PRA":
        return safe_stat_number(normalized_row, "PTS") + safe_stat_number(normalized_row, "REB") + safe_stat_number(normalized_row, "AST")
    if stat == "PR":
        return safe_stat_number(normalized_row, "PTS") + safe_stat_number(normalized_row, "REB")
    if stat == "PA":
        return safe_stat_number(normalized_row, "PTS") + safe_stat_number(normalized_row, "AST")
    if stat == "RA":
        return safe_stat_number(normalized_row, "REB") + safe_stat_number(normalized_row, "AST")

    column = STAT_MAP.get(stat)
    if not column:
        raise ValueError(f"Unsupported stat: {stat}")
    return safe_stat_number(normalized_row, column)


def parse_minutes_to_decimal(raw_minutes: Any) -> float:
    raw = str(raw_minutes or '').strip()
    if not raw:
        return 0.0
    if ':' in raw:
        try:
            mins, secs = raw.split(':', 1)
            return round(int(mins) + int(secs) / 60.0, 1)
        except Exception:
            return 0.0
    try:
        return round(float(raw), 1)
    except Exception:
        return 0.0


def safe_stat_number(row: dict[str, Any], key: str) -> float:
    try:
        value = row.get(key, 0)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return 0.0
        number = float(value or 0)
        if math.isnan(number) or math.isinf(number):
            return 0.0
        return number
    except Exception:
        return 0.0


def sanitize_game_log_row(row: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(row)
    for col in VALIDATED_BOX_SCORE_COLUMNS:
        cleaned[col] = round(safe_stat_number(cleaned, col), 3)
    cleaned["MIN"] = str(cleaned.get("MIN") or "").strip()
    cleaned["GAME_DATE"] = str(cleaned.get("GAME_DATE") or "").strip()
    cleaned["MATCHUP"] = str(cleaned.get("MATCHUP") or "").strip()
    game_id = str(cleaned.get("Game_ID") or cleaned.get("GAME_ID") or "").strip()
    if game_id:
        cleaned["GAME_ID"] = game_id
        cleaned["Game_ID"] = game_id
    return cleaned


def dedupe_game_log_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for raw_row in rows or []:
        row = sanitize_game_log_row(raw_row)
        row_key = str(row.get("GAME_ID") or row.get("Game_ID") or "").strip()
        if row_key:
            dedupe_key: tuple[Any, ...] = ("gid", row_key)
        else:
            dedupe_key = (
                "fallback",
                str(row.get("GAME_DATE") or ""),
                str(row.get("MATCHUP") or ""),
                str(row.get("WL") or ""),
                row.get("MIN"),
                *(row.get(col, 0.0) for col in VALIDATED_BOX_SCORE_COLUMNS),
            )
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped.append(row)
    deduped.sort(key=lambda item: parse_game_date_any(item.get("GAME_DATE")) or datetime.min, reverse=True)
    return deduped


def safe_int_score(*values: Any) -> int:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        try:
            number = float(value)
            if math.isnan(number):
                continue
            return int(number)
        except Exception:
            continue
    return 0


def average_or_zero(values: list[float], digits: int = 1) -> float:
    return round(sum(values) / len(values), digits) if values else 0.0


def parse_game_date_any(raw_value: Any) -> datetime | None:
    raw = str(raw_value or "").strip()
    if not raw:
        return None

    for fmt in ("%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            pass

    iso_candidate = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_candidate)
    except Exception:
        pass

    match = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y-%m-%d")
        except Exception:
            pass

    return None



@timed_call("team_game_log")
def fetch_team_game_log(team_id: int, season: str, season_type: str) -> list[dict[str, Any]]:
    cache_key = (int(team_id), str(season), str(season_type))
    cached = TEAM_GAME_LOG_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < TEAM_CONTEXT_TTL_SECONDS:
        return [dict(row) for row in (cached.get("rows") or [])]

    try:
        response = call_nba_with_retries(
            lambda: TeamGameLog(
                team_id=team_id,
                season=season,
                season_type_all_star=season_type,
                timeout=12,
            ),
            label="team game log request",
            attempts=2,
            base_delay=0.6,
        )
        df = response.get_data_frames()[0]
        rows = df.to_dict(orient="records") if not df.empty else []
        TEAM_GAME_LOG_CACHE[cache_key] = {"timestamp": now_ts, "rows": rows}
        return [dict(row) for row in rows]
    except Exception:
        if cached:
            return [dict(row) for row in (cached.get("rows") or [])]
        return []


def _team_possessions_proxy(row: dict[str, Any]) -> float:
    fga = safe_stat_number(row, "FGA")
    fta = safe_stat_number(row, "FTA")
    oreb = safe_stat_number(row, "OREB")
    tov = safe_stat_number(row, "TOV")
    return fga + (0.44 * fta) - oreb + tov


def build_team_recent_context(team_id: int | None, season: str, season_type: str, last_n: int = 10) -> dict[str, Any] | None:
    if not team_id:
        return None
    cache_key = (int(team_id), str(season), str(season_type), int(last_n))
    cached = TEAM_RECENT_CONTEXT_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < TEAM_CONTEXT_TTL_SECONDS:
        payload = cached.get("payload")
        return dict(payload) if isinstance(payload, dict) else None

    rows = fetch_team_game_log(int(team_id), season, season_type)
    recent_rows = rows[: max(1, int(last_n))]
    if not recent_rows:
        TEAM_RECENT_CONTEXT_CACHE[cache_key] = {"timestamp": now_ts, "payload": None}
        return None

    pace_samples = [_team_possessions_proxy(row) for row in recent_rows]
    pace_value = round(sum(pace_samples) / len(pace_samples), 2) if pace_samples else None

    plus_minus_samples: list[float] = []
    for row in recent_rows:
        raw_pm = row.get("PLUS_MINUS")
        try:
            plus_minus_samples.append(float(raw_pm or 0.0))
        except (TypeError, ValueError):
            continue
    avg_plus_minus = round(sum(plus_minus_samples) / len(plus_minus_samples), 2) if plus_minus_samples else None

    payload = {
        "team_id": int(team_id),
        "sample_games": len(recent_rows),
        "pace_proxy": pace_value,
        "avg_plus_minus": avg_plus_minus,
    }
    TEAM_RECENT_CONTEXT_CACHE[cache_key] = {"timestamp": now_ts, "payload": dict(payload)}
    return payload


def enrich_environment_with_team_context(environment: dict[str, Any], team_context: dict[str, Any] | None, opponent_context: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(environment or {})
    team_pace = None if not team_context else team_context.get("pace_proxy")
    opponent_pace = None if not opponent_context else opponent_context.get("pace_proxy")
    if isinstance(team_pace, (int, float)) and isinstance(opponent_pace, (int, float)):
        combined_pace = round((float(team_pace) + float(opponent_pace)) / 2.0, 2)
        payload["team_pace_proxy"] = round(float(team_pace), 2)
        payload["opponent_pace_proxy"] = round(float(opponent_pace), 2)
        payload["combined_pace_proxy"] = combined_pace
        if combined_pace >= 101.5:
            payload["pace_bucket"] = "fast"
            payload["pace_label"] = "Fast pace setup"
        elif combined_pace <= 97.5:
            payload["pace_bucket"] = "slow"
            payload["pace_label"] = "Slow pace caution"
        else:
            payload["pace_bucket"] = "neutral"
            payload["pace_label"] = "Neutral pace"

    team_pm = None if not team_context else team_context.get("avg_plus_minus")
    opp_pm = None if not opponent_context else opponent_context.get("avg_plus_minus")
    if isinstance(team_pm, (int, float)) and isinstance(opp_pm, (int, float)):
        projected_spread = round(float(team_pm) - float(opp_pm), 1)
        payload["projected_spread"] = projected_spread
        if projected_spread >= 13:
            payload["spread_bucket"] = "blowout"
            payload["spread_label"] = f"Blowout risk • est. -{abs(projected_spread):.1f}"
        elif projected_spread >= 5:
            payload["spread_bucket"] = "favorite"
            payload["spread_label"] = f"Favorite script • est. -{abs(projected_spread):.1f}"
        elif projected_spread <= -13:
            payload["spread_bucket"] = "blowout"
            payload["spread_label"] = f"Blowout risk • est. +{abs(projected_spread):.1f}"
        elif projected_spread <= -5:
            payload["spread_bucket"] = "underdog"
            payload["spread_label"] = f"Underdog script • est. +{abs(projected_spread):.1f}"
        else:
            payload["spread_bucket"] = "close"
            payload["spread_label"] = f"Close spread setup • est. {projected_spread:+.1f}"
    return payload

def build_game_environment_context(season_rows: list[dict[str, Any]], next_game: dict[str, Any] | None, team_id: int | None = None, season: str = "", season_type: str = "") -> dict[str, Any]:
    last_game_dt = parse_game_date_any(season_rows[0].get("GAME_DATE")) if season_rows else None
    next_game_dt = parse_game_date_any((next_game or {}).get("game_date"))

    rest_days: int | None = None
    is_back_to_back = False
    if last_game_dt and next_game_dt:
        rest_days = max(0, (next_game_dt.date() - last_game_dt.date()).days - 1)
        is_back_to_back = rest_days == 0

    games_last7 = 0
    if next_game_dt:
        for row in season_rows:
            row_dt = parse_game_date_any(row.get("GAME_DATE"))
            if not row_dt:
                continue
            gap = (next_game_dt.date() - row_dt.date()).days
            if 0 < gap <= 7:
                games_last7 += 1

    if is_back_to_back:
        headline = "Back-to-back spot"
        tone = "warning"
        summary = "No rest between games can trim ceiling and add volatility."
    elif isinstance(rest_days, int) and rest_days >= 2:
        headline = "Rest edge"
        tone = "good"
        summary = "Extra rest can support workload and late-game legs."
    else:
        headline = "Normal rest"
        tone = "neutral"
        summary = "Nothing unusual from the schedule spot right now."

    if games_last7 >= 4 and not is_back_to_back:
        summary = "Heavy recent schedule can still create fatigue even without a true back-to-back."
        tone = "warning"
        headline = "Busy schedule"

    return {
        "headline": headline,
        "tone": tone,
        "summary": summary,
        "rest_days": rest_days,
        "is_back_to_back": is_back_to_back,
        "games_last7": games_last7,
        "venue_label": "Home" if (next_game or {}).get("is_home") else ("Away" if next_game else "TBD"),
        "next_opponent": (next_game or {}).get("opponent_abbreviation") or "",
    }


def build_game_log_entry(row: dict[str, Any], stat: str, line: float) -> dict[str, Any]:
    value = round(compute_stat_value(row, stat), 1)
    pts = round(safe_stat_number(row, "PTS"), 1)
    reb = round(safe_stat_number(row, "REB"), 1)
    ast = round(safe_stat_number(row, "AST"), 1)
    entry = {
        "game_date": row["GAME_DATE"],
        "matchup": row.get("MATCHUP", ""),
        "value": value,
        "hit": value >= line,
        "minutes": parse_minutes_to_decimal(row.get("MIN")),
        "fga": round(safe_stat_number(row, "FGA"), 1),
        "fg3a": round(safe_stat_number(row, "FG3A"), 1),
        "fta": round(safe_stat_number(row, "FTA"), 1),
        "pts": pts,
        "reb": reb,
        "ast": ast,
        "components": {},
    }
    if stat == "PRA":
        entry["components"] = {"PTS": pts, "REB": reb, "AST": ast}
    elif stat == "PR":
        entry["components"] = {"PTS": pts, "REB": reb}
    elif stat == "PA":
        entry["components"] = {"PTS": pts, "AST": ast}
    elif stat == "RA":
        entry["components"] = {"REB": reb, "AST": ast}
    return entry

def parse_matchup_descriptor(matchup: str) -> dict[str, Any]:
    raw = str(matchup or '').strip()
    location = 'neutral'
    team_abbreviation = ''
    opponent_abbreviation = ''
    if ' vs. ' in raw:
        parts = raw.split(' vs. ', 1)
        location = 'home'
    elif ' @ ' in raw:
        parts = raw.split(' @ ', 1)
        location = 'away'
    else:
        parts = [raw, '']
    if parts:
        team_abbreviation = str(parts[0] or '').strip().upper()
    if len(parts) > 1:
        opponent_abbreviation = str(parts[1] or '').strip().upper()
    return {
        'location': location,
        'team_abbreviation': team_abbreviation,
        'opponent_abbreviation': opponent_abbreviation,
    }


def enrich_game_logs_with_context(season_rows: list[dict[str, Any]], team_id: int | None, season: str, season_type: str, player_id: int) -> list[dict[str, Any]]:
    cache_key = (player_id, season, season_type, team_id)
    cached = ENRICHED_LOG_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - cached['timestamp'] < CACHE_TTL_SECONDS:
        return [dict(row) for row in cached['rows']]

    scoreboards_by_date: dict[str, list[dict[str, Any]]] = {}
    enriched: list[dict[str, Any]] = []
    for row in season_rows:
        row_copy = dict(row)
        matchup_info = parse_matchup_descriptor(row_copy.get('MATCHUP', ''))
        row_copy['_location'] = matchup_info['location']
        row_copy['_opponent_abbreviation'] = matchup_info['opponent_abbreviation']
        row_copy['_minutes'] = parse_minutes_to_decimal(row_copy.get('MIN'))
        row_copy['_fga'] = round(safe_stat_number(row_copy, 'FGA'), 1)
        row_copy['_fg3a'] = round(safe_stat_number(row_copy, 'FG3A'), 1)
        row_copy['_fta'] = round(safe_stat_number(row_copy, 'FTA'), 1)
        wl = str(row_copy.get('WL') or '').strip().upper()
        row_copy['_result'] = 'win' if wl == 'W' else ('loss' if wl == 'L' else 'all')
        row_copy['_margin'] = None

        game_id = str(row_copy.get('Game_ID') or row_copy.get('GAME_ID') or '').strip()
        game_date = parse_game_date_any(row_copy.get('GAME_DATE'))
        if game_id and game_date:
            date_key = game_date.strftime('%Y-%m-%d')
            if date_key not in scoreboards_by_date:
                scoreboards_by_date[date_key] = fetch_scoreboard_games(date_key)
            game_row = next((item for item in scoreboards_by_date[date_key] if str(item.get('GAME_ID') or '').strip() == game_id), None)
            if game_row:
                home_team_id = int(game_row.get('HOME_TEAM_ID') or 0)
                away_team_id = int(game_row.get('VISITOR_TEAM_ID') or 0)
                home_score = safe_int_score(game_row.get('PTS_HOME'), 0)
                away_score = safe_int_score(game_row.get('PTS_AWAY'), 0)
                if team_id and team_id == home_team_id:
                    row_copy['_margin'] = home_score - away_score
                elif team_id and team_id == away_team_id:
                    row_copy['_margin'] = away_score - home_score
                elif matchup_info['location'] == 'home':
                    row_copy['_margin'] = home_score - away_score
                elif matchup_info['location'] == 'away':
                    row_copy['_margin'] = away_score - home_score

        enriched.append(row_copy)

    ENRICHED_LOG_CACHE[cache_key] = {'timestamp': now_ts, 'rows': [dict(row) for row in enriched]}
    return enriched


def enrich_game_logs_light(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in rows:
        row_copy = dict(row)
        matchup_info = parse_matchup_descriptor(row_copy.get('MATCHUP', ''))
        row_copy['_location'] = matchup_info['location']
        row_copy['_opponent_abbreviation'] = matchup_info['opponent_abbreviation']
        row_copy['_minutes'] = parse_minutes_to_decimal(row_copy.get('MIN'))
        row_copy['_fga'] = round(safe_stat_number(row_copy, 'FGA'), 1)
        row_copy['_fg3a'] = round(safe_stat_number(row_copy, 'FG3A'), 1)
        row_copy['_fta'] = round(safe_stat_number(row_copy, 'FTA'), 1)
        wl = str(row_copy.get('WL') or '').strip().upper()
        row_copy['_result'] = 'win' if wl == 'W' else ('loss' if wl == 'L' else 'all')
        row_copy['_margin'] = None
        enriched.append(row_copy)
    return enriched


def apply_game_log_filters(
    rows: list[dict[str, Any]],
    *,
    location: str = 'all',
    result: str = 'all',
    margin_min: float | None = None,
    margin_max: float | None = None,
    min_minutes: float | None = None,
    max_minutes: float | None = None,
    min_fga: float | None = None,
    max_fga: float | None = None,
    h2h_only: bool = False,
    opponent_abbreviation: str | None = None,
    opponent_rank_min: int | None = None,
    opponent_rank_max: int | None = None,
    without_player_game_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    cache_key = _build_filtered_pool_cache_key(
        rows,
        location=location,
        result=result,
        margin_min=margin_min,
        margin_max=margin_max,
        min_minutes=min_minutes,
        max_minutes=max_minutes,
        min_fga=min_fga,
        max_fga=max_fga,
        h2h_only=h2h_only,
        opponent_abbreviation=opponent_abbreviation,
        opponent_rank_min=opponent_rank_min,
        opponent_rank_max=opponent_rank_max,
        without_player_game_ids=tuple(sorted(without_player_game_ids or [])),
    )
    cached = FILTERED_POOL_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < FILTERED_POOL_CACHE_TTL_SECONDS:
        return [dict(row) for row in cached["rows"]]

    location = str(location or 'all').lower()
    result = str(result or 'all').lower()
    opponent_abbreviation = str(opponent_abbreviation or '').upper().strip()

    filtered: list[dict[str, Any]] = []
    for row in rows:
        if location in {'home', 'away'} and str(row.get('_location') or '').lower() != location:
            continue
        if result in {'win', 'loss'} and str(row.get('_result') or '').lower() != result:
            continue
        margin = row.get('_margin')
        abs_margin = abs(float(margin)) if isinstance(margin, (int, float)) else None
        if margin_min is not None and (abs_margin is None or abs_margin < margin_min):
            continue
        if margin_max is not None and (abs_margin is None or abs_margin > margin_max):
            continue
        minutes_value = float(row.get('_minutes') or 0)
        fga_value = float(row.get('_fga') or 0)
        if min_minutes is not None and minutes_value < min_minutes:
            continue
        if max_minutes is not None and minutes_value > max_minutes:
            continue
        if min_fga is not None and fga_value < min_fga:
            continue
        if max_fga is not None and fga_value > max_fga:
            continue
        if h2h_only:
            if not opponent_abbreviation or opponent_abbreviation not in str(row.get('MATCHUP') or '').upper():
                continue
        rank_value = row.get('_opponent_rank')
        if opponent_rank_min is not None and (not isinstance(rank_value, int) or rank_value < opponent_rank_min):
            continue
        if opponent_rank_max is not None and (not isinstance(rank_value, int) or rank_value > opponent_rank_max):
            continue
        if without_player_game_ids is not None:
            game_id = str(row.get('GAME_ID') or row.get('Game_ID') or '').strip()
            if not game_id or game_id in without_player_game_ids:
                continue
        filtered.append(row)
    FILTERED_POOL_CACHE[cache_key] = {"timestamp": now_ts, "rows": [dict(row) for row in filtered]}
    return filtered


def build_filter_summary(
    *,
    location: str = 'all',
    result: str = 'all',
    margin_min: float | None = None,
    margin_max: float | None = None,
    min_minutes: float | None = None,
    max_minutes: float | None = None,
    min_fga: float | None = None,
    max_fga: float | None = None,
    h2h_only: bool = False,
    opponent_rank_range: str | None = None,
    without_player_name: str | None = None,
    without_player_names: list[str] | None = None,
    debug: bool = False,
) -> dict[str, Any]:
    chips: list[str] = []
    if location == 'home':
        chips.append('Home')
    elif location == 'away':
        chips.append('Away')
    if result == 'win':
        chips.append('Wins')
    elif result == 'loss':
        chips.append('Losses')
    if margin_min is not None or margin_max is not None:
        if margin_min is not None and margin_max is not None:
            chips.append(f'Margin {margin_min:g}-{margin_max:g}')
        elif margin_min is not None:
            chips.append(f'Margin ≥ {margin_min:g}')
        else:
            chips.append(f'Margin ≤ {margin_max:g}')
    if min_minutes is not None or max_minutes is not None:
        if min_minutes is not None and max_minutes is not None:
            chips.append(f'MIN {min_minutes:g}-{max_minutes:g}')
        elif min_minutes is not None:
            chips.append(f'MIN ≥ {min_minutes:g}')
        else:
            chips.append(f'MIN ≤ {max_minutes:g}')
    if min_fga is not None or max_fga is not None:
        if min_fga is not None and max_fga is not None:
            chips.append(f'FGA {min_fga:g}-{max_fga:g}')
        elif min_fga is not None:
            chips.append(f'FGA ≥ {min_fga:g}')
        else:
            chips.append(f'FGA ≤ {max_fga:g}')
    if h2h_only:
        chips.append('H2H only')
    rank_min, rank_max, rank_label = normalize_opponent_rank_range(opponent_rank_range)
    if rank_min is not None and rank_max is not None:
        chips.append(f'Opp rank {rank_min}-{rank_max}')
    teammate_names = [str(name).strip() for name in (without_player_names or []) if str(name).strip()]
    teammate_name = str(without_player_name or '').strip()
    if teammate_names:
        chips.append(f"Without {', '.join(teammate_names)}")
    elif teammate_name:
        chips.append(f'Without {teammate_name}')
    return {
        'chips': chips,
        'label': ' • '.join(chips) if chips else 'All games',
        'has_filters': bool(chips),
    }


TEAM_RECORDS_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
TEAM_RECORDS_CACHE_TTL_SECONDS = 6 * 60 * 60
TEAMMATE_ABSENCE_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
TEAMMATE_ABSENCE_CACHE_TTL_SECONDS = 6 * 60 * 60


def build_team_rank_map(season: str, season_type: str = 'Regular Season') -> dict[int, int]:
    cache_key = (str(season), str(season_type))
    cached = TEAM_RECORDS_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get('timestamp') or 0.0) < TEAM_RECORDS_CACHE_TTL_SECONDS:
        return dict(cached.get('rank_map') or {})

    rows: list[dict[str, Any]] = []
    for team in TEAM_POOL:
        team_id = int(team.get('id') or 0)
        if not team_id:
            continue
        try:
            logs = fetch_team_game_log(team_id=team_id, season=season, season_type=season_type)
        except Exception:
            continue
        wins = 0
        losses = 0
        for row in logs:
            wl = str(row.get('WL') or '').upper().strip()
            if wl == 'W':
                wins += 1
            elif wl == 'L':
                losses += 1
        games = wins + losses
        pct = (wins / games) if games else 0.0
        rows.append({'team_id': team_id, 'wins': wins, 'losses': losses, 'win_pct': pct})

    rows.sort(key=lambda item: (-float(item.get('win_pct') or 0.0), -int(item.get('wins') or 0), TEAM_LOOKUP.get(int(item.get('team_id') or 0), {}).get('full_name', '')))
    rank_map = {int(item['team_id']): idx + 1 for idx, item in enumerate(rows)}
    TEAM_RECORDS_CACHE[cache_key] = {'timestamp': now_ts, 'rank_map': rank_map}
    return dict(rank_map)


def teammate_absence_game_ids(player_id: int, season: str, season_type: str = 'Regular Season') -> set[str]:
    cache_key = (int(player_id), str(season), str(season_type))
    cached = TEAMMATE_ABSENCE_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get('timestamp') or 0.0) < TEAMMATE_ABSENCE_CACHE_TTL_SECONDS:
        return set(cached.get('game_ids') or [])
    try:
        rows = fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
    except Exception:
        rows = []
    game_ids = {str(row.get('GAME_ID') or row.get('Game_ID') or '').strip() for row in rows if str(row.get('GAME_ID') or row.get('Game_ID') or '').strip()}
    TEAMMATE_ABSENCE_CACHE[cache_key] = {'timestamp': now_ts, 'game_ids': sorted(game_ids)}
    return set(game_ids)




def normalize_without_player_ids(raw_ids: Any) -> list[int]:
    if raw_ids is None:
        return []
    values = raw_ids if isinstance(raw_ids, (list, tuple, set)) else [raw_ids]
    cleaned: list[int] = []
    seen: set[int] = set()
    for value in values:
        text = str(value or '').strip()
        if not text:
            continue
        for chunk in text.split(','):
            chunk = chunk.strip()
            if not chunk:
                continue
            try:
                number = int(chunk)
            except Exception:
                continue
            if number > 0 and number not in seen:
                seen.add(number)
                cleaned.append(number)
    return cleaned


def resolve_without_player_names(player_ids: list[int]) -> list[str]:
    names: list[str] = []
    for player_id in player_ids:
        player = PLAYER_LOOKUP.get(int(player_id) or 0)
        name = str((player or {}).get('full_name') or '').strip()
        if name:
            names.append(name)
    return names


def build_without_player_union_game_ids(player_ids: list[int], season: str, season_type: str = 'Regular Season') -> set[str] | None:
    normalized_ids = normalize_without_player_ids(player_ids)
    if not normalized_ids:
        return None
    game_ids: set[str] = set()
    for teammate_id in normalized_ids:
        game_ids.update(teammate_absence_game_ids(int(teammate_id), season=season, season_type=season_type))
    return game_ids

def normalize_opponent_rank_range(opponent_rank_range: str | None) -> tuple[int | None, int | None, str]:
    raw = str(opponent_rank_range or '').strip().lower()
    if not raw or raw == 'all':
        return None, None, 'all'
    aliases = {
        'top10': (1, 10, 'top10'),
        'top_10': (1, 10, 'top10'),
        'top5': (1, 5, 'top5'),
        'top_5': (1, 5, 'top5'),
        'mid10': (11, 20, 'mid10'),
        'mid_10': (11, 20, 'mid10'),
        'bottom10': (21, 30, 'bottom10'),
        'bottom_10': (21, 30, 'bottom10'),
        'bottom5': (26, 30, 'bottom5'),
        'bottom_5': (26, 30, 'bottom5'),
    }
    if raw in aliases:
        return aliases[raw]
    match = re.match(r'^(\d{1,2})\s*[-:]\s*(\d{1,2})$', raw)
    if match:
        lo = max(1, min(30, int(match.group(1))))
        hi = max(1, min(30, int(match.group(2))))
        lo, hi = min(lo, hi), max(lo, hi)
        return lo, hi, f'{lo}-{hi}'
    return None, None, 'all'


def build_opportunity_context(season_rows: list[dict[str, Any]], last_n: int) -> dict[str, Any]:
    recent_rows = season_rows[: max(1, min(last_n, len(season_rows)))]
    short_rows = season_rows[: max(1, min(5, len(season_rows)))]
    older_rows = season_rows[5:10] if len(season_rows) >= 10 else season_rows[len(short_rows):]

    short_minutes = [parse_minutes_to_decimal(row.get("MIN")) for row in short_rows]
    recent_minutes = [parse_minutes_to_decimal(row.get("MIN")) for row in recent_rows]
    older_minutes = [parse_minutes_to_decimal(row.get("MIN")) for row in older_rows]

    short_fga = [safe_stat_number(row, "FGA") for row in short_rows]
    recent_fga = [safe_stat_number(row, "FGA") for row in recent_rows]
    older_fga = [safe_stat_number(row, "FGA") for row in older_rows]

    short_fg3a = [safe_stat_number(row, "FG3A") for row in short_rows]
    recent_fg3a = [safe_stat_number(row, "FG3A") for row in recent_rows]
    short_fta = [safe_stat_number(row, "FTA") for row in short_rows]
    recent_fta = [safe_stat_number(row, "FTA") for row in recent_rows]

    mins_last5 = average_or_zero(short_minutes)
    mins_sample = average_or_zero(recent_minutes)
    fga_last5 = average_or_zero(short_fga)
    fga_sample = average_or_zero(recent_fga)
    fg3a_last5 = average_or_zero(short_fg3a)
    fg3a_sample = average_or_zero(recent_fg3a)
    fta_last5 = average_or_zero(short_fta)
    fta_sample = average_or_zero(recent_fta)

    older_min_avg = average_or_zero(older_minutes) if older_minutes else mins_sample
    older_fga_avg = average_or_zero(older_fga) if older_fga else fga_sample

    minute_delta = round(mins_last5 - older_min_avg, 1)
    fga_delta = round(fga_last5 - older_fga_avg, 1)

    if minute_delta >= 2.0:
        minutes_trend = 'up'
        minutes_label = 'Minutes rising'
    elif minute_delta <= -2.0:
        minutes_trend = 'down'
        minutes_label = 'Minutes dipping'
    else:
        minutes_trend = 'steady'
        minutes_label = 'Minutes stable'

    if fga_delta >= 2.0:
        volume_trend = 'up'
        volume_label = 'Shot volume rising'
    elif fga_delta <= -2.0:
        volume_trend = 'down'
        volume_label = 'Shot volume dipping'
    else:
        volume_trend = 'steady'
        volume_label = 'Shot volume stable'

    return {
        'minutes_last5': mins_last5,
        'minutes_sample': mins_sample,
        'minutes_delta': minute_delta,
        'minutes_trend': minutes_trend,
        'minutes_label': minutes_label,
        'fga_last5': fga_last5,
        'fga_sample': fga_sample,
        'fga_delta': fga_delta,
        'volume_trend': volume_trend,
        'volume_label': volume_label,
        'fg3a_last5': fg3a_last5,
        'fg3a_sample': fg3a_sample,
        'fta_last5': fta_last5,
        'fta_sample': fta_sample,
        'summary': f'{minutes_label} • {mins_last5:.1f} MIN lately. {volume_label} • {fga_last5:.1f} FGA lately.',
    }


def _normalize_position_group(position_text: str | None) -> str:
    raw = str(position_text or '').upper().strip()
    if not raw:
        return ''
    if 'G' in raw and 'F' not in raw and 'C' not in raw:
        return 'G'
    if 'C' in raw:
        return 'C'
    if 'F' in raw:
        return 'F'
    if 'G' in raw:
        return 'G'
    return raw[:1]


def _injury_role_weight(target_position_group: str, injured_position_group: str, stat: str) -> float:
    target = _normalize_position_group(target_position_group)
    injured = _normalize_position_group(injured_position_group)
    stat = str(stat or '').upper().strip()
    if stat in {'AST', 'PA', 'RA'}:
        if injured == 'G':
            return 3.6 if target == 'G' else 1.1
        if injured == 'F':
            return 1.5 if target in {'G', 'F'} else 0.5
        if injured == 'C':
            return 0.9 if target in {'G', 'F'} else 0.4
    if stat in {'REB'}:
        if injured == 'C':
            return 3.8 if target in {'C', 'F'} else 0.8
        if injured == 'F':
            return 2.2 if target in {'F', 'C'} else 0.7
        if injured == 'G':
            return 0.7
    if stat in {'PTS', '3PM', 'PR', 'PRA', 'PA'}:
        if injured == 'G':
            return 3.2 if target == 'G' else 1.8
        if injured == 'F':
            return 2.6 if target in {'F', 'G'} else 0.9
        if injured == 'C':
            return 1.7 if target in {'C', 'F'} else 0.6
    if stat in {'BLK'}:
        return 2.8 if injured == 'C' and target in {'C', 'F'} else 0.8
    if stat in {'STL'}:
        return 1.9 if injured == 'G' and target == 'G' else 0.7
    return 1.0


def build_team_opportunity_context(
    team_name: str | None,
    player_name: str,
    stat: str,
    player_position: str | None = None,
    team_id: int | None = None,
    season: str | None = None,
) -> dict[str, Any]:
    payload = fetch_latest_injury_report_payload()
    team_name = str(team_name or '').strip()
    report_label = str(payload.get('report_label') or '')
    cache_key = (team_name, normalize_report_person_name(player_name), stat, str(player_position or ''), int(team_id or 0), str(season or ''), report_label)
    cached = TEAM_OPPORTUNITY_CACHE.get(cache_key)
    if cached:
        return copy.deepcopy(cached)

    empty = {
        'headline': 'No major same-team absences flagged',
        'tone': 'neutral',
        'summary': 'No major same-team absences are flagged on the latest report.',
        'listed_count': 0,
        'impact_count': 0,
        'players': [],
        'injury_adjustment': 0.0,
        'boost_score': 0,
        'penalty_score': 0,
        'net_adjustment': 0.0,
        'impact_tags': [],
        'impact_reasons': [],
        'opportunity_label': 'Neutral injury environment',
    }
    if not payload.get('ok') or not team_name:
        TEAM_OPPORTUNITY_CACHE[cache_key] = copy.deepcopy(empty)
        return empty

    rows = [row for row in (payload.get('rows') or []) if row.get('team_name') == team_name]
    if not rows:
        TEAM_OPPORTUNITY_CACHE[cache_key] = copy.deepcopy(empty)
        return empty

    roster_lookup: dict[str, str] = {}
    if team_id and season:
        try:
            roster_rows = fetch_team_roster(team_id=int(team_id), season=str(season))
            for roster_row in roster_rows:
                roster_name = str(roster_row.get('PLAYER') or '').strip()
                if roster_name:
                    roster_lookup[normalize_report_person_name(roster_name)] = str(roster_row.get('POSITION') or '').strip()
        except Exception:
            roster_lookup = {}

    player_keys = build_player_name_variants(player_name) or set()
    filtered = [row for row in rows if row.get('player_key') not in player_keys]
    impacted = [row for row in filtered if str(row.get('status') or '') in UNAVAILABLE_STATUSES.union(RISKY_STATUSES)]
    impacted.sort(key=lambda row: INJURY_STATUS_ORDER.get(str(row.get('status') or ''), 9))

    if not impacted:
        result = {
            **empty,
            'listed_count': len(filtered),
        }
        TEAM_OPPORTUNITY_CACHE[cache_key] = copy.deepcopy(result)
        return result

    out_count = sum(1 for row in impacted if str(row.get('status') or '') in UNAVAILABLE_STATUSES)
    risky_count = sum(1 for row in impacted if str(row.get('status') or '') in RISKY_STATUSES)
    target_group = _normalize_position_group(player_position)
    impact_tags: list[str] = []
    impact_reasons: list[str] = []
    boost_score = 0.0
    penalty_score = 0.0

    for row in impacted[:6]:
        status = str(row.get('status') or '').strip()
        injured_name = re.sub(r",(?!\s)", ", ", str(row.get('player_display') or '').strip())
        injured_key = normalize_report_person_name(injured_name)
        injured_pos = roster_lookup.get(injured_key, '')
        injured_group = _normalize_position_group(injured_pos)
        weight = _injury_role_weight(target_group, injured_group, stat)
        if status in UNAVAILABLE_STATUSES:
            adj = weight
        else:
            adj = weight * 0.45
        if adj >= 2.8:
            tag = 'Injury boost'
        elif adj >= 1.4:
            tag = 'Usage bump'
        else:
            tag = 'Thin rotation'
        boost_score += adj
        impact_tags.append(tag)
        pos_label = injured_group or 'rotation'
        impact_reasons.append(f"{injured_name} {status.lower()} ({pos_label}) can shift {get_stat_label_for_copy(stat).lower()} volume.")

    boost_score = round(min(12.0, boost_score), 1)
    net_adjustment = boost_score - penalty_score

    if stat in {'PTS', '3PM', 'PRA', 'PR', 'PA'}:
        angle = 'Same-team absences can open extra shots and usage for this prop.'
    elif stat in {'REB', 'RA'}:
        angle = 'Same-team absences can open rebound share and floor time.'
    elif stat in {'AST'}:
        angle = 'Same-team absences can shift playmaking responsibility.'
    else:
        angle = 'Same-team absences can change the player’s role and path to the line.'

    headline_parts = []
    if out_count:
        headline_parts.append(f'{out_count} out')
    if risky_count:
        headline_parts.append(f'{risky_count} questionable/doubtful')
    headline = ' • '.join(headline_parts)

    unique_tags = []
    for tag in impact_tags:
        if tag not in unique_tags:
            unique_tags.append(tag)

    result = {
        'headline': headline or 'Team absences flagged',
        'tone': 'warning' if out_count or risky_count else 'neutral',
        'summary': angle,
        'listed_count': len(filtered),
        'impact_count': len(impacted),
        'players': [
            {
                'name': re.sub(r",(?!\s)", ", ", str(row.get('player_display') or '').strip()),
                'status': str(row.get('status') or '').strip(),
            }
            for row in impacted[:4]
        ],
        'injury_adjustment': net_adjustment,
        'boost_score': round(boost_score, 1),
        'penalty_score': round(penalty_score, 1),
        'net_adjustment': round(net_adjustment, 1),
        'impact_tags': unique_tags[:4],
        'impact_reasons': impact_reasons[:4],
        'opportunity_label': 'Positive injury environment' if net_adjustment >= 4 else ('Mild injury boost' if net_adjustment > 0 else 'Neutral injury environment'),
    }
    TEAM_OPPORTUNITY_CACHE[cache_key] = copy.deepcopy(result)
    return result

def build_analyzer_interpretation(
    *,
    stat: str,
    line: float,
    hit_rate: float,
    average: float,
    availability: dict[str, Any],
    matchup: dict[str, Any] | None,
    opportunity: dict[str, Any],
    team_context: dict[str, Any],
    h2h: dict[str, Any],
    environment: dict[str, Any],
) -> dict[str, Any]:
    avg_edge = round(average - line, 1)
    matchup = matchup or {}
    lean = str((matchup.get('vs_position') or {}).get('lean') or 'Neutral')
    tone = 'neutral'

    if availability.get('is_unavailable'):
        return {
            'headline': 'Avoid for now',
            'tone': 'bad',
            'summary': 'The player is officially unavailable, so this prop should stay off the card until the status changes.',
            'bullets': [
                availability.get('status') or 'Unavailable',
                availability.get('reason') or 'Official report says he will not play.',
            ],
            'market_takeaway': 'Avoid this prop because the player is not expected to suit up.',
        }

    bullets: list[str] = []
    if availability.get('is_risky'):
        tone = 'warning'
        headline = 'Status check first'
        summary = 'There is injury risk on the player, so wait for a final status check before treating this as a live play.'
        bullets.append(f"{availability.get('status')}: {availability.get('reason') or availability.get('note')}")
    elif hit_rate >= 75 and avg_edge >= 1.0 and lean.lower() in {'good matchup', 'favorable', 'very favorable'} and not environment.get('is_back_to_back'):
        tone = 'good'
        headline = 'Strong over case'
        summary = f'Recent form, line clearance, and matchup support all point in the same direction for this {get_stat_label_for_copy(stat).lower()} over.'
    elif hit_rate >= 65 and avg_edge >= 0.5 and opportunity.get('minutes_trend') == 'up':
        tone = 'good'
        headline = 'Over lean'
        summary = f'The recent sample leans over this {get_stat_label_for_copy(stat).lower()} line, and the player is trending into enough minutes to keep the over live.'
    elif hit_rate <= 35 and avg_edge <= -1.0 and lean.lower() in {'tough', 'very tough', 'bad matchup'}:
        tone = 'bad'
        headline = 'Strong under case'
        summary = f'The trend and matchup both lean against this {get_stat_label_for_copy(stat).lower()} line, which makes the under look cleaner.'
    elif hit_rate <= 45 and avg_edge <= -0.5:
        tone = 'bad'
        headline = 'Under lean'
        summary = f'The player has been finishing below this {get_stat_label_for_copy(stat).lower()} line often enough to keep the under in front.'
    elif environment.get('is_back_to_back'):
        tone = 'warning'
        headline = 'Schedule caution'
        summary = 'The player is in a back-to-back spot, so the line becomes harder to trust as a plug-and-play over.'
    elif lean.lower() in {'very tough', 'tough'}:
        tone = 'warning'
        headline = 'Caution spot'
        summary = 'The recent numbers are playable, but the matchup is working against the prop enough to keep this in the caution tier.'
    elif team_context.get('impact_count') and opportunity.get('minutes_trend') == 'up':
        tone = 'good'
        headline = 'Opportunity building'
        summary = 'Minutes and team context both point to a slightly better role, which keeps this prop interesting.'
    elif h2h and h2h.get('games_count') and h2h.get('hit_count', 0) >= max(1, h2h.get('games_count', 0) - 1):
        tone = 'neutral'
        headline = 'Opponent history matters'
        summary = 'The recent sample is mixed, but this opponent history keeps the prop on the radar.'
    else:
        headline = 'Balanced spot'
        summary = 'Nothing here looks broken, but the signals are mixed enough that this reads as a lean instead of an automatic play.'

    bullets.append(f'Cleared the line in {hit_rate:.1f}% of the recent sample with a {average:.1f} average against a {line:.1f} line.')
    bullets.append(f"Recent role: {opportunity.get('minutes_last5', 0):.1f} MIN, {opportunity.get('fga_last5', 0):.1f} FGA, {opportunity.get('fg3a_last5', 0):.1f} 3PA, {opportunity.get('fta_last5', 0):.1f} FTA.")

    if lean.lower() != 'neutral':
        bullets.append(f"Matchup read: {lean} versus the next opponent.")
    if team_context.get('impact_count'):
        bullets.append(f"Team context: {team_context.get('headline')}. {team_context.get('summary')}")
    if environment.get('headline'):
        env_summary = environment.get('summary') or ''
        bullets.append(f"Schedule spot: {environment.get('headline')}. {env_summary}")
    elif h2h and h2h.get('games_count'):
        bullets.append(f"H2H: {h2h.get('hit_count')}/{h2h.get('games_count')} over this line versus {h2h.get('opponent_abbreviation') or h2h.get('opponent_name')}.")

    market_takeaway = summary
    if tone == 'good' and 'over' in headline.lower():
        market_takeaway = 'Lean: the over has enough support to stay in the shortlist.'
    elif tone == 'bad' and 'under' in headline.lower():
        market_takeaway = 'Lean: the under reads cleaner than the over here.'
    elif tone == 'warning':
        market_takeaway = 'Lean: usable, but not clean enough to force.'

    return {
        'headline': headline,
        'tone': tone,
        'summary': summary,
        'bullets': bullets[:4],
        'market_takeaway': market_takeaway,
    }

def get_stat_label_for_copy(stat: str) -> str:
    labels = {
        'PTS': 'Points', 'REB': 'Rebounds', 'AST': 'Assists', '3PM': 'Threes', 'STL': 'Steals', 'BLK': 'Blocks',
        'PRA': 'Points + rebounds + assists', 'PR': 'Points + rebounds', 'PA': 'Points + assists', 'RA': 'Rebounds + assists',
    }
    return labels.get(stat, stat)


def resolve_primary_position(raw_position: str | None) -> tuple[str | None, str | None]:
    if not raw_position:
        return None, None

    tokens = re.findall(r"[GFC]", str(raw_position).upper())
    if not tokens:
        return None, None

    primary = tokens[0]
    return primary, POSITION_LABELS.get(primary, primary)


def _game_log_cache_is_reliable_stale(cached: dict[str, Any] | None, cached_ts: float) -> bool:
    if not cached or not cached.get("rows"):
        return False
    return (time.time() - float(cached_ts or 0.0)) <= GAME_LOG_MAX_STALE_SECONDS


def _player_info_cache_is_reliable_stale(cached: dict[str, Any] | None, cached_ts: float) -> bool:
    if not cached or not cached.get("row"):
        return False
    return (time.time() - float(cached_ts or 0.0)) <= PLAYER_INFO_MAX_STALE_SECONDS


def _next_game_cache_is_reliable_stale(cached: dict[str, Any] | None, cached_ts: float) -> bool:
    if not cached:
        return False
    return (time.time() - float(cached_ts or 0.0)) <= NEXT_GAME_MAX_STALE_SECONDS


def _make_hashable(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple((str(k), _make_hashable(v)) for k, v in sorted(value.items(), key=lambda item: str(item[0])))
    if isinstance(value, (list, tuple)):
        return tuple(_make_hashable(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted(_make_hashable(item) for item in value))
    if isinstance(value, float):
        return round(value, 4)
    return value


def _build_filtered_pool_cache_key(
    rows: list[dict[str, Any]],
    **filters: Any,
) -> tuple[Any, ...]:
    """Build a stable cache key for filtered pools.

    This intentionally accepts arbitrary filter kwargs so future filter additions do
    not break callers with unexpected-keyword errors. All filter values are
    normalized and sorted by key name before being added to the cache key.
    """
    row_signature = tuple(
        (
            str(row.get("Game_ID") or row.get("GAME_ID") or ""),
            str(row.get("GAME_DATE") or ""),
            str(row.get("MATCHUP") or ""),
            str(row.get("WL") or ""),
            round(float(row.get("_minutes") or 0.0), 2),
            round(float(row.get("_fga") or 0.0), 2),
            round(float(row.get("_margin") or 0.0), 2) if isinstance(row.get("_margin"), (int, float)) else None,
        )
        for row in rows
    )

    normalized_filters = tuple(
        (
            str(key),
            _make_hashable(
                str(value).upper().strip() if key == "opponent_abbreviation" and value is not None else value
            ),
        )
        for key, value in sorted(filters.items(), key=lambda item: str(item[0]))
    )

    return (row_signature, normalized_filters)


def build_stat_summary_block(rows: list[dict[str, Any]], stat: str, line: float) -> dict[str, Any]:
    cache_key = (tuple(str(row.get("Game_ID") or row.get("GAME_ID") or row.get("GAME_DATE") or "") for row in rows), str(stat), float(line))
    cached = STAT_SUMMARY_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < STAT_SUMMARY_CACHE_TTL_SECONDS:
        return copy.deepcopy(cached["payload"])

    games: list[dict[str, Any]] = []
    hit_count = 0
    values: list[float] = []
    for row in rows:
        game_entry = build_game_log_entry(row, stat, line)
        games.append(game_entry)
        values.append(game_entry["value"])
        if game_entry["hit"]:
            hit_count += 1

    avg = round(sum(values) / len(values), 2) if values else 0
    hit_rate = round((hit_count / len(values)) * 100, 1) if values else 0

    # Variance / distribution metrics
    variance_metrics: dict[str, Any] = {}
    if len(values) >= 3:
        import statistics as _stats
        std_dev = round(_stats.stdev(values), 2)
        median = round(_stats.median(values), 2)
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        p25 = round(sorted_vals[max(0, int(n * 0.25))], 2)
        p75 = round(sorted_vals[min(n - 1, int(n * 0.75))], 2)
        # Consistency score: 0-100, higher = more consistent
        # Based on coefficient of variation (lower CV = higher consistency)
        cv = (std_dev / avg) if avg > 0 else 1.0
        consistency_score = round(max(0, min(100, 100 - (cv * 80))), 1)
        # Over-hit rate above median (games where player exceeded their own median)
        above_median = sum(1 for v in values if v > median)
        over_median_rate = round((above_median / len(values)) * 100, 1)
        # Floor / ceiling
        floor_val = round(sorted_vals[0], 2)
        ceiling_val = round(sorted_vals[-1], 2)
        variance_metrics = {
            "std_dev": std_dev,
            "median": median,
            "p25": p25,
            "p75": p75,
            "consistency_score": consistency_score,
            "over_median_rate": over_median_rate,
            "floor": floor_val,
            "ceiling": ceiling_val,
            "cv": round(cv, 3),
        }

    payload = {
        "games": games,
        "values": values,
        "hit_count": hit_count,
        "average": avg,
        "hit_rate": hit_rate,
        "variance": variance_metrics,
    }
    STAT_SUMMARY_CACHE[cache_key] = {"timestamp": now_ts, "payload": copy.deepcopy(payload)}
    return payload


def build_debug_metadata(*, cache_status: dict[str, Any], freshness: dict[str, Any], timings_enabled: bool) -> dict[str, Any]:
    return {
        "cache_status": copy.deepcopy(cache_status),
        "freshness": copy.deepcopy(freshness),
        "timing_enabled": bool(timings_enabled),
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def mask_api_key_for_display(api_key: str) -> str:
    raw = str(api_key or "").strip()
    if len(raw) <= 8:
        return raw
    return f"{raw[:4]}…{raw[-4:]}"


def odds_api_build_query(params: dict[str, Any]) -> str:
    search = []
    for key, value in params.items():
        if value in (None, ""):
            continue
        search.append((key, str(value)))
    return requests.compat.urlencode(search)


def convert_american_to_decimal(price: Any) -> float | None:
    try:
        value = float(price)
    except (TypeError, ValueError):
        return None
    if value == 0:
        return None
    if value > 0:
        return round((value / 100.0) + 1.0, 2)
    return round((100.0 / abs(value)) + 1.0, 2)


def normalize_decimal_price(price: Any, odds_format: str) -> float | None:
    try:
        value = float(price)
    except (TypeError, ValueError):
        return None
    if str(odds_format or "decimal").lower() == "american":
        return convert_american_to_decimal(value)
    return round(value, 2)


def odds_api_fetch(endpoint: str, api_key: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    key = str(api_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Missing Odds API key.")

    query = dict(params or {})
    query["apiKey"] = key
    url = f"{ODDS_API_BASE_URL}{endpoint}?{odds_api_build_query(query)}"
    response = requests.get(url, timeout=(5, 30), headers={"User-Agent": NBA_USER_AGENT, "Accept": "application/json"})
    remaining = response.headers.get("x-requests-remaining")
    used = response.headers.get("x-requests-used")
    last = response.headers.get("x-requests-last")

    if not response.ok:
        detail = response.text.strip() or f"Odds API error {response.status_code}"
        raise HTTPException(status_code=response.status_code, detail=detail)

    try:
        data = response.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Failed to parse Odds API response.") from exc

    return {
        "data": data,
        "quota": {"remaining": remaining, "used": used, "last": last},
    }


def build_odds_import_rows(event_payload: dict[str, Any], odds_format: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    # Keyed by (market_key, player_name, line_value) — bookmaker intentionally excluded
    # so that duplicate props from different books collapse into a single row.
    # The first bookmaker that supplies a complete over+under pair wins.
    pair_map: dict[tuple[str, str, str], dict[str, Any]] = {}

    # Capture event-level metadata for same-game parlay prevention
    event_id = str(event_payload.get("id") or "")
    home_team = str(event_payload.get("home_team") or "")
    away_team = str(event_payload.get("away_team") or "")
    game_label = f"{away_team} @ {home_team}" if home_team or away_team else ""

    for bookmaker in event_payload.get("bookmakers") or []:
        bookmaker_title = str(bookmaker.get("title") or bookmaker.get("key") or "Book")
        for market in bookmaker.get("markets") or []:
            market_key = str(market.get("key") or "")
            stat_code = ODDS_MARKET_TO_STAT.get(market_key)
            if not stat_code:
                continue
            market_updated = market.get("last_update") or bookmaker.get("last_update")
            for outcome in market.get("outcomes") or []:
                side = str(outcome.get("name") or "").strip().lower()
                if side not in {"over", "under"}:
                    continue
                player_name = str(outcome.get("description") or "").strip()
                line_value = outcome.get("point")
                decimal_odds = normalize_decimal_price(outcome.get("price"), odds_format)
                if not player_name or line_value in (None, "") or decimal_odds is None:
                    continue
                # Dedup key: market + player + line only (no bookmaker)
                pair_key = (market_key, player_name, str(line_value))
                bucket = pair_map.setdefault(pair_key, {
                    "bookmaker_title": bookmaker_title,
                    "market_key": market_key,
                    "player_name": player_name,
                    "stat": stat_code,
                    "line": float(line_value),
                    "over_odds": None,
                    "under_odds": None,
                    "market_last_update": market_updated,
                    "event_id": event_id,
                    "game_label": game_label,
                })
                # Only fill in odds that haven't been set yet (first bookmaker wins)
                if bucket[f"{side}_odds"] is None:
                    bucket[f"{side}_odds"] = decimal_odds

    for bucket in pair_map.values():
        if bucket.get("over_odds") is None or bucket.get("under_odds") is None:
            continue
        rows.append({
            "player_name": bucket["player_name"],
            "stat": bucket["stat"],
            "line": round(float(bucket["line"]), 1),
            "over_odds": float(bucket["over_odds"]),
            "under_odds": float(bucket["under_odds"]),
            "bookmaker_title": bucket["bookmaker_title"],
            "market_key": bucket["market_key"],
            "market_last_update": bucket["market_last_update"],
            "event_id": bucket.get("event_id", ""),
            "game_label": bucket.get("game_label", ""),
            "csv_row": f"{bucket['player_name']},{bucket['stat']},{round(float(bucket['line']),1)},{float(bucket['over_odds'])},{float(bucket['under_odds'])}",
        })
    rows.sort(key=lambda item: (item["player_name"].lower(), item["stat"], item["line"]))
    return rows


def _position_dash_cache_is_reliable_stale(cached: dict[str, Any] | None, cached_ts: float) -> bool:
    if not cached or not cached.get("rows"):
        return False
    return (time.time() - float(cached_ts or 0.0)) <= POSITION_DASH_MAX_STALE_SECONDS


@timed_call("fetch_player_game_log")
def fetch_player_game_log(player_id: int, season: str, season_type: str) -> list[dict[str, Any]]:
    cache_key = (player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
    cached = GAME_LOG_CACHE.get(cache_key)
    cached_ts = float((cached or {}).get("timestamp") or 0.0)
    now_ts = time.time()

    if cached and now_ts - cached_ts < CACHE_TTL_SECONDS:
        sanitized_cached_rows = dedupe_game_log_rows(cached["rows"])
        if sanitized_cached_rows != cached.get("rows"):
            GAME_LOG_CACHE[cache_key] = {"timestamp": cached_ts or time.time(), "rows": sanitized_cached_rows}
        return sanitized_cached_rows

    has_reliable_stale = bool(cached and _game_log_cache_is_reliable_stale(cached, cached_ts))

    failure_meta = GAME_LOG_FAILURE_META.get(cache_key) or {}
    last_failure = float(failure_meta.get("last_failure") or 0.0)
    if has_reliable_stale and now_ts - last_failure < GAME_LOG_FAILURE_COOLDOWN_SECONDS:
        return dedupe_game_log_rows(cached["rows"])

    total_budget_seconds = (
        GAME_LOG_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS
        if has_reliable_stale
        else GAME_LOG_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS
    )
    max_attempts = (
        GAME_LOG_FETCH_ATTEMPTS_WITH_RELIABLE_STALE
        if has_reliable_stale
        else GAME_LOG_FETCH_ATTEMPTS_NO_RELIABLE_STALE
    )

    started_at = time.monotonic()
    last_exc: Exception | None = None
    df = None

    for attempt in range(max_attempts):
        elapsed = time.monotonic() - started_at
        remaining_budget = total_budget_seconds - elapsed
        if remaining_budget <= 0:
            break

        timeout_seconds = max(
            GAME_LOG_FETCH_TIMEOUT_FLOOR_SECONDS,
            min(GAME_LOG_FETCH_TIMEOUT_CAP_SECONDS, int(remaining_budget)),
        )

        throttle_request()
        try:
            response = PlayerGameLog(
                player_id=player_id,
                season=season,
                season_type_all_star=season_type,
                timeout=timeout_seconds,
            )
            df = response.get_data_frames()[0]
            break
        except Exception as exc:
            last_exc = exc
            if not is_transient_nba_error(exc):
                break

            elapsed = time.monotonic() - started_at
            remaining_budget = total_budget_seconds - elapsed
            if attempt >= max_attempts - 1 or remaining_budget <= 0:
                break

            sleep_for = min(NBA_BACKOFF_FACTOR * (2 ** attempt), max(0.0, remaining_budget))
            if sleep_for > 0:
                time.sleep(sleep_for)

    if df is None:
        GAME_LOG_FAILURE_META[cache_key] = {"last_failure": time.time()}
        if has_reliable_stale:
            return dedupe_game_log_rows(cached["rows"])
        detail_suffix = f" Details: {last_exc}" if last_exc else ""
        raise HTTPException(
            status_code=502,
            detail=(
                "NBA data request failed. This can happen when NBA stats throttles or times out."
                f" Total live fetch budget: {int(total_budget_seconds)}s."
                f"{detail_suffix}"
            ),
        ) from last_exc

    if df.empty:
        raise HTTPException(status_code=404, detail="No game logs found for this player and season.")

    df["GAME_DATE"] = df["GAME_DATE"].astype(str)
    rows = dedupe_game_log_rows(df.to_dict(orient="records"))

    GAME_LOG_FAILURE_META.pop(cache_key, None)
    GAME_LOG_CACHE[cache_key] = {"timestamp": time.time(), "rows": rows}
    return rows


def fetch_recent_player_game_log(player_id: int, season: str, season_type: str, last_n: int) -> list[dict[str, Any]]:
    cache_key = (player_id, season, season_type, int(last_n), GAME_LOG_CACHE_SCHEMA_VERSION)
    cached = RECENT_GAME_LOG_CACHE.get(cache_key)
    now_ts = time.time()

    if cached and now_ts - cached["timestamp"] < CACHE_TTL_SECONDS:
        return cached["rows"]

    full_rows = fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
    recent_rows = full_rows[:last_n]
    RECENT_GAME_LOG_CACHE[cache_key] = {"timestamp": time.time(), "rows": recent_rows}
    return recent_rows


def _team_roster_cache_is_reliable_stale(cached: dict[str, Any] | None, cache_timestamp: float | None = None) -> bool:
    if not cached or not cached.get("rows"):
        return False
    cache_ts = float(cache_timestamp or cached.get("timestamp") or 0.0)
    if not cache_ts:
        return False
    return max(0.0, time.time() - cache_ts) <= TEAM_ROSTER_MAX_STALE_SECONDS


def fetch_team_roster(team_id: int, season: str) -> list[dict[str, Any]]:
    cache_key = (team_id, season)
    cached = ROSTER_CACHE.get(cache_key)
    now_ts = time.time()
    cached_ts = float((cached or {}).get("timestamp") or 0.0)

    if cached and now_ts - cached_ts < TEAM_ROSTER_CACHE_TTL_SECONDS:
        return cached["rows"]

    reliable_stale_exists = _team_roster_cache_is_reliable_stale(cached, cached_ts)
    failure_meta = TEAM_ROSTER_FAILURE_META.get(cache_key) or {}
    last_failure_ts = float(failure_meta.get("timestamp") or 0.0)

    if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < TEAM_ROSTER_FAILURE_COOLDOWN_SECONDS:
        return cached["rows"]

    budget_seconds = TEAM_ROSTER_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS if reliable_stale_exists else TEAM_ROSTER_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS
    attempts = TEAM_ROSTER_FETCH_ATTEMPTS_WITH_RELIABLE_STALE if reliable_stale_exists else TEAM_ROSTER_FETCH_ATTEMPTS_NO_RELIABLE_STALE
    started_at = time.monotonic()
    last_exc: Exception | None = None

    for _attempt_idx in range(max(1, attempts)):
        elapsed = time.monotonic() - started_at
        remaining = budget_seconds - elapsed
        if remaining <= 0:
            break
        timeout_seconds = max(TEAM_ROSTER_FETCH_TIMEOUT_FLOOR_SECONDS, min(TEAM_ROSTER_FETCH_TIMEOUT_CAP_SECONDS, int(math.ceil(remaining))))
        try:
            response = call_nba_with_retries(
                lambda timeout_seconds=timeout_seconds: CommonTeamRoster(team_id=team_id, season=season, timeout=timeout_seconds),
                label="team roster request",
                attempts=1,
                base_delay=TEAM_ROSTER_RETRY_BASE_DELAY,
            )
            df = response.get_data_frames()[0]
            if df.empty:
                raise HTTPException(status_code=404, detail="No roster found for this team and season.")

            rows = df.to_dict(orient="records")

            def jersey_sort_key(row: dict[str, Any]) -> tuple[int, str]:
                raw_num = str(row.get("NUM", "")).strip()
                jersey_num = int(raw_num) if raw_num.isdigit() else 999
                return jersey_num, str(row.get("PLAYER", "")).lower()

            rows.sort(key=jersey_sort_key)
            payload = {"timestamp": time.time(), "rows": rows}
            ROSTER_CACHE[cache_key] = payload
            TEAM_ROSTER_FAILURE_META.pop(cache_key, None)
            save_persistent_caches_async()
            return rows
        except HTTPException:
            raise
        except Exception as exc:
            last_exc = exc
            TEAM_ROSTER_FAILURE_META[cache_key] = {"timestamp": time.time()}

    if cached and cached.get("rows"):
        return cached["rows"]

    detail = "Team roster request failed. This can happen when NBA stats throttles or times out."
    if last_exc is not None:
        detail = f"{detail} Details: {last_exc}"
    raise HTTPException(status_code=502, detail=detail)


@timed_call("fetch_common_player_info")
def fetch_common_player_info(player_id: int) -> dict[str, Any]:
    cached = PLAYER_INFO_CACHE.get(player_id)
    now_ts = time.time()
    cached_ts = float((cached or {}).get("timestamp") or 0.0)

    if cached and now_ts - cached_ts < PROFILE_TTL_SECONDS:
        return cached["row"]

    reliable_stale_exists = _player_info_cache_is_reliable_stale(cached, cached_ts)
    failure_meta = PLAYER_INFO_FAILURE_META.get(player_id) or {}
    last_failure_ts = float(failure_meta.get("timestamp") or 0.0)

    if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < PLAYER_INFO_FAILURE_COOLDOWN_SECONDS:
        return cached["row"]

    budget_seconds = PLAYER_INFO_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS if reliable_stale_exists else PLAYER_INFO_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS
    attempts = PLAYER_INFO_FETCH_ATTEMPTS_WITH_RELIABLE_STALE if reliable_stale_exists else PLAYER_INFO_FETCH_ATTEMPTS_NO_RELIABLE_STALE
    started_at = time.monotonic()
    last_exc: Exception | None = None

    for attempt_idx in range(max(1, attempts)):
        elapsed = time.monotonic() - started_at
        remaining = budget_seconds - elapsed
        if remaining <= 0:
            break
        timeout_seconds = max(PLAYER_INFO_FETCH_TIMEOUT_FLOOR_SECONDS, min(PLAYER_INFO_FETCH_TIMEOUT_CAP_SECONDS, int(math.ceil(remaining))))
        try:
            response = call_nba_with_retries(
                lambda timeout_seconds=timeout_seconds: CommonPlayerInfo(player_id=player_id, timeout=timeout_seconds),
                label="player info request",
                attempts=1,
                base_delay=PLAYER_INFO_RETRY_BASE_DELAY,
            )
            df = response.get_data_frames()[0]
            if df.empty:
                raise HTTPException(status_code=404, detail="Player info not found.")
            row = df.to_dict(orient="records")[0]
            PLAYER_INFO_CACHE[player_id] = {"timestamp": time.time(), "row": row}
            PLAYER_INFO_FAILURE_META.pop(player_id, None)
            save_persistent_caches_async()
            return row
        except HTTPException:
            raise
        except Exception as exc:
            last_exc = exc
            PLAYER_INFO_FAILURE_META[player_id] = {"timestamp": time.time()}
            if reliable_stale_exists:
                return cached["row"]
            if attempt_idx < max(1, attempts) - 1:
                time.sleep(min(PLAYER_INFO_RETRY_BASE_DELAY * (attempt_idx + 1), max(0.0, max(remaining - 1, 0.0))))

    if reliable_stale_exists and cached and cached.get("row"):
        return cached["row"]
    if cached and cached.get("row") and _player_info_cache_is_reliable_stale(cached, cached_ts):
        return cached["row"]
    raise HTTPException(
        status_code=502,
        detail=(
            "Player info request failed. This can happen when NBA stats throttles or times out. "
            f"Details: {last_exc}"
        ),
    ) from last_exc


def fetch_next_game(player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
    cache_key = (player_id, season, season_type)
    cached = NEXT_GAME_CACHE.get(cache_key)
    now_ts = time.time()
    cached_ts = float((cached or {}).get("timestamp") or 0.0)

    if cached and now_ts - cached_ts < NEXT_GAME_TTL_SECONDS:
        return cached.get("row")

    reliable_stale_exists = _next_game_cache_is_reliable_stale(cached, cached_ts)
    failure_meta = NEXT_GAME_FAILURE_META.get(cache_key) or {}
    last_failure_ts = float(failure_meta.get("timestamp") or 0.0)
    if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < NEXT_GAME_FAILURE_COOLDOWN_SECONDS:
        return cached.get("row")

    budget_seconds = NEXT_GAME_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS if reliable_stale_exists else NEXT_GAME_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS
    attempts = NEXT_GAME_FETCH_ATTEMPTS_WITH_RELIABLE_STALE if reliable_stale_exists else NEXT_GAME_FETCH_ATTEMPTS_NO_RELIABLE_STALE
    started_at = time.monotonic()
    last_exc: Exception | None = None

    for attempt_idx in range(max(1, attempts)):
        elapsed = time.monotonic() - started_at
        remaining = budget_seconds - elapsed
        if remaining <= 0:
            break
        timeout_seconds = max(NEXT_GAME_FETCH_TIMEOUT_FLOOR_SECONDS, min(NEXT_GAME_FETCH_TIMEOUT_CAP_SECONDS, int(math.ceil(remaining))))
        try:
            response = call_nba_with_retries(
                lambda timeout_seconds=timeout_seconds: PlayerNextNGames(
                    player_id=player_id,
                    number_of_games=1,
                    season_all=season,
                    season_type_all_star=season_type,
                    timeout=timeout_seconds,
                ),
                label="next game request",
                attempts=1,
                base_delay=NEXT_GAME_RETRY_BASE_DELAY,
            )
            df = response.get_data_frames()[0]
            row = None if df.empty else df.to_dict(orient="records")[0]
            NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": row}
            NEXT_GAME_FAILURE_META.pop(cache_key, None)
            return row
        except Exception as exc:
            last_exc = exc
            NEXT_GAME_FAILURE_META[cache_key] = {"timestamp": time.time()}
            if reliable_stale_exists:
                return cached.get("row")
            if attempt_idx < max(1, attempts) - 1:
                time.sleep(min(NEXT_GAME_RETRY_BASE_DELAY * (attempt_idx + 1), max(0.0, max(remaining - 1, 0.0))))

    if cached:
        return cached.get("row")
    return None


def _scoreboard_cache_is_reliable_stale(game_date: str, cached: dict[str, Any] | None, cached_ts: float) -> bool:
    if not cached:
        return False
    rows = cached.get("rows") or []
    if not rows:
        return False
    age_seconds = time.time() - float(cached_ts or 0.0)
    if age_seconds > SCOREBOARD_MAX_STALE_SECONDS:
        return False
    current_date = current_nba_game_date()
    return str(game_date or "").strip() == str(current_date or "").strip()


def _get_scoreboard_failure_meta(game_date: str) -> dict[str, Any]:
    return SCOREBOARD_CACHE.setdefault(f"__failure__::{game_date}", {})


def fetch_scoreboard_games(game_date: str) -> list[dict[str, Any]]:
    game_date = str(game_date or "").strip()
    cached = SCOREBOARD_CACHE.get(game_date)
    now_ts = time.time()

    if cached and now_ts - float(cached.get("timestamp") or 0.0) < SCOREBOARD_CACHE_TTL_SECONDS:
        return cached.get("rows") or []

    cached_ts = float(cached.get("timestamp") or 0.0) if cached else 0.0
    reliable_stale_exists = _scoreboard_cache_is_reliable_stale(game_date, cached, cached_ts)
    failure_meta = _get_scoreboard_failure_meta(game_date)
    last_failure_ts = float(failure_meta.get("timestamp") or 0.0)

    if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < SCOREBOARD_FAILURE_COOLDOWN_SECONDS:
        return cached.get("rows") or []

    total_budget = (
        SCOREBOARD_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS
        if reliable_stale_exists
        else SCOREBOARD_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS
    )
    max_attempts = (
        SCOREBOARD_FETCH_ATTEMPTS_WITH_RELIABLE_STALE
        if reliable_stale_exists
        else SCOREBOARD_FETCH_ATTEMPTS_NO_RELIABLE_STALE
    )

    start_ts = time.monotonic()
    attempts_made = 0
    last_exc: Exception | None = None

    while attempts_made < max_attempts:
        elapsed = time.monotonic() - start_ts
        remaining_budget = total_budget - elapsed
        if remaining_budget <= 0:
            break

        timeout_seconds = max(
            SCOREBOARD_FETCH_TIMEOUT_FLOOR_SECONDS,
            min(SCOREBOARD_FETCH_TIMEOUT_CAP_SECONDS, int(remaining_budget)),
        )

        try:
            response = call_nba_with_retries(
                lambda timeout_seconds=timeout_seconds: ScoreboardV2(
                    game_date=game_date,
                    day_offset=0,
                    league_id="00",
                    timeout=timeout_seconds,
                ),
                label="scoreboard request",
                attempts=1,
                base_delay=SCOREBOARD_RETRY_BASE_DELAY,
            )
            header_df = response.game_header.get_data_frame()
            try:
                line_score_df = response.line_score.get_data_frame()
            except Exception:
                line_score_df = None

            rows = header_df.to_dict(orient="records") if not header_df.empty else []
            if not rows:
                SCOREBOARD_CACHE[game_date] = {"timestamp": time.time(), "rows": []}
                SCOREBOARD_CACHE.pop(f"__failure__::{game_date}", None)
                return []

            line_score_lookup: dict[tuple[str, int], dict[str, Any]] = {}
            if line_score_df is not None and not line_score_df.empty:
                for score_row in line_score_df.to_dict(orient="records"):
                    row_game_id = str(score_row.get("GAME_ID") or "").strip()
                    team_id = int(score_row.get("TEAM_ID") or 0)
                    if row_game_id and team_id:
                        line_score_lookup[(row_game_id, team_id)] = score_row

            enriched_rows: list[dict[str, Any]] = []
            for row in rows:
                row_game_id = str(row.get("GAME_ID") or "").strip()
                home_team_id = int(row.get("HOME_TEAM_ID") or 0)
                away_team_id = int(row.get("VISITOR_TEAM_ID") or 0)
                home_score_row = line_score_lookup.get((row_game_id, home_team_id), {})
                away_score_row = line_score_lookup.get((row_game_id, away_team_id), {})

                enriched_row = dict(row)
                enriched_row["PTS_HOME"] = safe_int_score(home_score_row.get("PTS"), row.get("PTS_HOME"), 0)
                enriched_row["PTS_AWAY"] = safe_int_score(away_score_row.get("PTS"), row.get("PTS_AWAY"), 0)
                enriched_rows.append(enriched_row)

            SCOREBOARD_CACHE[game_date] = {"timestamp": time.time(), "rows": enriched_rows}
            SCOREBOARD_CACHE.pop(f"__failure__::{game_date}", None)
            return enriched_rows
        except Exception as exc:
            attempts_made += 1
            last_exc = exc
            if reliable_stale_exists:
                SCOREBOARD_CACHE[f"__failure__::{game_date}"] = {"timestamp": time.time(), "error": str(exc)}
                return cached.get("rows") or []
            if attempts_made >= max_attempts:
                break

    if reliable_stale_exists:
        SCOREBOARD_CACHE[f"__failure__::{game_date}"] = {"timestamp": time.time(), "error": str(last_exc) if last_exc else "timeout"}
        return cached.get("rows") or []

    if cached:
        # For non-current dates, any existing cache is better than failing hard.
        SCOREBOARD_CACHE[f"__failure__::{game_date}"] = {"timestamp": time.time(), "error": str(last_exc) if last_exc else "timeout"}
        return cached.get("rows") or []

    return []


def build_scoreboard_next_game_payload(
    game_row: dict[str, Any],
    player_team_id: int | None,
) -> dict[str, Any] | None:
    if not player_team_id:
        return None

    home_team_id = int(game_row.get("HOME_TEAM_ID") or 0)
    visitor_team_id = int(game_row.get("VISITOR_TEAM_ID") or 0)

    if player_team_id == home_team_id:
        is_home = True
        opponent_team_id = visitor_team_id
        opponent = TEAM_LOOKUP.get(visitor_team_id, {})
        player_team = TEAM_LOOKUP.get(home_team_id, {})
        opponent_abbreviation = str(opponent.get("abbreviation") or "").strip()
        matchup_label = f"vs {opponent_abbreviation}" if opponent_abbreviation else "vs Opponent"
    elif player_team_id == visitor_team_id:
        is_home = False
        opponent_team_id = home_team_id
        opponent = TEAM_LOOKUP.get(home_team_id, {})
        player_team = TEAM_LOOKUP.get(visitor_team_id, {})
        opponent_abbreviation = str(opponent.get("abbreviation") or "").strip()
        matchup_label = f"@ {opponent_abbreviation}" if opponent_abbreviation else "@ Opponent"
    else:
        return None

    game_date = str(game_row.get("GAME_DATE_EST") or "").strip()

    return {
        "game_date": game_date,
        "game_time": "",
        "is_home": is_home,
        "matchup_label": matchup_label,
        "opponent_team_id": opponent_team_id,
        "opponent_name": str(opponent.get("full_name") or "").strip(),
        "opponent_abbreviation": opponent_abbreviation,
        "player_team_abbreviation": str(player_team.get("abbreviation") or "").strip(),
    }


def find_team_next_game_via_scoreboard(team_id: int | None, lookahead_days: int = 10) -> dict[str, Any] | None:
    if not team_id:
        return None

    start_date = datetime.now(ZoneInfo("America/New_York")).date()
    for offset in range(lookahead_days + 1):
        game_date = (start_date + timedelta(days=offset)).strftime("%Y-%m-%d")
        rows = fetch_scoreboard_games(game_date)
        for row in rows:
            payload = build_scoreboard_next_game_payload(row, team_id)
            if payload:
                return payload

    return None


def fetch_position_dash(
    season: str,
    season_type: str,
    position_code: str,
    opponent_team_id: int = 0,
) -> list[dict[str, Any]]:
    cache_key = (season, season_type, position_code, opponent_team_id)
    cached = POSITION_DASH_CACHE.get(cache_key)
    now_ts = time.time()
    cached_ts = float((cached or {}).get("timestamp") or 0.0)

    if cached and now_ts - cached_ts < POSITION_TTL_SECONDS:
        return cached["rows"]

    reliable_stale_exists = _position_dash_cache_is_reliable_stale(cached, cached_ts)

    failure_meta = POSITION_DASH_FAILURE_META.get(cache_key) or {}
    last_failure_ts = float(failure_meta.get("timestamp") or 0.0)
    if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < POSITION_DASH_FAILURE_COOLDOWN_SECONDS:
        return cached.get("rows") or []

    total_budget = (
        POSITION_DASH_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS
        if reliable_stale_exists
        else POSITION_DASH_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS
    )
    attempts = (
        POSITION_DASH_FETCH_ATTEMPTS_WITH_RELIABLE_STALE
        if reliable_stale_exists
        else POSITION_DASH_FETCH_ATTEMPTS_NO_RELIABLE_STALE
    )

    start_monotonic = time.monotonic()

    for attempt in range(attempts):
        elapsed = time.monotonic() - start_monotonic
        remaining = total_budget - elapsed
        if remaining <= 0:
            break

        per_attempt_timeout = max(
            POSITION_DASH_FETCH_TIMEOUT_FLOOR_SECONDS,
            min(POSITION_DASH_FETCH_TIMEOUT_CAP_SECONDS, int(math.ceil(remaining))),
        )

        try:
            response = call_nba_with_retries(
                lambda: LeagueDashPlayerStats(
                    season=season,
                    season_type_all_star=season_type,
                    per_mode_detailed="Totals",
                    player_position_abbreviation_nullable=position_code,
                    opponent_team_id=opponent_team_id,
                    timeout=per_attempt_timeout,
                ),
                label="position dashboard request",
                attempts=1,
            )
            df = response.get_data_frames()[0]
            rows = df.to_dict(orient="records") if not df.empty else []
            POSITION_DASH_CACHE[cache_key] = {"timestamp": time.time(), "rows": rows}
            POSITION_DASH_FAILURE_META.pop(cache_key, None)
            return rows
        except Exception as exc:
            POSITION_DASH_FAILURE_META[cache_key] = {"timestamp": time.time()}

            remaining_after = total_budget - (time.monotonic() - start_monotonic)
            if reliable_stale_exists:
                return cached.get("rows") or []

            if attempt >= attempts - 1 or remaining_after <= 0 or not is_transient_nba_error(exc):
                break

            time.sleep(min(0.6 * (2 ** attempt), max(0.0, remaining_after)))

    if reliable_stale_exists and cached:
        return cached.get("rows") or []

    if cached:
        return cached.get("rows") or []

    return []


def summarize_position_environment(rows: list[dict[str, Any]], stat: str) -> dict[str, Any] | None:
    if not rows:
        return None

    cache_key = (str(stat), id(rows), str(len(rows)))
    cached = POSITION_SUMMARY_CACHE.get(cache_key)
    now_ts = time.time()

    if cached and now_ts - float(cached.get("timestamp") or 0.0) < POSITION_TTL_SECONDS:
        summary = cached.get("summary")
        return dict(summary) if isinstance(summary, dict) else None

    total_gp = 0.0
    total_value = 0.0
    players_count = 0

    for row in rows:
        gp = float(row.get("GP") or 0)
        if gp <= 0:
            continue
        players_count += 1
        total_gp += gp
        total_value += compute_stat_value(row, stat)

    if total_gp <= 0:
        POSITION_SUMMARY_CACHE[cache_key] = {"timestamp": now_ts, "summary": None}
        return None

    summary = {
        "players_count": players_count,
        "sample_gp": round(total_gp, 1),
        "per_player_game": round(total_value / total_gp, 2),
        "total_value": round(total_value, 1),
    }
    POSITION_SUMMARY_CACHE[cache_key] = {"timestamp": now_ts, "summary": dict(summary)}
    return summary



def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def decimal_implied_probability(odds: float | None) -> float | None:
    if odds in (None, 0):
        return None
    try:
        odds_value = float(odds)
    except (TypeError, ValueError):
        return None
    if odds_value <= 1:
        return None
    return 1 / odds_value


def resolve_team_from_text(team_text: str | None) -> dict[str, Any] | None:
    if not team_text:
        return None
    key = normalize_name(team_text)
    return TEAM_ALIAS_LOOKUP.get(key)


def resolve_opponent_team_from_matchup(matchup: str, player_team_id: int | None) -> dict[str, Any] | None:
    """Parse a MATCHUP string (e.g. 'LAL vs. GSW' or 'LAL @ GSW') and return the
    opponent team dict, excluding the player's own team identified by player_team_id."""
    info = parse_matchup_descriptor(matchup)
    team_abbr = info.get('team_abbreviation', '')
    opp_abbr = info.get('opponent_abbreviation', '')

    team_candidate = TEAM_ALIAS_LOOKUP.get(normalize_name(team_abbr)) if team_abbr else None
    opp_candidate = TEAM_ALIAS_LOOKUP.get(normalize_name(opp_abbr)) if opp_abbr else None

    # If we know the player's team, return whichever candidate is NOT their team
    if player_team_id is not None:
        if opp_candidate and int(opp_candidate.get('id') or 0) != player_team_id:
            return opp_candidate
        if team_candidate and int(team_candidate.get('id') or 0) != player_team_id:
            return team_candidate
        return opp_candidate  # fallback

    # No team context — return the opponent abbreviation side
    return opp_candidate or team_candidate


def find_player_by_name(player_name: str, team_id: int | None = None) -> dict[str, Any] | None:
    needles = build_player_name_variants(player_name)
    if not needles:
        return None

    roster_ids: set[int] | None = None
    if team_id is not None:
        try:
            roster = fetch_team_roster(team_id=team_id, season=current_nba_season())
            roster_ids = {int(row.get("PLAYER_ID")) for row in roster if row.get("PLAYER_ID") not in (None, "")}
        except HTTPException:
            roster_ids = None

    def player_sort_key(item: dict[str, Any]) -> tuple[bool, str]:
        return (not item.get("is_active", False), str(item.get("full_name", "")))

    exact_matches: list[dict[str, Any]] = []
    partial_matches: list[dict[str, Any]] = []

    for player_id, player in PLAYER_LOOKUP.items():
        player_variants = PLAYER_VARIANTS_LOOKUP.get(player_id, set())
        if not player_variants:
            continue
        if needles & player_variants:
            exact_matches.append(player)
            continue
        if any(
            needle in candidate or candidate in needle
            for needle in needles
            for candidate in player_variants
        ):
            partial_matches.append(player)

    if roster_ids is not None:
        for collection in (exact_matches, partial_matches):
            roster_matches = [player for player in collection if int(player["id"]) in roster_ids]
            if roster_matches:
                return sorted(roster_matches, key=player_sort_key)[0]

    if exact_matches:
        return sorted(exact_matches, key=player_sort_key)[0]
    if partial_matches:
        return sorted(partial_matches, key=player_sort_key)[0]
    return None


def estimate_model_probabilities(
    hit_rate_pct: float,
    average: float,
    line: float,
    matchup_delta_pct: float | None = None,
    *,
    stat: str | None = None,
    opportunity: dict[str, Any] | None = None,
    team_context: dict[str, Any] | None = None,
    environment: dict[str, Any] | None = None,
    variance: dict[str, Any] | None = None,
) -> tuple[float, float]:
    opportunity = opportunity or {}
    team_context = team_context or {}
    environment = environment or {}
    variance = variance or {}

    base = hit_rate_pct / 100.0
    edge_term = 0.0
    scale = max(1.0, max(line, 1.0) * 0.18)
    edge_term += clamp((average - line) / scale, -0.18, 0.18)

    # --- Variance-aware adjustment ---
    # If we have distribution data, prefer median over mean for edge, and penalize high variance
    variance_adjustment = 0.0
    if variance:
        std_dev = float(variance.get("std_dev") or 0.0)
        median = float(variance.get("median") or average)
        consistency_score = float(variance.get("consistency_score") or 50.0)
        p25 = float(variance.get("p25") or 0.0)
        p75 = float(variance.get("p75") or 0.0)

        # Median-based edge: more robust than mean for skewed distributions
        median_edge_term = clamp((median - line) / scale, -0.18, 0.18)
        # Blend mean-edge and median-edge (median weighted higher for noisy stats)
        noisy_stats = {"STL", "BLK", "3PM"}
        median_weight = 0.65 if stat in noisy_stats else 0.45
        edge_term = edge_term * (1 - median_weight) + median_edge_term * median_weight

        # Consistency penalty: highly inconsistent players get shrinkage toward 0.5
        consistency_factor = consistency_score / 100.0  # 0.0 - 1.0
        variance_adjustment = clamp((consistency_factor - 0.5) * 0.10, -0.06, 0.06)

        # Percentile-based confidence: if p25 > line → floor is above line (strong over signal)
        if p25 > line:
            variance_adjustment += 0.05
        elif p75 < line:
            variance_adjustment -= 0.05

        # High std_dev relative to line → dampen edge (more uncertainty)
        cv = float(variance.get("cv") or 0.0)
        if cv > 0.5:
            # Very volatile player — shrink edge contribution
            edge_term *= max(0.4, 1.0 - (cv - 0.5))

    matchup_term = 0.0
    if matchup_delta_pct is not None:
        # Reduce matchup weight for volatile/noisy stats
        noisy_stats = {"STL", "BLK"}
        matchup_weight = 0.08 if stat in noisy_stats else 0.2
        matchup_term = clamp(matchup_delta_pct / 100.0 * matchup_weight, -0.12, 0.12)

    role_term = 0.0
    if opportunity.get('minutes_trend') == 'up':
        role_term += 0.03
    elif opportunity.get('minutes_trend') == 'down':
        role_term -= 0.04

    scoring_stats = {'PTS', '3PM', 'PRA', 'PR', 'PA'}
    if opportunity.get('volume_trend') == 'up':
        role_term += 0.04 if stat in scoring_stats else 0.02
    elif opportunity.get('volume_trend') == 'down':
        role_term -= 0.04 if stat in scoring_stats else 0.02

    if int(team_context.get('impact_count') or 0) > 0:
        role_term += min(0.03, int(team_context.get('impact_count') or 0) * 0.01)

    environment_term = 0.0
    if environment.get('is_back_to_back'):
        environment_term -= 0.03
    elif isinstance(environment.get('rest_days'), int) and environment.get('rest_days') >= 2:
        environment_term += 0.02

    model_over = clamp(base + edge_term + matchup_term + role_term + environment_term + variance_adjustment, 0.02, 0.98)
    return round(model_over, 4), round(1 - model_over, 4)


@timed_call("build_prop_analysis_payload")
def build_prop_analysis_payload(
    player_id: int,
    stat: str,
    line: float,
    last_n: int,
    season: str,
    season_type: str,
    team_id: int | None = None,
    player_position: str | None = None,
    location: str = 'all',
    result: str = 'all',
    margin_min: float | None = None,
    margin_max: float | None = None,
    min_minutes: float | None = None,
    max_minutes: float | None = None,
    min_fga: float | None = None,
    max_fga: float | None = None,
    h2h_only: bool = False,
    opponent_rank_range: str | None = None,
    without_player_id: int | None = None,
    without_player_name: str | None = None,
    without_player_ids: list[int] | None = None,
    without_player_names: list[str] | None = None,
    debug: bool = False,
    override_opponent_id: int | None = None,
) -> dict[str, Any]:
    normalized_without_player_ids = normalize_without_player_ids(without_player_ids)
    if not normalized_without_player_ids and without_player_id:
        normalized_without_player_ids = [int(without_player_id)]
    normalized_without_player_names = resolve_without_player_names(normalized_without_player_ids)
    if not normalized_without_player_names and without_player_name:
        fallback_name = str(without_player_name).strip()
        if fallback_name:
            normalized_without_player_names = [fallback_name]
    # NOTE: last_n is intentionally excluded from the cache key.
    # All window sizes (5 / 10 / 15) share the same filtered_pool cached here.
    # last_n slicing happens after cache retrieval so they never diverge.
    cache_key = (ANALYSIS_CACHE_SCHEMA_VERSION, player_id, stat, float(line), season, season_type, team_id, player_position or '', location, result, margin_min, margin_max, min_minutes, max_minutes, min_fga, max_fga, h2h_only, str(opponent_rank_range or ''), tuple(normalized_without_player_ids), tuple(normalized_without_player_names), override_opponent_id)
    cached = ANALYSIS_CACHE.get(cache_key)
    now_ts = time.time()
    analysis_cache_age = now_ts - float(cached.get('timestamp') or 0.0) if cached else None
    if cached and analysis_cache_age is not None and analysis_cache_age < ANALYSIS_CACHE_TTL_SECONDS:
        payload = copy.deepcopy(cached['payload'])
        # Re-slice and recompute ALL last_n-dependent fields from the cached filtered_pool
        cached_filtered_pool = cached.get('filtered_pool', [])
        if cached_filtered_pool:
            sliced_rows = cached_filtered_pool[:last_n]
            sliced_stat_summary = build_stat_summary_block(sliced_rows, stat, line)
            payload['games'] = list(reversed(sliced_stat_summary['games']))
            payload['average'] = sliced_stat_summary['average']
            payload['hit_count'] = sliced_stat_summary['hit_count']
            payload['hit_rate'] = sliced_stat_summary['hit_rate']
            payload['games_count'] = len(sliced_stat_summary['values'])
            payload['last_n'] = last_n
            payload['variance'] = sliced_stat_summary.get('variance') or {}
            sliced_opportunity = build_opportunity_context(cached_filtered_pool, last_n)
            payload['opportunity'] = sliced_opportunity
            # --- Recompute model outputs that depend on last_n sample ---
            _cached_vs_position = (payload.get('matchup') or {}).get('vs_position')
            _cached_matchup_delta = (_cached_vs_position or {}).get('delta_pct') if isinstance(_cached_vs_position, dict) else None
            _cached_team_context = payload.get('team_context') or {}
            _cached_environment = payload.get('environment') or {}
            _cached_availability = payload.get('availability') or {}
            _cached_player = payload.get('player') or {}
            _sliced_model_over, _sliced_model_under = estimate_model_probabilities(
                hit_rate_pct=float(sliced_stat_summary['hit_rate']),
                average=float(sliced_stat_summary['average']),
                line=float(line),
                matchup_delta_pct=float(_cached_matchup_delta) if _cached_matchup_delta is not None else None,
                stat=str(stat),
                opportunity=sliced_opportunity,
                team_context=_cached_team_context,
                environment=_cached_environment,
                variance=sliced_stat_summary.get('variance') or {},
            )
            _sliced_recommended_side = 'OVER' if _sliced_model_over >= _sliced_model_under else 'UNDER'
            _sliced_side_prob = _sliced_model_over if _sliced_recommended_side == 'OVER' else _sliced_model_under
            _sliced_edge = round((_sliced_side_prob - 0.50) * 100, 1)
            _sliced_ev = _sliced_side_prob - 0.50
            _sliced_confidence = build_confidence_engine(
                side=_sliced_recommended_side,
                hit_rate=float(sliced_stat_summary['hit_rate']),
                games_count=int(len(sliced_stat_summary['values'])),
                edge=_sliced_edge,
                ev=_sliced_ev,
                matchup_delta_pct=float(_cached_matchup_delta) if _cached_matchup_delta is not None else None,
                availability=_cached_availability,
                opportunity=sliced_opportunity,
                team_context=_cached_team_context,
                environment=_cached_environment,
                stat=str(stat),
                player_position=str(_cached_player.get('position') or ''),
                line=float(line),
                average=float(sliced_stat_summary['average']),
            )
            payload['recommended_side'] = _sliced_recommended_side
            payload['confidence'] = _sliced_confidence
            # Recompute traffic light
            _sliced_traffic_tone = 'yellow'
            _sliced_traffic_label = 'Caution'
            if _cached_availability.get('is_unavailable'):
                _sliced_traffic_tone = 'red'
                _sliced_traffic_label = 'Avoid'
            elif _sliced_confidence['score'] >= 80:
                _sliced_traffic_tone = 'green'
                _sliced_traffic_label = f"Strong {_sliced_recommended_side}"
            elif _sliced_confidence['score'] >= 68:
                _sliced_traffic_tone = 'green'
                _sliced_traffic_label = f"Lean {_sliced_recommended_side}"
            elif _sliced_confidence['score'] <= 44:
                _sliced_traffic_tone = 'red'
                _sliced_traffic_label = 'Pass'
            payload['traffic_light'] = {
                'label': _sliced_traffic_label,
                'tone': _sliced_traffic_tone,
                'summary': _sliced_confidence.get('summary') or (payload.get('interpretation') or {}).get('market_takeaway') or '',
            }
            # Small sample warning
            sliced_count = len(sliced_stat_summary['values'])
            if sliced_count < 5:
                payload['sample_warning'] = f"Only {sliced_count} games in filtered sample — treat recommendation with caution."
            else:
                payload.pop('sample_warning', None)
        freshness = {"analysis_cached_seconds_ago": round(analysis_cache_age, 2)}
        if HYBRID_ANALYZER_ENABLED:
            player_cache_rows, _ = get_player_game_log_hybrid(player_id=player_id, season=season, season_type=season_type)
            cached_player = payload.get("player") or {}
            cached_team_name = cached_player.get("team_name") or TEAM_LOOKUP.get(int(cached_player.get("team_id") or 0), {}).get("full_name")
            payload, freshness_overlay = overlay_sensitive_analysis_sections(
                payload=payload,
                player_name=str(cached_player.get("full_name") or player.get("full_name") if 'player' in locals() else ""),
                team_name=str(cached_team_name or "") or None,
                analysis_source_rows=player_cache_rows,
                resolved_team_id=int(cached_player.get("team_id") or 0) or None,
                player_id=player_id,
                season=season,
                season_type=season_type,
                stat=stat,
                line=line,
            )
            freshness.update(freshness_overlay)
            if analysis_cache_age >= ANALYSIS_CACHE_TTL_SECONDS * 0.5:
                enqueue_hybrid_refresh("game_log", (player_id, season, season_type))
                if payload.get("player", {}).get("team_id"):
                    enqueue_hybrid_refresh("team_next_game", (int(payload["player"]["team_id"]), player_id, season, season_type))
        if debug and DEBUG_METADATA_ENABLED:
            payload["debug"] = build_debug_metadata(
                cache_status={"analysis_cache": "hit"},
                freshness=freshness,
                timings_enabled=NBA_TIMING_ENABLED,
            )
        return payload

    player = PLAYER_LOOKUP.get(int(player_id))
    if not player:
        raise HTTPException(status_code=404, detail="Player not found.")

    season_rows, game_log_meta = get_player_game_log_hybrid(player_id=player_id, season=season, season_type=season_type) if HYBRID_ANALYZER_ENABLED else (fetch_player_game_log(player_id=player_id, season=season, season_type=season_type), {"source": "live", "seconds_ago": None, "refresh_queued": False})

    profile_row = None
    player_info_meta = {"source": "unused", "seconds_ago": None, "refresh_queued": False}
    resolved_team_id = team_id
    resolved_position = player_position

    if resolved_team_id is None or not resolved_position:
        if HYBRID_ANALYZER_ENABLED:
            profile_row, player_info_meta = get_player_info_hybrid(player_id)
        else:
            try:
                profile_row = fetch_common_player_info(player_id)
            except HTTPException:
                profile_row = None
        if profile_row:
            if resolved_team_id is None:
                raw_team_id = profile_row.get("TEAM_ID")
                resolved_team_id = int(raw_team_id) if raw_team_id not in (None, "") else None
            if not resolved_position:
                resolved_position = str(profile_row.get("POSITION", "")).strip()

    position_code, position_label = resolve_primary_position(resolved_position)

    # If caller supplied an override opponent, build a synthetic next_game
    if override_opponent_id and override_opponent_id != 0:
        override_opp = TEAM_LOOKUP.get(int(override_opponent_id))
        if override_opp:
            player_team = TEAM_LOOKUP.get(resolved_team_id, {}) if resolved_team_id else {}
            override_abbr = str(override_opp.get("abbreviation") or "").strip()
            next_game = {
                "game_date": "",
                "game_time": "",
                "is_home": None,
                "matchup_label": f"vs {override_abbr}",
                "opponent_team_id": int(override_opponent_id),
                "opponent_name": str(override_opp.get("full_name") or "").strip(),
                "opponent_abbreviation": override_abbr,
                "player_team_abbreviation": str(player_team.get("abbreviation") or "").strip(),
                "is_override": True,
            }
        else:
            next_game, next_game_meta = get_team_next_game_hybrid(
                team_id=resolved_team_id,
                primary_player_id=player_id,
                season=season,
                season_type=season_type,
            ) if HYBRID_ANALYZER_ENABLED else (resolve_team_next_game(
                team_id=resolved_team_id,
                primary_player_id=player_id,
                season=season,
                season_type=season_type,
            ), {"source": "live", "seconds_ago": None, "refresh_queued": False})
    else:
        next_game, next_game_meta = get_team_next_game_hybrid(
            team_id=resolved_team_id,
            primary_player_id=player_id,
            season=season,
            season_type=season_type,
        ) if HYBRID_ANALYZER_ENABLED else (resolve_team_next_game(
            team_id=resolved_team_id,
            primary_player_id=player_id,
            season=season,
            season_type=season_type,
        ), {"source": "live", "seconds_ago": None, "refresh_queued": False})

    needs_margin_context = margin_min is not None or margin_max is not None
    if needs_margin_context:
        enriched_rows = enrich_game_logs_with_context(
            season_rows,
            resolved_team_id,
            season,
            season_type,
            player_id,
        )
    else:
        enriched_rows = enrich_game_logs_light(season_rows)

    opponent_rank_min, opponent_rank_max, normalized_opponent_rank_range = normalize_opponent_rank_range(opponent_rank_range)
    if opponent_rank_min is not None or opponent_rank_max is not None:
        team_rank_map = build_team_rank_map(season=season, season_type=season_type)
        for row in enriched_rows:
            matchup = str(row.get('MATCHUP') or '')
            opp_team = resolve_opponent_team_from_matchup(matchup, resolved_team_id)
            opp_team_id = int(opp_team['id']) if opp_team else None
            row['_opponent_rank'] = team_rank_map.get(opp_team_id) if opp_team_id else None
    else:
        for row in enriched_rows:
            row.setdefault('_opponent_rank', None)

    without_player_game_ids = build_without_player_union_game_ids(normalized_without_player_ids, season=season, season_type=season_type)
    if not normalized_without_player_names and without_player_name:
        fallback_name = str(without_player_name).strip()
        if fallback_name:
            normalized_without_player_names = [fallback_name]
    without_player_name = ', '.join(normalized_without_player_names)

    filtered_pool = apply_game_log_filters(
        enriched_rows,
        location=location,
        result=result,
        margin_min=margin_min,
        margin_max=margin_max,
        min_minutes=min_minutes,
        max_minutes=max_minutes,
        min_fga=min_fga,
        max_fga=max_fga,
        h2h_only=h2h_only,
        opponent_abbreviation=(next_game or {}).get('opponent_abbreviation'),
        opponent_rank_min=opponent_rank_min,
        opponent_rank_max=opponent_rank_max,
        without_player_game_ids=without_player_game_ids,
    )
    rows = filtered_pool[:last_n]

    cache_status = {
        "analysis_cache": "miss",
        "game_log_cache": "hit" if GAME_LOG_CACHE.get((player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) else "miss",
        "player_info_cache": "hit" if PLAYER_INFO_CACHE.get(player_id) else "miss",
        "next_game_cache": "hit" if TEAM_NEXT_GAME_CACHE.get((resolved_team_id, season, season_type)) else "miss",
        "position_matchup_cache": "unknown",
        "filtered_pool_cache": "unknown",
        "stat_summary_cache": "unknown",
    }

    filtered_pool_cache_key = _build_filtered_pool_cache_key(
        enriched_rows,
        location=location,
        result=result,
        margin_min=margin_min,
        margin_max=margin_max,
        min_minutes=min_minutes,
        max_minutes=max_minutes,
        min_fga=min_fga,
        max_fga=max_fga,
        h2h_only=h2h_only,
        opponent_abbreviation=(next_game or {}).get('opponent_abbreviation'),
        opponent_rank_min=opponent_rank_min,
        opponent_rank_max=opponent_rank_max,
        without_player_game_ids=without_player_game_ids,
    )
    if FILTERED_POOL_CACHE.get(filtered_pool_cache_key):
        cache_status["filtered_pool_cache"] = "hit"
    else:
        cache_status["filtered_pool_cache"] = "miss"

    stat_summary_cache_key = (tuple(str(row.get("Game_ID") or row.get("GAME_ID") or row.get("GAME_DATE") or "") for row in rows), str(stat), float(line))
    cache_status["stat_summary_cache"] = "hit" if STAT_SUMMARY_CACHE.get(stat_summary_cache_key) else "miss"
    stat_summary = build_stat_summary_block(rows, stat, line)
    games = stat_summary["games"]
    values = stat_summary["values"]
    hit_count = stat_summary["hit_count"]
    average = stat_summary["average"]
    hit_rate = stat_summary["hit_rate"]

    team_name = TEAM_LOOKUP.get(resolved_team_id, {}).get("full_name") if resolved_team_id else None

    position_cache_key = None
    if next_game and position_code:
        position_cache_key = (int(next_game["opponent_team_id"]), str(position_code), str(stat), str(season), str(season_type))
        cache_status["position_matchup_cache"] = "hit" if POSITION_MATCHUP_CACHE.get(position_cache_key) else "miss"

    def _compute_availability() -> dict[str, Any]:
        return build_availability_payload(player_name=player["full_name"], team_name=team_name)

    def _compute_vs_position() -> dict[str, Any] | None:
        if not next_game or not position_code:
            return None
        return build_position_matchup(
            opponent_team_id=next_game["opponent_team_id"],
            position_code=position_code,
            stat=stat,
            season=season,
            season_type=season_type,
        )

    availability = None
    vs_position = None
    # Always run both computations in parallel when parallel mode is on —
    # previously gated on position_cache_key being truthy, which silently
    # skipped vs_position whenever position_code was not yet resolved.
    if ANALYSIS_PARALLEL_ENABLED:
        with ThreadPoolExecutor(max_workers=min(ANALYSIS_PARALLEL_MAX_WORKERS, 2)) as executor:
            future_availability = executor.submit(_compute_availability)
            future_vs_position = executor.submit(_compute_vs_position)
            availability = future_availability.result()
            vs_position = future_vs_position.result()
    else:
        availability = _compute_availability()
        vs_position = _compute_vs_position()

    h2h_payload = build_h2h_payload_from_rows(filtered_pool or enriched_rows, next_game, stat, line)

    analysis_source_rows = filtered_pool or enriched_rows or season_rows

    def _compute_opportunity() -> dict[str, Any]:
        return build_opportunity_context(analysis_source_rows, last_n)

    def _compute_team_context() -> dict[str, Any]:
        return build_team_opportunity_context(team_name=team_name, player_name=player["full_name"], stat=stat, player_position=resolved_position, team_id=resolved_team_id, season=season)

    def _compute_environment() -> dict[str, Any]:
        return build_game_environment_context(analysis_source_rows, next_game, team_id=resolved_team_id, season=season, season_type=season_type)

    if ANALYSIS_PARALLEL_ENABLED:
        with ThreadPoolExecutor(max_workers=min(ANALYSIS_PARALLEL_MAX_WORKERS, 3)) as executor:
            future_opportunity = executor.submit(_compute_opportunity)
            future_team_context = executor.submit(_compute_team_context)
            future_environment = executor.submit(_compute_environment)
            opportunity = future_opportunity.result()
            team_context = future_team_context.result()
            environment = future_environment.result()
    else:
        opportunity = _compute_opportunity()
        team_context = _compute_team_context()
        environment = _compute_environment()

    matchup_delta_pct = (vs_position or {}).get('delta_pct') if isinstance(vs_position, dict) else None
    variance_data = stat_summary.get("variance") or {}
    model_over, model_under = estimate_model_probabilities(
        hit_rate_pct=float(hit_rate),
        average=float(average),
        line=float(line),
        matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
        stat=str(stat),
        opportunity=opportunity,
        team_context=team_context,
        environment=environment,
        variance=variance_data,
    )
    recommended_side = 'OVER' if model_over >= model_under else 'UNDER'
    # True edge: model probability minus fair implied probability (50% when no market odds)
    # If market odds are available via vs_position or other context, use them; otherwise use 0.5 as fair line
    side_model_prob = model_over if recommended_side == 'OVER' else model_under
    fair_implied = 0.50  # default fair market (no-vig line assumed at 50%)
    chosen_edge = round((side_model_prob - fair_implied) * 100, 1)  # expressed as percentage edge
    chosen_ev = side_model_prob - fair_implied  # expected value vs fair line
    confidence_engine = build_confidence_engine(
        side=recommended_side,
        hit_rate=float(hit_rate),
        games_count=int(len(values)),
        edge=chosen_edge,
        ev=chosen_ev,
        matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
        availability=availability,
        opportunity=opportunity,
        team_context=team_context,
        environment=environment,
        stat=stat,
        player_position=resolved_position,
        line=line,
        average=average,
    )
    traffic_tone = 'yellow'
    traffic_label = 'Caution'
    if availability.get('is_unavailable'):
        traffic_tone = 'red'
        traffic_label = 'Avoid'
    elif confidence_engine['score'] >= 80:
        traffic_tone = 'green'
        traffic_label = f"Strong {recommended_side}"
    elif confidence_engine['score'] >= 68:
        traffic_tone = 'green'
        traffic_label = f"Lean {recommended_side}"
    elif confidence_engine['score'] <= 44:
        traffic_tone = 'red'
        traffic_label = 'Pass'

    interpretation = build_analyzer_interpretation(
        stat=stat,
        line=line,
        hit_rate=hit_rate,
        average=average,
        availability=availability,
        matchup={"next_game": next_game, "vs_position": vs_position},
        opportunity=opportunity,
        team_context=team_context,
        h2h=h2h_payload,
        environment=environment,
    )

    filter_summary = build_filter_summary(
        location=location,
        result=result,
        margin_min=margin_min,
        margin_max=margin_max,
        min_minutes=min_minutes,
        max_minutes=max_minutes,
        min_fga=min_fga,
        max_fga=max_fga,
        h2h_only=h2h_only,
        opponent_rank_range=normalized_opponent_rank_range,
        without_player_name=without_player_name,
        without_player_names=normalized_without_player_names,
        debug=debug,
    )

    if not rows:
        interpretation = {
            'headline': 'No filtered sample',
            'tone': 'warning',
            'summary': 'The current filters removed every game from the sample. Relax the split to rebuild the trend view.',
            'bullets': [
                f"Active filters: {filter_summary['label']}",
                f"Season pool: {len(season_rows)} games",
            ],
            'market_takeaway': 'No lean until the filter sample is rebuilt.',
        }

    payload = {
        "player": {
            "id": player["id"],
            "full_name": player["full_name"],
            "is_active": player.get("is_active", False),
            "team_id": resolved_team_id,
            "position": resolved_position or "",
            "position_group": position_code or "",
            "position_group_label": position_label or "",
        },
        "season": season,
        "season_type": season_type,
        "stat": stat,
        "line": line,
        "last_n": last_n,
        "average": average,
        "hit_count": hit_count,
        "games_count": len(values),
        "hit_rate": hit_rate,
        "games": list(reversed(games)),
        "availability": availability,
        "matchup": {
            "next_game": next_game,
            "vs_position": vs_position,
        },
        "h2h": h2h_payload,
        "opportunity": opportunity,
        "team_context": team_context,
        "environment": environment,
        "confidence": confidence_engine,
        "recommended_side": recommended_side,
        "traffic_light": {
            "label": traffic_label,
            "tone": traffic_tone,
            "summary": confidence_engine.get("summary") or interpretation.get("market_takeaway") or interpretation.get("summary"),
        },
        "interpretation": interpretation,
        "active_filters": filter_summary,
        "filter_options": {
            "opponent_rank_range": normalized_opponent_rank_range,
            "without_player_id": int(normalized_without_player_ids[0]) if normalized_without_player_ids else None,
            "without_player_ids": normalized_without_player_ids,
            "without_player_name": without_player_name,
            "without_player_names": normalized_without_player_names,
        },
        "filtered_pool_count": len(filtered_pool),
        "season_pool_count": len(season_rows),
        "variance": variance_data,
        "sample_warning": f"Only {len(values)} games in filtered sample — treat recommendation with caution." if len(values) < 5 else None,
    }
    freshness = {
        "game_log_seconds_ago": round(time.time() - float((GAME_LOG_CACHE.get((player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) or {}).get("timestamp") or 0.0), 2) if GAME_LOG_CACHE.get((player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) else None,
        "player_info_seconds_ago": round(time.time() - float((PLAYER_INFO_CACHE.get(player_id) or {}).get("timestamp") or 0.0), 2) if PLAYER_INFO_CACHE.get(player_id) else None,
        "next_game_seconds_ago": round(time.time() - float((TEAM_NEXT_GAME_CACHE.get((resolved_team_id, season, season_type)) or {}).get("timestamp") or 0.0), 2) if TEAM_NEXT_GAME_CACHE.get((resolved_team_id, season, season_type)) else None,
        "game_log_source": game_log_meta.get("source") if 'game_log_meta' in locals() else None,
        "game_log_refresh_queued": game_log_meta.get("refresh_queued") if 'game_log_meta' in locals() else False,
        "player_info_source": player_info_meta.get("source") if 'player_info_meta' in locals() else None,
        "player_info_refresh_queued": player_info_meta.get("refresh_queued") if 'player_info_meta' in locals() else False,
        "next_game_source": next_game_meta.get("source") if 'next_game_meta' in locals() else None,
        "next_game_refresh_queued": next_game_meta.get("refresh_queued") if 'next_game_meta' in locals() else False,
        "injury_report_seconds_ago": round(time.time() - float(INJURY_REPORT_CACHE.get("timestamp") or 0.0), 2) if INJURY_REPORT_CACHE.get("timestamp") else None,
    }
    payload["freshness"] = dict(freshness)
    ANALYSIS_CACHE[cache_key] = {"timestamp": time.time(), "payload": copy.deepcopy(payload), "filtered_pool": [dict(r) for r in filtered_pool]}
    if debug and DEBUG_METADATA_ENABLED:
        payload["debug"] = build_debug_metadata(cache_status=cache_status, freshness=freshness, timings_enabled=NBA_TIMING_ENABLED)
    return payload


def compute_recent_hit_streak(hit_flags: list[bool]) -> int:
    streak = 0
    for hit in hit_flags:
        if hit:
            streak += 1
        else:
            break
    return streak

@timed_call("build_position_matchup")
def build_position_matchup(
    opponent_team_id: int,
    position_code: str,
    stat: str,
    season: str,
    season_type: str,
) -> dict[str, Any] | None:
    cache_key = (int(opponent_team_id), str(position_code), str(stat), str(season), str(season_type))
    cached = POSITION_MATCHUP_CACHE.get(cache_key)
    now_ts = time.time()

    if cached and now_ts - float(cached.get("timestamp") or 0.0) < POSITION_TTL_SECONDS:
        payload = cached.get("payload")
        return dict(payload) if isinstance(payload, dict) else None

    opponent_rows = fetch_position_dash(
        season=season,
        season_type=season_type,
        position_code=position_code,
        opponent_team_id=opponent_team_id,
    )
    league_rows = fetch_position_dash(
        season=season,
        season_type=season_type,
        position_code=position_code,
        opponent_team_id=0,
    )

    opponent_summary = summarize_position_environment(opponent_rows, stat)
    league_summary = summarize_position_environment(league_rows, stat)
    if not opponent_summary or not league_summary:
        POSITION_MATCHUP_CACHE[cache_key] = {"timestamp": now_ts, "payload": None}
        return None

    opponent_value = opponent_summary["per_player_game"]
    league_average = league_summary["per_player_game"]
    delta = round(opponent_value - league_average, 2)
    delta_pct = round((delta / league_average) * 100, 1) if league_average else 0.0

    # Build defense rank by querying each NBA team individually.
    # league_rows (opponent_team_id=0) returns season totals without per-opponent
    # breakdown, so OPP_TEAM_ID is absent. We use cached per-team queries instead,
    # falling back gracefully if they are unavailable to avoid extra API calls.
    defense_rank = None
    rank_label = None
    team_summaries: list[dict[str, Any]] = []

    for team in TEAM_POOL:
        tid = int(team.get("id") or 0)
        if not tid:
            continue
        per_team_cache_key = (season, season_type, position_code, tid)
        per_team_cached = POSITION_DASH_CACHE.get(per_team_cache_key)
        if not per_team_cached or not per_team_cached.get("rows"):
            # Only use already-cached per-team data to avoid 30 extra API calls.
            # The opponent team itself will always be cached from the query above.
            if tid != int(opponent_team_id):
                continue
            per_team_rows = opponent_rows
        else:
            per_team_rows = per_team_cached["rows"]
        summary = summarize_position_environment(per_team_rows, stat)
        if not summary:
            continue
        team_summaries.append({
            "team_id": tid,
            "per_player_game": summary["per_player_game"],
        })

    if team_summaries:
        ranked = sorted(team_summaries, key=lambda item: float(item.get("per_player_game") or 0.0), reverse=True)
        for index, item in enumerate(ranked, start=1):
            if int(item.get("team_id") or 0) == int(opponent_team_id):
                defense_rank = index
                break
        if defense_rank:
            rank_label = f"#{defense_rank} vs {POSITION_LABELS.get(position_code, position_code)}"

    if delta_pct >= 12:
        lean = "Very favorable"
        lean_tone = "good"
    elif delta_pct >= 5:
        lean = "Favorable"
        lean_tone = "good"
    elif delta_pct <= -12:
        lean = "Very tough"
        lean_tone = "bad"
    elif delta_pct <= -5:
        lean = "Tough"
        lean_tone = "bad"
    else:
        lean = "Neutral"
        lean_tone = "neutral"

    payload = {
        "position_code": position_code,
        "position_label": POSITION_LABELS.get(position_code, position_code),
        "stat": stat,
        "opponent_value": opponent_value,
        "league_average": league_average,
        "delta": delta,
        "delta_pct": delta_pct,
        "lean": lean,
        "lean_tone": lean_tone,
        "sample_gp": opponent_summary["sample_gp"],
        "players_count": opponent_summary["players_count"],
        "def_rank": defense_rank,
        "rank_label": rank_label,
        "team_count": len(team_summaries),
    }
    POSITION_MATCHUP_CACHE[cache_key] = {"timestamp": now_ts, "payload": dict(payload)}
    return payload


def build_next_game_payload(
    next_game_row: dict[str, Any] | None,
    player_team_id: int | None,
) -> dict[str, Any] | None:
    if not next_game_row or not player_team_id:
        return None

    home_team_id = int(next_game_row.get("HOME_TEAM_ID") or 0)
    visitor_team_id = int(next_game_row.get("VISITOR_TEAM_ID") or 0)

    if player_team_id == home_team_id:
        is_home = True
        opponent_team_id = visitor_team_id
        opponent_name = next_game_row.get("VISITOR_TEAM_NAME")
        opponent_abbreviation = next_game_row.get("VISITOR_TEAM_ABBREVIATION")
        player_team_abbreviation = next_game_row.get("HOME_TEAM_ABBREVIATION")
        matchup_label = f"vs {opponent_abbreviation}"
    elif player_team_id == visitor_team_id:
        is_home = False
        opponent_team_id = home_team_id
        opponent_name = next_game_row.get("HOME_TEAM_NAME")
        opponent_abbreviation = next_game_row.get("HOME_TEAM_ABBREVIATION")
        player_team_abbreviation = next_game_row.get("VISITOR_TEAM_ABBREVIATION")
        matchup_label = f"@ {opponent_abbreviation}"
    else:
        return None

    return {
        "game_date": str(next_game_row.get("GAME_DATE", "")).strip(),
        "game_time": str(next_game_row.get("GAME_TIME", "")).strip(),
        "is_home": is_home,
        "matchup_label": matchup_label,
        "opponent_team_id": opponent_team_id,
        "opponent_name": str(opponent_name or "").strip(),
        "opponent_abbreviation": str(opponent_abbreviation or "").strip(),
        "player_team_abbreviation": str(player_team_abbreviation or "").strip(),
    }


@timed_call("resolve_team_next_game")
def resolve_team_next_game(
    team_id: int | None,
    primary_player_id: int,
    season: str,
    season_type: str,
) -> dict[str, Any] | None:
    if not team_id:
        return None

    cache_key = (team_id, season, season_type)
    cached = TEAM_NEXT_GAME_CACHE.get(cache_key)
    now_ts = time.time()
    cached_ts = float((cached or {}).get("timestamp") or 0.0)

    if cached and now_ts - cached_ts < NEXT_GAME_TTL_SECONDS:
        return cached["row"]

    reliable_stale_exists = _next_game_cache_is_reliable_stale(cached, cached_ts)
    failure_meta = NEXT_GAME_FAILURE_META.get(cache_key) or {}
    last_failure_ts = float(failure_meta.get("timestamp") or 0.0)
    if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < NEXT_GAME_FAILURE_COOLDOWN_SECONDS:
        return cached.get("row")

    scoreboard_next_game = find_team_next_game_via_scoreboard(team_id=team_id, lookahead_days=10)
    if scoreboard_next_game:
        TEAM_NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": scoreboard_next_game}
        NEXT_GAME_FAILURE_META.pop(cache_key, None)
        save_persistent_caches_async()
        return scoreboard_next_game

    next_game_row = fetch_next_game(primary_player_id, season, season_type)
    next_game = build_next_game_payload(next_game_row, team_id)
    if next_game:
        TEAM_NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": next_game}
        NEXT_GAME_FAILURE_META.pop(cache_key, None)
        save_persistent_caches_async()
        return next_game

    NEXT_GAME_FAILURE_META[cache_key] = {"timestamp": time.time()}
    if reliable_stale_exists and cached:
        return cached.get("row")

    TEAM_NEXT_GAME_CACHE[cache_key] = {"timestamp": time.time(), "row": None}
    save_persistent_caches_async()
    return None




@app.on_event("startup")
def _startup_warm_cache() -> None:
    start_hybrid_refresh_workers()
    # Run cache warming in a background thread so uvicorn can start serving
    # immediately. This ensures Render's health check at /health passes right
    # away instead of timing out while 30 roster fetches + injury PDF download
    # block the startup event handler.
    threading.Thread(target=warm_cache_on_startup, name="startup-warm-cache", daemon=True).start()

@app.api_route("/", methods=["GET", "HEAD"])
def root() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.api_route("/health", methods=["GET", "HEAD"])
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/api/teams")
def get_teams() -> dict[str, Any]:
    return {
        "results": [
            {
                "id": team["id"],
                "full_name": team["full_name"],
                "abbreviation": team["abbreviation"],
                "nickname": team["nickname"],
                "city": team["city"],
            }
            for team in TEAM_POOL
        ]
    }


@app.get("/api/teams/{team_id}/roster")
def get_team_roster(team_id: int, season: str | None = None) -> dict[str, Any]:
    selected_season = season or current_nba_season()
    team = TEAM_LOOKUP.get(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")

    rows = fetch_team_roster(team_id=team_id, season=selected_season)

    # Fetch injury data for this specific team only — filter by team_name so
    # players with the same normalized name on different teams don't bleed across.
    team_name = str(team.get("full_name") or "")
    report_payload = fetch_latest_injury_report_payload()
    all_injury_rows = report_payload.get("rows") or []
    # Build lookup: player_key -> row, scoped to this team only
    injury_lookup: dict[str, dict[str, Any]] = {}
    for ir in all_injury_rows:
        if str(ir.get("team_name") or "").strip() != team_name:
            continue
        pk = str(ir.get("player_key") or "").strip()
        if pk and pk not in injury_lookup:
            injury_lookup[pk] = ir

    roster = []
    for row in rows:
        player_id = row.get("PLAYER_ID")
        if not player_id:
            continue
        full_name = str(row.get("PLAYER", "")).strip()
        norm_name = normalize_report_person_name(full_name)
        inj = injury_lookup.get(norm_name) or {}
        inj_status = str(inj.get("status") or "")
        roster.append(
            {
                "id": int(player_id),
                "full_name": full_name,
                "jersey": str(row.get("NUM", "")).strip(),
                "position": str(row.get("POSITION", "")).strip(),
                "is_active": True,
                "team_id": team["id"],
                "team_name": team["full_name"],
                "team_abbreviation": team["abbreviation"],
                "injury_status": inj_status,
                "injury_reason": str(inj.get("reason") or ""),
                "is_unavailable": inj_status in UNAVAILABLE_STATUSES,
                "is_risky": inj_status in RISKY_STATUSES,
            }
        )

    return {
        "team": {
            "id": team["id"],
            "full_name": team["full_name"],
            "abbreviation": team["abbreviation"],
        },
        "season": selected_season,
        "results": roster,
    }


@app.get("/api/teams/{team_id}/injury-report")
def get_team_injury_report(team_id: int) -> dict[str, Any]:
    """Return injury report rows for a specific team, from the latest official NBA PDF."""
    team = TEAM_LOOKUP.get(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")

    team_name = str(team.get("full_name") or "")
    summary = build_team_availability_summary(team_name)
    payload = fetch_latest_injury_report_payload()

    # Build full player list with statuses
    rows = [row for row in (payload.get("rows") or []) if row.get("team_name") == team_name]
    players = [
        {
            "name": re.sub(r",(?!\s)", ", ", str(row.get("player_display") or "").strip()),
            "status": str(row.get("status") or "").strip(),
            "reason": str(row.get("reason") or "").strip(),
            "is_unavailable": str(row.get("status") or "") in UNAVAILABLE_STATUSES,
            "is_risky": str(row.get("status") or "") in RISKY_STATUSES,
        }
        for row in rows
    ]
    players.sort(key=lambda p: INJURY_STATUS_ORDER.get(p["status"], 3))

    return {
        "team_id": team_id,
        "team_name": team_name,
        "team_abbreviation": team.get("abbreviation", ""),
        "headline": summary.get("headline", ""),
        "tone": summary.get("tone", "neutral"),
        "report_label": payload.get("report_label", ""),
        "report_url": payload.get("report_url", ""),
        "players": players,
        "ok": payload.get("ok", False),
    }


@app.get("/api/players/search")
def search_players(q: str = Query(..., min_length=1, max_length=50)) -> dict[str, Any]:
    needles = build_player_name_variants(q)
    matches: list[dict[str, Any]] = []

    for player_id, player in PLAYER_LOOKUP.items():
        full_name = str(player.get("full_name", ""))
        variants = PLAYER_VARIANTS_LOOKUP.get(player_id, set())
        if not variants:
            continue
        if any(
            needle in candidate or candidate in needle
            for needle in needles
            for candidate in variants
        ):
            matches.append(
                {
                    "id": player_id,
                    "full_name": full_name,
                    "is_active": player.get("is_active", False),
                }
            )

    matches.sort(key=lambda item: (not item["is_active"], item["full_name"]))
    return {"results": matches[:15]}




@app.get("/api/bet-finder")
def bet_finder(
    team_id: int,
    stat: str = Query(..., pattern="^(PTS|REB|AST|3PM|STL|BLK|PRA|PR|PA|RA)$"),
    line: float = Query(..., ge=0),
    last_n: int = Query(10, ge=3, le=30),
    season: str | None = None,
    season_type: str = Query("Regular Season"),
    min_games: int = Query(5, ge=1, le=30),
    limit: int = Query(8, ge=1, le=20),
) -> dict[str, Any]:
    selected_season = season or current_nba_season()
    stat = stat.upper()

    team = TEAM_LOOKUP.get(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")

    roster_rows = fetch_team_roster(team_id=team_id, season=selected_season)
    results: list[dict[str, Any]] = []

    for row in roster_rows:
        raw_player_id = row.get("PLAYER_ID")
        if raw_player_id in (None, ""):
            continue

        player_id = int(raw_player_id)
        try:
            game_rows = fetch_recent_player_game_log(
                player_id=player_id,
                season=selected_season,
                season_type=season_type,
                last_n=last_n,
            )
        except HTTPException:
            continue

        sample_rows = game_rows
        if len(sample_rows) < min_games:
            continue

        values: list[float] = []
        hit_flags: list[bool] = []
        for game_row in sample_rows:
            value = round(compute_stat_value(game_row, stat), 1)
            hit = value >= line
            values.append(value)
            hit_flags.append(hit)

        if not values:
            continue

        hit_count = sum(1 for hit in hit_flags if hit)
        games_count = len(values)
        hit_rate = round((hit_count / games_count) * 100, 1)
        average = round(sum(values) / games_count, 2)
        avg_edge = round(average - line, 2)
        hit_streak = compute_recent_hit_streak(hit_flags)
        last_value = values[0]

        results.append(
            {
                "player": {
                    "id": player_id,
                    "full_name": str(row.get("PLAYER", "")).strip(),
                    "team_id": team["id"],
                    "team_name": team["full_name"],
                    "team_abbreviation": team["abbreviation"],
                    "position": str(row.get("POSITION", "")).strip(),
                    "jersey": str(row.get("NUM", "")).strip(),
                    "is_active": True,
                },
                "stat": stat,
                "line": line,
                "games_count": games_count,
                "hit_count": hit_count,
                "hit_rate": hit_rate,
                "average": average,
                "avg_edge": avg_edge,
                "last_value": last_value,
                "hit_streak": hit_streak,
            }
        )

    results.sort(
        key=lambda item: (
            item["hit_rate"],
            item["avg_edge"],
            item["hit_count"],
            item["average"],
        ),
        reverse=True,
    )

    return {
        "team": {
            "id": team["id"],
            "full_name": team["full_name"],
            "abbreviation": team["abbreviation"],
        },
        "season": selected_season,
        "season_type": season_type,
        "stat": stat,
        "line": line,
        "last_n": last_n,
        "min_games": min_games,
        "results": results[:limit],
    }


@app.post("/api/market-scan")
def market_scan(
    payload: dict[str, Any] = Body(...),
) -> dict[str, Any]:
    rows = payload.get("rows") or []
    default_last_n = int(payload.get("last_n") or 10)
    selected_season = str(payload.get("season") or current_nba_season())
    season_type = str(payload.get("season_type") or "Regular Season")

    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="Please provide at least one market row.")

    errors: list[dict[str, Any]] = []
    prepared_rows: list[tuple[int, dict[str, Any], float, float, str, str, dict[str, Any] | None, dict[str, Any] | None, int | None]] = []

    try:
        fetch_latest_injury_report_payload()
    except Exception:
        pass

    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            errors.append({"row": index, "reason": "Invalid row format."})
            continue

        player_name = str(row.get("player_name") or "").strip()
        stat = str(row.get("stat") or "").upper().strip()
        team_text = str(row.get("team") or "").strip()
        opponent_text = str(row.get("opponent") or "").strip()
        if stat not in STAT_MAP:
            errors.append({"row": index, "player_name": player_name, "reason": f"Unsupported stat: {stat}"})
            continue
        try:
            line = float(row.get("line"))
            over_odds = float(row.get("over_odds"))
            under_odds = float(row.get("under_odds"))
        except (TypeError, ValueError):
            errors.append({"row": index, "player_name": player_name, "reason": "Line and odds must be numeric."})
            continue

        team = resolve_team_from_text(team_text) if team_text else None
        opponent = resolve_team_from_text(opponent_text) if opponent_text else None
        team_id = int(team["id"]) if team else None
        player = find_player_by_name(player_name, team_id=team_id)
        if not player:
            errors.append({"row": index, "player_name": player_name, "reason": "Player not found."})
            continue

        bulk_row = {
            "player_id": int(player["id"]),
            "player_name": player_name or str(player.get("full_name") or ""),
            "stat": stat,
            "line": line,
            "team_id": team_id,
            "player_position": None,
        }
        prepared_rows.append((index, bulk_row, over_odds, under_odds, team_text, opponent_text, team, opponent, team_id))

    defaults = {
        "last_n": default_last_n,
        "season": selected_season,
        "season_type": season_type,
    }
    requested_max_workers = payload.get("max_workers")
    max_workers = BULK_ANALYSIS_MAX_WORKERS
    try:
        if requested_max_workers not in (None, ""):
            max_workers = max(1, min(BULK_ANALYSIS_MAX_WORKERS, int(requested_max_workers)))
    except (TypeError, ValueError):
        max_workers = BULK_ANALYSIS_MAX_WORKERS
    max_workers = min(max_workers, max(1, len(prepared_rows)))

    local_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    analysis_by_row: dict[int, dict[str, Any]] = {}

    if max_workers <= 1:
        for row_index, bulk_row, *_ in prepared_rows:
            try:
                analysis_by_row[row_index] = _build_bulk_prop_item(row_index, bulk_row, defaults, local_cache)
            except HTTPException as exc:
                errors.append({"row": row_index, "player_name": bulk_row.get("player_name"), "reason": exc.detail})
            except Exception as exc:
                errors.append({"row": row_index, "player_name": bulk_row.get("player_name"), "reason": str(exc)})
    else:
        futures: list[tuple[int, dict[str, Any], Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for row_index, bulk_row, *_ in prepared_rows:
                futures.append((row_index, bulk_row, executor.submit(_build_bulk_prop_item, row_index, bulk_row, defaults, local_cache)))
            for row_index, bulk_row, future in futures:
                try:
                    analysis_by_row[row_index] = future.result()
                except HTTPException as exc:
                    errors.append({"row": row_index, "player_name": bulk_row.get("player_name"), "reason": exc.detail})
                except Exception as exc:
                    errors.append({"row": row_index, "player_name": bulk_row.get("player_name"), "reason": str(exc)})

    results: list[dict[str, Any]] = []
    for index, bulk_row, over_odds, under_odds, team_text, opponent_text, team, opponent, team_id in prepared_rows:
        bulk_item = analysis_by_row.get(index)
        if not bulk_item:
            continue
        analysis = bulk_item.get("analysis") or {}

        matchup = analysis.get("matchup", {})
        next_game = matchup.get("next_game") or {}
        vs_position = matchup.get("vs_position") or {}
        availability = analysis.get("availability") or {}
        matchup_delta_pct = vs_position.get("delta_pct") if isinstance(vs_position, dict) else None
        model_over, model_under = estimate_model_probabilities(
            hit_rate_pct=float(analysis["hit_rate"]),
            average=float(analysis["average"]),
            line=float(bulk_row["line"]),
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            stat=str(bulk_row["stat"]),
            opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {},
            environment=analysis.get("environment") or {},
        )
        implied_over = decimal_implied_probability(over_odds)
        implied_under = decimal_implied_probability(under_odds)
        over_edge = round((model_over - implied_over) * 100, 1) if implied_over is not None else None
        under_edge = round((model_under - implied_under) * 100, 1) if implied_under is not None else None
        over_ev = round(model_over * over_odds - 1, 3)
        under_ev = round(model_under * under_odds - 1, 3)

        if under_ev > over_ev:
            best_side = "UNDER"
            best_edge = under_edge if under_edge is not None else round(under_ev * 100, 1)
            best_ev = under_ev
            best_model = model_under
            best_implied = implied_under
            market_odds = under_odds
        else:
            best_side = "OVER"
            best_edge = over_edge if over_edge is not None else round(over_ev * 100, 1)
            best_ev = over_ev
            best_model = model_over
            best_implied = implied_over
            market_odds = over_odds

        confidence_engine = build_confidence_engine(
            side=best_side,
            hit_rate=float(analysis["hit_rate"]),
            games_count=int(analysis["games_count"]),
            edge=best_edge,
            ev=best_ev,
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            availability=availability,
            opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {},
            environment=analysis.get("environment") or {},
            stat=str(bulk_row["stat"]),
            player_position=analysis.get("player", {}).get("position") or '',
            line=float(bulk_row["line"]),
            average=float(analysis["average"]),
        )

        display_side = best_side
        if availability.get("is_unavailable"):
            display_side = "AVOID"
        elif availability.get("is_risky"):
            display_side = f"{best_side}?"

        resolved_team_id = analysis["player"].get("team_id") or team_id
        resolved_team = TEAM_LOOKUP.get(int(resolved_team_id)) if resolved_team_id else None
        resolved_team_abbreviation = (
            (resolved_team or {}).get("abbreviation")
            or next_game.get("player_team_abbreviation")
            or (team.get("abbreviation") if team else team_text)
        )
        resolved_opponent_abbreviation = (
            next_game.get("opponent_abbreviation")
            or (opponent.get("abbreviation") if opponent else opponent_text)
        )

        results.append({
            "row": index,
            "player": {
                "id": analysis["player"]["id"],
                "full_name": analysis["player"]["full_name"],
                "team_id": resolved_team_id,
                "team": resolved_team_abbreviation,
                "opponent": resolved_opponent_abbreviation,
                "position": analysis["player"].get("position") or "",
            },
            "market": {
                "stat": str(bulk_row["stat"]),
                "line": float(bulk_row["line"]),
                "over_odds": over_odds,
                "under_odds": under_odds,
            },
            "analysis": {
                "average": analysis["average"],
                "hit_rate": analysis["hit_rate"],
                "hit_count": analysis["hit_count"],
                "games_count": analysis["games_count"],
                "last_n": analysis["last_n"],
                "over_streak": compute_recent_hit_streak([game.get("hit") for game in reversed(analysis["games"])]),
                "last_value": analysis["games"][-1]["value"] if analysis["games"] else None,
                "availability": availability,
                "matchup": {
                    "next_game": next_game,
                    "vs_position": vs_position,
                },
                "h2h": analysis.get("h2h") or {},
                "opportunity": analysis.get("opportunity") or {},
                "team_context": analysis.get("team_context") or {},
                "environment": analysis.get("environment") or {},
                "interpretation": analysis.get("interpretation") or {},
            },
            "model": {
                "over_probability": round(model_over * 100, 1),
                "under_probability": round(model_under * 100, 1),
                "over_implied": round(implied_over * 100, 1) if implied_over is not None else None,
                "under_implied": round(implied_under * 100, 1) if implied_under is not None else None,
                "over_edge": over_edge,
                "under_edge": under_edge,
                "over_ev": round(over_ev * 100, 1),
                "under_ev": round(under_ev * 100, 1),
            },
            "best_bet": {
                "side": best_side,
                "display_side": display_side,
                "edge": round(best_edge, 1) if best_edge is not None else None,
                "ev": round(best_ev * 100, 1),
                "model_probability": round(best_model * 100, 1),
                "implied_probability": round(best_implied * 100, 1) if best_implied is not None else None,
                "odds": market_odds,
                "confidence": confidence_engine["grade"],
                "confidence_score": confidence_engine["score"],
                "confidence_summary": confidence_engine["summary"],
                "confidence_tone": confidence_engine["tone"],
                "confidence_tier": confidence_engine.get("tier"),
                "confidence_tags": confidence_engine.get("tags") or [],
                "confidence_components": confidence_engine.get("components") or {},
                "playable": not availability.get("is_unavailable", False),
                "user_read": analysis.get("interpretation", {}).get("market_takeaway") or confidence_engine["summary"],
            },
            "availability": availability,
            "matchup": {
                "next_game": next_game,
                "vs_position": vs_position,
            },
        })

    results.sort(
        key=lambda item: (
            item["best_bet"].get("ev") if item["best_bet"].get("ev") is not None else float("-inf"),
            item["best_bet"].get("edge") if item["best_bet"].get("edge") is not None else float("-inf"),
            item["analysis"].get("hit_rate") if item["analysis"].get("hit_rate") is not None else float("-inf"),
            item["best_bet"].get("confidence_score", 0),
            -1 * int(item.get("availability", {}).get("sort_rank", 3) or 3),
        ),
        reverse=True,
    )

    errors.sort(key=lambda item: int(item.get("row") or 0))

    return {
        "season": selected_season,
        "season_type": season_type,
        "last_n": default_last_n,
        "template": "player_name,stat,line,over_odds,under_odds",
        "results": results,
        "errors": errors,
    }


@app.post("/api/odds/events")
def odds_events(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    api_key = str(payload.get("api_key") or "").strip()
    sport = str(payload.get("sport") or "basketball_nba")
    result = odds_api_fetch(f"/sports/{sport}/events", api_key, {"dateFormat": "iso"})
    return {
        "events": result["data"],
        "quota": result["quota"],
        "api_key_used": api_key,
        "api_key_masked": mask_api_key_for_display(api_key),
        "sport": sport,
    }


@app.post("/api/odds/player-props-import")
def odds_player_props_import(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    api_key = str(payload.get("api_key") or "").strip()
    sport = str(payload.get("sport") or "basketball_nba")
    event_id = str(payload.get("event_id") or "").strip()
    regions = str(payload.get("regions") or "us")
    odds_format = str(payload.get("odds_format") or "decimal")
    markets_value = payload.get("markets") or ",".join(ODDS_DEFAULT_MARKETS)
    if isinstance(markets_value, list):
        markets = ",".join(str(item).strip() for item in markets_value if str(item).strip())
    else:
        markets = str(markets_value).strip()
    if not event_id:
        raise HTTPException(status_code=400, detail="Missing event_id.")

    result = odds_api_fetch(
        f"/sports/{sport}/events/{event_id}/odds",
        api_key,
        {
            "regions": regions,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": "iso",
        },
    )
    event_payload = result["data"] or {}
    import_rows = build_odds_import_rows(event_payload, odds_format)
    return {
        "event": {
            "id": event_payload.get("id"),
            "sport_key": event_payload.get("sport_key"),
            "sport_title": event_payload.get("sport_title"),
            "commence_time": event_payload.get("commence_time"),
            "home_team": event_payload.get("home_team"),
            "away_team": event_payload.get("away_team"),
        },
        "import_rows": import_rows,
        "csv_rows": [row["csv_row"] for row in import_rows],
        "quota": result["quota"],
        "api_key_used": api_key,
        "api_key_masked": mask_api_key_for_display(api_key),
        "odds_format": odds_format,
        "regions": regions,
        "sport": sport,
        "markets": markets,
    }


@app.post("/api/parlay-builder")
def parlay_builder(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """
    Scrape selected NBA events + all markets in batches using rotating API keys,
    analyze every prop via the existing bulk engine, rank by hit rate, and build
    the best N-leg parlay the user requested.

    Payload:
      api_keys      list[str]   – one or more Odds API keys (rotated round-robin)
      event_ids     list[str]   – specific event IDs to scrape (from /api/odds/events)
      legs          int         – 2–6 (number of parlay legs)
      sport         str         – default "basketball_nba"
      regions       str         – default "us"
      odds_format   str         – "decimal" | "american"
      last_n        int         – recent-game sample size for analysis (default 10)
      season        str         – e.g. "2024-25"
      season_type   str         – "Regular Season"
      batch_size    int         – events per batch (default 3, keeps credit bursts small)
    """
    # ── Validate inputs ─────────────────────────────────────────────────
    raw_keys = payload.get("api_keys") or []
    if isinstance(raw_keys, str):
        raw_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
    api_keys: list[str] = [str(k).strip() for k in raw_keys if str(k).strip()]
    if not api_keys:
        raise HTTPException(status_code=400, detail="Provide at least one Odds API key in 'api_keys'.")

    legs = int(payload.get("legs") or 3)
    if legs < 2 or legs > 6:
        raise HTTPException(status_code=400, detail="'legs' must be between 2 and 6.")

    sport        = str(payload.get("sport") or "basketball_nba")
    regions      = str(payload.get("regions") or "us")
    odds_format  = str(payload.get("odds_format") or "decimal")
    last_n       = int(payload.get("last_n") or 10)
    season       = str(payload.get("season") or current_nba_season())
    season_type  = str(payload.get("season_type") or "Regular Season")
    batch_size   = max(1, int(payload.get("batch_size") or 3))
    bookmaker    = str(payload.get("bookmaker") or "draftkings").strip().lower()
    markets      = ",".join(ODDS_DEFAULT_MARKETS)

    # event_ids — if provided, skip fetching the events list entirely (saves 1 credit)
    requested_event_ids: set[str] = set()
    raw_event_ids = payload.get("event_ids") or []
    if isinstance(raw_event_ids, list):
        requested_event_ids = {str(e).strip() for e in raw_event_ids if str(e).strip()}

    # ── Phase 1: Resolve events ──────────────────────────────────────────
    key_index = 0
    quota_log: list[dict[str, Any]] = []

    def next_key() -> str:
        nonlocal key_index
        key = api_keys[key_index % len(api_keys)]
        key_index += 1
        return key

    if requested_event_ids:
        # User already selected specific events — build stub dicts directly
        # so we don't spend a credit on the events list
        events: list[dict[str, Any]] = [{"id": eid} for eid in requested_event_ids]
    else:
        try:
            events_result = odds_api_fetch(
                f"/sports/{sport}/events",
                next_key(),
                {"dateFormat": "iso"},
            )
        except HTTPException as exc:
            raise HTTPException(status_code=exc.status_code, detail=f"Failed to fetch events: {exc.detail}")

        events = events_result["data"] or []
        quota_log.append({"call": "events_list", "quota": events_result["quota"]})

    if not events:
        return {
            "legs": legs,
            "parlay": [],
            "parlay_odds": None,
            "all_props_scored": [],
            "events_scraped": 0,
            "props_found": 0,
            "errors": [],
            "quota_log": quota_log,
            "message": "No events found for today.",
        }

    # ── Phase 2: Fetch odds per event in batches, rotating keys ─────────
    all_import_rows: list[dict[str, Any]] = []
    scrape_errors: list[dict[str, Any]] = []

    for batch_start in range(0, len(events), batch_size):
        batch = events[batch_start: batch_start + batch_size]
        for event in batch:
            event_id = str(event.get("id") or "")
            if not event_id:
                continue
            try:
                result = odds_api_fetch(
                    f"/sports/{sport}/events/{event_id}/odds",
                    next_key(),
                    {
                        "regions": regions,
                        "markets": markets,
                        "oddsFormat": odds_format,
                        "dateFormat": "iso",
                        # 1 bookmaker only — we deduplicate anyway, saves ~80% credits
                        "bookmakers": bookmaker,
                    },
                )
                quota_log.append({"call": f"event_{event_id[:8]}", "quota": result["quota"]})
                rows = build_odds_import_rows(result["data"] or {}, odds_format)
                all_import_rows.extend(rows)
            except HTTPException as exc:
                scrape_errors.append({
                    "event_id": event_id,
                    "home_team": event.get("home_team"),
                    "away_team": event.get("away_team"),
                    "reason": exc.detail,
                    "status_code": exc.status_code,
                })
                # 402 / 429 → key is exhausted; advance to next key
                if exc.status_code in (402, 429):
                    key_index += 1
            except Exception as exc:
                scrape_errors.append({"event_id": event_id, "reason": str(exc)})

    if not all_import_rows:
        return {
            "legs": legs,
            "parlay": [],
            "parlay_odds": None,
            "all_props_scored": [],
            "events_scraped": len(events),
            "props_found": 0,
            "errors": scrape_errors,
            "quota_log": quota_log,
            "message": "No props found across today's events. Check your API keys or try again later.",
        }

    # ── Phase 3a: Pre-warm game-log cache with one bulk LeagueDash call ────
    # This single call fetches season stats for ALL players, warming the cache
    # so individual fetch_player_game_log calls hit cache instead of NBA API.
    try:
        _bulk_dash = LeagueDashPlayerStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed="PerGame",
            timeout=15,
        )
        _bulk_df = _bulk_dash.get_data_frames()[0]
        # Build a quick lookup so we can seed GAME_LOG_CACHE for each player
        # (LeagueDash gives season averages, not per-game logs — we can't fully
        # replace PlayerGameLog, but we CAN use it to pre-populate player info
        # and skip fetch_common_player_info calls for every player.)
        _bulk_rows = _bulk_df.to_dict(orient="records") if not _bulk_df.empty else []
        _dash_lookup: dict[int, dict[str, Any]] = {
            int(r.get("PLAYER_ID", 0)): r for r in _bulk_rows if r.get("PLAYER_ID")
        }
        # Seed PLAYER_INFO_CACHE for every player we're about to analyze
        now_ts_bulk = time.time()
        for row in _bulk_rows:
            pid = int(row.get("PLAYER_ID", 0))
            if not pid:
                continue
            if pid not in PLAYER_INFO_CACHE or (now_ts_bulk - float((PLAYER_INFO_CACHE.get(pid) or {}).get("timestamp", 0))) > PROFILE_TTL_SECONDS:
                team_id_val = row.get("TEAM_ID")
                PLAYER_INFO_CACHE[pid] = {
                    "timestamp": now_ts_bulk,
                    "row": {
                        "TEAM_ID": team_id_val,
                        "TEAM_ABBREVIATION": row.get("TEAM_ABBREVIATION", ""),
                        "POSITION": row.get("PLAYER_POSITION", ""),
                        "DISPLAY_FIRST_LAST": row.get("PLAYER_NAME", ""),
                    }
                }
        LOGGER.info("Parlay pre-warm: seeded PLAYER_INFO_CACHE for %d players from LeagueDash", len(_dash_lookup))
    except Exception as _bulk_exc:
        LOGGER.warning("Parlay pre-warm LeagueDash failed (non-fatal): %s", _bulk_exc)

    # ── Phase 3: Bulk-analyze all props in one shot (free, parallel) ──
    defaults = {"last_n": last_n, "season": season, "season_type": season_type}
    local_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    analysis_rows: list[dict[str, Any]] = []
    analysis_errors: list[dict[str, Any]] = []

    # Resolve player IDs first (same logic as market-scan)
    prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in all_import_rows:
        player_name = str(row.get("player_name") or "").strip()
        player = find_player_by_name(player_name)
        if not player:
            analysis_errors.append({"player_name": player_name, "reason": "Player not found."})
            continue
        bulk_row = {
            "player_id": int(player["id"]),
            "player_name": player_name,
            "stat": row["stat"],
            "line": row["line"],
            "team_id": None,
            "player_position": None,
        }
        prepared.append((bulk_row, row))

    # Deduplicate: one analysis per (player_id, stat, line)
    seen_analysis_keys: set[tuple[Any, ...]] = set()
    deduped_prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for bulk_row, orig_row in prepared:
        ak = (bulk_row["player_id"], bulk_row["stat"], float(bulk_row["line"]))
        if ak not in seen_analysis_keys:
            seen_analysis_keys.add(ak)
            deduped_prepared.append((bulk_row, orig_row))

    # Pre-warm game log cache concurrently for all unique player IDs
    # This fires off all PlayerGameLog requests in parallel with a short timeout
    # so the sequential analysis loop below hits cache instead of blocking 1-by-1.
    unique_player_ids: set[int] = {int(br["player_id"]) for br, _ in deduped_prepared}
    already_cached_ids: set[int] = {
        pid for pid in unique_player_ids
        if GAME_LOG_CACHE.get((pid, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) and
           (time.time() - float(GAME_LOG_CACHE[(pid, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)].get("timestamp", 0))) < CACHE_TTL_SECONDS
    }
    ids_to_fetch = unique_player_ids - already_cached_ids
    if ids_to_fetch:
        LOGGER.info("Parlay pre-fetching game logs for %d uncached players", len(ids_to_fetch))
        def _prefetch_log(pid: int) -> None:
            try:
                fetch_player_game_log(player_id=pid, season=season, season_type=season_type)
            except Exception:
                pass  # failures are tolerated; analysis loop will handle them
        prewarm_workers = min(BULK_ANALYSIS_MAX_WORKERS, len(ids_to_fetch), 8)
        with ThreadPoolExecutor(max_workers=prewarm_workers) as _pre_ex:
            list(_pre_ex.map(_prefetch_log, ids_to_fetch))
        LOGGER.info("Parlay game-log pre-warm complete for %d players", len(ids_to_fetch))

    max_workers = min(BULK_ANALYSIS_MAX_WORKERS, max(1, len(deduped_prepared)))

    if max_workers <= 1:
        for idx, (bulk_row, orig_row) in enumerate(deduped_prepared, start=1):
            try:
                result = _build_bulk_prop_item(idx, bulk_row, defaults, local_cache)
                analysis_rows.append((result, orig_row))
            except Exception as exc:
                analysis_errors.append({"player_name": bulk_row["player_name"], "reason": str(exc)})
    else:
        futures_list: list[tuple[int, dict[str, Any], dict[str, Any], Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for idx, (bulk_row, orig_row) in enumerate(deduped_prepared, start=1):
                fut = executor.submit(_build_bulk_prop_item, idx, bulk_row, defaults, local_cache)
                futures_list.append((idx, bulk_row, orig_row, fut))
            for idx, bulk_row, orig_row, fut in futures_list:
                try:
                    result = fut.result()
                    analysis_rows.append((result, orig_row))
                except Exception as exc:
                    analysis_errors.append({"player_name": bulk_row["player_name"], "reason": str(exc)})

    # ── Phase 4: Score every prop by hit rate, pick best N legs ─────────
    scored: list[dict[str, Any]] = []
    for result, orig_row in analysis_rows:
        analysis = result.get("analysis") or {}
        hit_rate  = float(analysis.get("hit_rate") or 0)
        avg       = float(analysis.get("average") or 0)
        line      = float(result.get("line") or orig_row.get("line") or 0)
        stat      = str(result.get("stat") or orig_row.get("stat") or "")

        # Determine best side purely by hit rate vs 50%
        # Over hit_rate > 50 → OVER is the stronger side
        if hit_rate >= 50:
            side = "OVER"
            odds = float(orig_row.get("over_odds") or 1.91)
            side_hit_rate = hit_rate
        else:
            side = "UNDER"
            odds = float(orig_row.get("under_odds") or 1.91)
            side_hit_rate = 100.0 - hit_rate

        # Skip unavailable players
        availability = analysis.get("availability") or {}
        if availability.get("is_unavailable"):
            continue

        # Skip props with odds below 1.40 — too low to be meaningful in a parlay
        if odds < 1.40:
            continue

        matchup = analysis.get("matchup") or {}
        next_game_info = matchup.get("next_game") or {}
        vs_position = matchup.get("vs_position") or {}
        matchup_delta_pct = vs_position.get("delta_pct") if isinstance(vs_position, dict) else None
        confidence_engine = build_confidence_engine(
            side=side,
            hit_rate=float(side_hit_rate),
            games_count=int(analysis.get("games_count") or 0),
            edge=round(abs(avg - line), 1),
            ev=max(0.0, (side_hit_rate / 100.0) - decimal_implied_probability(odds)),
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            availability=availability,
            opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {},
            environment=analysis.get("environment") or {},
            stat=stat,
            player_position=(analysis.get("player") or {}).get("position") or '',
            line=line,
            average=avg,
        )

        # Resolve team + opponent IDs from the analysis payload so the frontend
        # can auto-populate the analyzer without the user having to re-select manually.
        player_info    = analysis.get("player") or {}
        resolved_team_id_scored    = player_info.get("team_id")
        resolved_opponent_team_id  = next_game_info.get("opponent_team_id")
        resolved_opponent_abbr     = str(next_game_info.get("opponent_abbreviation") or "").strip()

        scored.append({
            "player_name": result.get("player_name") or "",
            "player_id": result.get("player_id"),
            "team_id": resolved_team_id_scored,
            "team_name": player_info.get("team_name") or "",
            "team_abbreviation": player_info.get("team_abbreviation") or "",
            "player_position": player_info.get("position") or "",
            "player_jersey": player_info.get("jersey") or "",
            "opponent_team_id": resolved_opponent_team_id,
            "opponent_abbreviation": resolved_opponent_abbr,
            "stat": stat,
            "line": line,
            "side": side,
            "odds": odds,
            "hit_rate": round(side_hit_rate, 1),
            "average": round(avg, 2),
            "games_count": int(analysis.get("games_count") or 0),
            "last_n": int(analysis.get("last_n") or last_n),
            "bookmaker": orig_row.get("bookmaker_title") or "N/A",
            "market_key": orig_row.get("market_key") or "",
            "availability_label": availability.get("label") or "Active",
            "availability": copy.deepcopy(availability),
            "matchup": copy.deepcopy(analysis.get("matchup") or {}),
            "environment": copy.deepcopy(analysis.get("environment") or {}),
            "confidence": confidence_engine.get("grade"),
            "confidence_score": confidence_engine.get("score"),
            "confidence_tone": confidence_engine.get("tone"),
            "confidence_tier": confidence_engine.get("tier"),
            "confidence_summary": confidence_engine.get("summary"),
            "confidence_tags": confidence_engine.get("tags") or [],
            "event_id": orig_row.get("event_id") or "",
            "game_label": orig_row.get("game_label") or "",
        })

    # Sort descending by hit rate (primary), then odds (prefer value)
    scored.sort(key=lambda x: (x["hit_rate"], x["odds"]), reverse=True)

    # Pick top N — enforce one leg per player AND one leg per game (no SGP)
    parlay_legs: list[dict[str, Any]] = []
    seen_player_ids: set[int] = set()
    seen_event_ids: set[str] = set()
    for prop in scored:
        if len(parlay_legs) >= legs:
            break
        pid = prop.get("player_id")
        eid = str(prop.get("event_id") or "")
        if pid in seen_player_ids:
            continue
        # Block same-game parlay: if this event already has a leg, skip
        if eid and eid in seen_event_ids:
            continue
        seen_player_ids.add(pid)
        if eid:
            seen_event_ids.add(eid)
        parlay_legs.append(prop)

    # Calculate combined parlay odds (decimal product)
    parlay_odds: float | None = None
    if parlay_legs:
        parlay_odds = 1.0
        for leg in parlay_legs:
            parlay_odds *= leg["odds"]
        parlay_odds = round(parlay_odds, 2)

    all_errors = scrape_errors + analysis_errors

    return {
        "legs": legs,
        "parlay": parlay_legs,
        "parlay_odds": parlay_odds,
        "all_props_scored": scored,
        "events_scraped": len(events),
        "props_found": len(all_import_rows),
        "props_analyzed": len(scored),
        "errors": all_errors,
        "quota_log": quota_log,
    }


@app.get("/api/team-injuries")
def get_team_injuries(team_name: str = Query(..., min_length=1)) -> dict[str, Any]:
    """
    Return injured/risky players for a team with their resolved NBA player IDs.
    Used by the injury-aware parlay builder and Player Analyzer injury boost UI.
    """
    payload = fetch_latest_injury_report_payload()
    rows = [row for row in (payload.get("rows") or []) if row.get("team_name") == team_name]

    results: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get("status") or "").strip()
        if status not in UNAVAILABLE_STATUSES and status not in RISKY_STATUSES:
            continue
        raw_display = re.sub(r",(?!\s)", ", ", str(row.get("player_display") or "").strip())
        # Convert "Last, First" → "First Last" for player lookup
        parts = [p.strip() for p in raw_display.split(",")]
        name_for_lookup = f"{parts[1]} {parts[0]}" if len(parts) == 2 else raw_display
        player = find_player_by_name(name_for_lookup)
        results.append({
            "display_name": raw_display,
            "lookup_name": name_for_lookup,
            "player_id": int(player["id"]) if player else None,
            "status": status,
            "reason": str(row.get("reason") or "").strip(),
            "is_unavailable": status in UNAVAILABLE_STATUSES,
            "is_risky": status in RISKY_STATUSES,
        })

    results.sort(key=lambda p: (0 if p["is_unavailable"] else 1, p["display_name"]))
    return {
        "team_name": team_name,
        "players": results,
        "out_count": sum(1 for p in results if p["is_unavailable"]),
        "risky_count": sum(1 for p in results if p["is_risky"]),
        "report_label": payload.get("report_label", ""),
        "ok": payload.get("ok", False),
    }


@app.post("/api/parlay-builder-injury-aware")
def parlay_builder_injury_aware(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """
    Injury-aware variant of the parlay builder.
    Identical scraping pipeline as /api/parlay-builder, but after scoring each prop,
    re-runs the analysis with the team's injured players as without_player_ids.
    Adaptive: tries 1 injured player first (most impactful), checks games_count >= 5,
    falls back to no filter if sample is too small.
    """
    raw_keys = payload.get("api_keys") or []
    if isinstance(raw_keys, str):
        raw_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
    api_keys: list[str] = [str(k).strip() for k in raw_keys if str(k).strip()]
    if not api_keys:
        raise HTTPException(status_code=400, detail="Provide at least one Odds API key in 'api_keys'.")

    legs = int(payload.get("legs") or 3)
    if legs < 2 or legs > 6:
        raise HTTPException(status_code=400, detail="'legs' must be between 2 and 6.")

    sport       = str(payload.get("sport") or "basketball_nba")
    regions     = str(payload.get("regions") or "us")
    odds_format = str(payload.get("odds_format") or "decimal")
    last_n      = int(payload.get("last_n") or 10)
    season      = str(payload.get("season") or current_nba_season())
    season_type = str(payload.get("season_type") or "Regular Season")
    batch_size  = max(1, int(payload.get("batch_size") or 3))
    bookmaker   = str(payload.get("bookmaker") or "draftkings").strip().lower()
    markets     = ",".join(ODDS_DEFAULT_MARKETS)

    requested_event_ids: set[str] = set()
    raw_event_ids = payload.get("event_ids") or []
    if isinstance(raw_event_ids, list):
        requested_event_ids = {str(e).strip() for e in raw_event_ids if str(e).strip()}

    key_index = 0
    quota_log: list[dict[str, Any]] = []

    def next_key() -> str:
        nonlocal key_index
        key = api_keys[key_index % len(api_keys)]
        key_index += 1
        return key

    if requested_event_ids:
        events: list[dict[str, Any]] = [{"id": eid} for eid in requested_event_ids]
    else:
        try:
            events_result = odds_api_fetch(f"/sports/{sport}/events", next_key(), {"dateFormat": "iso"})
        except HTTPException as exc:
            raise HTTPException(status_code=exc.status_code, detail=f"Failed to fetch events: {exc.detail}")
        events = events_result["data"] or []
        quota_log.append({"call": "events_list", "quota": events_result["quota"]})

    if not events:
        return {"legs": legs, "parlay": [], "parlay_odds": None, "all_props_scored": [],
                "events_scraped": 0, "props_found": 0, "errors": [], "quota_log": quota_log,
                "injury_summary": [], "message": "No events found for today."}

    all_import_rows: list[dict[str, Any]] = []
    scrape_errors: list[dict[str, Any]] = []

    for batch_start in range(0, len(events), batch_size):
        batch = events[batch_start: batch_start + batch_size]
        for event in batch:
            event_id = str(event.get("id") or "")
            if not event_id:
                continue
            try:
                result = odds_api_fetch(
                    f"/sports/{sport}/events/{event_id}/odds", next_key(),
                    {"regions": regions, "markets": markets, "oddsFormat": odds_format,
                     "dateFormat": "iso", "bookmakers": bookmaker},
                )
                quota_log.append({"call": f"event_{event_id[:8]}", "quota": result["quota"]})
                rows = build_odds_import_rows(result["data"] or {}, odds_format)
                all_import_rows.extend(rows)
            except HTTPException as exc:
                scrape_errors.append({"event_id": event_id, "reason": exc.detail, "status_code": exc.status_code})
                if exc.status_code in (402, 429):
                    key_index += 1
            except Exception as exc:
                scrape_errors.append({"event_id": event_id, "reason": str(exc)})

    if not all_import_rows:
        return {"legs": legs, "parlay": [], "parlay_odds": None, "all_props_scored": [],
                "events_scraped": len(events), "props_found": 0, "errors": scrape_errors,
                "quota_log": quota_log, "injury_summary": [],
                "message": "No props found across today's events. Check your API keys or try again later."}

    # Pre-warm caches (same as regular parlay builder)
    try:
        _bulk_dash = LeagueDashPlayerStats(season=season, season_type_all_star=season_type,
                                           per_mode_detailed="PerGame", timeout=15)
        _bulk_df = _bulk_dash.get_data_frames()[0]
        _bulk_rows = _bulk_df.to_dict(orient="records") if not _bulk_df.empty else []
        now_ts_bulk = time.time()
        for row in _bulk_rows:
            pid = int(row.get("PLAYER_ID", 0))
            if not pid:
                continue
            if pid not in PLAYER_INFO_CACHE or (now_ts_bulk - float((PLAYER_INFO_CACHE.get(pid) or {}).get("timestamp", 0))) > PROFILE_TTL_SECONDS:
                PLAYER_INFO_CACHE[pid] = {
                    "timestamp": now_ts_bulk,
                    "row": {"TEAM_ID": row.get("TEAM_ID"), "TEAM_ABBREVIATION": row.get("TEAM_ABBREVIATION", ""),
                            "POSITION": row.get("PLAYER_POSITION", ""), "DISPLAY_FIRST_LAST": row.get("PLAYER_NAME", "")},
                }
    except Exception as _e:
        LOGGER.warning("Injury-aware parlay pre-warm failed (non-fatal): %s", _e)

    # ── Pre-fetch injury report once ─────────────────────────────────────
    try:
        inj_report = fetch_latest_injury_report_payload()
    except Exception:
        inj_report = {"ok": False, "rows": []}

    # Build per-team injured player IDs cache: team_name → list[int]
    _team_injured_ids_cache: dict[str, list[int]] = {}

    def get_injured_ids_for_team(team_name: str) -> list[int]:
        if team_name in _team_injured_ids_cache:
            return _team_injured_ids_cache[team_name]
        # Normalize for fuzzy matching — injury report team names come from PDF parsing
        # and may differ in casing/punctuation from NBA stats API team names.
        norm_target = normalize_compact_text(team_name)
        rows_inj = [r for r in (inj_report.get("rows") or [])
                    if normalize_compact_text(str(r.get("team_name") or "")) == norm_target]
        ids: list[int] = []
        for row in rows_inj:
            status = str(row.get("status") or "")
            if status not in UNAVAILABLE_STATUSES and status not in RISKY_STATUSES:
                continue
            raw_disp = re.sub(r",(?!\s)", ", ", str(row.get("player_display") or "").strip())
            parts = [p.strip() for p in raw_disp.split(",")]
            name_lk = f"{parts[1]} {parts[0]}" if len(parts) == 2 else raw_disp
            player = find_player_by_name(name_lk)
            if player:
                ids.append(int(player["id"]))
        _team_injured_ids_cache[team_name] = ids
        return ids

    defaults = {"last_n": last_n, "season": season, "season_type": season_type}
    local_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    analysis_rows: list[dict[str, Any]] = []
    analysis_errors: list[dict[str, Any]] = []

    prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in all_import_rows:
        player_name = str(row.get("player_name") or "").strip()
        player = find_player_by_name(player_name)
        if not player:
            analysis_errors.append({"player_name": player_name, "reason": "Player not found."})
            continue
        bulk_row = {"player_id": int(player["id"]), "player_name": player_name,
                    "stat": row["stat"], "line": row["line"], "team_id": None, "player_position": None}
        prepared.append((bulk_row, row))

    seen_analysis_keys: set[tuple[Any, ...]] = set()
    deduped_prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for bulk_row, orig_row in prepared:
        ak = (bulk_row["player_id"], bulk_row["stat"], float(bulk_row["line"]))
        if ak not in seen_analysis_keys:
            seen_analysis_keys.add(ak)
            deduped_prepared.append((bulk_row, orig_row))

    unique_player_ids: set[int] = {int(br["player_id"]) for br, _ in deduped_prepared}
    already_cached_ids = {pid for pid in unique_player_ids
                          if GAME_LOG_CACHE.get((pid, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) and
                          (time.time() - float(GAME_LOG_CACHE[(pid, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)].get("timestamp", 0))) < CACHE_TTL_SECONDS}
    ids_to_fetch = unique_player_ids - already_cached_ids
    if ids_to_fetch:
        def _prefetch_log(pid: int) -> None:
            try:
                fetch_player_game_log(player_id=pid, season=season, season_type=season_type)
            except Exception:
                pass
        prewarm_workers = min(BULK_ANALYSIS_MAX_WORKERS, len(ids_to_fetch), 8)
        with ThreadPoolExecutor(max_workers=prewarm_workers) as _pre_ex:
            list(_pre_ex.map(_prefetch_log, ids_to_fetch))

    max_workers = min(BULK_ANALYSIS_MAX_WORKERS, max(1, len(deduped_prepared)))

    if max_workers <= 1:
        for idx, (bulk_row, orig_row) in enumerate(deduped_prepared, start=1):
            try:
                result = _build_bulk_prop_item(idx, bulk_row, defaults, local_cache)
                analysis_rows.append((result, orig_row))
            except Exception as exc:
                analysis_errors.append({"player_name": bulk_row["player_name"], "reason": str(exc)})
    else:
        futures_list: list[tuple[int, dict[str, Any], dict[str, Any], Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for idx, (bulk_row, orig_row) in enumerate(deduped_prepared, start=1):
                fut = executor.submit(_build_bulk_prop_item, idx, bulk_row, defaults, local_cache)
                futures_list.append((idx, bulk_row, orig_row, fut))
            for idx, bulk_row, orig_row, fut in futures_list:
                try:
                    result = fut.result()
                    analysis_rows.append((result, orig_row))
                except Exception as exc:
                    analysis_errors.append({"player_name": bulk_row["player_name"], "reason": str(exc)})

    # ── Injury-aware scoring ─────────────────────────────────────────────
    def injury_aware_score(result: dict[str, Any], orig_row: dict[str, Any]) -> dict[str, Any] | None:
        """Score a prop with injury-aware without_player boosting."""
        analysis = result.get("analysis") or {}
        hit_rate = float(analysis.get("hit_rate") or 0)
        avg      = float(analysis.get("average") or 0)
        line     = float(result.get("line") or orig_row.get("line") or 0)
        stat     = str(result.get("stat") or orig_row.get("stat") or "")

        availability = analysis.get("availability") or {}
        if availability.get("is_unavailable"):
            return None
        if float(orig_row.get("over_odds") or 1.91) < 1.40 and float(orig_row.get("under_odds") or 1.91) < 1.40:
            return None

        player_info = analysis.get("player") or {}
        player_id   = int(result.get("player_id") or 0)
        # player dict carries team_id but often no team_name — resolve via TEAM_LOOKUP.
        # Fall back to TEAM_ALIAS_LOOKUP using orig_row team abbreviation if still empty.
        team_id_raw = player_info.get("team_id") or 0
        team_name   = str(player_info.get("team_name") or "")
        if not team_name and team_id_raw:
            team_name = TEAM_LOOKUP.get(int(team_id_raw), {}).get("full_name", "")
        if not team_name:
            # Last resort: derive from orig_row game_label (e.g. "HOU @ GSW") via abbreviation
            game_label = str(orig_row.get("game_label") or "")
            home_abbr  = str(orig_row.get("home_team") or "")
            away_abbr  = str(orig_row.get("away_team") or "")
            player_nm  = str(result.get("player_name") or "").lower()
            for abbr in [home_abbr, away_abbr]:
                candidate = TEAM_ALIAS_LOOKUP.get(abbr.strip().lower(), {})
                if candidate:
                    team_name = candidate.get("full_name", "")
                    break
        LOGGER.debug(
            "injury_aware_score team resolution: player=%s team_id=%s → team_name=%r",
            result.get("player_name"), team_id_raw, team_name
        )

        # Try injury-aware re-analysis
        injury_boost = False
        injury_filter_player_ids: list[int] = []
        injury_filter_player_names: list[str] = []
        team_injury_player_names: list[str] = []
        base_hit_rate = hit_rate
        base_games_count = int(analysis.get("games_count") or 0)

        if team_name and player_id:
            inj_ids = get_injured_ids_for_team(team_name)
            # Collect names for display
            team_injury_player_names = resolve_without_player_names(inj_ids)
            # Try 1 injured player first, then up to 2 if sample is still good
            for max_filters in [1, 2]:
                candidate_ids = inj_ids[:max_filters]
                if not candidate_ids:
                    break
                try:
                    boosted_analysis = build_prop_analysis_payload(
                        player_id=player_id,
                        stat=stat,
                        line=line,
                        last_n=last_n,
                        season=season,
                        season_type=season_type,
                        without_player_ids=candidate_ids,
                    )
                    boosted_games = int(boosted_analysis.get("games_count") or 0)
                    boosted_hr    = float(boosted_analysis.get("hit_rate") or 0)
                    if boosted_games >= 5 and boosted_hr > hit_rate:
                        hit_rate = boosted_hr
                        avg      = float(boosted_analysis.get("average") or avg)
                        injury_boost = True
                        injury_filter_player_ids = candidate_ids
                        injury_filter_player_names = resolve_without_player_names(candidate_ids)
                        base_games_count = boosted_games
                        break  # good result with this many filters
                    elif boosted_games >= 5:
                        # Sample is usable but no boost — still record filter used
                        injury_filter_player_ids = candidate_ids
                        injury_filter_player_names = resolve_without_player_names(candidate_ids)
                        base_games_count = boosted_games
                        break
                except Exception:
                    break

        if hit_rate >= 50:
            side = "OVER"
            odds = float(orig_row.get("over_odds") or 1.91)
            side_hit_rate = hit_rate
        else:
            side = "UNDER"
            odds = float(orig_row.get("under_odds") or 1.91)
            side_hit_rate = 100.0 - hit_rate

        if odds < 1.40:
            return None

        matchup = analysis.get("matchup") or {}
        next_game_info = matchup.get("next_game") or {}
        vs_position = matchup.get("vs_position") or {}
        matchup_delta_pct = vs_position.get("delta_pct") if isinstance(vs_position, dict) else None
        confidence_engine = build_confidence_engine(
            side=side, hit_rate=float(side_hit_rate), games_count=base_games_count,
            edge=round(abs(avg - line), 1),
            ev=max(0.0, (side_hit_rate / 100.0) - decimal_implied_probability(odds)),
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            availability=availability, opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {}, environment=analysis.get("environment") or {},
            stat=stat, player_position=(analysis.get("player") or {}).get("position") or "",
            line=line, average=avg,
        )

        resolved_team_id = player_info.get("team_id")
        opponent_info    = next_game_info

        return {
            "player_name": result.get("player_name") or "",
            "player_id": player_id,
            "team_id": resolved_team_id,
            "team_name": team_name,
            "team_abbreviation": player_info.get("team_abbreviation") or "",
            "player_position": player_info.get("position") or "",
            "player_jersey": player_info.get("jersey") or "",
            "opponent_team_id": opponent_info.get("opponent_team_id"),
            "opponent_abbreviation": str(opponent_info.get("opponent_abbreviation") or ""),
            "stat": stat, "line": line, "side": side, "odds": odds,
            "hit_rate": round(side_hit_rate, 1),
            "base_hit_rate": round(base_hit_rate, 1),
            "average": round(avg, 2),
            "games_count": base_games_count,
            "last_n": last_n,
            "bookmaker": orig_row.get("bookmaker_title") or "N/A",
            "market_key": orig_row.get("market_key") or "",
            "availability_label": availability.get("label") or "Active",
            "availability": copy.deepcopy(availability),
            "matchup": copy.deepcopy(analysis.get("matchup") or {}),
            "environment": copy.deepcopy(analysis.get("environment") or {}),
            "confidence": confidence_engine.get("grade"),
            "confidence_score": confidence_engine.get("score"),
            "confidence_tone": confidence_engine.get("tone"),
            "confidence_tier": confidence_engine.get("tier"),
            "confidence_summary": confidence_engine.get("summary"),
            "confidence_tags": confidence_engine.get("tags") or [],
            "event_id": orig_row.get("event_id") or "",
            "game_label": orig_row.get("game_label") or "",
            # Injury-aware fields
            "injury_boost": injury_boost,
            "injury_filter_player_ids": injury_filter_player_ids,
            "injury_filter_player_names": injury_filter_player_names,
            "team_injury_player_names": team_injury_player_names,
        }

    scored: list[dict[str, Any]] = []
    for result, orig_row in analysis_rows:
        item = injury_aware_score(result, orig_row)
        if item:
            scored.append(item)

    scored.sort(key=lambda x: (x["hit_rate"], x["odds"]), reverse=True)

    # Pick top N — one leg per player, one leg per game
    parlay_legs: list[dict[str, Any]] = []
    seen_player_ids: set[int] = set()
    seen_event_ids: set[str] = set()
    for prop in scored:
        if len(parlay_legs) >= legs:
            break
        pid = prop.get("player_id")
        eid = str(prop.get("event_id") or "")
        if pid in seen_player_ids:
            continue
        if eid and eid in seen_event_ids:
            continue
        seen_player_ids.add(pid)
        if eid:
            seen_event_ids.add(eid)
        parlay_legs.append(prop)

    parlay_odds: float | None = None
    if parlay_legs:
        parlay_odds = 1.0
        for leg in parlay_legs:
            parlay_odds *= leg["odds"]
        parlay_odds = round(parlay_odds, 2)

    # Build injury summary for display
    all_team_names: set[str] = {p.get("team_name", "") for p in scored if p.get("team_name")}
    injury_summary: list[dict[str, Any]] = []
    for tn in sorted(all_team_names):
        inj_ids = get_injured_ids_for_team(tn)
        if inj_ids:
            injury_summary.append({
                "team_name": tn,
                "injured_player_names": resolve_without_player_names(inj_ids),
                "count": len(inj_ids),
            })

    all_errors = scrape_errors + analysis_errors
    return {
        "legs": legs,
        "parlay": parlay_legs,
        "parlay_odds": parlay_odds,
        "all_props_scored": scored,
        "events_scraped": len(events),
        "props_found": len(all_import_rows),
        "props_analyzed": len(scored),
        "errors": all_errors,
        "quota_log": quota_log,
        "injury_summary": injury_summary,
    }


@app.get("/api/todays-games")
@timed_call("todays_games_endpoint")
def todays_games(game_date: str | None = None) -> dict[str, Any]:
    requested_date = game_date or current_nba_game_date()
    resolved_date = requested_date
    rows = fetch_scoreboard_games(requested_date)
    fallback_used = False

    if not rows:
        base_date = datetime.strptime(requested_date, "%Y-%m-%d").date()
        for offset in range(1, 4):
            probe_date = (base_date + timedelta(days=offset)).strftime("%Y-%m-%d")
            probe_rows = fetch_scoreboard_games(probe_date)
            if probe_rows:
                rows = probe_rows
                resolved_date = probe_date
                fallback_used = True
                break

    report_payload = fetch_latest_injury_report_payload()
    games: list[dict[str, Any]] = []

    for row in rows:
        home_team_id = int(row.get("HOME_TEAM_ID") or 0)
        away_team_id = int(row.get("VISITOR_TEAM_ID") or 0)
        home_team = TEAM_LOOKUP.get(home_team_id, {})
        away_team = TEAM_LOOKUP.get(away_team_id, {})
        game_status = str(row.get("GAME_STATUS_TEXT") or "").strip()
        home_score = safe_int_score(row.get("PTS_HOME"), 0)
        away_score = safe_int_score(row.get("PTS_AWAY"), 0)
        home_summary = build_team_availability_summary(str(home_team.get("full_name") or ""), report_payload)
        away_summary = build_team_availability_summary(str(away_team.get("full_name") or ""), report_payload)

        def _inj_players(team_full_name: str) -> list[dict[str, Any]]:
            seen_keys: set[str] = set()
            result: list[dict[str, Any]] = []
            team_full_name_norm = team_full_name.strip()
            for ir in (report_payload.get("rows") or []):
                status = str(ir.get("status") or "").strip()
                if status not in UNAVAILABLE_STATUSES and status not in RISKY_STATUSES:
                    continue
                # Use exact team name match (same as build_team_availability_summary)
                # to prevent players bleeding across teams when PDF parsing misassigns rows.
                ir_team = str(ir.get("team_name") or "").strip()
                if ir_team != team_full_name_norm:
                    continue
                pk = str(ir.get("player_key") or "")
                if pk in seen_keys:
                    continue
                seen_keys.add(pk)
                raw_display = str(ir.get("player_display") or "").strip()
                # Normalise display: PDF text-extract gives "Last,First" (no space).
                # Insert space after comma if missing so the UI shows "Last, First".
                display = re.sub(r",(?!\s)", ", ", raw_display)
                # Derive a short name: if "Last, First" format use last name, else last word.
                if "," in display:
                    short = display.split(",")[0].strip()
                else:
                    short = display.split()[-1] if display.split() else display
                result.append({
                    "full_name": display,
                    "short_name": short,
                    "status": status,
                    "injury_reason": str(ir.get("reason") or ""),
                    "is_unavailable": status in UNAVAILABLE_STATUSES,
                    "is_risky": status in RISKY_STATUSES,
                })
            return result

        games.append({
            "game_id": str(row.get("GAME_ID") or "").strip(),
            "game_date": resolved_date,
            "status_text": game_status or "TBD",
            "status_category": "final" if "Final" in game_status else ("live" if "Q" in game_status or "Halftime" in game_status else "scheduled"),
            "game_label": f"{away_team.get('abbreviation', '')} @ {home_team.get('abbreviation', '')}",
            "home": {
                "team_id": home_team_id,
                "full_name": str(home_team.get("full_name") or "").strip(),
                "abbreviation": str(home_team.get("abbreviation") or "").strip(),
                "score": home_score,
                "availability": home_summary,
                "injury_players": _inj_players(str(home_team.get("full_name") or "")),
            },
            "away": {
                "team_id": away_team_id,
                "full_name": str(away_team.get("full_name") or "").strip(),
                "abbreviation": str(away_team.get("abbreviation") or "").strip(),
                "score": away_score,
                "availability": away_summary,
                "injury_players": _inj_players(str(away_team.get("full_name") or "")),
            },
        })

    return {
        "requested_date": requested_date,
        "resolved_date": resolved_date,
        "fallback_used": fallback_used,
        "report_label": report_payload.get("report_label") or "",
        "games": games,
    }


# ── Live boxscore stat for tracker ────────────────────────────────────────────
_LIVE_BOX_CACHE: dict[str, dict[str, Any]] = {}
_LIVE_BOX_CACHE_TTL = 30  # seconds

# NBA CDN live scoreboard — much faster than ScoreboardV2
_LIVE_SCOREBOARD_CACHE: dict[str, Any] = {}
_LIVE_SCOREBOARD_TTL = 20  # seconds

_NBA_CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
}


def _fetch_nba_live_scoreboard() -> dict[str, Any] | None:
    """Fetch NBA CDN live scoreboard — real-time game states and IDs."""
    cached = _LIVE_SCOREBOARD_CACHE.get("data")
    if cached and time.time() - _LIVE_SCOREBOARD_CACHE.get("ts", 0) < _LIVE_SCOREBOARD_TTL:
        return cached
    try:
        url = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
        resp = requests.get(url, timeout=8, headers=_NBA_CDN_HEADERS)
        if resp.status_code != 200:
            return None
        data = resp.json()
        _LIVE_SCOREBOARD_CACHE["data"] = data
        _LIVE_SCOREBOARD_CACHE["ts"] = time.time()
        return data
    except Exception:
        return None


def _fetch_live_boxscore(game_id: str) -> dict[str, Any] | None:
    """Fetch live/final boxscore from NBA CDN live data endpoint."""
    cached = _LIVE_BOX_CACHE.get(game_id)
    if cached and time.time() - cached["ts"] < _LIVE_BOX_CACHE_TTL:
        return cached["data"]
    try:
        url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        resp = requests.get(url, timeout=8, headers=_NBA_CDN_HEADERS)
        if resp.status_code != 200:
            return None
        data = resp.json()
        _LIVE_BOX_CACHE[game_id] = {"ts": time.time(), "data": data}
        return data
    except Exception:
        return None


@app.get("/api/tracker/live-stat")
def tracker_live_stat(player_id: int, stat: str = Query(..., pattern="^(PTS|REB|AST|3PM|STL|BLK|PRA|PR|PA|RA)$")) -> dict[str, Any]:
    """Return today's live/final in-game stat for a player using NBA CDN live data."""
    stat = stat.upper()

    player = PLAYER_LOOKUP.get(player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found.")

    live_val: float | None = None
    game_status: str = "no_game"
    game_label: str = ""
    period: str = ""
    clock: str = ""

    # ── 1. NBA CDN live scoreboard (20s cache) ─────────────────────────────
    sb = _fetch_nba_live_scoreboard()
    game_id: str | None = None

    if sb:
        try:
            games = sb.get("scoreboard", {}).get("games", [])
            for g in games:
                home_tid = int((g.get("homeTeam") or {}).get("teamId") or 0)
                away_tid = int((g.get("awayTeam") or {}).get("teamId") or 0)

                # Match by checking boxscore player list (fast path: match team first via player profile)
                # We'll try to match team_id from player profile cache
                player_team_id: int | None = None
                try:
                    info = fetch_common_player_info(player_id)
                    raw = info.get("TEAM_ID")
                    player_team_id = int(raw) if raw not in (None, "") else None
                except Exception:
                    pass

                if player_team_id and player_team_id not in (home_tid, away_tid):
                    continue  # not this player's game

                gstatus = int(g.get("gameStatus") or 1)
                # gameStatus: 1=scheduled, 2=live, 3=final
                home_abbr = (g.get("homeTeam") or {}).get("teamTricode", "")
                away_abbr = (g.get("awayTeam") or {}).get("teamTricode", "")
                game_label = f"{away_abbr} @ {home_abbr}"
                gid = str(g.get("gameId") or "").strip()
                period_num = g.get("period", 0)
                game_clock = str(g.get("gameClock") or "").replace("PT", "").replace("M", ":").replace("S", "").strip()

                if gstatus == 1:
                    game_status = "scheduled"
                    game_id = gid
                    break
                elif gstatus in (2, 3):
                    game_id = gid
                    game_status = "live" if gstatus == 2 else "final"
                    period = f"Q{period_num}" if period_num else ""
                    clock = game_clock
                    break
        except Exception:
            pass

    # ── 2. Pull live boxscore from NBA CDN ────────────────────────────────
    if game_id and game_status in ("live", "final"):
        box = _fetch_live_boxscore(game_id)
        if box:
            try:
                home_players = box["game"]["homeTeam"]["players"]
                away_players = box["game"]["awayTeam"]["players"]
                all_players = home_players + away_players
                p_row = next((p for p in all_players if int(p.get("personId", -1)) == player_id), None)
                if p_row:
                    stats = p_row.get("statistics", {})
                    def gs(k: str) -> float:
                        return float(stats.get(k) or 0)
                    if stat == "PTS":   live_val = gs("points")
                    elif stat == "REB": live_val = gs("reboundsTotal")
                    elif stat == "AST": live_val = gs("assists")
                    elif stat == "3PM": live_val = gs("threePointersMade")
                    elif stat == "STL": live_val = gs("steals")
                    elif stat == "BLK": live_val = gs("blocks")
                    elif stat == "PRA": live_val = gs("points") + gs("reboundsTotal") + gs("assists")
                    elif stat == "PR":  live_val = gs("points") + gs("reboundsTotal")
                    elif stat == "PA":  live_val = gs("points") + gs("assists")
                    elif stat == "RA":  live_val = gs("reboundsTotal") + gs("assists")
            except (KeyError, TypeError, StopIteration):
                pass

    # ── 3. Fallback to last game log if no live data ───────────────────────
    fallback_val: float | None = None
    fallback_date: str | None = None
    try:
        season = current_nba_season()
        rows = fetch_player_game_log(player_id=player_id, season=season, season_type="Regular Season")
        if rows:
            r = rows[0]
            fallback_val = compute_stat_value(r, stat)
            fallback_date = str(r.get("GAME_DATE") or "")
    except Exception:
        pass

    # ── 4. Injury status check ────────────────────────────────────────────
    injury_status: str = ""
    is_injured: bool = False
    try:
        avail = build_availability_payload(player.get("full_name", ""))
        injury_status = avail.get("status") or ""
        is_injured = bool(avail.get("is_unavailable")) or bool(avail.get("is_risky"))
    except Exception:
        pass

    return {
        "player_id": player_id,
        "stat": stat,
        "live_val": live_val,
        "game_status": game_status,
        "game_label": game_label,
        "period": period,
        "clock": clock,
        "fallback_val": fallback_val,
        "fallback_date": fallback_date,
        "injury_status": injury_status,
        "is_injured": is_injured,
    }


@app.get("/api/player-prop")
@timed_call("player_prop_endpoint")
def player_prop(
    player_id: int,
    stat: str = Query(..., pattern="^(PTS|REB|AST|3PM|STL|BLK|PRA|PR|PA|RA)$"),
    line: float = Query(..., ge=0),
    last_n: int = Query(10, ge=3, le=30),
    season: str | None = None,
    season_type: str = Query("Regular Season"),
    team_id: int | None = Query(None),
    player_position: str | None = Query(None),
    location: str = Query('all', pattern='^(all|home|away)$'),
    result: str = Query('all', pattern='^(all|win|loss)$'),
    margin_min: float | None = Query(None, ge=0),
    margin_max: float | None = Query(None, ge=0),
    min_minutes: float | None = Query(None, ge=0),
    max_minutes: float | None = Query(None, ge=0),
    min_fga: float | None = Query(None, ge=0),
    max_fga: float | None = Query(None, ge=0),
    h2h_only: bool = Query(False),
    opponent_rank_range: str | None = Query(None),
    without_player_id: int | None = Query(None),
    without_player_ids: list[int] | None = Query(None),
    without_player_name: str | None = Query(None),
    override_opponent_id: int | None = Query(None),
) -> dict[str, Any]:
    selected_season = season or current_nba_season()
    stat = stat.upper()
    return build_prop_analysis_payload(
        player_id=player_id,
        stat=stat,
        line=line,
        last_n=last_n,
        season=selected_season,
        season_type=season_type,
        team_id=team_id,
        player_position=player_position,
        location=location,
        result=result,
        margin_min=margin_min,
        margin_max=margin_max,
        min_minutes=min_minutes,
        max_minutes=max_minutes,
        min_fga=min_fga,
        max_fga=max_fga,
        h2h_only=h2h_only,
        opponent_rank_range=opponent_rank_range,
        without_player_id=without_player_id,
        without_player_ids=without_player_ids,
        without_player_name=without_player_name,
        override_opponent_id=override_opponent_id,
    )

BULK_ANALYSIS_ENABLED = os.getenv("NBA_BULK_ANALYSIS_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
BULK_ANALYSIS_MAX_WORKERS = max(1, int(os.getenv("NBA_BULK_ANALYSIS_MAX_WORKERS", "4")))
BULK_ANALYSIS_MAX_ROWS = max(1, int(os.getenv("NBA_BULK_ANALYSIS_MAX_ROWS", "100")))
# WARM_CACHE_* vars declared at module top (lines 82-89); removed duplicate declarations here.


def _build_bulk_prop_item(row_index: int, row: dict[str, Any], defaults: dict[str, Any], local_cache: dict[tuple[Any, ...], dict[str, Any]]) -> dict[str, Any]:
    player_id_raw = row.get("player_id")
    player_name = str(row.get("player_name") or "").strip()
    stat = str(row.get("stat") or defaults.get("stat") or "").upper().strip()
    if stat not in STAT_MAP:
        raise HTTPException(status_code=400, detail=f"Row {row_index}: unsupported stat '{stat}'.")

    try:
        line = float(row.get("line"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"Row {row_index}: line must be numeric.")

    season = str(row.get("season") or defaults.get("season") or current_nba_season())
    season_type = str(row.get("season_type") or defaults.get("season_type") or "Regular Season")
    last_n = int(row.get("last_n") or defaults.get("last_n") or 10)
    team_id = row.get("team_id", defaults.get("team_id"))
    team_id = int(team_id) if team_id not in (None, "") else None
    player_position = row.get("player_position", defaults.get("player_position"))
    location = str(row.get("location") or defaults.get("location") or "all")
    result = str(row.get("result") or defaults.get("result") or "all")
    margin_min = row.get("margin_min", defaults.get("margin_min"))
    margin_max = row.get("margin_max", defaults.get("margin_max"))
    min_minutes = row.get("min_minutes", defaults.get("min_minutes"))
    max_minutes = row.get("max_minutes", defaults.get("max_minutes"))
    min_fga = row.get("min_fga", defaults.get("min_fga"))
    max_fga = row.get("max_fga", defaults.get("max_fga"))
    h2h_only = bool(row.get("h2h_only", defaults.get("h2h_only", False)))
    debug = bool(row.get("debug", defaults.get("debug", False)))

    player: dict[str, Any] | None = None
    if player_id_raw not in (None, ""):
        try:
            player = PLAYER_LOOKUP.get(int(player_id_raw))
        except (TypeError, ValueError):
            player = None
    if not player and player_name:
        player = find_player_by_name(player_name, team_id=team_id)
    if not player:
        raise HTTPException(status_code=404, detail=f"Row {row_index}: player not found.")

    resolved_player_id = int(player["id"])
    resolved_team_id = team_id or int(player.get("team_id") or 0) or None

    cache_key = (
        resolved_player_id,
        stat,
        float(line),
        int(last_n),
        season,
        season_type,
        resolved_team_id,
        str(player_position or ""),
        location,
        result,
        margin_min,
        margin_max,
        min_minutes,
        max_minutes,
        min_fga,
        max_fga,
        bool(h2h_only),
        bool(debug),
    )

    if cache_key in local_cache:
        analysis = copy.deepcopy(local_cache[cache_key])
    else:
        analysis = build_prop_analysis_payload(
            player_id=resolved_player_id,
            stat=stat,
            line=float(line),
            last_n=int(last_n),
            season=season,
            season_type=season_type,
            team_id=resolved_team_id,
            player_position=player_position,
            location=location,
            result=result,
            margin_min=float(margin_min) if margin_min not in (None, "") else None,
            margin_max=float(margin_max) if margin_max not in (None, "") else None,
            min_minutes=float(min_minutes) if min_minutes not in (None, "") else None,
            max_minutes=float(max_minutes) if max_minutes not in (None, "") else None,
            min_fga=float(min_fga) if min_fga not in (None, "") else None,
            max_fga=float(max_fga) if max_fga not in (None, "") else None,
            h2h_only=bool(h2h_only),
            debug=bool(debug),
            override_opponent_id=int(defaults.get("override_opponent_id")) if defaults.get("override_opponent_id") else None,
        )
        local_cache[cache_key] = copy.deepcopy(analysis)

    return {
        "row": row_index,
        "player_id": resolved_player_id,
        "player_name": analysis.get("player", {}).get("full_name") or player_name,
        "stat": stat,
        "line": float(line),
        "analysis": analysis,
    }


@app.post("/api/player-props/bulk")
def bulk_player_props(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    if not BULK_ANALYSIS_ENABLED:
        raise HTTPException(status_code=503, detail="Bulk analysis endpoint is disabled.")

    rows = payload.get("rows") or []
    defaults = payload.get("defaults") or {}
    requested_max_workers = payload.get("max_workers")

    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="Please provide at least one prop row.")
    if len(rows) > BULK_ANALYSIS_MAX_ROWS:
        raise HTTPException(status_code=400, detail=f"Maximum {BULK_ANALYSIS_MAX_ROWS} prop rows per request.")
    if not isinstance(defaults, dict):
        raise HTTPException(status_code=400, detail="defaults must be an object when provided.")

    try:
        fetch_latest_injury_report_payload()
    except Exception:
        pass

    max_workers = BULK_ANALYSIS_MAX_WORKERS
    try:
        if requested_max_workers not in (None, ""):
            max_workers = max(1, min(BULK_ANALYSIS_MAX_WORKERS, int(requested_max_workers)))
    except (TypeError, ValueError):
        max_workers = BULK_ANALYSIS_MAX_WORKERS
    max_workers = min(max_workers, len(rows))

    local_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    if max_workers <= 1:
        for row_index, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                errors.append({"row": row_index, "reason": "Invalid row format."})
                continue
            try:
                results.append(_build_bulk_prop_item(row_index, row, defaults, local_cache))
            except HTTPException as exc:
                errors.append({"row": row_index, "reason": exc.detail})
            except Exception as exc:
                errors.append({"row": row_index, "reason": str(exc)})
    else:
        futures: list[tuple[int, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for row_index, row in enumerate(rows, start=1):
                if not isinstance(row, dict):
                    errors.append({"row": row_index, "reason": "Invalid row format."})
                    continue
                futures.append((row_index, executor.submit(_build_bulk_prop_item, row_index, row, defaults, local_cache)))
            for row_index, future in futures:
                try:
                    results.append(future.result())
                except HTTPException as exc:
                    errors.append({"row": row_index, "reason": exc.detail})
                except Exception as exc:
                    errors.append({"row": row_index, "reason": str(exc)})

    results.sort(key=lambda item: int(item.get("row") or 0))
    errors.sort(key=lambda item: int(item.get("row") or 0))

    return {
        "count": len(results),
        "error_count": len(errors),
        "results": results,
        "errors": errors,
        "defaults": defaults,
        "max_workers": max_workers,
    }


@app.get("/api/debug/injury-report-raw")
def debug_injury_report_raw() -> dict[str, Any]:
    """Debug endpoint: returns raw extracted PDF text and parsed rows so the
    team-assignment logic can be inspected without guessing."""
    payload = fetch_latest_injury_report_payload()
    raw_text = str(payload.get("raw_text") or "")
    rows = payload.get("rows") or []
    # Group rows by team so misassignments are obvious
    by_team: dict[str, list[str]] = {}
    for r in rows:
        t = str(r.get("team_name") or "UNKNOWN")
        by_team.setdefault(t, []).append(
            f"{r.get('player_display')} | {r.get('status')} | {r.get('reason','')}"
        )
    return {
        "ok": payload.get("ok"),
        "report_label": payload.get("report_label"),
        "parse_method": payload.get("parse_method"),
        "total_rows": len(rows),
        "teams_found": sorted(by_team.keys()),
        "by_team": by_team,
        # First 3000 chars of raw text so we can see what the PDF extraction produced
        "raw_text_preview": raw_text[:3000],
        "raw_text_lines": raw_text.splitlines()[:80],
    }


# ─────────────────────────────────────────────────────────────────────────────
# BACKTEST ENGINE
# Logs predictions and actual results, computes ROI / hit-rate by bucket.
# SQLite-backed backtest store — survives process restarts (ephemeral on Render
# free tier unless a paid Disk is attached, but persists across hot-reloads and
# short sleep/wake cycles within the same deployment).
# ─────────────────────────────────────────────────────────────────────────────

import sqlite3

_BACKTEST_LOCK = threading.Lock()
_BACKTEST_DB_PATH = PERSISTENT_CACHE_DIR / "backtest.sqlite3"


def _backtest_db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(_BACKTEST_DB_PATH), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def _init_backtest_db() -> None:
    """Create the backtest table if it doesn't exist yet."""
    _ensure_persistent_cache_dir()
    with _backtest_db_connect() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS backtest (
                id               TEXT PRIMARY KEY,
                player           TEXT,
                stat             TEXT,
                line             REAL,
                side             TEXT,
                confidence_score INTEGER,
                confidence_tier  TEXT,
                model_prob       REAL,
                odds             REAL,
                result           TEXT DEFAULT 'pending',
                actual_value     REAL,
                logged_at        TEXT,
                resolved_at      TEXT
            )
        """)
        con.commit()


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def _backtest_new_id() -> str:
    import uuid
    return str(uuid.uuid4())[:8]


# Initialise DB at import time (fast – just a CREATE TABLE IF NOT EXISTS).
try:
    _init_backtest_db()
except Exception as _e:
    LOGGER.warning("Could not initialise backtest SQLite DB: %s", _e)


def _compute_backtest_stats(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate hit-rate, ROI, EV accuracy by confidence tier, stat, and odds band."""
    resolved = [e for e in entries if e.get("result") in ("hit", "miss")]
    total = len(resolved)
    if total == 0:
        return {"total": 0, "pending": len(entries) - total}

    hits = sum(1 for e in resolved if e["result"] == "hit")
    win_rate = round(hits / total * 100, 1)

    # ROI: assumes -110 odds (decimal 1.909) unless odds supplied
    roi_list = []
    for e in resolved:
        decimal_odds = float(e.get("odds") or 1.909)
        if e["result"] == "hit":
            roi_list.append(decimal_odds - 1)   # profit on 1 unit
        else:
            roi_list.append(-1.0)
    roi = round(sum(roi_list) / total * 100, 2) if roi_list else 0.0

    # By confidence tier
    tier_stats: dict[str, dict] = {}
    for e in resolved:
        tier = str(e.get("confidence_tier") or "Unknown")
        tier_stats.setdefault(tier, {"hits": 0, "total": 0})
        tier_stats[tier]["total"] += 1
        if e["result"] == "hit":
            tier_stats[tier]["hits"] += 1
    tier_summary = {
        t: {
            "win_rate": round(v["hits"] / v["total"] * 100, 1),
            "total": v["total"],
        }
        for t, v in tier_stats.items()
    }

    # By stat type
    stat_stats: dict[str, dict] = {}
    for e in resolved:
        s = str(e.get("stat") or "Unknown")
        stat_stats.setdefault(s, {"hits": 0, "total": 0})
        stat_stats[s]["total"] += 1
        if e["result"] == "hit":
            stat_stats[s]["hits"] += 1
    stat_summary = {
        s: {
            "win_rate": round(v["hits"] / v["total"] * 100, 1),
            "total": v["total"],
        }
        for s, v in stat_stats.items()
    }

    # By side
    side_stats: dict[str, dict] = {}
    for e in resolved:
        sd = str(e.get("side") or "Unknown")
        side_stats.setdefault(sd, {"hits": 0, "total": 0})
        side_stats[sd]["total"] += 1
        if e["result"] == "hit":
            side_stats[sd]["hits"] += 1
    side_summary = {
        sd: {
            "win_rate": round(v["hits"] / v["total"] * 100, 1),
            "total": v["total"],
        }
        for sd, v in side_stats.items()
    }

    return {
        "total": total,
        "pending": len(entries) - total,
        "hits": hits,
        "misses": total - hits,
        "win_rate": win_rate,
        "roi_pct": roi,
        "by_tier": tier_summary,
        "by_stat": stat_summary,
        "by_side": side_summary,
    }


@app.post("/api/backtest/log")
def backtest_log_prediction(payload: dict = Body(...)) -> dict[str, Any]:
    """
    Log a prediction before a game.
    Body: { player, stat, line, side, confidence_score, confidence_tier, model_prob, odds? }
    """
    entry = {
        "id": _backtest_new_id(),
        "player": str(payload.get("player") or ""),
        "stat": str(payload.get("stat") or ""),
        "line": float(payload.get("line") or 0),
        "side": str(payload.get("side") or ""),
        "confidence_score": int(payload.get("confidence_score") or 0),
        "confidence_tier": str(payload.get("confidence_tier") or ""),
        "model_prob": float(payload.get("model_prob") or 0.5),
        "odds": payload.get("odds"),
        "result": "pending",
        "actual_value": None,
        "logged_at": datetime.utcnow().isoformat() + "Z",
        "resolved_at": None,
    }
    with _BACKTEST_LOCK:
        try:
            with _backtest_db_connect() as con:
                con.execute(
                    """INSERT INTO backtest
                       (id, player, stat, line, side, confidence_score, confidence_tier,
                        model_prob, odds, result, actual_value, logged_at, resolved_at)
                       VALUES (:id,:player,:stat,:line,:side,:confidence_score,:confidence_tier,
                               :model_prob,:odds,:result,:actual_value,:logged_at,:resolved_at)""",
                    entry,
                )
                con.commit()
        except Exception as db_err:
            LOGGER.warning("backtest SQLite insert failed: %s", db_err)
    return {"ok": True, "id": entry["id"], "entry": entry}


@app.post("/api/backtest/resolve")
def backtest_resolve_prediction(payload: dict = Body(...)) -> dict[str, Any]:
    """
    Mark a logged prediction as hit or miss.
    Body: { id, actual_value }
    """
    pred_id = str(payload.get("id") or "")
    actual_value = payload.get("actual_value")
    if actual_value is None:
        raise HTTPException(status_code=400, detail="actual_value is required")
    actual_value = float(actual_value)

    with _BACKTEST_LOCK:
        try:
            with _backtest_db_connect() as con:
                row = con.execute("SELECT * FROM backtest WHERE id = ?", (pred_id,)).fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail=f"No prediction found with id={pred_id}")
                entry = _row_to_dict(row)
                hit = (
                    actual_value > entry["line"] if entry["side"] == "OVER"
                    else actual_value < entry["line"]
                )
                result = "hit" if hit else "miss"
                resolved_at = datetime.utcnow().isoformat() + "Z"
                con.execute(
                    "UPDATE backtest SET result=?, actual_value=?, resolved_at=? WHERE id=?",
                    (result, actual_value, resolved_at, pred_id),
                )
                con.commit()
                entry.update(result=result, actual_value=actual_value, resolved_at=resolved_at)
                return {"ok": True, "entry": entry}
        except HTTPException:
            raise
        except Exception as db_err:
            LOGGER.warning("backtest SQLite resolve failed: %s", db_err)
            raise HTTPException(status_code=500, detail="Database error")


@app.get("/api/backtest/log")
def backtest_get_log(limit: int = Query(200, ge=1, le=1000)) -> dict[str, Any]:
    """Return the backtest log and aggregate stats."""
    with _BACKTEST_LOCK:
        try:
            with _backtest_db_connect() as con:
                rows = con.execute(
                    "SELECT * FROM backtest ORDER BY logged_at DESC LIMIT ?", (limit,)
                ).fetchall()
                all_rows = con.execute("SELECT * FROM backtest").fetchall()
            entries = [_row_to_dict(r) for r in rows]
            all_entries = [_row_to_dict(r) for r in all_rows]
            stats = _compute_backtest_stats(all_entries)
            return {"stats": stats, "entries": entries}
        except Exception as db_err:
            LOGGER.warning("backtest SQLite get failed: %s", db_err)
            return {"stats": {}, "entries": []}


@app.delete("/api/backtest/log/{entry_id}")
def backtest_delete_entry(entry_id: str) -> dict[str, Any]:
    """Delete a single backtest entry by ID."""
    with _BACKTEST_LOCK:
        try:
            with _backtest_db_connect() as con:
                cur = con.execute("DELETE FROM backtest WHERE id = ?", (entry_id,))
                con.commit()
                if cur.rowcount == 0:
                    raise HTTPException(status_code=404, detail=f"No entry found with id={entry_id}")
            return {"ok": True, "deleted": entry_id}
        except HTTPException:
            raise
        except Exception as db_err:
            LOGGER.warning("backtest SQLite delete failed: %s", db_err)
            raise HTTPException(status_code=500, detail="Database error")


@app.delete("/api/backtest/log")
def backtest_clear_log() -> dict[str, Any]:
    """Clear all backtest entries."""
    with _BACKTEST_LOCK:
        try:
            with _backtest_db_connect() as con:
                cur = con.execute("SELECT COUNT(*) FROM backtest")
                count = cur.fetchone()[0]
                con.execute("DELETE FROM backtest")
                con.commit()
            return {"ok": True, "cleared": count}
        except Exception as db_err:
            LOGGER.warning("backtest SQLite clear failed: %s", db_err)
            raise HTTPException(status_code=500, detail="Database error")


@app.post("/api/odds/check-quota")
def odds_check_quota(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Check remaining Odds API quota without loading events (uses a lightweight sports list call)."""
    api_key = str(payload.get("api_key") or "").strip()
    if not api_key:
        raise HTTPException(status_code=400, detail="Missing API key.")
    # Hit the /sports endpoint — it's the cheapest call (costs 0 credits on free tier)
    result = odds_api_fetch("/sports", api_key, {"all": "false"})
    return {
        "ok": True,
        "quota": result["quota"],
        "api_key_masked": mask_api_key_for_display(api_key),
    }

