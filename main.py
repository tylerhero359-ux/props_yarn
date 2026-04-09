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
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from threading import Lock
from typing import Any

import requests
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
from identity_utils import (
    PlayerSearchIndex,
    build_player_name_variants,
    build_team_alias_lookup,
    normalize_compact_text,
    normalize_name,
    normalize_report_person_name,
)
from injury_service import InjuryReportService
from player_data_service import PlayerDataService
from schedule_service import ScheduleDataService
from runtime_utils import (
    build_timed_call,
    call_with_retries,
    create_retry_http_session,
    is_transient_request_error,
    load_persistent_caches as runtime_load_persistent_caches,
    save_persistent_caches as runtime_save_persistent_caches,
)

LOGGER = logging.getLogger("nba_props_app")
if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

NBA_TIMING_ENABLED = os.getenv("NBA_TIMING_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
NBA_TIMING_LOG_ALL = os.getenv("NBA_TIMING_LOG_ALL", "0").strip().lower() in {"1", "true", "yes", "on"}
NBA_TIMING_SLOW_MS = float(os.getenv("NBA_TIMING_SLOW_MS", "800"))

ANALYSIS_PARALLEL_ENABLED = os.getenv("NBA_ANALYSIS_PARALLEL_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
ANALYSIS_PARALLEL_MAX_WORKERS = max(1, int(os.getenv("NBA_ANALYSIS_PARALLEL_MAX_WORKERS", "4")))

timed_call = build_timed_call(
    logger=LOGGER,
    enabled=NBA_TIMING_ENABLED,
    log_all=NBA_TIMING_LOG_ALL,
    slow_ms=NBA_TIMING_SLOW_MS,
)

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
BACKTEST_PERSIST_PATH = PERSISTENT_CACHE_DIR / "backtest_log.json"
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


def save_persistent_caches() -> None:
    runtime_save_persistent_caches(
        enabled=PERSISTENT_CACHE_ENABLED,
        cache_dir=PERSISTENT_CACHE_DIR,
        cache_path=PERSISTENT_CACHE_PATH,
        cache_lock=PERSISTENT_CACHE_LOCK,
        named_caches={
            "roster_cache": (ROSTER_CACHE, PERSISTENT_CACHE_MAX_ROSTERS),
            "player_info_cache": (PLAYER_INFO_CACHE, PERSISTENT_CACHE_MAX_PLAYER_INFO),
            "next_game_cache": (NEXT_GAME_CACHE, PERSISTENT_CACHE_MAX_NEXT_GAMES),
            "team_next_game_cache": (TEAM_NEXT_GAME_CACHE, PERSISTENT_CACHE_MAX_NEXT_GAMES),
        },
        logger=LOGGER,
    )


def load_persistent_caches() -> None:
    runtime_load_persistent_caches(
        enabled=PERSISTENT_CACHE_ENABLED,
        cache_path=PERSISTENT_CACHE_PATH,
        cache_lock=PERSISTENT_CACHE_LOCK,
        named_targets={
            "roster_cache": ROSTER_CACHE,
            "player_info_cache": PLAYER_INFO_CACHE,
            "next_game_cache": NEXT_GAME_CACHE,
            "team_next_game_cache": TEAM_NEXT_GAME_CACHE,
        },
        logger=LOGGER,
    )
    if PERSISTENT_CACHE_ENABLED and PERSISTENT_CACHE_PATH.exists():
        LOGGER.info(
            "Loaded persistent cache snapshot: %s rosters, %s player info, %s next games, %s team next games",
            len(ROSTER_CACHE),
            len(PLAYER_INFO_CACHE),
            len(NEXT_GAME_CACHE),
            len(TEAM_NEXT_GAME_CACHE),
        )


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
TEAM_ALIAS_LOOKUP = build_team_alias_lookup(TEAM_POOL)
PLAYER_SEARCH_INDEX = PlayerSearchIndex.build(PLAYER_POOL)
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
TEAM_OPPORTUNITY_CACHE: dict[tuple[str, str, str, str], dict[str, Any]] = {}
INJURY_REPORT_TTL_SECONDS = 600
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


def _build_analysis_cache_key(
    *,
    player_id: int,
    stat: str,
    line: float,
    last_n: int,
    season: str,
    season_type: str,
    team_id: int | None,
    player_position: str | None,
    location: str,
    result: str,
    margin_min: float | None,
    margin_max: float | None,
    min_minutes: float | None,
    max_minutes: float | None,
    min_fga: float | None,
    max_fga: float | None,
    h2h_only: bool,
    opponent_rank_range: str | None,
    without_player_ids: list[int],
    without_player_name: str | None,
    override_opponent_id: int | None,
    debug: bool,
) -> tuple[Any, ...]:
    cached_injury_payload = INJURY_SERVICE.report_cache.get("payload") or {}
    report_label = str(cached_injury_payload.get("report_label") or "")
    report_url = str(cached_injury_payload.get("report_url") or "")
    return (
        ANALYSIS_CACHE_SCHEMA_VERSION,
        int(player_id),
        str(stat).upper().strip(),
        float(line),
        int(last_n),
        str(season),
        str(season_type),
        int(team_id) if team_id not in (None, "") else None,
        str(player_position or "").strip(),
        str(location or "all").lower(),
        str(result or "all").lower(),
        float(margin_min) if margin_min not in (None, "") else None,
        float(margin_max) if margin_max not in (None, "") else None,
        float(min_minutes) if min_minutes not in (None, "") else None,
        float(max_minutes) if max_minutes not in (None, "") else None,
        float(min_fga) if min_fga not in (None, "") else None,
        float(max_fga) if max_fga not in (None, "") else None,
        bool(h2h_only),
        str(opponent_rank_range or "").strip().lower(),
        tuple(normalize_without_player_ids(without_player_ids)),
        str(without_player_name or "").strip(),
        int(override_opponent_id) if override_opponent_id not in (None, "") else None,
        bool(debug),
        report_label,
        report_url,
    )
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


NBA_HTTP = create_retry_http_session(
    user_agent=NBA_USER_AGENT,
    retry_total=NBA_RETRY_TOTAL,
    backoff_factor=NBA_BACKOFF_FACTOR,
    retry_status_codes=NBA_RETRY_STATUS_CODES,
)


def is_transient_nba_error(exc: Exception) -> bool:
    return is_transient_request_error(exc)


def nba_http_get(url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None, timeout: tuple[int, int] | int | None = None) -> requests.Response:
    response = NBA_HTTP.get(url, params=params, headers=headers, timeout=timeout or NBA_REQUEST_TIMEOUT)
    response.raise_for_status()
    return response


INJURY_SERVICE = InjuryReportService(
    team_pool=TEAM_POOL,
    http_get=nba_http_get,
    timed_call=timed_call,
    team_alias_lookup=TEAM_ALIAS_LOOKUP,
    pdfplumber_module=pdfplumber,
    page_timeout=INJURY_REPORT_PAGE_TIMEOUT,
    pdf_timeout=INJURY_REPORT_PDF_TIMEOUT,
    report_ttl_seconds=INJURY_REPORT_TTL_SECONDS if 'INJURY_REPORT_TTL_SECONDS' in globals() else 600,
    links_ttl_seconds=INJURY_REPORT_LINKS_TTL_SECONDS,
    failure_cooldown_seconds=INJURY_REPORT_FAILURE_COOLDOWN_SECONDS,
    max_stale_seconds=INJURY_REPORT_MAX_STALE_SECONDS,
)
INJURY_STATUS_ORDER = INJURY_SERVICE.status_order
UNAVAILABLE_STATUSES = INJURY_SERVICE.unavailable_statuses
RISKY_STATUSES = INJURY_SERVICE.risky_statuses
GOOD_STATUSES = INJURY_SERVICE.good_statuses
REPORT_STATUSES = INJURY_SERVICE.report_statuses
STATUS_PATTERN = INJURY_SERVICE.status_pattern


def call_nba_with_retries(factory, *, label: str, attempts: int = NBA_RETRY_TOTAL, base_delay: float = NBA_BACKOFF_FACTOR):
    return call_with_retries(
        factory,
        label=label,
        attempts=attempts,
        base_delay=base_delay,
        retry_predicate=is_transient_nba_error,
        before_attempt=throttle_request,
    )


def current_nba_season() -> str:
    now = datetime.now(ZoneInfo("America/New_York"))
    year = now.year
    if now.month >= 10:
        start_year = year
    else:
        start_year = year - 1
    end_year_short = str(start_year + 1)[-2:]
    return f"{start_year}-{end_year_short}"


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
ODDS_GAME_CONTEXT_MARKETS = [
    "spreads",
    "totals",
]
ODDS_PARLAY_MARKETS = [*ODDS_DEFAULT_MARKETS, *ODDS_GAME_CONTEXT_MARKETS]
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
ODDS_DEFAULT_BOOKMAKERS = ["draftkings", "fanduel", "betmgm"]


def parse_requested_bookmakers(value: Any) -> list[str]:
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = str(value or "").split(",")
    seen: set[str] = set()
    result: list[str] = []
    for item in raw_items:
        book = str(item or "").strip().lower()
        if not book or book in seen:
            continue
        seen.add(book)
        result.append(book)
    return result


def normalize_line_key(line_value: Any) -> str:
    try:
        numeric = float(line_value)
    except (TypeError, ValueError):
        return str(line_value)
    if abs(numeric - round(numeric)) < 1e-9:
        return str(int(round(numeric)))
    return f"{numeric:.3f}".rstrip("0").rstrip(".")


def safe_mean(values: list[float]) -> float | None:
    cleaned = [float(v) for v in values if v is not None]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def safe_float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def devig_two_way_market(over_odds: float | None, under_odds: float | None) -> dict[str, float] | None:
    if over_odds is None or under_odds is None:
        return None
    if over_odds <= 1.0 or under_odds <= 1.0:
        return None
    over_raw = 1.0 / float(over_odds)
    under_raw = 1.0 / float(under_odds)
    total = over_raw + under_raw
    if total <= 0.0:
        return None
    return {
        "over_raw_prob": over_raw,
        "under_raw_prob": under_raw,
        "over_fair_prob": over_raw / total,
        "under_fair_prob": under_raw / total,
        "hold": max(0.0, total - 1.0),
    }


def build_event_market_context(event_payload: dict[str, Any]) -> dict[str, Any]:
    home_team = str(event_payload.get("home_team") or "").strip()
    away_team = str(event_payload.get("away_team") or "").strip()
    home_spreads: list[float] = []
    away_spreads: list[float] = []
    totals: list[float] = []

    for bookmaker in event_payload.get("bookmakers") or []:
        for market in bookmaker.get("markets") or []:
            market_key = str(market.get("key") or "").strip().lower()
            outcomes = market.get("outcomes") or []
            if market_key == "spreads":
                home_spread = None
                away_spread = None
                for outcome in outcomes:
                    point = safe_float_or_none(outcome.get("point"))
                    if point is None:
                        continue
                    outcome_name = str(outcome.get("name") or "").strip()
                    if outcome_name == home_team:
                        home_spread = point
                    elif outcome_name == away_team:
                        away_spread = point
                if home_spread is None and away_spread is not None:
                    home_spread = -away_spread
                if away_spread is None and home_spread is not None:
                    away_spread = -home_spread
                if home_spread is not None and away_spread is not None:
                    home_spreads.append(home_spread)
                    away_spreads.append(away_spread)
            elif market_key == "totals":
                market_total = next(
                    (safe_float_or_none(outcome.get("point")) for outcome in outcomes if safe_float_or_none(outcome.get("point")) is not None),
                    None,
                )
                if market_total is not None:
                    totals.append(market_total)

    home_spread_avg = safe_mean(home_spreads)
    away_spread_avg = safe_mean(away_spreads)
    game_total_avg = safe_mean(totals)

    home_implied_total = None
    away_implied_total = None
    if game_total_avg is not None:
        if home_spread_avg is not None:
            home_implied_total = (game_total_avg / 2.0) - (home_spread_avg / 2.0)
            away_implied_total = game_total_avg - home_implied_total
        elif away_spread_avg is not None:
            away_implied_total = (game_total_avg / 2.0) - (away_spread_avg / 2.0)
            home_implied_total = game_total_avg - away_implied_total

    return {
        "market_game_total": round(float(game_total_avg), 1) if game_total_avg is not None else None,
        "market_home_spread": round(float(home_spread_avg), 1) if home_spread_avg is not None else None,
        "market_away_spread": round(float(away_spread_avg), 1) if away_spread_avg is not None else None,
        "market_home_implied_total": round(float(home_implied_total), 1) if home_implied_total is not None else None,
        "market_away_implied_total": round(float(away_implied_total), 1) if away_implied_total is not None else None,
    }


def _event_matches_teams(
    event_payload: dict[str, Any],
    *,
    team_name: str = "",
    opponent_name: str = "",
    team_abbreviation: str = "",
    opponent_abbreviation: str = "",
) -> bool:
    home_team = str(event_payload.get("home_team") or "").strip()
    away_team = str(event_payload.get("away_team") or "").strip()
    home_norm = normalize_name(home_team)
    away_norm = normalize_name(away_team)
    provided_names = {normalize_name(team_name), normalize_name(opponent_name)} - {""}
    provided_abbrs = {normalize_name(team_abbreviation), normalize_name(opponent_abbreviation)} - {""}

    home_aliases = {home_norm}
    away_aliases = {away_norm}
    home_resolved = resolve_team_from_text(home_team)
    away_resolved = resolve_team_from_text(away_team)
    if home_resolved:
        home_aliases.add(normalize_name(home_resolved.get("abbreviation") or ""))
        home_aliases.add(normalize_name(home_resolved.get("full_name") or ""))
    if away_resolved:
        away_aliases.add(normalize_name(away_resolved.get("abbreviation") or ""))
        away_aliases.add(normalize_name(away_resolved.get("full_name") or ""))

    event_aliases = home_aliases | away_aliases
    if provided_names and not provided_names.issubset(event_aliases):
        return False
    if provided_abbrs and not provided_abbrs.issubset(event_aliases):
        return False
    return bool(provided_names or provided_abbrs)


def approximate_odds_api_region_equivalent(bookmakers: list[str]) -> int:
    count = max(1, len(bookmakers or []))
    return max(1, math.ceil(count / 10.0))


def build_odds_api_cost_hint(markets: str, bookmakers: list[str]) -> dict[str, Any]:
    market_count = len([item for item in str(markets or "").split(",") if str(item).strip()]) or 1
    region_equivalent = approximate_odds_api_region_equivalent(bookmakers)
    return {
        "bookmakers_count": len(bookmakers),
        "region_equivalent": region_equivalent,
        "market_count": market_count,
        "estimated_request_cost": market_count * region_equivalent,
        "note": "Approximation based on The Odds API pricing model for markets x region-equivalent; up to 10 explicitly requested bookmakers is typically 1 region-equivalent.",
    }

def report_name_variants(name: str) -> set[str]:
    """Backward-compatible alias for player name variant generation."""
    return build_player_name_variants(name)


def compute_recent_hit_streak(hit_flags: list[bool]) -> int:
    streak = 0
    for hit in hit_flags:
        if hit:
            streak += 1
        else:
            break
    return streak


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
    return TEAM_ALIAS_LOOKUP.get(normalize_name(team_text))


def resolve_opponent_team_from_matchup(matchup: str, player_team_id: int | None) -> dict[str, Any] | None:
    info = parse_matchup_descriptor(matchup)
    team_abbr = info.get("team_abbreviation", "")
    opp_abbr = info.get("opponent_abbreviation", "")
    team_candidate = TEAM_ALIAS_LOOKUP.get(normalize_name(team_abbr)) if team_abbr else None
    opp_candidate = TEAM_ALIAS_LOOKUP.get(normalize_name(opp_abbr)) if opp_abbr else None
    if player_team_id is not None:
        if opp_candidate and int(opp_candidate.get("id") or 0) != player_team_id:
            return opp_candidate
        if team_candidate and int(team_candidate.get("id") or 0) != player_team_id:
            return team_candidate
    return opp_candidate or team_candidate


def find_player_by_name(player_name: str, team_id: int | None = None) -> dict[str, Any] | None:
    roster_ids: set[int] | None = None
    if team_id is not None:
        try:
            roster = fetch_team_roster(team_id=team_id, season=current_nba_season())
            roster_ids = {int(row.get("PLAYER_ID")) for row in roster if row.get("PLAYER_ID") not in (None, "")}
        except HTTPException:
            roster_ids = None
    return PLAYER_SEARCH_INDEX.find_player(player_name, roster_ids=roster_ids)


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


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

    variance_adjustment = 0.0
    if variance:
        median = float(variance.get("median") or average)
        consistency_score = float(variance.get("consistency_score") or 50.0)
        p25 = float(variance.get("p25") or 0.0)
        p75 = float(variance.get("p75") or 0.0)
        median_edge_term = clamp((median - line) / scale, -0.18, 0.18)
        noisy_stats = {"STL", "BLK", "3PM"}
        median_weight = 0.65 if stat in noisy_stats else 0.45
        edge_term = edge_term * (1 - median_weight) + median_edge_term * median_weight
        consistency_factor = consistency_score / 100.0
        variance_adjustment = clamp((consistency_factor - 0.5) * 0.10, -0.06, 0.06)
        if p25 > line:
            variance_adjustment += 0.05
        elif p75 < line:
            variance_adjustment -= 0.05
        cv = float(variance.get("cv") or 0.0)
        if cv > 0.5:
            edge_term *= max(0.4, 1.0 - (cv - 0.5))

    matchup_term = 0.0
    if matchup_delta_pct is not None:
        noisy_stats = {"STL", "BLK"}
        matchup_weight = 0.08 if stat in noisy_stats else 0.2
        matchup_term = clamp(matchup_delta_pct / 100.0 * matchup_weight, -0.12, 0.12)

    role_term = 0.0
    if opportunity.get("minutes_trend") == "up":
        role_term += 0.03
    elif opportunity.get("minutes_trend") == "down":
        role_term -= 0.04

    scoring_stats = {"PTS", "3PM", "PRA", "PR", "PA"}
    if opportunity.get("volume_trend") == "up":
        role_term += 0.04 if stat in scoring_stats else 0.02
    elif opportunity.get("volume_trend") == "down":
        role_term -= 0.04 if stat in scoring_stats else 0.02

    if int(team_context.get("impact_count") or 0) > 0:
        role_term += min(0.03, int(team_context.get("impact_count") or 0) * 0.01)

    environment_term = 0.0
    if environment.get("is_back_to_back"):
        environment_term -= 0.03
    elif isinstance(environment.get("rest_days"), int) and environment.get("rest_days") >= 2:
        environment_term += 0.02
    market_signal = derive_market_environment_signal(stat, environment)
    environment_term += float(market_signal.get("over_adjustment") or 0.0)

    model_over = clamp(base + edge_term + matchup_term + role_term + environment_term + variance_adjustment, 0.02, 0.98)
    return round(model_over, 4), round(1 - model_over, 4)


def fetch_position_dash(season: str, season_type: str, position_code: str, opponent_team_id: int = 0) -> list[dict[str, Any]]:
    normalized_position = str(position_code or "").upper().strip()
    normalized_season = str(season or "").strip()
    normalized_season_type = str(season_type or "Regular Season").strip()
    cache_key = (normalized_season, normalized_season_type, normalized_position, int(opponent_team_id or 0))
    cached = POSITION_DASH_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < POSITION_TTL_SECONDS:
        return [dict(row) for row in (cached.get("rows") or [])]

    if not normalized_position:
        return []

    try:
        throttle_request()
        response = call_nba_with_retries(
            lambda: LeagueDashPlayerStats(
                season=normalized_season,
                season_type_all_star=normalized_season_type,
                per_mode_detailed="PerGame",
                measure_type_detailed_defense="Base",
                player_position_abbreviation_nullable=normalized_position,
                opponent_team_id=int(opponent_team_id or 0),
                timeout=15,
            ),
            label="position dash request",
            attempts=2,
            base_delay=0.6,
        )
        df = response.get_data_frames()[0]
        rows = df.to_dict(orient="records") if not df.empty else []
    except Exception:
        rows = []

    POSITION_DASH_CACHE[cache_key] = {"timestamp": now_ts, "rows": [dict(row) for row in rows]}
    return [dict(row) for row in rows]


def summarize_position_environment(rows: list[dict[str, Any]], stat: str) -> dict[str, Any] | None:
    if not rows:
        return None
    total_gp = 0.0
    total_weighted_value = 0.0
    players_count = 0
    for row in rows:
        gp = float(row.get("GP") or 0)
        if gp <= 0:
            continue
        per_game_value = compute_stat_value(row, stat)
        players_count += 1
        total_gp += gp
        total_weighted_value += per_game_value * gp
    if total_gp <= 0:
        return None
    return {
        "players_count": players_count,
        "sample_gp": round(total_gp, 1),
        # LeagueDashPlayerStats returns per-game player rows, so we need a
        # GP-weighted average instead of dividing those per-game values by GP again.
        "per_player_game": round(total_weighted_value / total_gp, 2),
        "total_value": round(total_weighted_value, 1),
    }


def build_position_matchup(
    opponent_team_id: int,
    position_code: str,
    stat: str,
    season: str,
    season_type: str,
) -> dict[str, Any] | None:
    normalized_position = str(position_code or "").upper().strip()
    normalized_stat = str(stat or "").upper().strip()
    normalized_season = str(season or "").strip()
    normalized_season_type = str(season_type or "Regular Season").strip()
    cache_key = (int(opponent_team_id or 0), normalized_position, normalized_stat, normalized_season, normalized_season_type)
    cached = POSITION_MATCHUP_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < POSITION_TTL_SECONDS:
        payload = cached.get("payload")
        return dict(payload) if isinstance(payload, dict) else None

    if int(opponent_team_id or 0) <= 0 or not normalized_position or normalized_stat not in STAT_MAP:
        return None

    league_rows = fetch_position_dash(normalized_season, normalized_season_type, normalized_position, opponent_team_id=0)
    opponent_rows = fetch_position_dash(normalized_season, normalized_season_type, normalized_position, opponent_team_id=int(opponent_team_id))
    league_summary = summarize_position_environment(league_rows, normalized_stat)
    opponent_summary = summarize_position_environment(opponent_rows, normalized_stat)
    if not league_summary or not opponent_summary:
        POSITION_MATCHUP_CACHE[cache_key] = {"timestamp": now_ts, "payload": None}
        return None

    league_average = float(league_summary.get("per_player_game") or 0.0)
    opponent_value = float(opponent_summary.get("per_player_game") or 0.0)
    if league_average <= 0:
        POSITION_MATCHUP_CACHE[cache_key] = {"timestamp": now_ts, "payload": None}
        return None

    delta_pct = round(((opponent_value - league_average) / league_average) * 100.0, 1)
    if delta_pct >= 8:
        lean = "Soft position matchup"
        lean_tone = "good"
    elif delta_pct >= 3:
        lean = "Favorable matchup"
        lean_tone = "good"
    elif delta_pct <= -8:
        lean = "Tough position matchup"
        lean_tone = "bad"
    elif delta_pct <= -3:
        lean = "Below-average matchup"
        lean_tone = "bad"
    else:
        lean = "Neutral"
        lean_tone = "neutral"

    payload = {
        "opponent_team_id": int(opponent_team_id),
        "position_code": normalized_position,
        "position_label": POSITION_LABELS.get(normalized_position, normalized_position),
        "stat": normalized_stat,
        "opponent_value": round(opponent_value, 2),
        "league_average": round(league_average, 2),
        "delta_pct": delta_pct,
        "sample_gp": round(float(opponent_summary.get("sample_gp") or 0.0), 1),
        "players_count": int(opponent_summary.get("players_count") or 0),
        "lean": lean,
        "lean_tone": lean_tone,
    }
    POSITION_MATCHUP_CACHE[cache_key] = {"timestamp": now_ts, "payload": dict(payload)}
    return dict(payload)


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
    location: str = "all",
    result: str = "all",
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
    player = PLAYER_LOOKUP.get(int(player_id))
    if not player:
        raise HTTPException(status_code=404, detail="Player not found.")

    normalized_without_player_ids = normalize_without_player_ids(without_player_ids)
    if not normalized_without_player_ids and without_player_id:
        normalized_without_player_ids = [int(without_player_id)]
    normalized_without_player_names = resolve_without_player_names(normalized_without_player_ids)
    if not normalized_without_player_names and without_player_name:
        fallback_name = str(without_player_name).strip()
        if fallback_name:
            normalized_without_player_names = [fallback_name]

    analysis_cache_key = _build_analysis_cache_key(
        player_id=player_id,
        stat=stat,
        line=line,
        last_n=last_n,
        season=season,
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
        without_player_ids=normalized_without_player_ids,
        without_player_name=without_player_name,
        override_opponent_id=override_opponent_id,
        debug=debug,
    )
    now_ts = time.time()
    cached_analysis = ANALYSIS_CACHE.get(analysis_cache_key)
    if cached_analysis and now_ts - float(cached_analysis.get("timestamp") or 0.0) < ANALYSIS_CACHE_TTL_SECONDS:
        cached_payload = copy.deepcopy(cached_analysis.get("payload") or {})
        if debug and DEBUG_METADATA_ENABLED:
            cached_payload.setdefault("debug", {})
            cached_payload["debug"]["cache_status"] = {
                **dict((cached_payload.get("debug") or {}).get("cache_status") or {}),
                "analysis_cache": "hit",
            }
        return cached_payload

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

    next_game_meta = {"source": "live", "seconds_ago": None, "refresh_queued": False}
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
            next_game, next_game_meta = get_team_next_game_hybrid(team_id=resolved_team_id, primary_player_id=player_id, season=season, season_type=season_type)
    else:
        next_game, next_game_meta = get_team_next_game_hybrid(team_id=resolved_team_id, primary_player_id=player_id, season=season, season_type=season_type)

    needs_margin_context = margin_min is not None or margin_max is not None
    enriched_rows = enrich_game_logs_with_context(season_rows, resolved_team_id, season, season_type, player_id) if needs_margin_context else enrich_game_logs_light(season_rows)

    opponent_rank_min, opponent_rank_max, normalized_opponent_rank_range = normalize_opponent_rank_range(opponent_rank_range)
    if opponent_rank_min is not None or opponent_rank_max is not None:
        team_rank_map = build_team_rank_map(season=season, season_type=season_type)
        for row in enriched_rows:
            opp_team = resolve_opponent_team_from_matchup(str(row.get("MATCHUP") or ""), resolved_team_id)
            opp_team_id = int(opp_team["id"]) if opp_team else None
            row["_opponent_rank"] = team_rank_map.get(opp_team_id) if opp_team_id else None
    else:
        for row in enriched_rows:
            row.setdefault("_opponent_rank", None)

    without_player_game_ids = build_without_player_union_game_ids(normalized_without_player_ids, season=season, season_type=season_type)
    without_player_name = ", ".join(normalized_without_player_names)

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
        opponent_abbreviation=(next_game or {}).get("opponent_abbreviation"),
        opponent_rank_min=opponent_rank_min,
        opponent_rank_max=opponent_rank_max,
        without_player_game_ids=without_player_game_ids,
    )
    rows = filtered_pool[:last_n]
    stat_summary = build_stat_summary_block(rows, stat, line)
    games = stat_summary["games"]
    values = stat_summary["values"]
    hit_count = stat_summary["hit_count"]
    average = stat_summary["average"]
    hit_rate = stat_summary["hit_rate"]
    variance_data = stat_summary.get("variance") or {}

    team_name = TEAM_LOOKUP.get(resolved_team_id, {}).get("full_name") if resolved_team_id else None
    availability = build_availability_payload(player_name=player["full_name"], team_name=team_name)
    vs_position = build_position_matchup(opponent_team_id=int((next_game or {}).get("opponent_team_id") or 0), position_code=position_code or "", stat=stat, season=season, season_type=season_type) if next_game and position_code else None
    h2h_payload = build_h2h_payload_from_rows(filtered_pool or enriched_rows, next_game, stat, line)
    analysis_source_rows = filtered_pool or enriched_rows or season_rows
    opportunity = build_opportunity_context(analysis_source_rows, last_n)
    team_context = build_team_opportunity_context(team_name=team_name, player_name=player["full_name"], stat=stat, player_position=resolved_position, team_id=resolved_team_id, season=season)
    environment = build_game_environment_context(analysis_source_rows, next_game, team_id=resolved_team_id, season=season, season_type=season_type)
    matchup_delta_pct = (vs_position or {}).get("delta_pct") if isinstance(vs_position, dict) else None
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
    recommended_side = "OVER" if model_over >= model_under else "UNDER"
    side_model_prob = model_over if recommended_side == "OVER" else model_under
    chosen_edge = round((side_model_prob - 0.50) * 100, 1)
    chosen_ev = side_model_prob - 0.50
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

    traffic_tone = "yellow"
    traffic_label = "Caution"
    if availability.get("is_unavailable"):
        traffic_tone = "red"
        traffic_label = "Avoid"
    elif confidence_engine["score"] >= 80:
        traffic_tone = "green"
        traffic_label = f"Strong {recommended_side}"
    elif confidence_engine["score"] >= 68:
        traffic_tone = "green"
        traffic_label = f"Lean {recommended_side}"
    elif confidence_engine["score"] <= 44:
        traffic_tone = "red"
        traffic_label = "Pass"

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

    payload = {
        "player": {
            "id": player["id"],
            "full_name": player["full_name"],
            "is_active": player.get("is_active", False),
            "team_id": resolved_team_id,
            "team_name": team_name or "",
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
        "matchup": {"next_game": next_game, "vs_position": vs_position},
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
        "sample_warning": f"Only {len(values)} games in filtered sample â€” treat recommendation with caution." if len(values) < 5 else None,
    }
    freshness = {
        "game_log_seconds_ago": round(time.time() - float((GAME_LOG_CACHE.get((player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) or {}).get("timestamp") or 0.0), 2) if GAME_LOG_CACHE.get((player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) else None,
        "player_info_seconds_ago": round(time.time() - float((PLAYER_INFO_CACHE.get(player_id) or {}).get("timestamp") or 0.0), 2) if PLAYER_INFO_CACHE.get(player_id) else None,
        "next_game_seconds_ago": round(time.time() - float((TEAM_NEXT_GAME_CACHE.get((resolved_team_id, season, season_type)) or {}).get("timestamp") or 0.0), 2) if TEAM_NEXT_GAME_CACHE.get((resolved_team_id, season, season_type)) else None,
        "game_log_source": game_log_meta.get("source"),
        "game_log_refresh_queued": game_log_meta.get("refresh_queued"),
        "player_info_source": player_info_meta.get("source"),
        "player_info_refresh_queued": player_info_meta.get("refresh_queued"),
        "next_game_source": next_game_meta.get("source"),
        "next_game_refresh_queued": next_game_meta.get("refresh_queued"),
        "injury_report_seconds_ago": INJURY_SERVICE.report_cache_age_seconds(),
    }
    payload["freshness"] = dict(freshness)
    if debug and DEBUG_METADATA_ENABLED:
        payload["debug"] = build_debug_metadata(
            cache_status={
                "analysis_cache": "miss",
                "game_log_cache": "hit" if GAME_LOG_CACHE.get((player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) else "miss",
                "player_info_cache": "hit" if PLAYER_INFO_CACHE.get(player_id) else "miss",
                "next_game_cache": "hit" if TEAM_NEXT_GAME_CACHE.get((resolved_team_id, season, season_type)) else "miss",
            },
            freshness=freshness,
            timings_enabled=NBA_TIMING_ENABLED,
        )
    ANALYSIS_CACHE[analysis_cache_key] = {"timestamp": now_ts, "payload": copy.deepcopy(payload)}
    return payload


def parse_injury_report_timestamp(url: str) -> datetime:
    return INJURY_SERVICE.parse_report_timestamp(url)


def format_injury_report_timestamp(report_dt: datetime | None) -> str:
    return INJURY_SERVICE.format_report_timestamp(report_dt)


def extract_team_prefix(text_line: str) -> tuple[str | None, str]:
    return INJURY_SERVICE.extract_team_prefix(text_line)


def parse_injury_report_rows(report_text: str) -> dict[str, Any]:
    return INJURY_SERVICE.parse_report_rows(report_text)


def extract_injury_report_rows_from_table(pdf_bytes: bytes) -> list[dict[str, Any]] | None:
    return INJURY_SERVICE.extract_report_rows_from_table(pdf_bytes)


def extract_injury_report_text_candidates(pdf_bytes: bytes) -> list[dict[str, str]]:
    return INJURY_SERVICE.extract_report_text_candidates(pdf_bytes)


def choose_best_injury_report_parse(pdf_bytes: bytes) -> dict[str, Any]:
    return INJURY_SERVICE.choose_best_report_parse(pdf_bytes)


def list_recent_injury_report_links(limit: int = 12) -> list[str]:
    return INJURY_SERVICE.list_recent_report_links(limit=limit)


def fetch_injury_report_payload_for_url(report_url: str) -> dict[str, Any]:
    return INJURY_SERVICE.fetch_report_payload_for_url(report_url)


def search_report_payload_for_player(report_payload: dict[str, Any], player_name: str, team_name: str | None = None) -> dict[str, Any] | None:
    return INJURY_SERVICE.search_report_payload_for_player(report_payload, player_name=player_name, team_name=team_name)


def find_player_in_recent_reports(player_name: str, team_name: str | None = None, max_reports: int = 8) -> dict[str, Any] | None:
    return INJURY_SERVICE.find_player_in_recent_reports(player_name=player_name, team_name=team_name, max_reports=max_reports)


def try_direct_report_match(report_text: str, player_name: str, team_name: str | None = None) -> dict[str, Any] | None:
    return INJURY_SERVICE.try_direct_report_match(report_text, player_name=player_name, team_name=team_name)


def fetch_latest_injury_report_payload() -> dict[str, Any]:
    return INJURY_SERVICE.fetch_latest_report_payload()


def build_availability_payload(player_name: str, team_name: str | None = None) -> dict[str, Any]:
    return INJURY_SERVICE.build_availability_payload(player_name, team_name=team_name)


def build_team_availability_summary(team_name: str | None, report_payload: dict[str, Any] | None = None, game_date: str | None = None) -> dict[str, Any]:
    return INJURY_SERVICE.build_team_availability_summary(team_name, report_payload=report_payload, game_date=game_date)

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

    market_signal = derive_market_environment_signal(stat, environment)
    market_component = max(
        -8.0,
        min(8.0, float(market_signal.get("over_adjustment") or 0.0) * 100.0 * (1 if side == "OVER" else -1) * 0.75),
    )
    if market_component >= 2:
        for tag in market_signal.get("support_tags") or []:
            if tag not in tags:
                tags.append(tag)
    elif market_component <= -2:
        for tag in market_signal.get("caution_tags") or []:
            if tag not in tags:
                tags.append(tag)

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
        'market_environment': round(market_component, 1),
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
        summary_parts.append('strong lineup-context support')
    elif injury_component > 0:
        summary_parts.append('mild lineup-context support')

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
    elif abs(market_component) >= 4 and market_signal.get("summary"):
        summary_parts.append(f"market context: {market_signal.get('summary')}")

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


def _confidence_band_from_score(score: int) -> tuple[str, str, str]:
    if score >= 85:
        return 'A', 'elite', 'Elite'
    if score >= 72:
        return 'B', 'good', 'High'
    if score >= 60:
        return 'C', 'warm', 'Medium'
    if score >= 48:
        return 'D', 'neutral', 'Low'
    return 'F', 'bad', 'Very low'


def apply_market_confidence_adjustment(
    confidence_engine: dict[str, Any],
    *,
    side: str,
    over_probability: float | None,
    under_probability: float | None,
    odds: float | None,
) -> dict[str, Any]:
    adjusted = copy.deepcopy(confidence_engine)
    try:
        over_prob = float(over_probability) if over_probability is not None else None
        under_prob = float(under_probability) if under_probability is not None else None
    except (TypeError, ValueError):
        over_prob = None
        under_prob = None

    adjusted["market_side"] = None
    adjusted["market_disagrees"] = False
    adjusted["market_penalty"] = 0
    adjusted["market_support_pct"] = None
    adjusted["ranking_score"] = int(adjusted.get("score") or 0)

    if over_prob is None or under_prob is None:
        return adjusted

    market_side = "OVER" if over_prob >= under_prob else "UNDER"
    chosen_prob = over_prob if side == "OVER" else under_prob
    opposite_prob = under_prob if side == "OVER" else over_prob
    disagreement = market_side != side
    penalty = 0.0
    odds_value = float(odds or 0.0)

    if disagreement:
        probability_gap = max(0.0, opposite_prob - chosen_prob) * 100.0
        penalty = max(6.0, probability_gap * 1.6)
        if odds_value >= 2.0:
            penalty += 6.0
        elif odds_value >= 1.9:
            penalty += 3.0

    adjusted_score = int(max(0, min(99, round(float(adjusted.get("score") or 0) - penalty))))
    grade, tone, tier = _confidence_band_from_score(adjusted_score)
    adjusted["score"] = adjusted_score
    adjusted["grade"] = grade
    adjusted["tone"] = tone
    adjusted["tier"] = tier
    adjusted["market_side"] = market_side
    adjusted["market_disagrees"] = disagreement
    adjusted["market_penalty"] = round(penalty, 1)
    adjusted["market_support_pct"] = round(chosen_prob * 100.0, 1)
    adjusted["ranking_score"] = adjusted_score

    tags = list(adjusted.get("tags") or [])
    if disagreement and "Market disagreement" not in tags:
        tags.append("Market disagreement")
    elif not disagreement and "Market aligned" not in tags:
        tags.append("Market aligned")
    adjusted["tags"] = tags[:6]

    summary = str(adjusted.get("summary") or "").strip()
    if disagreement:
        note = f"market leans {market_side.lower()}"
        adjusted["summary"] = f"{summary} • {note}".strip(" •")
    return adjusted

def _build_parlay_reason_fragments(prop: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    try:
        hit_rate = float(prop.get("hit_rate") or 0.0)
    except (TypeError, ValueError):
        hit_rate = 0.0
    try:
        games_count = int(prop.get("games_count") or 0)
    except (TypeError, ValueError):
        games_count = 0
    try:
        average = float(prop.get("average") or 0.0)
    except (TypeError, ValueError):
        average = 0.0
    try:
        line = float(prop.get("line") or 0.0)
    except (TypeError, ValueError):
        line = 0.0

    side = str(prop.get("side") or "").upper()
    market_side = str(prop.get("market_side") or "").upper()
    confidence_tier = str(prop.get("confidence_tier") or "")
    ranking_score = prop.get("ranking_score")
    injury_boost = bool(prop.get("injury_boost"))
    injury_names = prop.get("injury_filter_player_names") or prop.get("team_injury_player_names") or []
    matchup = prop.get("matchup") or {}
    matchup_lean = str(((matchup.get("vs_position") or {}).get("lean")) or "").strip()

    if confidence_tier:
        reasons.append(f"{confidence_tier} confidence profile")
    elif ranking_score is not None:
        reasons.append(f"rank score {ranking_score}")

    if hit_rate >= 1 and games_count > 0:
        reasons.append(f"{round(hit_rate, 1)}% hit rate over {games_count} games")

    cushion = (average - line) if side == "OVER" else (line - average)
    if cushion >= 0.75:
        reasons.append(f"{round(cushion, 1)} of cushion versus the line")

    if market_side:
        if market_side == side:
            reasons.append(f"market leans {market_side.lower()} too")
        else:
            reasons.append(f"picked despite market leaning {market_side.lower()}")

    if injury_boost and injury_names:
        reasons.append(f"lineup context improved with {', '.join(str(name) for name in injury_names[:2])} out")
    elif injury_names:
        reasons.append("same-team absences were considered")

    lean_lower = matchup_lean.lower()
    if lean_lower in {"good matchup", "favorable", "very favorable"}:
        reasons.append("matchup context is favorable")
    elif lean_lower in {"tough", "very tough", "bad matchup"}:
        reasons.append("matchup context is tougher than average")

    return reasons[:4]


def annotate_parlay_selection(scored: list[dict[str, Any]], legs: int) -> list[dict[str, Any]]:
    parlay_legs: list[dict[str, Any]] = []
    seen_player_ids: set[int] = set()
    seen_event_ids: set[str] = set()

    for idx, prop in enumerate(scored, start=1):
        pid = prop.get("player_id")
        eid = str(prop.get("event_id") or "")
        prop["board_rank"] = idx
        prop["selection_reason_parts"] = _build_parlay_reason_fragments(prop)
        prop["selection_reason"] = ""
        prop["selection_status"] = "not_selected"

        if len(parlay_legs) >= legs:
            prop["selection_reason"] = f"Ranked below the final cutoff for this {legs}-leg ticket."
            continue
        if pid in seen_player_ids:
            prop["selection_reason"] = "Skipped because a higher-ranked prop from the same player was already selected."
            continue
        if eid and eid in seen_event_ids:
            prop["selection_reason"] = "Skipped because the ticket avoids same-game parlays."
            continue

        seen_player_ids.add(pid)
        if eid:
            seen_event_ids.add(eid)
        prop["selection_status"] = "selected"
        why = prop.get("selection_reason_parts") or []
        prop["selection_reason"] = "Selected for " + "; ".join(why[:3]) + "." if why else "Selected as one of the strongest board fits."
        parlay_legs.append(prop)

    return parlay_legs


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

def derive_market_environment_signal(stat: str | None, environment: dict[str, Any] | None = None) -> dict[str, Any]:
    env = environment or {}
    normalized_stat = str(stat or "").upper().strip()
    scoring_stats = {"PTS", "3PM", "PRA", "PR", "PA"}
    playmaking_stats = {"AST", "PA", "PRA"}
    rebound_stats = {"REB", "RA", "PR", "PRA"}

    over_adjustment = 0.0
    support_tags: list[str] = []
    caution_tags: list[str] = []
    summary_bits: list[str] = []

    team_total = safe_float_or_none(env.get("market_team_total"))
    game_total = safe_float_or_none(env.get("market_game_total"))
    spread = safe_float_or_none(env.get("market_spread"))

    if team_total is not None:
        if team_total >= 118:
            support_tags.append("Strong team total")
            summary_bits.append(f"team total {team_total:.1f}")
        elif team_total <= 108:
            caution_tags.append("Low team total")
            summary_bits.append(f"team total {team_total:.1f}")

        if normalized_stat in scoring_stats:
            over_adjustment += clamp((team_total - 112.0) * 0.006, -0.04, 0.04)
        elif normalized_stat in playmaking_stats:
            over_adjustment += clamp((team_total - 112.0) * 0.0045, -0.03, 0.03)
        elif normalized_stat in rebound_stats:
            over_adjustment += clamp((team_total - 112.0) * 0.002, -0.015, 0.015)

    if game_total is not None:
        if game_total >= 232:
            support_tags.append("High game total")
            summary_bits.append(f"game total {game_total:.1f}")
        elif game_total <= 220:
            caution_tags.append("Low game total")
            summary_bits.append(f"game total {game_total:.1f}")

        if normalized_stat in scoring_stats:
            over_adjustment += clamp((game_total - 226.0) * 0.003, -0.025, 0.025)
        elif normalized_stat in playmaking_stats:
            over_adjustment += clamp((game_total - 226.0) * 0.0025, -0.02, 0.02)
        elif normalized_stat in rebound_stats:
            over_adjustment += clamp((game_total - 226.0) * 0.0015, -0.015, 0.015)

    if spread is not None:
        abs_spread = abs(spread)
        if abs_spread <= 3.5:
            support_tags.append("Tight spread")
            summary_bits.append(f"close spread {spread:+.1f}")
        elif abs_spread >= 10:
            caution_tags.append("Blowout risk")
            summary_bits.append(f"spread {spread:+.1f}")

        if abs_spread <= 3.5 and normalized_stat in scoring_stats.union(playmaking_stats):
            over_adjustment += 0.01
        elif abs_spread >= 10 and normalized_stat in scoring_stats.union(playmaking_stats):
            over_adjustment -= 0.02
        elif abs_spread >= 10 and normalized_stat in rebound_stats:
            over_adjustment -= 0.01

    return {
        "over_adjustment": round(clamp(over_adjustment, -0.08, 0.08), 4),
        "support_tags": support_tags,
        "caution_tags": caution_tags,
        "summary": " • ".join(summary_bits),
    }


def enrich_environment_with_market_context(
    environment: dict[str, Any] | None,
    event_row: dict[str, Any] | None,
    *,
    player_team_name: str = "",
    player_team_abbreviation: str = "",
) -> dict[str, Any]:
    payload = copy.deepcopy(environment or {})
    row = event_row or {}
    home_team_text = str(row.get("home_team") or "").strip()
    away_team_text = str(row.get("away_team") or "").strip()
    team_text = player_team_name or player_team_abbreviation

    player_team = resolve_team_from_text(team_text)
    home_team = resolve_team_from_text(home_team_text)
    away_team = resolve_team_from_text(away_team_text)

    player_side = None
    if player_team and home_team and int(player_team.get("id") or 0) == int(home_team.get("id") or 0):
        player_side = "home"
    elif player_team and away_team and int(player_team.get("id") or 0) == int(away_team.get("id") or 0):
        player_side = "away"
    elif team_text and normalize_name(team_text) == normalize_name(home_team_text):
        player_side = "home"
    elif team_text and normalize_name(team_text) == normalize_name(away_team_text):
        player_side = "away"

    game_total = safe_float_or_none(row.get("market_game_total"))
    home_spread = safe_float_or_none(row.get("market_home_spread"))
    away_spread = safe_float_or_none(row.get("market_away_spread"))
    home_total = safe_float_or_none(row.get("market_home_implied_total"))
    away_total = safe_float_or_none(row.get("market_away_implied_total"))

    team_total = None
    opponent_total = None
    team_spread = None
    if player_side == "home":
        team_total = home_total
        opponent_total = away_total
        team_spread = home_spread
    elif player_side == "away":
        team_total = away_total
        opponent_total = home_total
        team_spread = away_spread

    if game_total is not None:
        payload["market_game_total"] = round(game_total, 1)
    if team_total is not None:
        payload["market_team_total"] = round(team_total, 1)
    if opponent_total is not None:
        payload["market_opponent_total"] = round(opponent_total, 1)
    if team_spread is not None:
        team_spread = round(team_spread, 1)
        payload["market_spread"] = team_spread
        payload["projected_spread"] = team_spread
        if abs(team_spread) >= 10:
            payload["spread_bucket"] = "blowout"
            payload["spread_label"] = f"Market blowout risk • {team_spread:+.1f}"
        elif abs(team_spread) <= 3.5:
            payload["spread_bucket"] = "close"
            payload["spread_label"] = f"Market close spread • {team_spread:+.1f}"
        elif team_spread < 0:
            payload["spread_bucket"] = "favorite"
            payload["spread_label"] = f"Market favorite • {team_spread:+.1f}"
        else:
            payload["spread_bucket"] = "underdog"
            payload["spread_label"] = f"Market underdog • {team_spread:+.1f}"

    market_bits: list[str] = []
    if team_total is not None:
        market_bits.append(f"team total {team_total:.1f}")
    if game_total is not None:
        market_bits.append(f"game total {game_total:.1f}")
    if team_spread is not None:
        market_bits.append(f"spread {team_spread:+.1f}")
    if market_bits:
        market_summary = "Market context: " + " • ".join(market_bits) + "."
        payload["market_summary"] = market_summary
        existing_summary = str(payload.get("summary") or "").strip()
        payload["summary"] = f"{existing_summary} {market_summary}".strip() if existing_summary else market_summary
    payload["market_context"] = derive_market_environment_signal(None, payload)
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
TEAMMATE_IMPACT_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
TEAMMATE_IMPACT_CACHE_TTL_SECONDS = 6 * 60 * 60


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


def teammate_impact_score(player_id: int, season: str, season_type: str = 'Regular Season') -> float:
    cache_key = (int(player_id), str(season), str(season_type))
    cached = TEAMMATE_IMPACT_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get('timestamp') or 0.0) < TEAMMATE_IMPACT_CACHE_TTL_SECONDS:
        return float(cached.get('score') or 0.0)

    score = 0.0
    try:
        rows = fetch_recent_player_game_log(player_id=player_id, season=season, season_type=season_type, last_n=10)
    except Exception:
        rows = []

    if rows:
        minutes_vals = [float(row.get('MIN') or row.get('_minutes') or 0.0) for row in rows]
        pts_vals = [float(row.get('PTS') or 0.0) for row in rows]
        ast_vals = [float(row.get('AST') or 0.0) for row in rows]
        reb_vals = [float(row.get('REB') or 0.0) for row in rows]
        fga_vals = [float(row.get('FGA') or row.get('_fga') or 0.0) for row in rows]
        avg_min = safe_mean(minutes_vals) or 0.0
        avg_pts = safe_mean(pts_vals) or 0.0
        avg_ast = safe_mean(ast_vals) or 0.0
        avg_reb = safe_mean(reb_vals) or 0.0
        avg_fga = safe_mean(fga_vals) or 0.0
        score = (
            avg_min * 0.55
            + avg_pts * 1.0
            + avg_ast * 1.7
            + avg_reb * 0.45
            + avg_fga * 0.8
        )

    if score <= 0.0:
        player = PLAYER_LOOKUP.get(int(player_id) or 0) or {}
        fallback_name = str(player.get('full_name') or '')
        score = max(1.0, float(len(fallback_name.split()) or 1))

    TEAMMATE_IMPACT_CACHE[cache_key] = {'timestamp': now_ts, 'score': score}
    return float(score)




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
        'opportunity_label': 'Neutral lineup context',
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
            tag = 'Lineup shift'
        elif adj >= 1.4:
            tag = 'Role change'
        else:
            tag = 'Rotation change'
        boost_score += adj
        impact_tags.append(tag)
        pos_label = injured_group or 'rotation'
        impact_reasons.append(f"{injured_name} {status.lower()} ({pos_label}) changes the lineup context for this {get_stat_label_for_copy(stat).lower()} prop.")

    boost_score = round(min(12.0, boost_score), 1)
    net_adjustment = boost_score - penalty_score

    if stat in {'PTS', '3PM', 'PRA', 'PR', 'PA'}:
        angle = 'Same-team absences can materially change the scoring and usage environment for this prop.'
    elif stat in {'REB', 'RA'}:
        angle = 'Same-team absences can change rebound share, role, and floor time.'
    elif stat in {'AST'}:
        angle = 'Same-team absences can shift playmaking responsibility and assist paths.'
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
        'opportunity_label': 'Strong lineup context' if net_adjustment >= 4 else ('Mild lineup context' if net_adjustment > 0 else 'Neutral lineup context'),
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
        bullets.append(f"Lineup context: {team_context.get('headline')}. {team_context.get('summary')}")
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


def _fetch_event_odds_payload(
    *,
    event_id: str,
    api_key: str,
    sport: str,
    regions: str,
    markets: str,
    odds_format: str,
    requested_bookmakers: list[str],
) -> dict[str, Any]:
    try:
        result = odds_api_fetch(
            f"/sports/{sport}/events/{event_id}/odds",
            api_key,
            {
                "regions": regions,
                "markets": markets,
                "oddsFormat": odds_format,
                "dateFormat": "iso",
                "bookmakers": ",".join(requested_bookmakers),
            },
        )
        return {
            "event_id": event_id,
            "rows": build_odds_import_rows(result["data"] or {}, odds_format),
            "quota": result["quota"],
            "error": None,
            "status_code": None,
        }
    except HTTPException as exc:
        return {
            "event_id": event_id,
            "rows": [],
            "quota": None,
            "error": exc.detail,
            "status_code": exc.status_code,
        }
    except Exception as exc:
        return {
            "event_id": event_id,
            "rows": [],
            "quota": None,
            "error": str(exc),
            "status_code": None,
        }


def build_odds_import_rows(event_payload: dict[str, Any], odds_format: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pair_map: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    event_id = str(event_payload.get("id") or "")
    home_team = str(event_payload.get("home_team") or "")
    away_team = str(event_payload.get("away_team") or "")
    game_label = f"{away_team} @ {home_team}" if home_team or away_team else ""
    market_context = build_event_market_context(event_payload)

    for bookmaker in event_payload.get("bookmakers") or []:
        bookmaker_title = str(bookmaker.get("title") or bookmaker.get("key") or "Book")
        bookmaker_key = str(bookmaker.get("key") or bookmaker_title).strip().lower()
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
                pair_key = (bookmaker_key, market_key, player_name, normalize_line_key(line_value))
                bucket = pair_map.setdefault(pair_key, {
                    "bookmaker_key": bookmaker_key,
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
                    "home_team": home_team,
                    "away_team": away_team,
                })
                bucket[f"{side}_odds"] = decimal_odds
                if market_updated and not bucket.get("market_last_update"):
                    bucket["market_last_update"] = market_updated

    grouped_rows: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for bucket in pair_map.values():
        if bucket.get("over_odds") is None or bucket.get("under_odds") is None:
            continue
        devig = devig_two_way_market(bucket.get("over_odds"), bucket.get("under_odds"))
        if not devig:
            continue
        enriched = {
            **bucket,
            **devig,
        }
        group_key = (bucket["market_key"], bucket["player_name"], normalize_line_key(bucket["line"]))
        grouped_rows.setdefault(group_key, []).append(enriched)

    for grouped in grouped_rows.values():
        if not grouped:
            continue
        representative = min(
            grouped,
            key=lambda item: (
                float(item.get("hold") or 999.0),
                -(float(item.get("over_odds") or 0.0) + float(item.get("under_odds") or 0.0)),
            ),
        )
        best_over = max(grouped, key=lambda item: float(item.get("over_odds") or 0.0))
        best_under = max(grouped, key=lambda item: float(item.get("under_odds") or 0.0))
        consensus_over_fair = safe_mean([float(item.get("over_fair_prob") or 0.0) for item in grouped])
        consensus_under_fair = safe_mean([float(item.get("under_fair_prob") or 0.0) for item in grouped])
        consensus_hold = safe_mean([float(item.get("hold") or 0.0) for item in grouped])
        books_count = len(grouped)
        market_implied_line = float(representative["line"])

        rows.append({
            "player_name": representative["player_name"],
            "stat": representative["stat"],
            "line": round(float(representative["line"]), 1),
            "over_odds": float(representative["over_odds"]),
            "under_odds": float(representative["under_odds"]),
            "bookmaker_title": representative["bookmaker_title"],
            "bookmaker_key": representative["bookmaker_key"],
            "market_key": representative["market_key"],
            "market_last_update": representative["market_last_update"],
            "event_id": representative.get("event_id", ""),
            "game_label": representative.get("game_label", ""),
            "home_team": representative.get("home_team", ""),
            "away_team": representative.get("away_team", ""),
            "same_book_pair": True,
            "over_raw_prob": round(float(representative["over_raw_prob"]), 6),
            "under_raw_prob": round(float(representative["under_raw_prob"]), 6),
            "over_fair_prob": round(float(representative["over_fair_prob"]), 6),
            "under_fair_prob": round(float(representative["under_fair_prob"]), 6),
            "hold_percent": round(float(representative["hold"]) * 100.0, 3),
            "books_count": books_count,
            "best_over_odds": float(best_over["over_odds"]),
            "best_under_odds": float(best_under["under_odds"]),
            "best_over_bookmaker": best_over["bookmaker_title"],
            "best_under_bookmaker": best_under["bookmaker_title"],
            "consensus_over_fair_prob": round(float(consensus_over_fair or representative["over_fair_prob"]), 6),
            "consensus_under_fair_prob": round(float(consensus_under_fair or representative["under_fair_prob"]), 6),
            "consensus_hold_percent": round(float(consensus_hold or representative["hold"]) * 100.0, 3),
            "market_implied_line": round(market_implied_line, 1),
            "market_implied_source": f"consensus_{books_count}_book" if books_count > 1 else "single_book",
            **market_context,
            "csv_row": f"{representative['player_name']},{representative['stat']},{round(float(representative['line']),1)},{float(representative['over_odds'])},{float(representative['under_odds'])}",
        })
    rows.sort(key=lambda item: (item["player_name"].lower(), item["stat"], item["line"]))
    return rows


def _position_dash_cache_is_reliable_stale(cached: dict[str, Any] | None, cached_ts: float) -> bool:
    if not cached or not cached.get("rows"):
        return False
    return (time.time() - float(cached_ts or 0.0)) <= POSITION_DASH_MAX_STALE_SECONDS


@timed_call("fetch_player_game_log")
def fetch_player_game_log(player_id: int, season: str, season_type: str) -> list[dict[str, Any]]:
    return PLAYER_DATA_SERVICE.fetch_player_game_log(player_id, season, season_type)


def fetch_recent_player_game_log(player_id: int, season: str, season_type: str, last_n: int) -> list[dict[str, Any]]:
    return PLAYER_DATA_SERVICE.fetch_recent_player_game_log(player_id, season, season_type, last_n)


def _team_roster_cache_is_reliable_stale(cached: dict[str, Any] | None, cache_timestamp: float | None = None) -> bool:
    if not cached or not cached.get("rows"):
        return False
    cache_ts = float(cache_timestamp or cached.get("timestamp") or 0.0)
    if not cache_ts:
        return False
    return max(0.0, time.time() - cache_ts) <= TEAM_ROSTER_MAX_STALE_SECONDS


def fetch_team_roster(team_id: int, season: str) -> list[dict[str, Any]]:
    return PLAYER_DATA_SERVICE.fetch_team_roster(team_id, season)


@timed_call("fetch_common_player_info")
def fetch_common_player_info(player_id: int) -> dict[str, Any]:
    return PLAYER_DATA_SERVICE.fetch_common_player_info(player_id)


def fetch_next_game(player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
    return PLAYER_DATA_SERVICE.fetch_next_game(player_id, season, season_type)



PLAYER_DATA_SERVICE = PlayerDataService(
    game_log_cache_schema_version=GAME_LOG_CACHE_SCHEMA_VERSION,
    cache_ttl_seconds=CACHE_TTL_SECONDS,
    profile_ttl_seconds=PROFILE_TTL_SECONDS,
    team_roster_cache_ttl_seconds=TEAM_ROSTER_CACHE_TTL_SECONDS,
    game_log_cache=GAME_LOG_CACHE,
    recent_game_log_cache=RECENT_GAME_LOG_CACHE,
    game_log_failure_meta=GAME_LOG_FAILURE_META,
    roster_cache=ROSTER_CACHE,
    team_roster_failure_meta=TEAM_ROSTER_FAILURE_META,
    player_info_cache=PLAYER_INFO_CACHE,
    player_info_failure_meta=PLAYER_INFO_FAILURE_META,
    next_game_cache=NEXT_GAME_CACHE,
    next_game_failure_meta=NEXT_GAME_FAILURE_META,
    player_game_log_cls=PlayerGameLog,
    common_team_roster_cls=CommonTeamRoster,
    common_player_info_cls=CommonPlayerInfo,
    player_next_n_games_cls=PlayerNextNGames,
    dedupe_game_log_rows=dedupe_game_log_rows,
    throttle_request=throttle_request,
    call_nba_with_retries=call_nba_with_retries,
    save_persistent_caches=save_persistent_caches,
    is_transient_nba_error=is_transient_nba_error,
    game_log_cache_is_reliable_stale=_game_log_cache_is_reliable_stale,
    player_info_cache_is_reliable_stale=_player_info_cache_is_reliable_stale,
    next_game_cache_is_reliable_stale=_next_game_cache_is_reliable_stale,
    team_roster_cache_is_reliable_stale=_team_roster_cache_is_reliable_stale,
    game_log_failure_cooldown_seconds=GAME_LOG_FAILURE_COOLDOWN_SECONDS,
    game_log_fetch_budget_with_reliable_stale_seconds=GAME_LOG_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS,
    game_log_fetch_budget_no_reliable_stale_seconds=GAME_LOG_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS,
    game_log_fetch_attempts_with_reliable_stale=GAME_LOG_FETCH_ATTEMPTS_WITH_RELIABLE_STALE,
    game_log_fetch_attempts_no_reliable_stale=GAME_LOG_FETCH_ATTEMPTS_NO_RELIABLE_STALE,
    game_log_fetch_timeout_cap_seconds=GAME_LOG_FETCH_TIMEOUT_CAP_SECONDS,
    game_log_fetch_timeout_floor_seconds=GAME_LOG_FETCH_TIMEOUT_FLOOR_SECONDS,
    nba_backoff_factor=NBA_BACKOFF_FACTOR,
    team_roster_failure_cooldown_seconds=TEAM_ROSTER_FAILURE_COOLDOWN_SECONDS,
    team_roster_fetch_budget_with_reliable_stale_seconds=TEAM_ROSTER_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS,
    team_roster_fetch_budget_no_reliable_stale_seconds=TEAM_ROSTER_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS,
    team_roster_fetch_attempts_with_reliable_stale=TEAM_ROSTER_FETCH_ATTEMPTS_WITH_RELIABLE_STALE,
    team_roster_fetch_attempts_no_reliable_stale=TEAM_ROSTER_FETCH_ATTEMPTS_NO_RELIABLE_STALE,
    team_roster_fetch_timeout_cap_seconds=TEAM_ROSTER_FETCH_TIMEOUT_CAP_SECONDS,
    team_roster_fetch_timeout_floor_seconds=TEAM_ROSTER_FETCH_TIMEOUT_FLOOR_SECONDS,
    team_roster_retry_base_delay=TEAM_ROSTER_RETRY_BASE_DELAY,
    player_info_failure_cooldown_seconds=PLAYER_INFO_FAILURE_COOLDOWN_SECONDS,
    player_info_fetch_budget_with_reliable_stale_seconds=PLAYER_INFO_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS,
    player_info_fetch_budget_no_reliable_stale_seconds=PLAYER_INFO_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS,
    player_info_fetch_attempts_with_reliable_stale=PLAYER_INFO_FETCH_ATTEMPTS_WITH_RELIABLE_STALE,
    player_info_fetch_attempts_no_reliable_stale=PLAYER_INFO_FETCH_ATTEMPTS_NO_RELIABLE_STALE,
    player_info_fetch_timeout_cap_seconds=PLAYER_INFO_FETCH_TIMEOUT_CAP_SECONDS,
    player_info_fetch_timeout_floor_seconds=PLAYER_INFO_FETCH_TIMEOUT_FLOOR_SECONDS,
    player_info_retry_base_delay=PLAYER_INFO_RETRY_BASE_DELAY,
    next_game_ttl_seconds=NEXT_GAME_TTL_SECONDS,
    next_game_failure_cooldown_seconds=NEXT_GAME_FAILURE_COOLDOWN_SECONDS,
    next_game_fetch_budget_with_reliable_stale_seconds=NEXT_GAME_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS,
    next_game_fetch_budget_no_reliable_stale_seconds=NEXT_GAME_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS,
    next_game_fetch_attempts_with_reliable_stale=NEXT_GAME_FETCH_ATTEMPTS_WITH_RELIABLE_STALE,
    next_game_fetch_attempts_no_reliable_stale=NEXT_GAME_FETCH_ATTEMPTS_NO_RELIABLE_STALE,
    next_game_fetch_timeout_cap_seconds=NEXT_GAME_FETCH_TIMEOUT_CAP_SECONDS,
    next_game_fetch_timeout_floor_seconds=NEXT_GAME_FETCH_TIMEOUT_FLOOR_SECONDS,
    next_game_retry_base_delay=NEXT_GAME_RETRY_BASE_DELAY,
    http_exception_cls=HTTPException,
)


SCHEDULE_SERVICE = ScheduleDataService(
    team_lookup=TEAM_LOOKUP,
    scoreboard_cache=SCOREBOARD_CACHE,
    next_game_cache=TEAM_NEXT_GAME_CACHE,
    next_game_failure_meta=NEXT_GAME_FAILURE_META,
    scoreboard_v2_cls=ScoreboardV2,
    call_with_retries=call_nba_with_retries,
    fetch_next_game=fetch_next_game,
    save_persistent_caches=save_persistent_caches,
    safe_int_score=safe_int_score,
    current_nba_game_date=current_nba_game_date,
    timed_call=timed_call,
    next_game_ttl_seconds=NEXT_GAME_TTL_SECONDS,
    next_game_failure_cooldown_seconds=NEXT_GAME_FAILURE_COOLDOWN_SECONDS,
    scoreboard_cache_ttl_seconds=SCOREBOARD_CACHE_TTL_SECONDS,
    scoreboard_max_stale_seconds=SCOREBOARD_MAX_STALE_SECONDS,
    scoreboard_failure_cooldown_seconds=SCOREBOARD_FAILURE_COOLDOWN_SECONDS,
    scoreboard_fetch_budget_with_reliable_stale_seconds=SCOREBOARD_FETCH_BUDGET_WITH_RELIABLE_STALE_SECONDS,
    scoreboard_fetch_budget_no_reliable_stale_seconds=SCOREBOARD_FETCH_BUDGET_NO_RELIABLE_STALE_SECONDS,
    scoreboard_fetch_attempts_with_reliable_stale=SCOREBOARD_FETCH_ATTEMPTS_WITH_RELIABLE_STALE,
    scoreboard_fetch_attempts_no_reliable_stale=SCOREBOARD_FETCH_ATTEMPTS_NO_RELIABLE_STALE,
    scoreboard_fetch_timeout_cap_seconds=SCOREBOARD_FETCH_TIMEOUT_CAP_SECONDS,
    scoreboard_fetch_timeout_floor_seconds=SCOREBOARD_FETCH_TIMEOUT_FLOOR_SECONDS,
    scoreboard_retry_base_delay=SCOREBOARD_RETRY_BASE_DELAY,
    next_game_cache_is_reliable_stale=_next_game_cache_is_reliable_stale,
)


def _scoreboard_cache_is_reliable_stale(game_date: str, cached: dict[str, Any] | None, cached_ts: float) -> bool:
    return SCHEDULE_SERVICE._scoreboard_cache_is_reliable_stale(game_date, cached, cached_ts)


def _get_scoreboard_failure_meta(game_date: str) -> dict[str, Any]:
    return SCHEDULE_SERVICE._get_scoreboard_failure_meta(game_date)


def fetch_scoreboard_games(game_date: str) -> list[dict[str, Any]]:
    return SCHEDULE_SERVICE.fetch_scoreboard_games(game_date)


def build_scoreboard_next_game_payload(game_row: dict[str, Any], player_team_id: int | None) -> dict[str, Any] | None:
    return SCHEDULE_SERVICE.build_scoreboard_next_game_payload(game_row, player_team_id)


def find_team_next_game_via_scoreboard(team_id: int | None, lookahead_days: int = 10) -> dict[str, Any] | None:
    return SCHEDULE_SERVICE.find_team_next_game_via_scoreboard(team_id, lookahead_days=lookahead_days)


def build_next_game_payload(next_game_row: dict[str, Any] | None, player_team_id: int | None) -> dict[str, Any] | None:
    return SCHEDULE_SERVICE.build_next_game_payload(next_game_row, player_team_id)


def resolve_team_next_game(team_id: int | None, primary_player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
    return SCHEDULE_SERVICE.resolve_team_next_game(team_id, primary_player_id, season, season_type)



@app.on_event("startup")
def _startup_warm_cache() -> None:
    start_hybrid_refresh_workers()
    warm_cache_on_startup()

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
    return {"results": PLAYER_SEARCH_INDEX.search(q, limit=15)}




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
            "over_fair_prob": row.get("over_fair_prob"),
            "under_fair_prob": row.get("under_fair_prob"),
            "consensus_over_fair_prob": row.get("consensus_over_fair_prob"),
            "consensus_under_fair_prob": row.get("consensus_under_fair_prob"),
            "hold_percent": row.get("hold_percent"),
            "books_count": row.get("books_count"),
            "best_over_odds": row.get("best_over_odds"),
            "best_under_odds": row.get("best_under_odds"),
            "best_over_bookmaker": row.get("best_over_bookmaker"),
            "best_under_bookmaker": row.get("best_under_bookmaker"),
            "market_implied_line": row.get("market_implied_line"),
            "bookmaker_title": row.get("bookmaker_title"),
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
        fair_over = bulk_row.get("consensus_over_fair_prob")
        fair_under = bulk_row.get("consensus_under_fair_prob")
        if fair_over is None:
            fair_over = bulk_row.get("over_fair_prob")
        if fair_under is None:
            fair_under = bulk_row.get("under_fair_prob")
        try:
            fair_over = float(fair_over) if fair_over is not None else implied_over
        except (TypeError, ValueError):
            fair_over = implied_over
        try:
            fair_under = float(fair_under) if fair_under is not None else implied_under
        except (TypeError, ValueError):
            fair_under = implied_under
        over_edge = round((model_over - fair_over) * 100, 1) if fair_over is not None else None
        under_edge = round((model_under - fair_under) * 100, 1) if fair_under is not None else None
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
                "team_name": (resolved_team or {}).get("full_name") or analysis["player"].get("team_name") or team_text,
                "team_abbreviation": resolved_team_abbreviation,
                "team": resolved_team_abbreviation,
                "opponent_name": next_game.get("opponent_name") or (opponent.get("full_name") if opponent else opponent_text),
                "opponent": resolved_opponent_abbreviation,
                "position": analysis["player"].get("position") or "",
                "jersey": analysis["player"].get("jersey") or "",
            },
            "market": {
                "stat": str(bulk_row["stat"]),
                "line": float(bulk_row["line"]),
                "over_odds": over_odds,
                "under_odds": under_odds,
                "over_fair_prob": round(fair_over, 6) if fair_over is not None else None,
                "under_fair_prob": round(fair_under, 6) if fair_under is not None else None,
                "hold_percent": bulk_row.get("hold_percent"),
                "books_count": bulk_row.get("books_count"),
                "best_over_odds": bulk_row.get("best_over_odds"),
                "best_under_odds": bulk_row.get("best_under_odds"),
                "best_over_bookmaker": bulk_row.get("best_over_bookmaker"),
                "best_under_bookmaker": bulk_row.get("best_under_bookmaker"),
                "market_implied_line": bulk_row.get("market_implied_line"),
                "bookmaker_title": bulk_row.get("bookmaker_title"),
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
                "over_fair_probability": round(fair_over * 100, 1) if fair_over is not None else None,
                "under_fair_probability": round(fair_under * 100, 1) if fair_under is not None else None,
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

    requested_bookmakers = parse_requested_bookmakers(payload.get("bookmakers") or payload.get("bookmaker") or ODDS_DEFAULT_BOOKMAKERS)
    result = odds_api_fetch(
        f"/sports/{sport}/events/{event_id}/odds",
        api_key,
        {
            "regions": regions,
            "markets": markets,
            "oddsFormat": odds_format,
            "dateFormat": "iso",
            "bookmakers": ",".join(requested_bookmakers),
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
        "bookmakers": requested_bookmakers,
        "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers),
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
    requested_bookmakers = parse_requested_bookmakers(payload.get("bookmakers") or payload.get("bookmaker") or ODDS_DEFAULT_BOOKMAKERS)
    markets      = ",".join(ODDS_PARLAY_MARKETS)

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
            "bookmakers": requested_bookmakers,
            "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers),
            "message": "No events found for today.",
        }

    # ── Phase 2: Fetch odds per event in batches, rotating keys ─────────
    all_import_rows: list[dict[str, Any]] = []
    scrape_errors: list[dict[str, Any]] = []

    for batch_start in range(0, len(events), batch_size):
        batch = events[batch_start: batch_start + batch_size]
        batch_jobs = [
            {
                "event_id": str(event.get("id") or ""),
                "api_key": next_key(),
                "home_team": event.get("home_team"),
                "away_team": event.get("away_team"),
            }
            for event in batch
            if str(event.get("id") or "")
        ]
        if not batch_jobs:
            continue
        batch_workers = min(len(batch_jobs), max(1, len(api_keys)), batch_size, 6)
        if batch_workers <= 1:
            batch_results = [
                (
                    job,
                    _fetch_event_odds_payload(
                        event_id=job["event_id"],
                        api_key=job["api_key"],
                        sport=sport,
                        regions=regions,
                        markets=markets,
                        odds_format=odds_format,
                        requested_bookmakers=requested_bookmakers,
                    ),
                )
                for job in batch_jobs
            ]
        else:
            with ThreadPoolExecutor(max_workers=batch_workers) as executor:
                futures = [
                    (
                        job,
                        executor.submit(
                            _fetch_event_odds_payload,
                            event_id=job["event_id"],
                            api_key=job["api_key"],
                            sport=sport,
                            regions=regions,
                            markets=markets,
                            odds_format=odds_format,
                            requested_bookmakers=requested_bookmakers,
                        ),
                    )
                    for job in batch_jobs
                ]
                batch_results = [(job, future.result()) for job, future in futures]
        for job, result in batch_results:
            event_id = str(result.get("event_id") or job["event_id"] or "")
            if result.get("error"):
                scrape_errors.append({
                    "event_id": event_id,
                    "home_team": job.get("home_team"),
                    "away_team": job.get("away_team"),
                    "reason": result.get("error"),
                    "status_code": result.get("status_code"),
                })
                continue
            quota_log.append({"call": f"event_{event_id[:8]}", "quota": result.get("quota")})
            all_import_rows.extend(result.get("rows") or [])

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
            "bookmakers": requested_bookmakers,
            "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers),
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
        fair_over_prob = orig_row.get("consensus_over_fair_prob")
        fair_under_prob = orig_row.get("consensus_under_fair_prob")
        if fair_over_prob is None:
            fair_over_prob = orig_row.get("over_fair_prob")
        if fair_under_prob is None:
            fair_under_prob = orig_row.get("under_fair_prob")
        if fair_over_prob is None:
            fair_over_prob = decimal_implied_probability(orig_row.get("over_odds"))
        if fair_under_prob is None:
            fair_under_prob = decimal_implied_probability(orig_row.get("under_odds"))
        fair_prob = fair_over_prob if side == "OVER" else fair_under_prob
        try:
            fair_prob = float(fair_prob) if fair_prob is not None else decimal_implied_probability(odds)
        except (TypeError, ValueError):
            fair_prob = decimal_implied_probability(odds)
        enriched_environment = enrich_environment_with_market_context(
            analysis.get("environment") or {},
            orig_row,
            player_team_name=(analysis.get("player") or {}).get("team_name") or "",
            player_team_abbreviation=(analysis.get("player") or {}).get("team_abbreviation") or "",
        )
        confidence_engine = build_confidence_engine(
            side=side,
            hit_rate=float(side_hit_rate),
            games_count=int(analysis.get("games_count") or 0),
            edge=round((side_hit_rate / 100.0 - fair_prob) * 100.0, 1) if fair_prob is not None else round(abs(avg - line), 1),
            ev=max(0.0, (side_hit_rate / 100.0) - fair_prob) if fair_prob is not None else max(0.0, (side_hit_rate / 100.0) - decimal_implied_probability(odds)),
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            availability=availability,
            opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {},
            environment=enriched_environment,
            stat=stat,
            player_position=(analysis.get("player") or {}).get("position") or '',
            line=line,
            average=avg,
        )
        confidence_engine = apply_market_confidence_adjustment(
            confidence_engine,
            side=side,
            over_probability=fair_over_prob,
            under_probability=fair_under_prob,
            odds=odds,
        )

        # Resolve team + opponent IDs from the analysis payload so the frontend
        # can auto-populate the analyzer without the user having to re-select manually.
        player_info    = analysis.get("player") or {}
        resolved_team_id_scored    = player_info.get("team_id")
        resolved_opponent_team_id  = next_game_info.get("opponent_team_id")
        resolved_opponent_abbr     = str(next_game_info.get("opponent_abbreviation") or "").strip()
        if not resolved_opponent_team_id:
            home_candidate = resolve_team_from_text(str(orig_row.get("home_team") or ""))
            away_candidate = resolve_team_from_text(str(orig_row.get("away_team") or ""))
            player_team_id_int = int(resolved_team_id_scored or 0)
            for candidate in [away_candidate, home_candidate]:
                if candidate and int(candidate.get("id") or 0) != player_team_id_int:
                    resolved_opponent_team_id = int(candidate.get("id") or 0)
                    if not resolved_opponent_abbr:
                        resolved_opponent_abbr = str(candidate.get("abbreviation") or "").strip()
                    break

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
            "environment": copy.deepcopy(enriched_environment),
            "confidence": confidence_engine.get("grade"),
            "confidence_score": confidence_engine.get("score"),
            "confidence_tone": confidence_engine.get("tone"),
            "confidence_tier": confidence_engine.get("tier"),
            "confidence_summary": confidence_engine.get("summary"),
            "confidence_tags": confidence_engine.get("tags") or [],
            "market_side": confidence_engine.get("market_side"),
            "market_disagrees": confidence_engine.get("market_disagrees"),
            "market_penalty": confidence_engine.get("market_penalty"),
            "ranking_score": confidence_engine.get("ranking_score"),
            "event_id": orig_row.get("event_id") or "",
            "game_label": orig_row.get("game_label") or "",
            "home_team": orig_row.get("home_team") or "",
            "away_team": orig_row.get("away_team") or "",
            "books_count": orig_row.get("books_count") or 1,
            "hold_percent": orig_row.get("hold_percent"),
            "fair_probability": round(fair_prob * 100.0, 1) if fair_prob is not None else None,
            "best_over_odds": orig_row.get("best_over_odds"),
            "best_under_odds": orig_row.get("best_under_odds"),
            "best_over_bookmaker": orig_row.get("best_over_bookmaker"),
            "best_under_bookmaker": orig_row.get("best_under_bookmaker"),
        })

    # Sort by market-adjusted confidence first so sides fighting the market
    # do not float to the top on raw hit rate alone.
    scored.sort(key=lambda x: (x.get("ranking_score", x["confidence_score"]), x["hit_rate"], x["odds"]), reverse=True)

    # Pick top N and annotate why each row was selected or skipped.
    parlay_legs = annotate_parlay_selection(scored, legs)

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
        "bookmakers": requested_bookmakers,
        "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers),
    }


@app.get("/api/team-injuries")
def get_team_injuries(team_name: str = Query(..., min_length=1)) -> dict[str, Any]:
    """
    Return injured/risky players for a team with their resolved NBA player IDs.
    Used by the injury-aware parlay builder and Player Analyzer lineup-context UI.
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
    requested_bookmakers = parse_requested_bookmakers(payload.get("bookmakers") or payload.get("bookmaker") or ODDS_DEFAULT_BOOKMAKERS)
    markets     = ",".join(ODDS_PARLAY_MARKETS)

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
                "bookmakers": requested_bookmakers, "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers),
                "injury_summary": [], "message": "No events found for today."}

    all_import_rows: list[dict[str, Any]] = []
    scrape_errors: list[dict[str, Any]] = []

    for batch_start in range(0, len(events), batch_size):
        batch = events[batch_start: batch_start + batch_size]
        batch_jobs = [
            {
                "event_id": str(event.get("id") or ""),
                "api_key": next_key(),
            }
            for event in batch
            if str(event.get("id") or "")
        ]
        if not batch_jobs:
            continue
        batch_workers = min(len(batch_jobs), max(1, len(api_keys)), batch_size, 6)
        if batch_workers <= 1:
            batch_results = [
                _fetch_event_odds_payload(
                    event_id=job["event_id"],
                    api_key=job["api_key"],
                    sport=sport,
                    regions=regions,
                    markets=markets,
                    odds_format=odds_format,
                    requested_bookmakers=requested_bookmakers,
                )
                for job in batch_jobs
            ]
        else:
            with ThreadPoolExecutor(max_workers=batch_workers) as executor:
                futures = [
                    executor.submit(
                        _fetch_event_odds_payload,
                        event_id=job["event_id"],
                        api_key=job["api_key"],
                        sport=sport,
                        regions=regions,
                        markets=markets,
                        odds_format=odds_format,
                        requested_bookmakers=requested_bookmakers,
                    )
                    for job in batch_jobs
                ]
                batch_results = [future.result() for future in futures]
        for result in batch_results:
            event_id = str(result.get("event_id") or "")
            if result.get("error"):
                scrape_errors.append({"event_id": event_id, "reason": result.get("error"), "status_code": result.get("status_code")})
                continue
            quota_log.append({"call": f"event_{event_id[:8]}", "quota": result.get("quota")})
            all_import_rows.extend(result.get("rows") or [])

    if not all_import_rows:
        return {"legs": legs, "parlay": [], "parlay_odds": None, "all_props_scored": [],
                "events_scraped": len(events), "props_found": 0, "errors": scrape_errors,
                "quota_log": quota_log, "bookmakers": requested_bookmakers,
                "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers), "injury_summary": [],
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

    player_lookup_cache: dict[tuple[str, int | None], dict[str, Any] | None] = {}
    without_player_names_cache: dict[tuple[int, ...], list[str]] = {}
    boosted_analysis_cache: dict[tuple[int, str, float, int, str, str, tuple[int, ...]], dict[str, Any] | None] = {}
    teammate_impact_cache: dict[int, float] = {}

    def cached_find_player_by_name(player_name: str, team_id: int | None = None) -> dict[str, Any] | None:
        cache_key = (str(player_name or "").strip().lower(), team_id)
        if cache_key not in player_lookup_cache:
            player_lookup_cache[cache_key] = find_player_by_name(player_name, team_id=team_id)
        return player_lookup_cache[cache_key]

    def cached_without_player_names(player_ids: list[int]) -> list[str]:
        cache_key = tuple(normalize_without_player_ids(player_ids))
        if cache_key not in without_player_names_cache:
            without_player_names_cache[cache_key] = resolve_without_player_names(list(cache_key))
        return list(without_player_names_cache[cache_key])

    def cached_teammate_impact(player_id: int) -> float:
        normalized_player_id = int(player_id or 0)
        if normalized_player_id <= 0:
            return 0.0
        if normalized_player_id not in teammate_impact_cache:
            teammate_impact_cache[normalized_player_id] = teammate_impact_score(normalized_player_id, season=season, season_type=season_type)
        return float(teammate_impact_cache[normalized_player_id])

    # Build per-team injured player IDs cache: team_name → list[int]
    _team_injured_ids_cache: dict[str, list[int]] = {}

    def get_injured_ids_for_team(team_name: str) -> list[int]:
        if team_name in _team_injured_ids_cache:
            return _team_injured_ids_cache[team_name]
        rows_inj = INJURY_SERVICE.get_team_rows(inj_report, team_name)
        ids: list[int] = []
        for row in rows_inj:
            status = str(row.get("status") or "")
            if status not in UNAVAILABLE_STATUSES and status not in RISKY_STATUSES:
                continue
            raw_disp = re.sub(r",(?!\s)", ", ", str(row.get("player_display") or "").strip())
            parts = [p.strip() for p in raw_disp.split(",")]
            name_lk = f"{parts[1]} {parts[0]}" if len(parts) == 2 else raw_disp
            player = cached_find_player_by_name(name_lk)
            if player:
                ids.append(int(player["id"]))
        ids.sort(key=lambda teammate_id: (-cached_teammate_impact(teammate_id), teammate_id))
        _team_injured_ids_cache[team_name] = ids
        return ids

    defaults = {"last_n": last_n, "season": season, "season_type": season_type}
    local_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    analysis_rows: list[dict[str, Any]] = []
    analysis_errors: list[dict[str, Any]] = []

    prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in all_import_rows:
        player_name = str(row.get("player_name") or "").strip()
        player = cached_find_player_by_name(player_name)
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
        next_game_info = (analysis.get("matchup") or {}).get("next_game") or {}
        player_team_abbr = str(next_game_info.get("player_team_abbreviation") or "").strip()
        opponent_abbr = str(next_game_info.get("opponent_abbreviation") or "").strip()
        if not team_name and player_team_abbr:
            candidate_team = TEAM_ALIAS_LOOKUP.get(normalize_name(player_team_abbr))
            if candidate_team:
                team_name = str(candidate_team.get("full_name") or "")
        if not team_name:
            # Last resort: only trust the side that matches the player's next-game team abbreviation.
            home_team_text = str(orig_row.get("home_team") or "").strip()
            away_team_text = str(orig_row.get("away_team") or "").strip()
            for team_text in [away_team_text, home_team_text]:
                candidate = resolve_team_from_text(team_text)
                if not candidate:
                    continue
                candidate_abbr = str(candidate.get("abbreviation") or "").strip().upper()
                if player_team_abbr and candidate_abbr == player_team_abbr.upper():
                    team_name = str(candidate.get("full_name") or "")
                    break
            if not team_name and team_id_raw:
                for team_text in [away_team_text, home_team_text]:
                    candidate = resolve_team_from_text(team_text)
                    if candidate and int(candidate.get("id") or 0) == int(team_id_raw):
                        team_name = str(candidate.get("full_name") or "")
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
            team_injury_player_names = cached_without_player_names(inj_ids)
            best_usable_combo: tuple[list[int], float, float, int] | None = None
            # Try top 1, top 2, then top 3 impacted absences.
            for max_filters in [1, 2, 3]:
                candidate_ids = inj_ids[:max_filters]
                if not candidate_ids:
                    break
                try:
                    boost_cache_key = (player_id, stat, float(line), int(last_n), str(season), str(season_type), tuple(candidate_ids))
                    if boost_cache_key not in boosted_analysis_cache:
                        boosted_analysis_cache[boost_cache_key] = build_prop_analysis_payload(
                            player_id=player_id,
                            stat=stat,
                            line=line,
                            last_n=last_n,
                            season=season,
                            season_type=season_type,
                            without_player_ids=candidate_ids,
                        )
                    boosted_analysis = boosted_analysis_cache[boost_cache_key]
                    if not boosted_analysis:
                        break
                    boosted_games = int(boosted_analysis.get("games_count") or 0)
                    boosted_hr    = float(boosted_analysis.get("hit_rate") or 0)
                    if boosted_games >= 5:
                        boosted_avg = float(boosted_analysis.get("average") or avg)
                        if best_usable_combo is None or boosted_hr > best_usable_combo[1] or (
                            boosted_hr == best_usable_combo[1] and boosted_games > best_usable_combo[3]
                        ):
                            best_usable_combo = (list(candidate_ids), boosted_hr, boosted_avg, boosted_games)
                except Exception:
                    break
            if best_usable_combo is not None:
                injury_filter_player_ids = list(best_usable_combo[0])
                injury_filter_player_names = cached_without_player_names(injury_filter_player_ids)
                base_games_count = int(best_usable_combo[3])
                if float(best_usable_combo[1]) > hit_rate:
                    hit_rate = float(best_usable_combo[1])
                    avg = float(best_usable_combo[2])
                    injury_boost = True

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
        fair_over_prob = orig_row.get("consensus_over_fair_prob")
        fair_under_prob = orig_row.get("consensus_under_fair_prob")
        if fair_over_prob is None:
            fair_over_prob = orig_row.get("over_fair_prob")
        if fair_under_prob is None:
            fair_under_prob = orig_row.get("under_fair_prob")
        if fair_over_prob is None:
            fair_over_prob = decimal_implied_probability(orig_row.get("over_odds"))
        if fair_under_prob is None:
            fair_under_prob = decimal_implied_probability(orig_row.get("under_odds"))
        fair_prob = fair_over_prob if side == "OVER" else fair_under_prob
        try:
            fair_prob = float(fair_prob) if fair_prob is not None else decimal_implied_probability(odds)
        except (TypeError, ValueError):
            fair_prob = decimal_implied_probability(odds)
        enriched_environment = enrich_environment_with_market_context(
            analysis.get("environment") or {},
            orig_row,
            player_team_name=team_name,
            player_team_abbreviation=player_info.get("team_abbreviation") or "",
        )
        confidence_engine = build_confidence_engine(
            side=side, hit_rate=float(side_hit_rate), games_count=base_games_count,
            edge=round((side_hit_rate / 100.0 - fair_prob) * 100.0, 1) if fair_prob is not None else round(abs(avg - line), 1),
            ev=max(0.0, (side_hit_rate / 100.0) - fair_prob) if fair_prob is not None else max(0.0, (side_hit_rate / 100.0) - decimal_implied_probability(odds)),
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            availability=availability, opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {}, environment=enriched_environment,
            stat=stat, player_position=(analysis.get("player") or {}).get("position") or "",
            line=line, average=avg,
        )
        confidence_engine = apply_market_confidence_adjustment(
            confidence_engine,
            side=side,
            over_probability=fair_over_prob,
            under_probability=fair_under_prob,
            odds=odds,
        )

        resolved_team_id = player_info.get("team_id")
        opponent_info    = copy.deepcopy(next_game_info)
        if not opponent_info.get("opponent_team_id"):
            home_candidate = resolve_team_from_text(str(orig_row.get("home_team") or ""))
            away_candidate = resolve_team_from_text(str(orig_row.get("away_team") or ""))
            player_team_id_int = int(resolved_team_id or 0)
            for candidate in [away_candidate, home_candidate]:
                if candidate and int(candidate.get("id") or 0) != player_team_id_int:
                    opponent_info["opponent_team_id"] = int(candidate.get("id") or 0)
                    opponent_info["opponent_abbreviation"] = str(candidate.get("abbreviation") or "").strip()
                    opponent_info["opponent_name"] = str(candidate.get("full_name") or "").strip()
                    break

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
            "environment": copy.deepcopy(enriched_environment),
            "confidence": confidence_engine.get("grade"),
            "confidence_score": confidence_engine.get("score"),
            "confidence_tone": confidence_engine.get("tone"),
            "confidence_tier": confidence_engine.get("tier"),
            "confidence_summary": confidence_engine.get("summary"),
            "confidence_tags": confidence_engine.get("tags") or [],
            "market_side": confidence_engine.get("market_side"),
            "market_disagrees": confidence_engine.get("market_disagrees"),
            "market_penalty": confidence_engine.get("market_penalty"),
            "ranking_score": confidence_engine.get("ranking_score"),
            "event_id": orig_row.get("event_id") or "",
            "game_label": orig_row.get("game_label") or "",
            "home_team": orig_row.get("home_team") or "",
            "away_team": orig_row.get("away_team") or "",
            # Injury-aware fields
            "injury_boost": injury_boost,
            "injury_filter_player_ids": injury_filter_player_ids,
            "injury_filter_player_names": injury_filter_player_names,
            "team_injury_player_names": team_injury_player_names,
            "books_count": orig_row.get("books_count") or 1,
            "hold_percent": orig_row.get("hold_percent"),
            "fair_probability": round(fair_prob * 100.0, 1) if fair_prob is not None else None,
            "best_over_odds": orig_row.get("best_over_odds"),
            "best_under_odds": orig_row.get("best_under_odds"),
            "best_over_bookmaker": orig_row.get("best_over_bookmaker"),
            "best_under_bookmaker": orig_row.get("best_under_bookmaker"),
        }

    scored: list[dict[str, Any]] = []
    for result, orig_row in analysis_rows:
        item = injury_aware_score(result, orig_row)
        if item:
            scored.append(item)

    scored.sort(key=lambda x: (x.get("ranking_score", x["confidence_score"]), x["hit_rate"], x["odds"]), reverse=True)

    # Pick top N and annotate why each row was selected or skipped.
    parlay_legs = annotate_parlay_selection(scored, legs)

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
                "injured_player_names": cached_without_player_names(inj_ids),
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
        "bookmakers": requested_bookmakers,
        "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers),
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
    injury_rows_by_team: dict[str, list[dict[str, Any]]] = {}
    if report_payload.get("ok"):
        involved_teams = {
            str(team.get("full_name") or "").strip()
            for row in rows
            for team in (
                TEAM_LOOKUP.get(int(row.get("HOME_TEAM_ID") or 0), {}),
                TEAM_LOOKUP.get(int(row.get("VISITOR_TEAM_ID") or 0), {}),
            )
            if str(team.get("full_name") or "").strip()
        }
        injury_rows_by_team = {
            team_name: INJURY_SERVICE.get_team_rows(report_payload, team_name, game_date=resolved_date)
            for team_name in involved_teams
        }
    games: list[dict[str, Any]] = []

    for row in rows:
        home_team_id = int(row.get("HOME_TEAM_ID") or 0)
        away_team_id = int(row.get("VISITOR_TEAM_ID") or 0)
        home_team = TEAM_LOOKUP.get(home_team_id, {})
        away_team = TEAM_LOOKUP.get(away_team_id, {})
        game_status = str(row.get("GAME_STATUS_TEXT") or "").strip()
        home_score = safe_int_score(row.get("PTS_HOME"), 0)
        away_score = safe_int_score(row.get("PTS_AWAY"), 0)
        home_summary = build_team_availability_summary(str(home_team.get("full_name") or ""), report_payload, game_date=resolved_date)
        away_summary = build_team_availability_summary(str(away_team.get("full_name") or ""), report_payload, game_date=resolved_date)

        def _inj_players(team_full_name: str) -> list[dict[str, Any]]:
            seen_keys: set[str] = set()
            result: list[dict[str, Any]] = []
            team_full_name_norm = team_full_name.strip()
            for ir in injury_rows_by_team.get(team_full_name_norm, []):
                status = str(ir.get("status") or "").strip()
                if status not in UNAVAILABLE_STATUSES and status not in RISKY_STATUSES:
                    continue
                pk = str(ir.get("player_key") or "")
                if pk in seen_keys:
                    continue
                seen_keys.add(pk)
                raw_display = str(ir.get("player_display") or "").strip()
                if "@" in raw_display:
                    continue
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

WARM_CACHE_ON_STARTUP_ENABLED = os.getenv("NBA_WARM_CACHE_ON_STARTUP_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_STARTUP_MAX_WORKERS = max(1, int(os.getenv("NBA_WARM_CACHE_STARTUP_MAX_WORKERS", "4")))
WARM_CACHE_PRELOAD_TODAYS_GAMES = os.getenv("NBA_WARM_CACHE_PRELOAD_TODAYS_GAMES", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_PRELOAD_INJURIES = os.getenv("NBA_WARM_CACHE_PRELOAD_INJURIES", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_PRELOAD_PLAYERS = os.getenv("NBA_WARM_CACHE_PRELOAD_PLAYERS", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_PRELOAD_TEAMS = os.getenv("NBA_WARM_CACHE_PRELOAD_TEAMS", "1").strip().lower() not in {"0", "false", "no", "off"}


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
# In-memory store (resets on restart). Frontend can persist via localStorage.
# ─────────────────────────────────────────────────────────────────────────────

_BACKTEST_LOCK = threading.Lock()
_BACKTEST_LOG: list[dict[str, Any]] = []   # [{id, player, stat, line, side, confidence_score, confidence_tier, model_prob, result, hit, odds, ev, logged_at, resolved_at}]


def _backtest_new_id() -> str:
    import uuid
    return str(uuid.uuid4())[:8]


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
        "odds": payload.get("odds"),  # decimal odds, optional
        "result": "pending",
        "actual_value": None,
        "logged_at": datetime.utcnow().isoformat() + "Z",
        "resolved_at": None,
    }
    with _BACKTEST_LOCK:
        _BACKTEST_LOG.append(entry)
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
        for entry in _BACKTEST_LOG:
            if entry["id"] == pred_id:
                hit = (
                    actual_value > entry["line"] if entry["side"] == "OVER"
                    else actual_value < entry["line"]
                )
                entry["result"] = "hit" if hit else "miss"
                entry["actual_value"] = actual_value
                entry["resolved_at"] = datetime.utcnow().isoformat() + "Z"
                return {"ok": True, "entry": entry}
    raise HTTPException(status_code=404, detail=f"No prediction found with id={pred_id}")


@app.get("/api/backtest/log")
def backtest_get_log(limit: int = Query(200, ge=1, le=1000)) -> dict[str, Any]:
    """Return the backtest log and aggregate stats."""
    with _BACKTEST_LOCK:
        entries = list(reversed(_BACKTEST_LOG))[:limit]
        stats = _compute_backtest_stats(list(_BACKTEST_LOG))
    return {"stats": stats, "entries": entries}


@app.delete("/api/backtest/log/{entry_id}")
def backtest_delete_entry(entry_id: str) -> dict[str, Any]:
    """Delete a single backtest entry by ID."""
    with _BACKTEST_LOCK:
        before = len(_BACKTEST_LOG)
        _BACKTEST_LOG[:] = [e for e in _BACKTEST_LOG if e["id"] != entry_id]
        after = len(_BACKTEST_LOG)
    if before == after:
        raise HTTPException(status_code=404, detail=f"No entry found with id={entry_id}")
    return {"ok": True, "deleted": entry_id}


@app.delete("/api/backtest/log")
def backtest_clear_log() -> dict[str, Any]:
    """Clear all backtest entries."""
    with _BACKTEST_LOCK:
        count = len(_BACKTEST_LOG)
        _BACKTEST_LOG.clear()
    return {"ok": True, "cleared": count}


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


@app.post("/api/odds/game-context")
def odds_game_context(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    api_key = str(payload.get("api_key") or "").strip()
    if not api_key:
        raise HTTPException(status_code=400, detail="Provide an Odds API key.")

    sport = str(payload.get("sport") or "basketball_nba").strip()
    regions = str(payload.get("regions") or "us").strip()
    odds_format = str(payload.get("odds_format") or "decimal").strip()
    requested_bookmakers = parse_requested_bookmakers(payload.get("bookmakers") or payload.get("bookmaker") or ODDS_DEFAULT_BOOKMAKERS)
    team_name = str(payload.get("team_name") or "").strip()
    opponent_name = str(payload.get("opponent_name") or "").strip()
    team_abbreviation = str(payload.get("team_abbreviation") or "").strip()
    opponent_abbreviation = str(payload.get("opponent_abbreviation") or "").strip()

    events_result = odds_api_fetch(f"/sports/{sport}/events", api_key, {"dateFormat": "iso"})
    events = events_result.get("data") or []
    matched_event = next(
        (
            event for event in events
            if _event_matches_teams(
                event,
                team_name=team_name,
                opponent_name=opponent_name,
                team_abbreviation=team_abbreviation,
                opponent_abbreviation=opponent_abbreviation,
            )
        ),
        None,
    )
    if not matched_event:
        return {"ok": False, "context": {}, "environment": {}, "quota": events_result.get("quota"), "message": "No matching odds event found."}

    event_id = str(matched_event.get("id") or "").strip()
    if not event_id:
        return {"ok": False, "context": {}, "environment": {}, "quota": events_result.get("quota"), "message": "Matched odds event is missing an id."}

    odds_result = odds_api_fetch(
        f"/sports/{sport}/events/{event_id}/odds",
        api_key,
        {
            "regions": regions,
            "markets": ",".join(ODDS_GAME_CONTEXT_MARKETS),
            "oddsFormat": odds_format,
            "dateFormat": "iso",
            "bookmakers": ",".join(requested_bookmakers),
        },
    )
    odds_event = odds_result.get("data") or {}
    event_context = build_event_market_context(odds_event)
    environment = enrich_environment_with_market_context(
        {},
        {
            "home_team": odds_event.get("home_team") or matched_event.get("home_team") or "",
            "away_team": odds_event.get("away_team") or matched_event.get("away_team") or "",
            **event_context,
        },
        player_team_name=team_name,
        player_team_abbreviation=team_abbreviation,
    )
    return {
        "ok": True,
        "event_id": event_id,
        "home_team": odds_event.get("home_team") or matched_event.get("home_team") or "",
        "away_team": odds_event.get("away_team") or matched_event.get("away_team") or "",
        "context": event_context,
        "environment": environment,
        "quota": odds_result.get("quota") or events_result.get("quota"),
        "api_key_masked": mask_api_key_for_display(api_key),
    }
