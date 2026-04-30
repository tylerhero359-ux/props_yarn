from __future__ import annotations

import copy
import csv
import hashlib
import io
import json
import logging
import math
import os
import queue

import re
import datetime as dt
import time
import threading
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
try:
    import pdfplumber  # type: ignore
except Exception:
    pdfplumber = None
try:
    import psycopg2  # type: ignore
    from psycopg2 import pool as psycopg2_pool  # type: ignore
    from psycopg2.extras import Json as PgJson  # type: ignore
except Exception:
    psycopg2 = None
    psycopg2_pool = None
    PgJson = None
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None

from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from nba_api.stats.endpoints import (
    CommonPlayerInfo,
    CommonTeamRoster,
    LeagueDashTeamStats,
    LeagueDashPlayerStats,
    PlayerGameLog,
    PlayerNextNGames,
    ScoreboardV2,
    TeamGameLog,
)
from nba_api.stats.static import players as static_players
from nba_api.stats.static import teams as static_teams
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4
from identity_utils import (
    PlayerSearchIndex,
    build_player_name_variants,
    build_team_alias_lookup,
    normalize_compact_text,
    normalize_name,
    normalize_report_person_name,
)
from injury_service import InjuryReportService
from market_scan_core_service import MarketScanCoreService
from market_scan_service import MarketScanService
from parlay_service import ParlayService
from player_data_service import PlayerDataService
from progress_stream_service import ProgressStreamService
from persistence_utils import load_json_snapshot, save_json_snapshot
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

if load_dotenv:
    load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

NBA_TIMING_ENABLED = os.getenv("NBA_TIMING_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
NBA_TIMING_LOG_ALL = os.getenv("NBA_TIMING_LOG_ALL", "0").strip().lower() in {"1", "true", "yes", "on"}
NBA_TIMING_SLOW_MS = float(os.getenv("NBA_TIMING_SLOW_MS", "800"))
DEFAULT_SEASON_TYPE = "Combined"
SEASON_TYPE_REGULAR = "Regular Season"
SEASON_TYPE_PLAYOFFS = "Playoffs"
SEASON_TYPE_COMBINED = "Combined"
NBA_PLAYOFF_MODE_START_MONTH = 4
NBA_PLAYOFF_MODE_START_DAY = 18
NBA_PLAYOFF_MODE_END_MONTH = 7
NBA_PLAYOFF_MODE_END_DAY = 31

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
WARM_CACHE_PRELOAD_TEAM_RANKS = os.getenv("NBA_WARM_CACHE_PRELOAD_TEAM_RANKS", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_PRELOAD_TEAM_ROSTERS = os.getenv("NBA_WARM_CACHE_PRELOAD_TEAM_ROSTERS", "1").strip().lower() not in {"0", "false", "no", "off"}
WARM_CACHE_TEAM_ROSTERS_MAX_WORKERS = max(1, int(os.getenv("NBA_WARM_CACHE_TEAM_ROSTERS_MAX_WORKERS", "2")))

PERSISTENT_CACHE_DIR = BASE_DIR / ".runtime_cache"
PERSISTENT_CACHE_PATH = PERSISTENT_CACHE_DIR / "persistent_cache.json"
BACKTEST_PERSIST_PATH = PERSISTENT_CACHE_DIR / "backtest_log.json"
KEY_VAULT_PERSIST_PATH = PERSISTENT_CACHE_DIR / "key_vault.json"
FAVORITES_PERSIST_PATH = PERSISTENT_CACHE_DIR / "favorites.json"
TRACKER_PERSIST_PATH = PERSISTENT_CACHE_DIR / "tracker_props.json"
KEY_VAULT_SHRINK_GUARD_RATIO = min(0.95, max(0.05, float(os.getenv("NBA_KEY_VAULT_SHRINK_GUARD_RATIO", "0.5"))))
KEY_VAULT_SHRINK_GUARD_MIN_EXISTING = max(5, int(os.getenv("NBA_KEY_VAULT_SHRINK_GUARD_MIN_EXISTING", "10")))
PERSISTENT_CACHE_ENABLED = os.getenv("NBA_PERSISTENT_CACHE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
PERSISTENT_CACHE_MAX_ROSTERS = max(30, int(os.getenv("NBA_PERSISTENT_CACHE_MAX_ROSTERS", "120")))
PERSISTENT_CACHE_MAX_PLAYER_INFO = max(100, int(os.getenv("NBA_PERSISTENT_CACHE_MAX_PLAYER_INFO", "600")))
PERSISTENT_CACHE_MAX_NEXT_GAMES = max(30, int(os.getenv("NBA_PERSISTENT_CACHE_MAX_NEXT_GAMES", "240")))
PERSISTENT_CACHE_LOCK = Lock()

POSTGRES_DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
POSTGRES_CACHE_ENABLED = os.getenv("NBA_POSTGRES_CACHE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
POSTGRES_CACHE_WRITE_ENABLED = os.getenv("NBA_POSTGRES_CACHE_WRITE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
POSTGRES_CACHE_PRELOAD = os.getenv("NBA_POSTGRES_CACHE_PRELOAD", "1").strip().lower() not in {"0", "false", "no", "off"}
POSTGRES_CACHE_PRELOAD_LIMIT = max(100, int(os.getenv("NBA_POSTGRES_CACHE_PRELOAD_LIMIT", "1500")))
POSTGRES_CONNECT_TIMEOUT_SECONDS = max(2, int(os.getenv("NBA_POSTGRES_CONNECT_TIMEOUT_SECONDS", "5")))
POSTGRES_POOL_MIN_CONNECTIONS = max(1, int(os.getenv("NBA_POSTGRES_POOL_MIN_CONNECTIONS", "2")))
POSTGRES_POOL_MAX_CONNECTIONS = max(POSTGRES_POOL_MIN_CONNECTIONS, int(os.getenv("NBA_POSTGRES_POOL_MAX_CONNECTIONS", "10")))
POSTGRES_MAX_WRITE_WORKERS = max(1, int(os.getenv("NBA_POSTGRES_MAX_WRITE_WORKERS", "2")))
POSTGRES_WRITE_EXECUTOR = ThreadPoolExecutor(max_workers=POSTGRES_MAX_WRITE_WORKERS)
POSTGRES_SOURCE_OF_TRUTH = os.getenv("NBA_POSTGRES_SOURCE_OF_TRUTH", "1").strip().lower() not in {"0", "false", "no", "off"}
ASYNC_JOB_MAX_WORKERS = max(2, int(os.getenv("NBA_ASYNC_JOB_MAX_WORKERS", "4")))
ASYNC_JOB_RETENTION = max(50, int(os.getenv("NBA_ASYNC_JOB_RETENTION", "200")))
ASYNC_JOB_EXECUTOR = ThreadPoolExecutor(max_workers=ASYNC_JOB_MAX_WORKERS)
ASYNC_JOB_LOCK = Lock()
ASYNC_JOB_REGISTRY: dict[str, dict[str, Any]] = {}
POSTGRES_RETENTION_ODDS_DAYS = max(1, int(os.getenv("NBA_POSTGRES_RETENTION_ODDS_DAYS", "14")))
POSTGRES_RETENTION_MARKET_SCAN_DAYS = max(1, int(os.getenv("NBA_POSTGRES_RETENTION_MARKET_SCAN_DAYS", "30")))
POSTGRES_RETENTION_PARLAY_DAYS = max(1, int(os.getenv("NBA_POSTGRES_RETENTION_PARLAY_DAYS", "30")))
POSTGRES_RETENTION_INJURY_DAYS = max(1, int(os.getenv("NBA_POSTGRES_RETENTION_INJURY_DAYS", "30")))
POSTGRES_RETENTION_BACKTEST_DAYS = max(1, int(os.getenv("NBA_POSTGRES_RETENTION_BACKTEST_DAYS", "365")))
POSTGRES_DEDUPE_ENABLED = os.getenv("NBA_POSTGRES_DEDUPE_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
POSTGRES_BACKFILL_HASHES_ENABLED = os.getenv("NBA_POSTGRES_BACKFILL_HASHES_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
POSTGRES_BACKFILL_HASHES_LIMIT = max(100, int(os.getenv("NBA_POSTGRES_BACKFILL_HASHES_LIMIT", "5000")))
MARKET_SCAN_CACHE_TTL_SECONDS = max(0, int(os.getenv("NBA_MARKET_SCAN_CACHE_TTL_SECONDS", "900")))
PARLAY_BUILDER_CACHE_TTL_SECONDS = max(0, int(os.getenv("NBA_PARLAY_BUILDER_CACHE_TTL_SECONDS", "900")))
MARKET_SCAN_MAX_ROWS = max(1, int(os.getenv("NBA_MARKET_SCAN_MAX_ROWS", "500")))
PROP_ANALYSIS_MAX_LAST_N = max(1, int(os.getenv("NBA_PROP_ANALYSIS_MAX_LAST_N", "82")))
PARLAY_BUILDER_MAX_BATCH_SIZE = max(1, int(os.getenv("NBA_PARLAY_BUILDER_MAX_BATCH_SIZE", "6")))
ODDS_API_MAX_RETRIES = max(0, int(os.getenv("NBA_ODDS_API_MAX_RETRIES", "2")))
ODDS_API_RETRY_BACKOFF_SECONDS = max(0.2, float(os.getenv("NBA_ODDS_API_RETRY_BACKOFF_SECONDS", "0.8")))
ODDS_API_QUERY_AUTH_FALLBACK_ENABLED = os.getenv("NBA_ODDS_API_QUERY_AUTH_FALLBACK_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
APP_TIMEZONE = (os.getenv("APP_TIMEZONE", "Asia/Manila") or "Asia/Manila").strip()
NBA_GAME_DATE_TIMEZONE = (os.getenv("NBA_GAME_DATE_TIMEZONE", "America/New_York") or "America/New_York").strip()
ALLOWED_ORIGINS_RAW = str(os.getenv("ALLOWED_ORIGINS", "*")).strip()
RATE_LIMIT_ENABLED = os.getenv("NBA_RATE_LIMIT_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
RATE_LIMIT_WINDOW_SECONDS = max(5, int(os.getenv("NBA_RATE_LIMIT_WINDOW_SECONDS", "60")))
RATE_LIMIT_HEAVY_MAX_REQUESTS = max(1, int(os.getenv("NBA_RATE_LIMIT_HEAVY_MAX_REQUESTS", "20")))
RATE_LIMIT_READ_MAX_REQUESTS = max(1, int(os.getenv("NBA_RATE_LIMIT_READ_MAX_REQUESTS", "120")))

WARM_CACHE_START_LOCK = threading.Lock()
WARM_CACHE_STARTED = False
WARM_CACHE_THREAD_LOCK = threading.Lock()
WARM_CACHE_THREAD_STARTED = False
POSTGRES_CONNECTION_POOL: Any | None = None
_RATE_LIMIT_LOCK = Lock()
_RATE_LIMIT_BUCKETS: dict[str, list[float]] = {}

def _warm_cache_task(name: str, func):
    try:
        start = time.perf_counter()
        LOGGER.info("Warm-cache task %s started", name)
        func()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        LOGGER.info("Warm-cache task %s ready in %.1f ms", name, elapsed_ms)
        if NBA_TIMING_LOG_ALL or elapsed_ms >= NBA_TIMING_SLOW_MS:
            LOGGER.info("TIMING warm_cache:%s took %.1f ms", name, elapsed_ms)
    except Exception as exc:
        LOGGER.warning("Warm-cache task %s failed: %s", name, exc)

def _today_scoreboard_date() -> str:
    return nba_now().strftime("%Y-%m-%d")


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
    named_targets = {
        "roster_cache": ROSTER_CACHE,
        "player_info_cache": PLAYER_INFO_CACHE,
        "next_game_cache": NEXT_GAME_CACHE,
        "team_next_game_cache": TEAM_NEXT_GAME_CACHE,
    }
    if POSTGRES_SOURCE_OF_TRUTH:
        named_targets = {"roster_cache": ROSTER_CACHE}
    runtime_load_persistent_caches(
        enabled=PERSISTENT_CACHE_ENABLED,
        cache_path=PERSISTENT_CACHE_PATH,
        cache_lock=PERSISTENT_CACHE_LOCK,
        named_targets=named_targets,
        logger=LOGGER,
    )
    if PERSISTENT_CACHE_ENABLED and not POSTGRES_SOURCE_OF_TRUTH:
        for cache in (PLAYER_INFO_CACHE, NEXT_GAME_CACHE, TEAM_NEXT_GAME_CACHE):
            for entry in cache.values():
                if isinstance(entry, dict) and "source" not in entry:
                    entry["source"] = "persistent"
    if PERSISTENT_CACHE_ENABLED and PERSISTENT_CACHE_PATH.exists():
        LOGGER.info(
            "Loaded persistent cache snapshot: %s rosters, %s player info, %s next games, %s team next games",
            len(ROSTER_CACHE),
            len(PLAYER_INFO_CACHE),
            len(NEXT_GAME_CACHE),
            len(TEAM_NEXT_GAME_CACHE),
        )


def postgres_available() -> bool:
    return bool(POSTGRES_DATABASE_URL and POSTGRES_CACHE_ENABLED and psycopg2)


def init_postgres_pool() -> None:
    global POSTGRES_CONNECTION_POOL
    if POSTGRES_CONNECTION_POOL is not None:
        return
    if not postgres_available() or not psycopg2_pool:
        return
    try:
        POSTGRES_CONNECTION_POOL = psycopg2_pool.ThreadedConnectionPool(
            minconn=POSTGRES_POOL_MIN_CONNECTIONS,
            maxconn=POSTGRES_POOL_MAX_CONNECTIONS,
            dsn=POSTGRES_DATABASE_URL,
            connect_timeout=POSTGRES_CONNECT_TIMEOUT_SECONDS,
        )
        LOGGER.info(
            "Initialized Postgres connection pool (%s-%s)",
            POSTGRES_POOL_MIN_CONNECTIONS,
            POSTGRES_POOL_MAX_CONNECTIONS,
        )
    except Exception as exc:
        POSTGRES_CONNECTION_POOL = None
        LOGGER.warning("Postgres pool init failed; falling back to direct connections: %s", exc)


def close_postgres_pool() -> None:
    global POSTGRES_CONNECTION_POOL
    pool_obj = POSTGRES_CONNECTION_POOL
    POSTGRES_CONNECTION_POOL = None
    if not pool_obj:
        return
    try:
        pool_obj.closeall()
    except Exception as exc:
        LOGGER.debug("Postgres pool close failed: %s", exc)


@contextmanager
def _postgres_pool_connection():
    if not POSTGRES_CONNECTION_POOL:
        raise RuntimeError("Postgres pool unavailable")
    conn = POSTGRES_CONNECTION_POOL.getconn()
    try:
        yield conn
        try:
            conn.commit()
        except Exception:
            pass
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            POSTGRES_CONNECTION_POOL.putconn(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass


def postgres_connect():
    if POSTGRES_CONNECTION_POOL:
        return _postgres_pool_connection()
    return psycopg2.connect(POSTGRES_DATABASE_URL, connect_timeout=POSTGRES_CONNECT_TIMEOUT_SECONDS)


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _datetime_to_ts(value: Any) -> float | None:
    parsed = _coerce_datetime(value)
    if not parsed:
        return None
    try:
        return parsed.timestamp()
    except Exception:
        return None


def _utc_now_naive() -> datetime:
    return datetime.now(dt.timezone.utc).replace(tzinfo=None)


def _utc_iso_z() -> str:
    return _utc_now_naive().isoformat() + "Z"


def _pg_read_latest_injury_report() -> tuple[dict[str, Any] | None, float | None]:
    if not postgres_available():
        return None, None
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload, report_timestamp, fetched_at
                FROM injury_reports
                ORDER BY COALESCE(report_timestamp, fetched_at) DESC NULLS LAST
                LIMIT 1;
                """
            )
            row = cur.fetchone()
            if row and isinstance(row[0], dict):
                payload = row[0]
                report_ts = _datetime_to_ts(row[1])
                fetched_ts = _datetime_to_ts(row[2])
                return payload, (report_ts or fetched_ts)
    except Exception as exc:
        LOGGER.debug("Postgres injury report read failed: %s", exc)
    return None, None


def _pg_read_player_info(player_id: int) -> tuple[dict[str, Any] | None, float | None]:
    if not postgres_available():
        return None, None
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT payload, updated_at FROM player_info_cache WHERE player_id = %s;",
                (int(player_id),),
            )
            row = cur.fetchone()
            if row and isinstance(row[0], dict):
                return row[0], _datetime_to_ts(row[1])
    except Exception as exc:
        LOGGER.debug("Postgres player_info read failed: %s", exc)
    return None, None


def _pg_read_game_log(player_id: int, season: str, season_type: str) -> tuple[list[dict[str, Any]] | None, float | None]:
    if not postgres_available():
        return None, None
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload, updated_at
                FROM player_game_logs
                WHERE player_id = %s AND season = %s AND season_type = %s AND schema_version = %s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                (int(player_id), str(season), str(season_type), str(GAME_LOG_CACHE_SCHEMA_VERSION)),
            )
            row = cur.fetchone()
            if row and isinstance(row[0], dict):
                rows = row[0].get("rows") or []
                if isinstance(rows, list) and rows:
                    return rows, _datetime_to_ts(row[1])
    except Exception as exc:
        LOGGER.debug("Postgres game_log read failed: %s", exc)
    return None, None


def _pg_read_team_next_game(team_id: int, season: str, season_type: str) -> tuple[dict[str, Any] | None, float | None]:
    if not postgres_available():
        return None, None
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload, updated_at
                FROM team_next_game_cache
                WHERE team_id = %s AND season = %s AND season_type = %s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                (int(team_id), str(season), str(season_type)),
            )
            row = cur.fetchone()
            if row and isinstance(row[0], dict):
                return row[0], _datetime_to_ts(row[1])
    except Exception as exc:
        LOGGER.debug("Postgres team_next_game read failed: %s", exc)
    return None, None


def init_postgres_cache() -> None:
    if not postgres_available():
        return
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS player_info_cache (
                    player_id INTEGER PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS player_game_logs (
                    player_id INTEGER NOT NULL,
                    season TEXT NOT NULL,
                    season_type TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (player_id, season, season_type, schema_version)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS team_next_game_cache (
                    team_id INTEGER NOT NULL,
                    season TEXT NOT NULL,
                    season_type TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (team_id, season, season_type)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS injury_reports (
                    report_url TEXT PRIMARY KEY,
                    report_timestamp TIMESTAMPTZ,
                    report_label TEXT,
                    payload JSONB NOT NULL,
                    fetched_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS odds_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    endpoint TEXT NOT NULL,
                    params JSONB,
                    payload JSONB NOT NULL,
                    payload_hash TEXT,
                    fetched_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS market_scan_runs (
                    id BIGSERIAL PRIMARY KEY,
                    payload JSONB NOT NULL,
                    payload_hash TEXT,
                    request_hash TEXT,
                    requested_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS parlay_builder_runs (
                    id BIGSERIAL PRIMARY KEY,
                    payload JSONB NOT NULL,
                    payload_hash TEXT,
                    request_hash TEXT,
                    requested_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bet_finder_runs (
                    id BIGSERIAL PRIMARY KEY,
                    payload JSONB NOT NULL,
                    payload_hash TEXT,
                    requested_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cur.execute("ALTER TABLE odds_snapshots ADD COLUMN IF NOT EXISTS payload_hash TEXT;")
            cur.execute("ALTER TABLE market_scan_runs ADD COLUMN IF NOT EXISTS payload_hash TEXT;")
            cur.execute("ALTER TABLE market_scan_runs ADD COLUMN IF NOT EXISTS request_hash TEXT;")
            cur.execute("ALTER TABLE parlay_builder_runs ADD COLUMN IF NOT EXISTS payload_hash TEXT;")
            cur.execute("ALTER TABLE parlay_builder_runs ADD COLUMN IF NOT EXISTS request_hash TEXT;")
            cur.execute("ALTER TABLE bet_finder_runs ADD COLUMN IF NOT EXISTS payload_hash TEXT;")
            cur.execute("CREATE INDEX IF NOT EXISTS odds_snapshots_hash_idx ON odds_snapshots (endpoint, payload_hash);")
            cur.execute("CREATE INDEX IF NOT EXISTS market_scan_hash_idx ON market_scan_runs (payload_hash);")
            cur.execute("CREATE INDEX IF NOT EXISTS market_scan_request_hash_idx ON market_scan_runs (request_hash);")
            cur.execute("CREATE INDEX IF NOT EXISTS parlay_runs_hash_idx ON parlay_builder_runs (payload_hash);")
            cur.execute("CREATE INDEX IF NOT EXISTS parlay_runs_request_hash_idx ON parlay_builder_runs (request_hash);")
            cur.execute("CREATE INDEX IF NOT EXISTS bet_finder_hash_idx ON bet_finder_runs (payload_hash);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS backtest_log_entries (
                    entry_id TEXT PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS key_vault_state (
                    state_key TEXT PRIMARY KEY,
                    payload JSONB NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )
        LOGGER.info("Postgres cache tables ready")
    except Exception as exc:
        LOGGER.warning("Postgres cache init failed: %s", exc)


def _submit_pg_write(func, *args) -> None:
    if not (postgres_available() and POSTGRES_CACHE_WRITE_ENABLED):
        return
    try:
        POSTGRES_WRITE_EXECUTOR.submit(func, *args)
    except Exception as exc:
        LOGGER.debug("Postgres cache write skipped: %s", exc)


def _payload_hash(payload: Any) -> str:
    payload_text = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(payload_text.encode("utf-8")).hexdigest()


def _request_hash(cache_scope: str, request_payload: dict[str, Any]) -> str:
    return _payload_hash({"cache_scope": str(cache_scope), "request": request_payload})


def _parse_bounded_int(
    value: Any,
    *,
    default: int,
    min_value: int,
    max_value: int,
    field_name: str,
) -> int:
    raw_value = default if value in (None, "") else value
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"{field_name} must be an integer.")
    if parsed < min_value or parsed > max_value:
        raise HTTPException(status_code=400, detail=f"{field_name} must be between {min_value} and {max_value}.")
    return parsed


def _require_pg_backtest_write(func, *args) -> None:
    if not (postgres_available() and POSTGRES_CACHE_WRITE_ENABLED):
        return
    try:
        func(*args)
    except Exception as exc:
        LOGGER.warning("Postgres backtest write failed: %s", exc)
        raise HTTPException(status_code=503, detail=f"Backtest Postgres write failed: {exc}")


def _prune_async_jobs() -> None:
    with ASYNC_JOB_LOCK:
        if len(ASYNC_JOB_REGISTRY) <= ASYNC_JOB_RETENTION:
            return
        # Remove oldest finished jobs first
        finished = [k for k, v in ASYNC_JOB_REGISTRY.items() if v.get("status") in {"done", "error"}]
        finished.sort(key=lambda k: ASYNC_JOB_REGISTRY[k].get("finished_at") or 0.0)
        while len(ASYNC_JOB_REGISTRY) > ASYNC_JOB_RETENTION and finished:
            ASYNC_JOB_REGISTRY.pop(finished.pop(0), None)


def submit_async_job(job_type: str, func, payload: dict[str, Any]) -> dict[str, Any]:
    job_id = uuid4().hex
    now_ts = time.time()
    with ASYNC_JOB_LOCK:
        ASYNC_JOB_REGISTRY[job_id] = {
            "id": job_id,
            "type": job_type,
            "status": "queued",
            "created_at": now_ts,
            "started_at": None,
            "finished_at": None,
            "result": None,
            "error": None,
        }

    def _runner():
        with ASYNC_JOB_LOCK:
            ASYNC_JOB_REGISTRY[job_id]["status"] = "running"
            ASYNC_JOB_REGISTRY[job_id]["started_at"] = time.time()
        try:
            result = func(payload)
            with ASYNC_JOB_LOCK:
                ASYNC_JOB_REGISTRY[job_id]["status"] = "done"
                ASYNC_JOB_REGISTRY[job_id]["result"] = result
                ASYNC_JOB_REGISTRY[job_id]["finished_at"] = time.time()
        except Exception as exc:
            with ASYNC_JOB_LOCK:
                ASYNC_JOB_REGISTRY[job_id]["status"] = "error"
                ASYNC_JOB_REGISTRY[job_id]["error"] = str(exc)
                ASYNC_JOB_REGISTRY[job_id]["finished_at"] = time.time()
        _prune_async_jobs()

    ASYNC_JOB_EXECUTOR.submit(_runner)
    return {"ok": True, "job_id": job_id, "status": "queued"}


def get_async_job(job_id: str) -> dict[str, Any]:
    with ASYNC_JOB_LOCK:
        job = ASYNC_JOB_REGISTRY.get(job_id)
        if not job:
            return {"ok": False, "error": "Job not found."}
        return {"ok": True, **job}


def _pg_write_player_info(player_id: int, payload: dict[str, Any]) -> None:
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO player_info_cache (player_id, payload, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (player_id)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW();
                """,
                (int(player_id), PgJson(payload)),
            )
    except Exception as exc:
        LOGGER.debug("Postgres player_info write failed: %s", exc)


def _pg_write_game_log(player_id: int, season: str, season_type: str, schema_version: str, rows: list[dict[str, Any]]) -> None:
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO player_game_logs (player_id, season, season_type, schema_version, payload, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (player_id, season, season_type, schema_version)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW();
                """,
                (int(player_id), str(season), str(season_type), str(schema_version), PgJson({"rows": rows})),
            )
    except Exception as exc:
        LOGGER.debug("Postgres game_log write failed: %s", exc)


def _pg_write_team_next_game(team_id: int, season: str, season_type: str, payload: dict[str, Any] | None) -> None:
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO team_next_game_cache (team_id, season, season_type, payload, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (team_id, season, season_type)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW();
                """,
                (int(team_id), str(season), str(season_type), PgJson(payload or {})),
            )
    except Exception as exc:
        LOGGER.debug("Postgres team_next_game write failed: %s", exc)


def _pg_write_injury_report(payload: dict[str, Any]) -> None:
    try:
        report_url = str(payload.get("report_url") or "")
        if not report_url:
            return
        report_timestamp = payload.get("report_timestamp") or None
        report_label = str(payload.get("report_label") or "")
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO injury_reports (report_url, report_timestamp, report_label, payload, fetched_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (report_url)
                DO UPDATE SET payload = EXCLUDED.payload, report_timestamp = EXCLUDED.report_timestamp, report_label = EXCLUDED.report_label, fetched_at = NOW();
                """,
                (report_url, report_timestamp, report_label, PgJson(payload)),
            )
    except Exception as exc:
        LOGGER.debug("Postgres injury report write failed: %s", exc)


def _pg_write_odds_snapshot(endpoint: str, params: dict[str, Any] | None, payload: Any) -> None:
    try:
        payload_hash = _payload_hash({"endpoint": endpoint, "payload": payload})
        with postgres_connect() as conn, conn.cursor() as cur:
            if POSTGRES_DEDUPE_ENABLED:
                cur.execute(
                    "SELECT 1 FROM odds_snapshots WHERE endpoint = %s AND payload_hash = %s LIMIT 1;",
                    (str(endpoint), payload_hash),
                )
                if cur.fetchone():
                    return
            cur.execute(
                """
                INSERT INTO odds_snapshots (endpoint, params, payload, payload_hash, fetched_at)
                VALUES (%s, %s, %s, %s, NOW());
                """,
                (str(endpoint), PgJson(params or {}), PgJson(payload), payload_hash),
            )
    except Exception as exc:
        LOGGER.debug("Postgres odds snapshot write failed: %s", exc)


def _pg_write_market_scan_run(payload: dict[str, Any], request_hash: str | None = None) -> None:
    try:
        payload_hash = _payload_hash(payload)
        normalized_request_hash = str(request_hash or "").strip() or None
        with postgres_connect() as conn, conn.cursor() as cur:
            if POSTGRES_DEDUPE_ENABLED:
                if normalized_request_hash:
                    cur.execute(
                        "SELECT 1 FROM market_scan_runs WHERE request_hash = %s AND payload_hash = %s LIMIT 1;",
                        (normalized_request_hash, payload_hash),
                    )
                else:
                    cur.execute(
                        "SELECT 1 FROM market_scan_runs WHERE payload_hash = %s LIMIT 1;",
                        (payload_hash,),
                    )
                if cur.fetchone():
                    return
            cur.execute(
                """
                INSERT INTO market_scan_runs (payload, payload_hash, request_hash, requested_at)
                VALUES (%s, %s, %s, NOW());
                """,
                (PgJson(payload), payload_hash, normalized_request_hash),
            )
    except Exception as exc:
        LOGGER.debug("Postgres market scan write failed: %s", exc)


def _pg_write_parlay_builder_run(payload: dict[str, Any], request_hash: str | None = None) -> None:
    try:
        payload_hash = _payload_hash(payload)
        normalized_request_hash = str(request_hash or "").strip() or None
        with postgres_connect() as conn, conn.cursor() as cur:
            if POSTGRES_DEDUPE_ENABLED:
                if normalized_request_hash:
                    cur.execute(
                        "SELECT 1 FROM parlay_builder_runs WHERE request_hash = %s AND payload_hash = %s LIMIT 1;",
                        (normalized_request_hash, payload_hash),
                    )
                else:
                    cur.execute(
                        "SELECT 1 FROM parlay_builder_runs WHERE payload_hash = %s LIMIT 1;",
                        (payload_hash,),
                    )
                if cur.fetchone():
                    return
            cur.execute(
                """
                INSERT INTO parlay_builder_runs (payload, payload_hash, request_hash, requested_at)
                VALUES (%s, %s, %s, NOW());
                """,
                (PgJson(payload), payload_hash, normalized_request_hash),
            )
    except Exception as exc:
        LOGGER.debug("Postgres parlay write failed: %s", exc)


def _postgres_cached_run_is_fresh(requested_at: Any, ttl_seconds: int) -> tuple[bool, float | None]:
    if ttl_seconds <= 0:
        return False, None
    requested_ts = _datetime_to_ts(requested_at)
    if requested_ts is None:
        return False, None
    age_seconds = max(0.0, time.time() - float(requested_ts))
    return age_seconds <= ttl_seconds, age_seconds


def _build_postgres_cached_run(payload: Any, requested_at: Any, ttl_seconds: int, source: str) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    is_fresh, age_seconds = _postgres_cached_run_is_fresh(requested_at, ttl_seconds)
    if not is_fresh:
        return None
    cached = copy.deepcopy(payload)
    cached["cache_hit"] = True
    cached["cache_source"] = source
    cached["cache_age_seconds"] = round(age_seconds or 0.0, 2)
    return cached


def _pg_read_market_scan_cache(request_payload: dict[str, Any], *, cache_scope: str = "market_scan") -> dict[str, Any] | None:
    if not postgres_available():
        return None
    try:
        request_hash = _request_hash(cache_scope, request_payload)
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload, requested_at
                FROM market_scan_runs
                WHERE request_hash = %s
                ORDER BY requested_at DESC
                LIMIT 1;
                """,
                (request_hash,),
            )
            row = cur.fetchone()
            if row:
                cached = _build_postgres_cached_run(row[0], row[1], MARKET_SCAN_CACHE_TTL_SECONDS, "postgres")
                if cached:
                    return cached

            # Backward-compatible fallback for historical rows that only had payload_hash.
            legacy_payload_hash = _payload_hash(request_payload)
            cur.execute(
                """
                SELECT payload, requested_at
                FROM market_scan_runs
                WHERE payload_hash = %s
                ORDER BY requested_at DESC
                LIMIT 1;
                """,
                (legacy_payload_hash,),
            )
            row = cur.fetchone()
            if row:
                cached = _build_postgres_cached_run(row[0], row[1], MARKET_SCAN_CACHE_TTL_SECONDS, "postgres_legacy")
                if cached:
                    return cached
    except Exception as exc:
        LOGGER.debug("Postgres market scan cache read failed: %s", exc)
    return None


def _pg_read_parlay_builder_cache(request_payload: dict[str, Any], *, cache_scope: str = "parlay_builder") -> dict[str, Any] | None:
    if not postgres_available():
        return None
    try:
        request_hash = _request_hash(cache_scope, request_payload)
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload, requested_at
                FROM parlay_builder_runs
                WHERE request_hash = %s
                ORDER BY requested_at DESC
                LIMIT 1;
                """,
                (request_hash,),
            )
            row = cur.fetchone()
            if row:
                cached = _build_postgres_cached_run(row[0], row[1], PARLAY_BUILDER_CACHE_TTL_SECONDS, "postgres")
                if cached:
                    return cached

            # Backward-compatible fallback for historical rows that only had payload_hash.
            legacy_payload_hash = _payload_hash(request_payload)
            cur.execute(
                """
                SELECT payload, requested_at
                FROM parlay_builder_runs
                WHERE payload_hash = %s
                ORDER BY requested_at DESC
                LIMIT 1;
                """,
                (legacy_payload_hash,),
            )
            row = cur.fetchone()
            if row:
                cached = _build_postgres_cached_run(row[0], row[1], PARLAY_BUILDER_CACHE_TTL_SECONDS, "postgres_legacy")
                if cached:
                    return cached
    except Exception as exc:
        LOGGER.debug("Postgres parlay cache read failed: %s", exc)
    return None


def _pg_write_bet_finder_run(payload: dict[str, Any]) -> None:
    try:
        payload_hash = _payload_hash(payload)
        with postgres_connect() as conn, conn.cursor() as cur:
            if POSTGRES_DEDUPE_ENABLED:
                cur.execute(
                    "SELECT 1 FROM bet_finder_runs WHERE payload_hash = %s LIMIT 1;",
                    (payload_hash,),
                )
                if cur.fetchone():
                    return
            cur.execute(
                """
                INSERT INTO bet_finder_runs (payload, payload_hash, requested_at)
                VALUES (%s, %s, NOW());
                """,
                (PgJson(payload), payload_hash),
            )
    except Exception as exc:
        LOGGER.debug("Postgres bet finder write failed: %s", exc)


def _pg_write_backtest_entries(entries: list[dict[str, Any]]) -> None:
    with postgres_connect() as conn, conn.cursor() as cur:
        for entry in entries:
            entry_id = str(entry.get("id") or "")
            if not entry_id:
                continue
            cur.execute(
                """
                INSERT INTO backtest_log_entries (entry_id, payload, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (entry_id)
                DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW();
                """,
                (entry_id, PgJson(entry)),
            )


def _pg_delete_backtest_entry(entry_id: str) -> None:
    with postgres_connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM backtest_log_entries WHERE entry_id = %s;", (str(entry_id),))


def _pg_delete_backtest_entries(entry_ids: list[str]) -> None:
    cleaned = [str(entry_id).strip() for entry_id in (entry_ids or []) if str(entry_id).strip()]
    if not cleaned:
        return
    with postgres_connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM backtest_log_entries WHERE entry_id = ANY(%s);", (cleaned,))


def _pg_clear_backtest_entries() -> None:
    with postgres_connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM backtest_log_entries;")


def _pg_fetch_backtest_entries(limit: int = 5000) -> list[dict[str, Any]]:
    if not postgres_available():
        return []
    entries: list[dict[str, Any]] = []
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload
                FROM backtest_log_entries
                ORDER BY updated_at DESC
                LIMIT %s;
                """,
                (max(1, int(limit)),),
            )
            for (payload,) in cur.fetchall():
                if isinstance(payload, dict):
                    entries.append(payload)
    except Exception as exc:
        LOGGER.debug("Postgres backtest fetch failed: %s", exc)
    return entries


def _pg_read_key_vault_state() -> dict[str, Any] | None:
    if not postgres_available():
        return None
    for attempt in range(2):
        try:
            with postgres_connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT payload
                    FROM key_vault_state
                    WHERE state_key = 'default'
                    LIMIT 1;
                    """
                )
                row = cur.fetchone()
                if row and isinstance(row[0], dict):
                    return row[0]
                return None
        except Exception as exc:
            msg = str(exc).lower()
            recoverable = "ssl connection has been closed unexpectedly" in msg or "server closed the connection unexpectedly" in msg
            if recoverable and attempt == 0:
                LOGGER.warning("Postgres key vault read dropped SSL connection; resetting pool and retrying once")
                close_postgres_pool()
                init_postgres_pool()
                continue
            LOGGER.debug("Postgres key vault read failed: %s", exc)
            break
    return None


def _pg_write_key_vault_state(state: dict[str, Any]) -> None:
    for attempt in range(2):
        try:
            with postgres_connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO key_vault_state (state_key, payload, updated_at)
                    VALUES ('default', %s, NOW())
                    ON CONFLICT (state_key)
                    DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW();
                    """,
                    (PgJson(state),),
                )
                return
        except Exception as exc:
            msg = str(exc).lower()
            recoverable = "ssl connection has been closed unexpectedly" in msg or "server closed the connection unexpectedly" in msg
            if recoverable and attempt == 0:
                LOGGER.warning("Postgres key vault write dropped SSL connection; resetting pool and retrying once")
                close_postgres_pool()
                init_postgres_pool()
                continue
            raise


def preload_postgres_cache() -> None:
    if not (postgres_available() and POSTGRES_CACHE_PRELOAD):
        return
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT player_id, payload, updated_at FROM player_info_cache ORDER BY updated_at DESC LIMIT %s;",
                (POSTGRES_CACHE_PRELOAD_LIMIT,),
            )
            for player_id, payload, updated_at in cur.fetchall():
                if not isinstance(payload, dict):
                    continue
                PLAYER_INFO_CACHE[int(player_id)] = {
                    "timestamp": float(_datetime_to_ts(updated_at) or 0.0),
                    "row": payload,
                    "source": "postgres-preload",
                }
            cur.execute(
                "SELECT player_id, season, season_type, schema_version, payload, updated_at FROM player_game_logs ORDER BY updated_at DESC LIMIT %s;",
                (POSTGRES_CACHE_PRELOAD_LIMIT,),
            )
            for player_id, season, season_type, schema_version, payload, updated_at in cur.fetchall():
                rows = (payload or {}).get("rows") if isinstance(payload, dict) else None
                if not isinstance(rows, list):
                    continue
                GAME_LOG_CACHE[(int(player_id), str(season), str(season_type), str(schema_version))] = {
                    "timestamp": float(_datetime_to_ts(updated_at) or 0.0),
                    "rows": rows,
                    "source": "postgres-preload",
                }
            cur.execute(
                "SELECT team_id, season, season_type, payload, updated_at FROM team_next_game_cache ORDER BY updated_at DESC LIMIT %s;",
                (POSTGRES_CACHE_PRELOAD_LIMIT,),
            )
            for team_id, season, season_type, payload, updated_at in cur.fetchall():
                TEAM_NEXT_GAME_CACHE[(int(team_id), str(season), str(season_type))] = {
                    "timestamp": float(_datetime_to_ts(updated_at) or 0.0),
                    "row": payload if isinstance(payload, dict) else {},
                    "source": "postgres-preload",
                }
            cur.execute(
                """
                SELECT report_url, report_timestamp, report_label, payload, fetched_at
                FROM injury_reports
                ORDER BY report_timestamp DESC NULLS LAST, fetched_at DESC
                LIMIT 1;
                """
            )
            latest_report = cur.fetchone()
            if latest_report:
                report_url, report_timestamp, report_label, payload, fetched_at = latest_report
                if isinstance(payload, dict):
                    INJURY_SERVICE.report_cache["timestamp"] = float(_datetime_to_ts(fetched_at) or 0.0)
                    INJURY_SERVICE.report_cache["payload"] = {
                        **payload,
                        "report_url": report_url or payload.get("report_url"),
                        "report_timestamp": str(report_timestamp) if report_timestamp else payload.get("report_timestamp"),
                        "report_label": report_label or payload.get("report_label"),
                        "source": payload.get("source") or "postgres-preload",
                    }
        LOGGER.info(
            "Postgres cache preload complete: %d player info, %d game logs, %d team next games",
            len(PLAYER_INFO_CACHE),
            len(GAME_LOG_CACHE),
            len(TEAM_NEXT_GAME_CACHE),
        )
    except Exception as exc:
        LOGGER.warning("Postgres cache preload failed: %s", exc)


def cleanup_postgres_retention() -> None:
    if not postgres_available():
        return
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM odds_snapshots WHERE fetched_at < NOW() - (%s || ' days')::interval;",
                (str(POSTGRES_RETENTION_ODDS_DAYS),),
            )
            cur.execute(
                "DELETE FROM market_scan_runs WHERE requested_at < NOW() - (%s || ' days')::interval;",
                (str(POSTGRES_RETENTION_MARKET_SCAN_DAYS),),
            )
            cur.execute(
                "DELETE FROM parlay_builder_runs WHERE requested_at < NOW() - (%s || ' days')::interval;",
                (str(POSTGRES_RETENTION_PARLAY_DAYS),),
            )
            cur.execute(
                "DELETE FROM injury_reports WHERE fetched_at < NOW() - (%s || ' days')::interval;",
                (str(POSTGRES_RETENTION_INJURY_DAYS),),
            )
            cur.execute(
                "DELETE FROM backtest_log_entries WHERE updated_at < NOW() - (%s || ' days')::interval;",
                (str(POSTGRES_RETENTION_BACKTEST_DAYS),),
            )
        LOGGER.info("Postgres retention cleanup completed")
    except Exception as exc:
        LOGGER.warning("Postgres retention cleanup failed: %s", exc)


def backfill_postgres_hashes() -> None:
    if not (postgres_available() and POSTGRES_BACKFILL_HASHES_ENABLED):
        return
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_name='odds_snapshots' AND column_name='payload_hash';"
            )
            if not cur.fetchone():
                LOGGER.info("Postgres hash backfill skipped: odds_snapshots.payload_hash missing")
                return
            cur.execute(
                """
                SELECT id, endpoint, payload
                FROM odds_snapshots
                WHERE payload_hash IS NULL
                ORDER BY id ASC
                LIMIT %s;
                """,
                (POSTGRES_BACKFILL_HASHES_LIMIT,),
            )
            for row_id, endpoint, payload in cur.fetchall():
                payload_text = json.dumps({"endpoint": endpoint, "payload": payload}, sort_keys=True, default=str)
                payload_hash = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
                cur.execute(
                    "UPDATE odds_snapshots SET payload_hash = %s WHERE id = %s;",
                    (payload_hash, row_id),
                )

            cur.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_name='market_scan_runs' AND column_name='payload_hash';"
            )
            if not cur.fetchone():
                LOGGER.info("Postgres hash backfill skipped: market_scan_runs.payload_hash missing")
                return
            cur.execute(
                """
                SELECT id, payload
                FROM market_scan_runs
                WHERE payload_hash IS NULL
                ORDER BY id ASC
                LIMIT %s;
                """,
                (POSTGRES_BACKFILL_HASHES_LIMIT,),
            )
            for row_id, payload in cur.fetchall():
                payload_text = json.dumps(payload, sort_keys=True, default=str)
                payload_hash = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
                cur.execute(
                    "UPDATE market_scan_runs SET payload_hash = %s WHERE id = %s;",
                    (payload_hash, row_id),
                )

            cur.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_name='parlay_builder_runs' AND column_name='payload_hash';"
            )
            if not cur.fetchone():
                LOGGER.info("Postgres hash backfill skipped: parlay_builder_runs.payload_hash missing")
                return
            cur.execute(
                """
                SELECT id, payload
                FROM parlay_builder_runs
                WHERE payload_hash IS NULL
                ORDER BY id ASC
                LIMIT %s;
                """,
                (POSTGRES_BACKFILL_HASHES_LIMIT,),
            )
            for row_id, payload in cur.fetchall():
                payload_text = json.dumps(payload, sort_keys=True, default=str)
                payload_hash = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
                cur.execute(
                    "UPDATE parlay_builder_runs SET payload_hash = %s WHERE id = %s;",
                    (payload_hash, row_id),
                )
        LOGGER.info("Postgres hash backfill completed")
    except Exception as exc:
        LOGGER.warning("Postgres hash backfill failed: %s", exc)


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

    for chunk_start in range(0, len(team_ids), max_workers):
        chunk = team_ids[chunk_start:chunk_start + max_workers]
        futures = [_submit_shared_network_task(_fetch, team_id) for team_id in chunk]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                LOGGER.debug("Postgres team_next_game read failed: %s", exc)


def _warm_team_rank_map() -> None:
    season = current_nba_season()
    try:
        build_team_rank_map(season=season, season_type=DEFAULT_SEASON_TYPE)
        LOGGER.info("Warm-cache team ranks ready for %s", season)
    except Exception as exc:
        LOGGER.warning("Warm-cache team ranks failed for %s: %s", season, exc)


def is_nba_game_day() -> bool:
    """Return True if there are NBA games scheduled today."""
    try:
        games = fetch_scoreboard_games(_today_scoreboard_date())
        return len(games) > 0
    except Exception:
        return False


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
        tasks.append(("injuries", lambda: get_cached_injury_report_payload()))

    # Tighten injury report refresh interval on game days (3 min vs 10 min).
    # Done after tasks are defined so it runs in the background thread,
    # not at module import time.
    def _adjust_injury_ttl() -> None:
        try:
            if is_nba_game_day():
                ttl_seconds = 180
                if is_playoff_season():
                    today_date = _today_scoreboard_date()
                    now_et = nba_now()
                    today_rows = fetch_scoreboard_games(today_date)
                    for row in today_rows:
                        tipoff_et = parse_scoreboard_tipoff_et(str(row.get("GAME_STATUS_TEXT") or ""), today_date)
                        if not tipoff_et:
                            continue
                        seconds_until_tip = (tipoff_et - now_et).total_seconds()
                        if 0 <= seconds_until_tip <= 3600:
                            ttl_seconds = 60
                            break
                INJURY_SERVICE.report_ttl_seconds = ttl_seconds
                LOGGER.info("Game day detected - injury TTL set to %ss", ttl_seconds)
            else:
                LOGGER.info("Off day - injury TTL stays at %ss", INJURY_SERVICE.report_ttl_seconds)
        except Exception as exc:
            LOGGER.warning("Could not detect game day for injury TTL: %s", exc)

    tasks.append(("injury_ttl", _adjust_injury_ttl))

    if WARM_CACHE_PRELOAD_TEAM_ROSTERS:
        tasks.append(("team_rosters", preload_team_rosters_for_current_season))
    if WARM_CACHE_PRELOAD_TEAM_RANKS:
        tasks.append(("team_ranks", _warm_team_rank_map))

    if not tasks:
        return

    max_workers = min(WARM_CACHE_STARTUP_MAX_WORKERS, max(1, len(tasks)))
    LOGGER.info("Starting warm-cache preload with %s task(s) and %s worker(s)", len(tasks), max_workers)

    if max_workers <= 1:
        for task_name, task_func in tasks:
            _warm_cache_task(task_name, task_func)
        return

    for chunk_start in range(0, len(tasks), max_workers):
        chunk = tasks[chunk_start:chunk_start + max_workers]
        future_map = {
            _submit_shared_network_task(_warm_cache_task, task_name, task_func): task_name
            for task_name, task_func in chunk
        }
        for future in as_completed(future_map):
            try:
                future.result()
            except Exception as exc:
                LOGGER.warning("Warm-cache future %s failed: %s", future_map[future], exc)


def start_warm_cache_background() -> None:
    global WARM_CACHE_THREAD_STARTED
    if not WARM_CACHE_ON_STARTUP_ENABLED:
        return
    with WARM_CACHE_THREAD_LOCK:
        if WARM_CACHE_THREAD_STARTED:
            return
        WARM_CACHE_THREAD_STARTED = True
    thread = threading.Thread(target=warm_cache_on_startup, name="warm-cache-startup", daemon=True)
    thread.start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_postgres_pool()
    init_postgres_cache()
    preload_postgres_cache()
    backfill_postgres_hashes()
    cleanup_postgres_retention()
    _load_key_vault_state()
    _load_backtest_log()
    _load_favorites_state()
    _load_tracker_state()
    start_hybrid_refresh_workers()
    start_warm_cache_background()
    try:
        yield
    finally:
        try:
            _shutdown_shared_executors()
        except Exception:
            pass
        close_postgres_pool()


app = FastAPI(title="NBA Props Bar Chart App", lifespan=lifespan)
allowed_origins = ["*"]
if ALLOWED_ORIGINS_RAW and ALLOWED_ORIGINS_RAW != "*":
    allowed_origins = [origin.strip() for origin in ALLOWED_ORIGINS_RAW.split(",") if origin.strip()]
allow_credentials = allowed_origins != ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins or ["*"],
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

try:
    APP_ZONEINFO = ZoneInfo(APP_TIMEZONE)
except Exception:
    LOGGER.warning("Invalid APP_TIMEZONE=%r; defaulting to Asia/Manila", APP_TIMEZONE)
    APP_TIMEZONE = "Asia/Manila"
    APP_ZONEINFO = ZoneInfo(APP_TIMEZONE)
try:
    NBA_GAME_DATE_ZONEINFO = ZoneInfo(NBA_GAME_DATE_TIMEZONE)
except Exception:
    LOGGER.warning("Invalid NBA_GAME_DATE_TIMEZONE=%r; defaulting to America/New_York", NBA_GAME_DATE_TIMEZONE)
    NBA_GAME_DATE_TIMEZONE = "America/New_York"
    NBA_GAME_DATE_ZONEINFO = ZoneInfo(NBA_GAME_DATE_TIMEZONE)


def app_now() -> datetime:
    return datetime.now(APP_ZONEINFO)


def nba_now() -> datetime:
    return datetime.now(NBA_GAME_DATE_ZONEINFO)

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
SCORING_STATS = {"PTS", "3PM", "PRA", "PR", "PA"}
PLAYMAKING_STATS = {"AST", "PA", "PRA"}
REBOUND_STATS = {"REB", "RA", "PR", "PRA"}


def current_nba_game_date() -> str:
    return nba_now().strftime("%Y-%m-%d")


def is_playoff_season(now_value: datetime | None = None) -> bool:
    current = (now_value or nba_now()).date()
    start = dt.date(current.year, NBA_PLAYOFF_MODE_START_MONTH, NBA_PLAYOFF_MODE_START_DAY)
    end = dt.date(current.year, NBA_PLAYOFF_MODE_END_MONTH, NBA_PLAYOFF_MODE_END_DAY)
    return start <= current <= end


def parse_scoreboard_tipoff_et(status_text: str, game_date: str | None = None) -> datetime | None:
    raw = str(status_text or "").strip()
    match = re.match(r"^\s*(\d{1,2}):(\d{2})\s*([AaPp][Mm])\s*ET\s*$", raw)
    if not match:
        return None
    try:
        hour = int(match.group(1))
        minute = int(match.group(2))
        ampm = match.group(3).upper()
        if ampm == "PM" and hour != 12:
            hour += 12
        elif ampm == "AM" and hour == 12:
            hour = 0
        if game_date:
            base_date = datetime.strptime(str(game_date), "%Y-%m-%d").date()
        else:
            base_date = nba_now().date()
        return datetime(
            base_date.year,
            base_date.month,
            base_date.day,
            hour,
            minute,
            tzinfo=ZoneInfo("America/New_York"),
        )
    except Exception:
        return None

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
POSITION_SUMMARY_CACHE: dict[tuple[str, str, str, str, int], dict[str, Any]] = {}
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
ANALYSIS_CACHE_TTL_SECONDS = min(max(1, int(os.getenv("NBA_ANALYSIS_CACHE_TTL_SECONDS", "300"))), 300)
ANALYSIS_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
TEAM_OPPORTUNITY_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
INJURY_REPORT_TTL_SECONDS = max(300, int(os.getenv("NBA_INJURY_REPORT_TTL_SECONDS", "1800")))
INJURY_AWARE_BOOST_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
INJURY_AWARE_BOOST_CACHE_TTL_SECONDS = max(
    300,
    int(os.getenv("NBA_INJURY_AWARE_BOOST_CACHE_TTL_SECONDS", str(min(INJURY_REPORT_TTL_SECONDS, 1800))))
)
INJURY_AWARE_BOOST_CACHE_MAX_ENTRIES = max(250, int(os.getenv("NBA_INJURY_AWARE_BOOST_CACHE_MAX_ENTRIES", "4000")))
INJURY_AWARE_MAX_COMBO_TRIES = max(1, min(3, int(os.getenv("NBA_INJURY_AWARE_MAX_COMBO_TRIES", "3"))))
INJURY_AWARE_EARLY_STOP_GAIN_PCT = max(0.0, float(os.getenv("NBA_INJURY_AWARE_EARLY_STOP_GAIN_PCT", "1.6")))
INJURY_AWARE_EARLY_STOP_MIN_GAMES = max(3, int(os.getenv("NBA_INJURY_AWARE_EARLY_STOP_MIN_GAMES", "8")))
INJURY_AWARE_LOW_GAIN_CUTOFF_PCT = max(0.0, float(os.getenv("NBA_INJURY_AWARE_LOW_GAIN_CUTOFF_PCT", "0.25")))
INJURY_AWARE_BASE_HITRATE_EXPAND_THRESHOLD = float(os.getenv("NBA_INJURY_AWARE_BASE_HITRATE_EXPAND_THRESHOLD", "54.0"))
INJURY_AWARE_HIGH_PRIORITY_SHARE = min(1.0, max(0.05, float(os.getenv("NBA_INJURY_AWARE_HIGH_PRIORITY_SHARE", "0.30"))))
INJURY_AWARE_BOOST_CACHE_LOCK = Lock()
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
INJURY_REPORT_LINKS_TTL_SECONDS = max(30, int(os.getenv("NBA_INJURY_REPORT_LINKS_TTL_SECONDS", "120")))
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
    forced_side: str | None,
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
        str(forced_side or "").strip().upper(),
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
NBA_HTTP_TRUST_ENV = os.getenv("NBA_HTTP_TRUST_ENV", "0").strip().lower() in {"1", "true", "yes", "on"}


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
            LOGGER.warning("Hybrid refresh queue full; dropping %s refresh for key=%s", job_type, key)
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
        if len(key) >= 4:
            player_id, season, season_type, _schema_version = key[:4]
        else:
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
    if job_type == "injury_report":
        fetch_latest_injury_report_payload()
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
    cache_key = (player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
    cached = GAME_LOG_CACHE.get(cache_key)
    age_seconds = _cache_age_seconds(cached)
    normalized_season_type = normalize_requested_season_type(season_type)
    stale_window_seconds = HYBRID_GAME_LOG_MAX_STALE_SECONDS
    if normalized_season_type == SEASON_TYPE_PLAYOFFS:
        # Playoff series update rapidly; prefer live refresh after soft TTL.
        stale_window_seconds = HYBRID_GAME_LOG_SOFT_TTL_SECONDS
    if cached and isinstance(cached.get("rows"), list):
        cached_rows = cached.get("rows") or []
        low_playoff_sample = normalized_season_type == SEASON_TYPE_PLAYOFFS and len(cached_rows) < 3
        if low_playoff_sample:
            # Playoff series samples can lag in cache right after new games complete.
            # Force a live refresh path so H2H/series counts catch up promptly.
            try:
                GAME_LOG_CACHE.pop(cache_key, None)
                GAME_LOG_FAILURE_META.pop(cache_key, None)
                RECENT_GAME_LOG_CACHE.pop((player_id, season, season_type, 20, GAME_LOG_CACHE_SCHEMA_VERSION), None)
                rows = PLAYER_DATA_SERVICE.fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
                fresh_age = _cache_age_seconds(GAME_LOG_CACHE.get(cache_key))
                return rows, {"source": "live-forced", "seconds_ago": round(fresh_age, 2) if fresh_age is not None else None, "refresh_queued": False}
            except Exception:
                # Do not silently serve low-sample playoff cache rows.
                # Fall through to the normal live fetch path below.
                pass
            # Skip stale-cache return paths for low playoff samples.
            rows = fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
            fresh_age = _cache_age_seconds(GAME_LOG_CACHE.get(cache_key))
            return rows, {"source": "live-retry", "seconds_ago": round(fresh_age, 2) if fresh_age is not None else None, "refresh_queued": False}
        if age_seconds is not None and age_seconds < HYBRID_GAME_LOG_SOFT_TTL_SECONDS:
            return cached["rows"], {"source": "cache-fresh", "seconds_ago": round(age_seconds, 2), "refresh_queued": False}
        if age_seconds is not None and age_seconds < stale_window_seconds:
            queued = enqueue_hybrid_refresh("game_log", (player_id, season, season_type))
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
    def _extract_opponent_from_label(label: str) -> str:
        cleaned = str(label or "").strip().upper()
        if not cleaned:
            return ""
        if cleaned.startswith("VS "):
            return cleaned.replace("VS ", "", 1).strip()
        if cleaned.startswith("@ "):
            return cleaned.replace("@ ", "", 1).strip()
        if " VS " in cleaned or " @ " in cleaned:
            parsed = parse_matchup_descriptor(cleaned.replace(" VS ", " vs. ").replace(" @ ", " @ "))
            return str(parsed.get("opponent_abbreviation") or "").upper().strip()
        return ""

    payload = {
        "opponent_name": next_game.get("opponent_name") if next_game else "",
        "opponent_abbreviation": next_game.get("opponent_abbreviation") if next_game else "",
        "games_count": 0,
        "hit_count": 0,
        "hit_rate": 0.0,
        "average": 0.0,
        "games": [],
    }
    if not next_game:
        return payload
    opponent_abbreviation = str(next_game.get("opponent_abbreviation") or "").upper().strip()
    player_team_abbreviation = str(next_game.get("player_team_abbreviation") or "").upper().strip()
    if not player_team_abbreviation and rows_source:
        parsed = parse_matchup_descriptor(str(rows_source[0].get("MATCHUP") or ""))
        player_team_abbreviation = str(parsed.get("team_abbreviation") or "").upper().strip()
    if (not opponent_abbreviation or (player_team_abbreviation and opponent_abbreviation == player_team_abbreviation)) and next_game.get("opponent_team_id"):
        opponent_team = TEAM_LOOKUP.get(int(next_game.get("opponent_team_id") or 0), {})
        opponent_abbreviation = str(opponent_team.get("abbreviation") or "").upper().strip()
    if not opponent_abbreviation or (player_team_abbreviation and opponent_abbreviation == player_team_abbreviation):
        opponent_abbreviation = _extract_opponent_from_label(str(next_game.get("matchup_label") or ""))
    if not opponent_abbreviation:
        return payload
    h2h_rows = [
        row
        for row in (rows_source or [])
        if matchup_includes_opponent(row.get("MATCHUP"), opponent_abbreviation)
    ]
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
    trust_env=NBA_HTTP_TRUST_ENV,
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
    report_ttl_seconds=INJURY_REPORT_TTL_SECONDS,
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
    now = app_now()
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


def safe_mean(values: list[Any]) -> float | None:
    cleaned: list[float] = []
    for value in values:
        if value is None:
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isnan(numeric) or math.isinf(numeric):
            continue
        cleaned.append(numeric)
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


def fair_implied_probabilities(over_odds: float | None, under_odds: float | None) -> dict[str, float | None]:
    over_raw = decimal_implied_probability(over_odds)
    under_raw = decimal_implied_probability(under_odds)
    devig = devig_two_way_market(over_odds, under_odds)
    if devig:
        return {
            "over": devig.get("over_fair_prob"),
            "under": devig.get("under_fair_prob"),
            "over_raw": over_raw,
            "under_raw": under_raw,
            "hold": devig.get("hold"),
        }
    return {
        "over": over_raw,
        "under": under_raw,
        "over_raw": over_raw,
        "under_raw": under_raw,
        "hold": None,
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


def side_hit_rate_from_over_hit_rate(over_hit_rate: Any, side: str) -> float:
    try:
        over_rate_value = float(over_hit_rate)
    except (TypeError, ValueError):
        over_rate_value = 0.0
    over_rate = clamp(over_rate_value, 0.0, 100.0)
    if str(side or "").upper() == "UNDER":
        return round(100.0 - over_rate, 1)
    return round(over_rate, 1)


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


def canonicalize_team_name(team_name: str | None, team_id: int | None = None) -> str:
    if team_id:
        resolved = TEAM_LOOKUP.get(int(team_id))
        if resolved:
            return str(resolved.get("full_name") or "").strip()
    raw = str(team_name or "").strip()
    if not raw:
        return ""
    resolved = resolve_team_from_text(raw)
    if resolved:
        return str(resolved.get("full_name") or raw).strip()
    return raw


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


EDGE_DEFINITION_MODEL_FAIR = "model_minus_fair_market_prob_pct"


def resolve_fair_probability(fair_prob: float | None, fallback_prob: float | None = None) -> float:
    if fair_prob is not None:
        return clamp(float(fair_prob), 0.0, 1.0)
    if fallback_prob is not None:
        return clamp(float(fallback_prob), 0.0, 1.0)
    return 0.5


def edge_pct_from_model_and_fair(model_prob: float, fair_prob: float) -> float:
    return round((float(model_prob) - float(fair_prob)) * 100.0, 1)


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
    # Recalibrate so deviations from 50% carry more weight (but avoid extremes).
    base = 0.5 + (base - 0.5) * 1.20
    base = clamp(base, 0.02, 0.98)
    edge_term = 0.0
    scale = max(1.0, max(line, 1.0) * 0.14)
    edge_term += clamp((average - line) / scale, -0.22, 0.22)

    variance_adjustment = 0.0
    if variance:
        median = float(variance.get("median") or average)
        consistency_score = float(variance.get("consistency_score") or 50.0)
        p25 = float(variance.get("p25") or 0.0)
        p75 = float(variance.get("p75") or 0.0)
        median_edge_term = clamp((median - line) / scale, -0.18, 0.18)
        noisy_stats_variance = {"STL", "BLK", "3PM"}
        median_weight = 0.65 if stat in noisy_stats_variance else 0.45
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
        noisy_stats_matchup = {"STL", "BLK"}
        matchup_weight = 0.08 if stat in noisy_stats_matchup else 0.2
        matchup_term = clamp(matchup_delta_pct / 100.0 * matchup_weight, -0.12, 0.12)

    role_term = 0.0
    if opportunity.get("minutes_trend") == "up":
        role_term += 0.03
    elif opportunity.get("minutes_trend") == "down":
        role_term -= 0.04

    if opportunity.get("volume_trend") == "up":
        role_term += 0.04 if stat in SCORING_STATS else 0.02
    elif opportunity.get("volume_trend") == "down":
        role_term -= 0.04 if stat in SCORING_STATS else 0.02

    if int(team_context.get("impact_count") or 0) > 0:
        role_term += min(0.03, int(team_context.get("impact_count") or 0) * 0.01)

    environment_term = 0.0
    if environment.get("is_back_to_back"):
        environment_term -= 0.03
    elif isinstance(environment.get("rest_days"), int) and environment.get("rest_days") >= 2:
        environment_term += 0.02
    market_signal = derive_market_environment_signal(stat, environment)
    environment_term += float(market_signal.get("over_adjustment") or 0.0)
    # Pace signal: faster pace nudges volume stats upward, slower pace nudges downward.
    pace_proxy = safe_float_or_none(environment.get("combined_pace_proxy"))
    if pace_proxy is not None:
        pace_delta = pace_proxy - 99.5
        if stat in SCORING_STATS:
            environment_term += clamp(pace_delta * 0.004, -0.04, 0.04)
        elif stat in PLAYMAKING_STATS:
            environment_term += clamp(pace_delta * 0.003, -0.03, 0.03)
        elif stat in REBOUND_STATS:
            environment_term += clamp(pace_delta * 0.0025, -0.025, 0.025)
    # Home/away split: mild home boost, mild away drag (most impact on volume stats).
    is_home = environment.get("is_home")
    if is_home is not None:
        home_sign = 1.0 if bool(is_home) else -1.0
        if stat in SCORING_STATS:
            environment_term += 0.015 * home_sign
        elif stat in PLAYMAKING_STATS:
            environment_term += 0.010 * home_sign
        elif stat in REBOUND_STATS:
            environment_term += 0.008 * home_sign
    # Opponent rank weighting: strong opponents slightly suppress, weak opponents slightly lift.
    opponent_rank = safe_int_score(environment.get("opponent_rank"))
    if opponent_rank > 0:
        clamped_rank = min(30, max(1, opponent_rank))
        rank_strength = (31 - clamped_rank) / 30.0  # 1.0 = strongest, 0.0 = weakest
        rank_delta = 0.5 - rank_strength  # negative for strong, positive for weak
        if stat in SCORING_STATS:
            environment_term += clamp(rank_delta * 0.06, -0.03, 0.03)
        elif stat in PLAYMAKING_STATS:
            environment_term += clamp(rank_delta * 0.045, -0.022, 0.022)
        elif stat in REBOUND_STATS:
            environment_term += clamp(rank_delta * 0.04, -0.02, 0.02)

    model_over = clamp(base + edge_term + matchup_term + role_term + environment_term + variance_adjustment, 0.02, 0.98)
    return round(model_over, 4), round(1 - model_over, 4)


def fetch_position_dash(season: str, season_type: str, position_code: str, opponent_team_id: int = 0) -> list[dict[str, Any]]:
    normalized_position = str(position_code or "").upper().strip()
    normalized_season = str(season or "").strip()
    normalized_season_type = normalize_requested_season_type(season_type)
    cache_key = (normalized_season, normalized_season_type, normalized_position, int(opponent_team_id or 0))
    cached = POSITION_DASH_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < POSITION_TTL_SECONDS:
        return [dict(row) for row in (cached.get("rows") or [])]

    if not normalized_position:
        return []

    season_parts = season_types_for_analysis(normalized_season_type)
    if len(season_parts) > 1:
        merged_rows: list[dict[str, Any]] = []
        for part in season_parts:
            merged_rows.extend(
                fetch_position_dash(
                    normalized_season,
                    part,
                    normalized_position,
                    opponent_team_id=int(opponent_team_id or 0),
                )
            )
        POSITION_DASH_CACHE[cache_key] = {"timestamp": now_ts, "rows": [dict(row) for row in merged_rows]}
        return [dict(row) for row in merged_rows]

    try:
        throttle_request()
        response = call_nba_with_retries(
            lambda: LeagueDashPlayerStats(
                season=normalized_season,
                season_type_all_star=season_parts[0],
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
        "gp_weighted_per_game": round(total_weighted_value / total_gp, 2),
        # Backward-compatible alias used by existing consumers.
        "per_player_game": round(total_weighted_value / total_gp, 2),
        "total_value": round(total_weighted_value, 1),
    }


def _get_cached_position_summary(cache_key: tuple[str, str, str, str, int], now_ts: float) -> dict[str, Any] | None:
    cached = POSITION_SUMMARY_CACHE.get(cache_key)
    if not cached:
        return None
    cached_ts = float(cached.get("timestamp") or 0.0)
    if now_ts - cached_ts > POSITION_DASH_MAX_STALE_SECONDS:
        return None
    summary = cached.get("summary")
    return dict(summary) if isinstance(summary, dict) else None


def _set_position_summary_cache(cache_key: tuple[str, str, str, str, int], summary: dict[str, Any], now_ts: float) -> None:
    POSITION_SUMMARY_CACHE[cache_key] = {"timestamp": now_ts, "summary": dict(summary)}


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
    normalized_season_type = normalize_requested_season_type(season_type)
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
    league_cache_key = (normalized_season, normalized_season_type, normalized_position, normalized_stat, 0)
    if league_summary:
        _set_position_summary_cache(league_cache_key, league_summary, now_ts)
    else:
        league_summary = _get_cached_position_summary(league_cache_key, now_ts)

    opponent_summary = summarize_position_environment(opponent_rows, normalized_stat)
    opponent_cache_key = (normalized_season, normalized_season_type, normalized_position, normalized_stat, int(opponent_team_id))
    if opponent_summary:
        _set_position_summary_cache(opponent_cache_key, opponent_summary, now_ts)
    else:
        opponent_summary = _get_cached_position_summary(opponent_cache_key, now_ts)

    if not league_summary:
        POSITION_MATCHUP_CACHE[cache_key] = {"timestamp": now_ts, "payload": None}
        return None
    # Fallback to league-average when opponent matchup data is unavailable.
    if not opponent_summary:
        league_average = float(league_summary.get("per_player_game") or 0.0)
        if league_average <= 0:
            POSITION_MATCHUP_CACHE[cache_key] = {"timestamp": now_ts, "payload": None}
            return None
        fallback_payload = {
            "opponent_team_id": int(opponent_team_id),
            "position_code": normalized_position,
            "position_label": POSITION_LABELS.get(normalized_position, normalized_position),
            "stat": normalized_stat,
            "opponent_value": round(league_average, 2),
            "league_average": round(league_average, 2),
            "delta_pct": 0.0,
            "sample_gp": round(float(league_summary.get("sample_gp") or 0.0), 1),
            "players_count": int(league_summary.get("players_count") or 0),
            "lean": "Neutral",
            "lean_tone": "neutral",
            "fallback": True,
        }
        POSITION_MATCHUP_CACHE[cache_key] = {"timestamp": now_ts, "payload": dict(fallback_payload)}
        return dict(fallback_payload)

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
    over_odds: float | None = None,
    under_odds: float | None = None,
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
    forced_side: str | None = None,
    populate_player_info_cache: bool = False,
) -> dict[str, Any]:
    season_type = normalize_requested_season_type(season_type)
    player = PLAYER_LOOKUP.get(int(player_id))
    if not player:
        raise HTTPException(status_code=404, detail="Player not found.")
    if populate_player_info_cache:
        try:
            cached_row = fetch_common_player_info(player_id)
            if isinstance(cached_row, dict) and postgres_available():
                _submit_pg_write(_pg_write_player_info, player_id, cached_row)
        except Exception as exc:
            LOGGER.debug("Postgres game_log read failed: %s", exc)

    normalized_without_player_ids = normalize_without_player_ids(without_player_ids)
    if not normalized_without_player_ids and without_player_id:
        normalized_without_player_ids = [int(without_player_id)]
    normalized_without_player_names = resolve_without_player_names(normalized_without_player_ids)
    if not normalized_without_player_names and without_player_name:
        fallback_name = str(without_player_name).strip()
        if fallback_name:
            normalized_without_player_names = [fallback_name]

    forced_side_normalized = str(forced_side or "").strip().upper()
    if forced_side_normalized not in {"OVER", "UNDER"}:
        forced_side_normalized = ""

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
        forced_side=forced_side_normalized or None,
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
    # Hard guard: playoff samples should not silently stay at 1-game because of stale layers.
    # If we detect a low playoff sample, force a direct NBA fetch and prefer the larger result.
    if season_type == SEASON_TYPE_PLAYOFFS and len(season_rows or []) < 3:
        try:
            live_rows = PLAYER_DATA_SERVICE.fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
            merged_live_rows = merge_game_log_rows(live_rows or [])
            if len(merged_live_rows) > len(season_rows or []):
                season_rows = merged_live_rows
                cache_key = (player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
                GAME_LOG_CACHE[cache_key] = {"timestamp": time.time(), "rows": season_rows, "source": "live-guard"}
                _submit_pg_write(_pg_write_game_log, player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION, season_rows)
                game_log_meta = {"source": "live-guard", "seconds_ago": 0.0, "refresh_queued": False}
        except Exception:
            pass

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
    if override_opponent_id and override_opponent_id != 0 and (resolved_team_id is None or int(override_opponent_id) != int(resolved_team_id)):
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
    next_game = enrich_next_game_with_series_context(next_game, resolved_team_id)

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
    availability = build_availability_payload(
        player_name=player["full_name"],
        team_name=team_name,
        game_date=str((next_game or {}).get("game_date") or "") or None,
    )
    vs_position = build_position_matchup(opponent_team_id=int((next_game or {}).get("opponent_team_id") or 0), position_code=position_code or "", stat=stat, season=season, season_type=season_type) if next_game and position_code else None
    # Keep H2H fully aligned with the active filtered pool so tabs and counts
    # reflect the same filter context.
    h2h_payload = build_h2h_payload_from_rows(filtered_pool or [], next_game, stat, line)
    # Sample-driven trend/schedule context should reflect the same last_n window
    # used for hit rate / averages.
    sample_window_rows = rows if rows else (filtered_pool or enriched_rows or season_rows)
    # For freshness overlays, keep H2H source tied to the filtered pool.
    analysis_source_rows = filtered_pool
    opportunity = build_opportunity_context(sample_window_rows, last_n)
    team_context = build_team_opportunity_context(team_name=team_name, player_name=player["full_name"], stat=stat, player_position=resolved_position, team_id=resolved_team_id, season=season)
    if debug:
        team_context = {
            **team_context,
            "debug": {
                "resolved_team_name": team_name or "",
                "matched_injury_rows": list(team_context.get("players") or []),
                "impact_count": int(team_context.get("impact_count") or 0),
            },
        }
    environment = build_game_environment_context(sample_window_rows, next_game, team_id=resolved_team_id, season=season, season_type=season_type)
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
    market_probs = fair_implied_probabilities(over_odds, under_odds)
    implied_over = market_probs.get("over")
    implied_under = market_probs.get("under")
    model_over, model_under = apply_market_probability_penalty(
        model_over,
        model_under,
        implied_over=implied_over,
        implied_under=implied_under,
    )
    model_recommended_side = "OVER" if model_over >= model_under else "UNDER"
    recommended_side = forced_side_normalized or model_recommended_side
    side_hit_rate = side_hit_rate_from_over_hit_rate(hit_rate, recommended_side)
    side_model_prob = model_over if recommended_side == "OVER" else model_under
    chosen_fair_prob = implied_over if recommended_side == "OVER" else implied_under
    chosen_fair_prob = resolve_fair_probability(chosen_fair_prob, fallback_prob=0.5)
    chosen_edge_pct = edge_pct_from_model_and_fair(side_model_prob, chosen_fair_prob)
    # /api/player-prop supplies decimal odds; use them directly for EV.
    _chosen_odds_raw = over_odds if recommended_side == "OVER" else under_odds
    chosen_ev = compute_side_pricing_metrics(
        probability=side_model_prob,
        fair_probability=chosen_fair_prob,
        odds=safe_float_or_none(_chosen_odds_raw),
    )["ev"]
    confidence_engine = build_confidence_engine(
        side=recommended_side,
        hit_rate=float(side_hit_rate),
        games_count=int(len(values)),
        edge=chosen_edge_pct,
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
        season_type=season_type,
        playoff_game_number=int((next_game or {}).get("playoff_game_number") or 0) or None,
    )
    h2h_games_count, h2h_side_hit_count, h2h_side_hit_rate = resolve_side_h2h_metrics(
        games=games,
        h2h_payload=h2h_payload,
        side=recommended_side,
        opponent_abbreviation=(next_game or {}).get("opponent_abbreviation"),
    )
    ranking_context = build_parlay_ranking_context(
        season_type=season_type,
        side=recommended_side,
        stat=stat,
        side_hit_rate=float(side_hit_rate),
        games_count=int(len(values)),
        h2h_games_count=int(h2h_games_count or 0),
        h2h_side_hit_rate=float(h2h_side_hit_rate) if h2h_side_hit_rate is not None else None,
        model_probability=side_model_prob,
        fair_probability=chosen_fair_prob,
        edge_pct=chosen_edge_pct,
        ev_decimal=chosen_ev,
        confidence_score=confidence_engine.get("score"),
        engine_ranking_score=confidence_engine.get("ranking_score"),
        line=line,
        average=average,
    )
    confidence_engine = {
        **confidence_engine,
        "ranking_source": ranking_context.get("ranking_source"),
        "ranking_sort_score": ranking_context.get("ranking_sort_score"),
        "ranking_blend_score": ranking_context.get("ranking_blend_score"),
        "ranking_profile": ranking_context.get("ranking_profile") or {},
        "playoff_strategy_tips": ranking_context.get("playoff_strategy_tips") or [],
        "h2h_weight_pct": ranking_context.get("h2h_weight_pct"),
        "model_probability": round(side_model_prob * 100.0, 1),
        "implied_probability": round(chosen_fair_prob * 100.0, 1),
    }

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
        "recommended_side_source": "forced" if forced_side_normalized else "model",
        "side_hit_rate": round(side_hit_rate, 1),
        "h2h_side_hit_count": h2h_side_hit_count,
        "h2h_side_hit_rate": round(h2h_side_hit_rate, 1) if h2h_side_hit_rate is not None else None,
        "ranking_source": ranking_context.get("ranking_source"),
        "ranking_hit_rate": ranking_context.get("ranking_hit_rate"),
        "ranking_sort_score": ranking_context.get("ranking_sort_score"),
        "ranking_blend_score": ranking_context.get("ranking_blend_score"),
        "ranking_profile": ranking_context.get("ranking_profile") or {},
        "playoff_strategy_tips": ranking_context.get("playoff_strategy_tips") or [],
        "h2h_weight_pct": ranking_context.get("h2h_weight_pct"),
        "edge": chosen_edge_pct,
        "edge_pct": chosen_edge_pct,
        "ev": chosen_ev,
        "edge_definition": EDGE_DEFINITION_MODEL_FAIR,
        "fair_probability": round(chosen_fair_prob * 100.0, 1),
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
        "sample_warning": (
            f"Only {len(values)} playoff games  confidence grade is unreliable until Game 5+."
            if season_type == SEASON_TYPE_PLAYOFFS and len(values) < 8
            else (f"Only {len(values)} games in filtered sample  treat recommendation with caution." if len(values) < 5 else None)
        ),
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
        "injury_report_source": (INJURY_SERVICE.report_cache.get("payload") or {}).get("source"),
        "game_log_cache_source": (GAME_LOG_CACHE.get((player_id, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)) or {}).get("source"),
        "player_info_cache_source": (PLAYER_INFO_CACHE.get(player_id) or {}).get("source"),
        "next_game_cache_source": (TEAM_NEXT_GAME_CACHE.get((resolved_team_id, season, season_type)) or {}).get("source"),
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


def _persist_injury_report_payload(payload: dict[str, Any], sync_write: bool = False) -> None:
    if not payload.get("ok"):
        return
    if sync_write:
        try:
            _pg_write_injury_report(payload)
        except Exception as exc:
            LOGGER.debug("Postgres injury report sync write failed: %s", exc)
        return
    _submit_pg_write(_pg_write_injury_report, payload)


def _injury_report_has_newer_et_link(payload: dict[str, Any] | None, allow_network: bool = True) -> bool:
    current_url = str((payload or {}).get("report_url") or "").strip()
    links: list[str] = []
    if allow_network:
        try:
            links = INJURY_SERVICE.list_recent_report_links(limit=1)
        except Exception:
            return False
    else:
        cached_links = INJURY_SERVICE.links_cache.get("links") or []
        cached_ts = float(INJURY_SERVICE.links_cache.get("timestamp") or 0.0)
        now_ts = time.time()
        if cached_links and now_ts - cached_ts < float(INJURY_SERVICE.links_ttl_seconds):
            links = list(cached_links)[:1]
        else:
            return False
    if not links:
        return False
    latest_url = str(links[0] or "").strip()
    if not latest_url:
        return False
    if latest_url == current_url:
        return False
    latest_dt = parse_injury_report_timestamp(latest_url)
    current_dt = parse_injury_report_timestamp(current_url)
    return latest_dt > current_dt


def fetch_latest_injury_report_payload(sync_write: bool = False) -> dict[str, Any]:
    payload = INJURY_SERVICE.fetch_latest_report_payload()
    _persist_injury_report_payload(payload, sync_write=sync_write)
    return payload


def get_cached_injury_report_payload(force: bool = False) -> dict[str, Any]:
    cached_payload = INJURY_SERVICE.report_cache.get("payload")
    cached_ts = float(INJURY_SERVICE.report_cache.get("timestamp") or 0.0)
    if not force:
        if POSTGRES_SOURCE_OF_TRUTH:
            pg_payload, pg_ts = _pg_read_latest_injury_report()
            if isinstance(pg_payload, dict) and pg_payload.get("ok"):
                pg_age = time.time() - float(pg_ts or 0.0) if pg_ts else None
                if pg_age is not None and pg_age <= INJURY_REPORT_TTL_SECONDS:
                    cached_copy = dict(pg_payload)
                    cached_copy["source"] = cached_copy.get("source") or "postgres"
                    INJURY_SERVICE.report_cache["timestamp"] = float(pg_ts or time.time())
                    INJURY_SERVICE.report_cache["payload"] = cached_copy
                    if _injury_report_has_newer_et_link(cached_copy):
                        refreshed = fetch_latest_injury_report_payload(sync_write=True)
                        if refreshed.get("ok"):
                            return refreshed
                    return cached_copy
            if isinstance(cached_payload, dict) and cached_payload.get("ok"):
                age_seconds = INJURY_SERVICE.report_cache_age_seconds()
                if age_seconds is not None and age_seconds <= INJURY_REPORT_TTL_SECONDS:
                    if _injury_report_has_newer_et_link(cached_payload):
                        refreshed = fetch_latest_injury_report_payload(sync_write=True)
                        if refreshed.get("ok"):
                            return refreshed
                    return cached_payload
        else:
            if isinstance(cached_payload, dict) and cached_payload.get("ok"):
                age_seconds = INJURY_SERVICE.report_cache_age_seconds()
                if age_seconds is not None and age_seconds <= INJURY_REPORT_TTL_SECONDS:
                    if _injury_report_has_newer_et_link(cached_payload):
                        refreshed = fetch_latest_injury_report_payload(sync_write=True)
                        if refreshed.get("ok"):
                            return refreshed
                    return cached_payload
            pg_payload, pg_ts = _pg_read_latest_injury_report()
            if isinstance(pg_payload, dict) and pg_payload.get("ok"):
                cached_copy = dict(pg_payload)
                cached_copy["source"] = cached_copy.get("source") or "postgres"
                INJURY_SERVICE.report_cache["timestamp"] = float(pg_ts or time.time())
                INJURY_SERVICE.report_cache["payload"] = cached_copy
                if _injury_report_has_newer_et_link(cached_copy):
                    refreshed = fetch_latest_injury_report_payload(sync_write=True)
                    if refreshed.get("ok"):
                        return refreshed
                return cached_copy
    return fetch_latest_injury_report_payload(sync_write=True)


def get_cached_injury_report_payload_fast() -> dict[str, Any]:
    """
    Mostly non-blocking accessor for latency-sensitive endpoints.
    If the official page advertises a newer ET report link, it upgrades to a live fetch.
    """
    cached_payload = INJURY_SERVICE.report_cache.get("payload")
    if isinstance(cached_payload, dict) and cached_payload:
        cached_age = INJURY_SERVICE.report_cache_age_seconds()
        if _injury_report_has_newer_et_link(cached_payload, allow_network=False):
            refreshed = fetch_latest_injury_report_payload(sync_write=True)
            if refreshed.get("ok"):
                return refreshed
        if cached_age is None or cached_age > INJURY_REPORT_TTL_SECONDS:
            enqueue_hybrid_refresh("injury_report", ("latest",))
        return cached_payload
    pg_payload, pg_ts = _pg_read_latest_injury_report()
    if isinstance(pg_payload, dict) and pg_payload:
        payload_copy = dict(pg_payload)
        payload_copy["source"] = payload_copy.get("source") or "postgres"
        INJURY_SERVICE.report_cache["timestamp"] = float(pg_ts or time.time())
        INJURY_SERVICE.report_cache["payload"] = payload_copy
        if _injury_report_has_newer_et_link(payload_copy, allow_network=False):
            refreshed = fetch_latest_injury_report_payload(sync_write=True)
            if refreshed.get("ok"):
                return refreshed
        pg_age = max(0.0, time.time() - float(pg_ts or 0.0)) if pg_ts else None
        if pg_age is None or pg_age > INJURY_REPORT_TTL_SECONDS:
            enqueue_hybrid_refresh("injury_report", ("latest",))
        return payload_copy
    refreshed = fetch_latest_injury_report_payload(sync_write=True)
    if refreshed.get("ok"):
        return refreshed
    enqueue_hybrid_refresh("injury_report", ("latest",))
    return {
        "ok": False,
        "rows": [],
        "report_label": "",
        "report_url": "",
        "source": "none",
    }


def _get_injury_aware_boost_cache(cache_key: tuple[Any, ...]) -> dict[str, Any] | None:
    now_ts = time.time()
    with INJURY_AWARE_BOOST_CACHE_LOCK:
        cached = INJURY_AWARE_BOOST_CACHE.get(cache_key)
        if not cached:
            return None
        cached_ts = float(cached.get("timestamp") or 0.0)
        if now_ts - cached_ts > INJURY_AWARE_BOOST_CACHE_TTL_SECONDS:
            INJURY_AWARE_BOOST_CACHE.pop(cache_key, None)
            return None
        payload = cached.get("payload")
        if isinstance(payload, dict):
            return copy.deepcopy(payload)
        if payload is None:
            return None
        return None


def _set_injury_aware_boost_cache(cache_key: tuple[Any, ...], payload: dict[str, Any] | None) -> None:
    now_ts = time.time()
    with INJURY_AWARE_BOOST_CACHE_LOCK:
        INJURY_AWARE_BOOST_CACHE[cache_key] = {
            "timestamp": now_ts,
            "payload": copy.deepcopy(payload) if isinstance(payload, dict) else None,
        }
        if len(INJURY_AWARE_BOOST_CACHE) > INJURY_AWARE_BOOST_CACHE_MAX_ENTRIES:
            stale_keys = sorted(
                INJURY_AWARE_BOOST_CACHE.items(),
                key=lambda item: float((item[1] or {}).get("timestamp") or 0.0)
            )[: max(1, len(INJURY_AWARE_BOOST_CACHE) - INJURY_AWARE_BOOST_CACHE_MAX_ENTRIES)]
            for stale_key, _ in stale_keys:
                INJURY_AWARE_BOOST_CACHE.pop(stale_key, None)


def _build_injury_combo_plan(
    *,
    base_hit_rate: float,
    priority_rank: int | None = None,
    priority_pool: int | None = None,
) -> list[int]:
    max_plan = max(1, min(3, INJURY_AWARE_MAX_COMBO_TRIES))
    if max_plan == 1:
        return [1]

    if priority_rank and priority_pool and priority_pool > 0:
        percentile = float(priority_rank) / float(max(1, priority_pool))
        if percentile <= INJURY_AWARE_HIGH_PRIORITY_SHARE:
            plan_max = 3
        elif percentile <= 0.60:
            plan_max = 2
        else:
            plan_max = 1
    else:
        # Without explicit ranking context, keep it conservative.
        plan_max = 2 if float(base_hit_rate) < INJURY_AWARE_BASE_HITRATE_EXPAND_THRESHOLD else 1

    if float(base_hit_rate) < INJURY_AWARE_BASE_HITRATE_EXPAND_THRESHOLD:
        plan_max = min(3, plan_max + 1)

    plan_max = max(1, min(max_plan, plan_max))
    return list(range(1, plan_max + 1))


def _should_stop_injury_combo_search(
    *,
    base_hit_rate: float,
    boosted_hit_rate: float,
    boosted_games: int,
    combo_size: int,
    combo_plan: list[int],
    priority_rank: int | None = None,
    priority_pool: int | None = None,
) -> bool:
    gain = float(boosted_hit_rate) - float(base_hit_rate)
    if int(boosted_games) >= INJURY_AWARE_EARLY_STOP_MIN_GAMES and gain >= INJURY_AWARE_EARLY_STOP_GAIN_PCT:
        return True

    if combo_plan and combo_size == combo_plan[0] and gain <= INJURY_AWARE_LOW_GAIN_CUTOFF_PCT:
        # Only low-priority rows short-circuit on tiny gain.
        if not (priority_rank and priority_pool and priority_pool > 0):
            return True
        if float(priority_rank) / float(priority_pool) > INJURY_AWARE_HIGH_PRIORITY_SHARE:
            return True

    return False


def build_availability_payload(player_name: str, team_name: str | None = None, game_date: str | None = None) -> dict[str, Any]:
    return INJURY_SERVICE.build_availability_payload(player_name, team_name=team_name, game_date=game_date)


def build_team_availability_summary(team_name: str | None, report_payload: dict[str, Any] | None = None, game_date: str | None = None) -> dict[str, Any]:
    return INJURY_SERVICE.build_team_availability_summary(team_name, report_payload=report_payload, game_date=game_date)


CONFIDENCE_TIER_ELITE_MIN = 88
CONFIDENCE_TIER_HIGH_MIN = 75
CONFIDENCE_TIER_MEDIUM_MIN = 62
BACKTEST_RANKING_MIN_SAMPLES = 8
BACKTEST_RANKING_BASE_MIN_SAMPLES = 12
BACKTEST_RANKING_MAX_ADJUSTMENT = 6.0
BACKTEST_RANKING_CACHE_TTL_SECONDS = 120
_BACKTEST_RANKING_CACHE_LOCK = Lock()
_BACKTEST_RANKING_CACHE: dict[str, Any] = {"timestamp": 0.0, "metrics": {}}


def _invalidate_backtest_ranking_cache() -> None:
    with _BACKTEST_RANKING_CACHE_LOCK:
        _BACKTEST_RANKING_CACHE["timestamp"] = 0.0
        _BACKTEST_RANKING_CACHE["metrics"] = {}


def _apply_confidence_recalibration(
    *,
    score: int,
    games_count: int,
    season_type: str | None,
    playoff_game_number: int | None,
) -> tuple[int, dict[str, float], list[str]]:
    normalized_season_type = normalize_requested_season_type(season_type)
    sample_adj = 0.0
    playoff_adj = 0.0
    top_end_adj = 0.0
    notes: list[str] = []

    if games_count <= 2:
        sample_adj = -12.0
    elif games_count == 3:
        sample_adj = -9.0
    elif games_count == 4:
        sample_adj = -6.0
    elif games_count == 5:
        sample_adj = -4.0
    elif games_count == 6:
        sample_adj = -2.0

    if normalized_season_type == SEASON_TYPE_PLAYOFFS and games_count <= 4:
        playoff_adj -= 4.0
    if normalized_season_type == SEASON_TYPE_PLAYOFFS and int(playoff_game_number or 0) in {1, 2, 3}:
        playoff_adj -= 2.0

    # Compress the top end so 80+/90+ confidence is harder to reach.
    if score >= 90:
        top_end_adj -= round((score - 89) * 0.55, 1)
    elif score >= 80:
        top_end_adj -= round((score - 79) * 0.25, 1)

    if sample_adj < 0:
        notes.append("small sample recalibration")
    if playoff_adj < 0:
        notes.append("playoff volatility penalty")
    if top_end_adj < 0:
        notes.append("top-end confidence compression")

    adjusted = int(max(0, min(99, round(score + sample_adj + playoff_adj + top_end_adj))))
    components = {
        "sample_recalibration": round(sample_adj, 1),
        "playoff_recalibration": round(playoff_adj, 1),
        "top_end_recalibration": round(top_end_adj, 1),
    }
    return adjusted, components, notes


def _compute_backtest_ranking_metrics() -> dict[str, Any]:
    now_ts = time.time()
    with _BACKTEST_RANKING_CACHE_LOCK:
        cached_ts = float(_BACKTEST_RANKING_CACHE.get("timestamp") or 0.0)
        if now_ts - cached_ts < BACKTEST_RANKING_CACHE_TTL_SECONDS:
            cached_metrics = _BACKTEST_RANKING_CACHE.get("metrics")
            if isinstance(cached_metrics, dict):
                return cached_metrics

    with _BACKTEST_LOCK:
        entries = [copy.deepcopy(entry) for entry in _BACKTEST_LOG]

    stat_totals: dict[str, dict[str, float]] = {}
    stat_side_totals: dict[tuple[str, str], dict[str, float]] = {}
    for entry in entries:
        result = str(entry.get("result") or "").strip().lower()
        if result not in {"hit", "miss"}:
            continue
        stat = str(entry.get("stat") or "").strip().upper()
        side = str(entry.get("side") or "").strip().upper()
        if not stat or side not in {"OVER", "UNDER"}:
            continue
        odds = safe_float_or_none(entry.get("odds"))
        profit = (float(odds) - 1.0) if (result == "hit" and odds and float(odds) > 1.0) else (1.0 if result == "hit" else -1.0)

        stat_bucket = stat_totals.setdefault(stat, {"count": 0.0, "hits": 0.0, "profit": 0.0})
        stat_bucket["count"] += 1.0
        stat_bucket["hits"] += 1.0 if result == "hit" else 0.0
        stat_bucket["profit"] += float(profit)

        side_bucket = stat_side_totals.setdefault((stat, side), {"count": 0.0, "hits": 0.0, "profit": 0.0})
        side_bucket["count"] += 1.0
        side_bucket["hits"] += 1.0 if result == "hit" else 0.0
        side_bucket["profit"] += float(profit)

    metrics = {
        "stat_totals": stat_totals,
        "stat_side_totals": stat_side_totals,
    }
    with _BACKTEST_RANKING_CACHE_LOCK:
        _BACKTEST_RANKING_CACHE["timestamp"] = now_ts
        _BACKTEST_RANKING_CACHE["metrics"] = metrics
    return metrics


def get_backtest_ranking_adjustment(stat: str | None, side: str | None) -> tuple[float, dict[str, Any]]:
    stat_key = str(stat or "").strip().upper()
    side_key = str(side or "").strip().upper()
    if not stat_key or side_key not in {"OVER", "UNDER"}:
        return 0.0, {"reason": "missing_inputs"}

    metrics = _compute_backtest_ranking_metrics()
    stat_totals = metrics.get("stat_totals") or {}
    stat_side_totals = metrics.get("stat_side_totals") or {}
    base = stat_totals.get(stat_key) or {}
    segment = stat_side_totals.get((stat_key, side_key)) or {}
    base_count = int(base.get("count") or 0)
    segment_count = int(segment.get("count") or 0)
    if base_count < BACKTEST_RANKING_BASE_MIN_SAMPLES:
        return 0.0, {"reason": "insufficient_base", "base_count": base_count, "segment_count": segment_count}
    if segment_count < BACKTEST_RANKING_MIN_SAMPLES:
        return 0.0, {"reason": "insufficient_segment", "base_count": base_count, "segment_count": segment_count}

    base_hit_rate = (float(base.get("hits") or 0.0) / float(base_count)) * 100.0 if base_count > 0 else 0.0
    segment_hit_rate = (float(segment.get("hits") or 0.0) / float(segment_count)) * 100.0 if segment_count > 0 else 0.0
    base_roi = float(base.get("profit") or 0.0) / float(base_count) if base_count > 0 else 0.0
    segment_roi = float(segment.get("profit") or 0.0) / float(segment_count) if segment_count > 0 else 0.0

    hit_rate_delta = segment_hit_rate - base_hit_rate
    roi_delta_pct = (segment_roi - base_roi) * 100.0
    sample_strength = max(0.2, min(1.0, float(segment_count) / 40.0))
    raw_adjustment = ((hit_rate_delta * 0.22) + (roi_delta_pct * 0.18)) * sample_strength
    adjustment = max(-BACKTEST_RANKING_MAX_ADJUSTMENT, min(BACKTEST_RANKING_MAX_ADJUSTMENT, raw_adjustment))

    if abs(adjustment) < 0.15:
        return 0.0, {
            "reason": "neutral",
            "base_count": base_count,
            "segment_count": segment_count,
            "hit_rate_delta": round(hit_rate_delta, 2),
            "roi_delta_pct": round(roi_delta_pct, 2),
        }

    return round(adjustment, 1), {
        "reason": "applied",
        "base_count": base_count,
        "segment_count": segment_count,
        "hit_rate_delta": round(hit_rate_delta, 2),
        "roi_delta_pct": round(roi_delta_pct, 2),
    }


def apply_backtest_ranking_adjustment(
    confidence_engine: dict[str, Any],
    *,
    stat: str | None,
    side: str | None,
) -> dict[str, Any]:
    adjusted = copy.deepcopy(confidence_engine or {})
    base_ranking_score = int(adjusted.get("ranking_score") if adjusted.get("ranking_score") is not None else adjusted.get("score") or 0)
    adjustment, meta = get_backtest_ranking_adjustment(stat, side)
    adjusted["backtest_ranking_adjustment"] = adjustment
    adjusted["backtest_ranking_meta"] = meta
    if abs(adjustment) < 0.1:
        adjusted["ranking_score"] = base_ranking_score
        return adjusted

    ranking_score = int(max(0, min(99, round(base_ranking_score + adjustment))))
    adjusted["ranking_score"] = ranking_score

    components = dict(adjusted.get("components") or {})
    components["backtest_calibration"] = round(adjustment, 1)
    adjusted["components"] = components

    tags = list(adjusted.get("tags") or [])
    tag_text = f"Backtest adj {adjustment:+.1f}"
    if tag_text not in tags:
        tags.append(tag_text)
    adjusted["tags"] = tags[:6]
    return adjusted


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
    season_type: str | None = None,
    playoff_game_number: int | None = None,
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

    form_component = max(-10.0, min(14.0, (hit_rate - 50.0) * 0.42))
    sample_component = max(-4.0, min(6.0, (games_count - 5) * 0.5))
    ev_component = max(-12.0, min(14.0, ev * 85.0))
    edge_component = max(-10.0, min(12.0, edge_value * 0.8))
    line_value_component = max(-8.0, min(9.0, line_delta * 1.8))

    matchup_component = 0.0
    tags: list[str] = []
    if matchup_delta_pct is not None:
        matchup_component = max(-16.0, min(14.0, matchup_value * 0.5))
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

    playoff_series_component = 0.0
    normalized_season_type = normalize_requested_season_type(season_type)
    if normalized_season_type == SEASON_TYPE_PLAYOFFS:
        game_number = int(playoff_game_number or 0)
        if game_number == 1:
            playoff_series_component = -12.0
        elif game_number == 2:
            playoff_series_component = -8.0
        elif game_number == 3:
            playoff_series_component = -5.0
        elif game_number == 4:
            playoff_series_component = -3.0
        if playoff_series_component < 0:
            tags.append('Early series - caution')

    # market_component is zeroed here  the same market signal already adjusts
    # the model probability inside estimate_model_probabilities() via environment_term.
    # Counting it again in the confidence score would double-penalize/boost every prop.
    # Tags and summary text from the signal are still applied for UI context.
    market_signal = derive_market_environment_signal(stat, environment)
    market_component = 0.0
    _raw_market_adj = float(market_signal.get("over_adjustment") or 0.0) * (1 if side == "OVER" else -1)
    if _raw_market_adj >= 0.02:
        for tag in market_signal.get("support_tags") or []:
            if tag not in tags:
                tags.append(tag)
    elif _raw_market_adj <= -0.02:
        for tag in market_signal.get("caution_tags") or []:
            if tag not in tags:
                tags.append(tag)

    environment_weight = 0.6
    schedule_component *= environment_weight
    # market_component stays 0.0  no scaling needed

    score = 40.0
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
        'playoff_series': round(playoff_series_component, 1),
    }
    score += sum(components.values())
    score = int(max(0, min(99, round(score))))
    recalibrated_score, recalibration_components, recalibration_notes = _apply_confidence_recalibration(
        score=score,
        games_count=int(games_count or 0),
        season_type=season_type,
        playoff_game_number=playoff_game_number,
    )
    score = recalibrated_score
    components.update(recalibration_components)
    for note in recalibration_notes:
        if note not in tags:
            tags.append(note)

    grade, tone, tier = _confidence_band_from_score(score)

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
    elif playoff_series_component < 0:
        summary_parts.append('early series penalty applied')
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
        'summary': '  '.join(summary_parts).capitalize(),
        'components': components,
        'tags': unique_tags[:6],
        'line_delta': round(line_delta, 2),
        'raw_line_delta': round(raw_line_delta, 2),
        'player_position': player_position or '',
        'stat': stat or '',
    }


def _confidence_band_from_score(score: int) -> tuple[str, str, str]:
    if score >= CONFIDENCE_TIER_ELITE_MIN:
        return 'A', 'elite', 'Elite'
    if score >= CONFIDENCE_TIER_HIGH_MIN:
        return 'B', 'good', 'High'
    if score >= CONFIDENCE_TIER_MEDIUM_MIN:
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
        adjusted["summary"] = f"{summary}  {note}".strip(" ")
    return adjusted

def apply_market_probability_penalty(
    model_over: float,
    model_under: float,
    *,
    implied_over: float | None,
    implied_under: float | None,
) -> tuple[float, float]:
    """Penalize model probability when it disagrees with market pricing."""
    try:
        over_prob = float(implied_over) if implied_over is not None else None
        under_prob = float(implied_under) if implied_under is not None else None
    except (TypeError, ValueError):
        over_prob = None
        under_prob = None

    if over_prob is None or under_prob is None:
        return model_over, model_under

    market_side = "OVER" if over_prob >= under_prob else "UNDER"
    model_side = "OVER" if model_over >= model_under else "UNDER"
    if market_side == model_side:
        return model_over, model_under

    gap = max(0.0, abs(over_prob - under_prob))
    penalty = min(0.12, 0.03 + (gap * 0.7))

    if model_side == "OVER":
        adjusted_over = clamp(model_over - penalty, 0.02, 0.98)
        adjusted_under = clamp(1.0 - adjusted_over, 0.02, 0.98)
    else:
        adjusted_under = clamp(model_under - penalty, 0.02, 0.98)
        adjusted_over = clamp(1.0 - adjusted_under, 0.02, 0.98)

    return round(adjusted_over, 4), round(adjusted_under, 4)


def resolve_market_fair_inputs(
    market_row: dict[str, Any],
    over_odds: float | None,
    under_odds: float | None,
) -> dict[str, float | None]:
    market_probs = fair_implied_probabilities(over_odds, under_odds)
    implied_over = safe_float_or_none(market_probs.get("over"))
    implied_under = safe_float_or_none(market_probs.get("under"))
    hold = safe_float_or_none(market_probs.get("hold"))

    fair_over = safe_float_or_none(market_row.get("consensus_over_fair_prob"))
    fair_under = safe_float_or_none(market_row.get("consensus_under_fair_prob"))
    if fair_over is None:
        fair_over = safe_float_or_none(market_row.get("over_fair_prob"))
    if fair_under is None:
        fair_under = safe_float_or_none(market_row.get("under_fair_prob"))

    resolved_fair_over = resolve_fair_probability(fair_over, fallback_prob=implied_over)
    resolved_fair_under = resolve_fair_probability(fair_under, fallback_prob=implied_under)
    return {
        "implied_over": implied_over,
        "implied_under": implied_under,
        "fair_over": resolved_fair_over,
        "fair_under": resolved_fair_under,
        "hold": hold,
    }


CALIBRATION_SAMPLE_BASELINE_GAMES = 20.0
CALIBRATION_H2H_BASELINE_GAMES = 5.0
CALIBRATION_HOLD_NEUTRAL = 0.045
CALIBRATION_HOLD_PENALTY_MULTIPLIER = 1.1
CALIBRATION_RELIABILITY_MIN = 0.22
CALIBRATION_RELIABILITY_MAX = 0.92
CALIBRATION_SHRINK_MIN = 0.08
CALIBRATION_SHRINK_MAX = 0.72


def build_probability_reliability_profile(
    *,
    games_count: int,
    stat: str,
    variance: dict[str, Any] | None = None,
    h2h_games_count: int | None = None,
    market_hold: float | None = None,
) -> dict[str, float]:
    variance = variance or {}
    sample_factor = clamp(float(games_count or 0) / CALIBRATION_SAMPLE_BASELINE_GAMES, 0.0, 1.0)
    h2h_factor = clamp(float(h2h_games_count or 0) / CALIBRATION_H2H_BASELINE_GAMES, 0.0, 1.0)
    cv = safe_float_or_none(variance.get("cv"))
    consistency = safe_float_or_none(variance.get("consistency_score"))

    if cv is None:
        variance_factor = 0.58
    else:
        variance_factor = clamp(1.0 - max(0.0, cv - 0.45) * 0.9, 0.35, 1.0)
    consistency_factor = clamp((consistency if consistency is not None else 50.0) / 100.0, 0.35, 1.0)

    hold_penalty = 0.0
    if market_hold is not None:
        hold_penalty = clamp(
            max(0.0, market_hold - CALIBRATION_HOLD_NEUTRAL) * CALIBRATION_HOLD_PENALTY_MULTIPLIER,
            0.0,
            0.08,
        )

    stat_noise_penalty = 0.08 if str(stat or "").upper() in {"STL", "BLK", "3PM"} else 0.0
    reliability = (
        0.22
        + (sample_factor * 0.36)
        + (h2h_factor * 0.12)
        + (variance_factor * 0.16)
        + (consistency_factor * 0.14)
        - hold_penalty
        - stat_noise_penalty
    )
    reliability = clamp(reliability, CALIBRATION_RELIABILITY_MIN, CALIBRATION_RELIABILITY_MAX)
    shrink_strength = clamp(1.0 - reliability, CALIBRATION_SHRINK_MIN, CALIBRATION_SHRINK_MAX)
    return {
        "sample_factor": round(sample_factor, 4),
        "h2h_factor": round(h2h_factor, 4),
        "variance_factor": round(variance_factor, 4),
        "consistency_factor": round(consistency_factor, 4),
        "hold_penalty": round(hold_penalty, 4),
        "stat_noise_penalty": round(stat_noise_penalty, 4),
        "reliability": round(reliability, 4),
        "shrink_strength": round(shrink_strength, 4),
    }


def calibrate_two_way_probabilities(
    model_over: float,
    model_under: float,
    *,
    fair_over: float | None,
    fair_under: float | None,
    shrink_strength: float,
) -> tuple[float, float]:
    anchor_over = safe_float_or_none(fair_over)
    anchor_under = safe_float_or_none(fair_under)
    if anchor_over is None and anchor_under is None:
        anchor_over = 0.5
        anchor_under = 0.5
    elif anchor_over is None and anchor_under is not None:
        anchor_over = clamp(1.0 - anchor_under, 0.0, 1.0)
    elif anchor_under is None and anchor_over is not None:
        anchor_under = clamp(1.0 - anchor_over, 0.0, 1.0)

    calibrated_over = (float(model_over) * (1.0 - shrink_strength)) + (float(anchor_over or 0.5) * shrink_strength)
    calibrated_under = (float(model_under) * (1.0 - shrink_strength)) + (float(anchor_under or 0.5) * shrink_strength)
    total = calibrated_over + calibrated_under
    if total <= 0:
        return clamp(float(model_over), 0.02, 0.98), clamp(float(model_under), 0.02, 0.98)
    calibrated_over = clamp(calibrated_over / total, 0.02, 0.98)
    calibrated_under = clamp(1.0 - calibrated_over, 0.02, 0.98)
    return round(calibrated_over, 4), round(calibrated_under, 4)


def compute_side_pricing_metrics(probability: float, fair_probability: float, odds: float | None) -> dict[str, float]:
    fair_prob = resolve_fair_probability(safe_float_or_none(fair_probability), fallback_prob=decimal_implied_probability(odds))
    edge_pct = edge_pct_from_model_and_fair(float(probability), fair_prob)
    if odds and odds > 1.0:
        ev = round((float(probability) * float(odds)) - 1.0, 4)
    else:
        ev = round(float(probability) - fair_prob, 4)
    return {
        "edge_pct": edge_pct,
        "ev": ev,
        "fair_probability": round(fair_prob, 6),
    }


def risk_adjust_ev(ev_value: float, reliability: float) -> float:
    reliability_clamped = clamp(float(reliability), 0.0, 1.0)
    multiplier = 0.52 + (0.48 * reliability_clamped)
    return round(float(ev_value) * multiplier, 4)


def build_shared_market_pricing_snapshot(
    *,
    market_row: dict[str, Any],
    over_odds: float,
    under_odds: float,
    hit_rate_pct: float,
    average: float,
    line: float,
    stat: str,
    matchup_delta_pct: float | None,
    opportunity: dict[str, Any] | None = None,
    team_context: dict[str, Any] | None = None,
    environment: dict[str, Any] | None = None,
    variance: dict[str, Any] | None = None,
    games_count: int = 0,
    h2h_games_count: int | None = None,
) -> dict[str, Any]:
    fair_inputs = resolve_market_fair_inputs(market_row, over_odds, under_odds)
    model_over, model_under = estimate_model_probabilities(
        hit_rate_pct=float(hit_rate_pct),
        average=float(average),
        line=float(line),
        matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
        stat=str(stat),
        opportunity=opportunity or {},
        team_context=team_context or {},
        environment=environment or {},
        variance=variance or {},
    )
    raw_over, raw_under = apply_market_probability_penalty(
        model_over,
        model_under,
        implied_over=fair_inputs.get("implied_over"),
        implied_under=fair_inputs.get("implied_under"),
    )

    reliability = build_probability_reliability_profile(
        games_count=int(games_count or 0),
        stat=str(stat),
        variance=variance or {},
        h2h_games_count=h2h_games_count,
        market_hold=safe_float_or_none(fair_inputs.get("hold")),
    )
    calibrated_over, calibrated_under = calibrate_two_way_probabilities(
        raw_over,
        raw_under,
        fair_over=fair_inputs.get("fair_over"),
        fair_under=fair_inputs.get("fair_under"),
        shrink_strength=float(reliability["shrink_strength"]),
    )

    raw_over_metrics = compute_side_pricing_metrics(raw_over, float(fair_inputs["fair_over"]), over_odds)
    raw_under_metrics = compute_side_pricing_metrics(raw_under, float(fair_inputs["fair_under"]), under_odds)
    calibrated_over_metrics = compute_side_pricing_metrics(calibrated_over, float(fair_inputs["fair_over"]), over_odds)
    calibrated_under_metrics = compute_side_pricing_metrics(calibrated_under, float(fair_inputs["fair_under"]), under_odds)
    adjusted_over_ev = risk_adjust_ev(calibrated_over_metrics["ev"], float(reliability["reliability"]))
    adjusted_under_ev = risk_adjust_ev(calibrated_under_metrics["ev"], float(reliability["reliability"]))

    return {
        "market": fair_inputs,
        "raw": {
            "over_probability": raw_over,
            "under_probability": raw_under,
            "over_edge_pct": raw_over_metrics["edge_pct"],
            "under_edge_pct": raw_under_metrics["edge_pct"],
            "over_ev": raw_over_metrics["ev"],
            "under_ev": raw_under_metrics["ev"],
        },
        "calibrated": {
            "over_probability": calibrated_over,
            "under_probability": calibrated_under,
            "over_edge_pct": calibrated_over_metrics["edge_pct"],
            "under_edge_pct": calibrated_under_metrics["edge_pct"],
            "over_ev": calibrated_over_metrics["ev"],
            "under_ev": calibrated_under_metrics["ev"],
        },
        "adjusted": {
            "over_ev": adjusted_over_ev,
            "under_ev": adjusted_under_ev,
        },
        "reliability": reliability,
    }

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


def compute_side_h2h_metrics(h2h_payload: dict[str, Any] | None, side: str) -> tuple[int, int | None, float | None]:
    payload = h2h_payload or {}
    h2h_games_count = max(0, int(payload.get("games_count") or 0))
    h2h_over_hit_count: int | None = None
    try:
        raw_h2h_hit_count = payload.get("hit_count")
        if raw_h2h_hit_count is not None:
            h2h_over_hit_count = max(0, min(h2h_games_count, int(raw_h2h_hit_count)))
    except (TypeError, ValueError):
        h2h_over_hit_count = None
    if h2h_over_hit_count is None:
        try:
            raw_rate = float(payload.get("hit_rate"))
            if h2h_games_count > 0:
                h2h_over_hit_count = max(0, min(h2h_games_count, int(round((raw_rate / 100.0) * h2h_games_count))))
        except (TypeError, ValueError):
            h2h_over_hit_count = None

    if h2h_games_count <= 0 or h2h_over_hit_count is None:
        return h2h_games_count, None, None

    clamped_h2h_over_hits = max(0, min(h2h_games_count, h2h_over_hit_count))
    if str(side).upper() == "UNDER":
        h2h_side_hit_count = max(0, h2h_games_count - clamped_h2h_over_hits)
    else:
        h2h_side_hit_count = clamped_h2h_over_hits
    h2h_side_hit_rate = (h2h_side_hit_count / h2h_games_count) * 100.0
    return h2h_games_count, h2h_side_hit_count, h2h_side_hit_rate


def compute_side_h2h_metrics_from_games(
    games: list[dict[str, Any]] | None,
    side: str,
    opponent_abbreviation: str | None,
) -> tuple[int, int | None, float | None]:
    opponent = str(opponent_abbreviation or "").upper().strip()
    if not opponent:
        return 0, None, None
    rows = [
        game
        for game in (games or [])
        if matchup_includes_opponent((game or {}).get("matchup"), opponent)
    ]
    total = len(rows)
    if total <= 0:
        return 0, None, None
    side_upper = str(side).upper()
    side_hits = 0
    for game in rows:
        over_hit = bool((game or {}).get("hit"))
        side_hit = (not over_hit) if side_upper == "UNDER" else over_hit
        if side_hit:
            side_hits += 1
    side_rate = (side_hits / total) * 100.0
    return total, side_hits, side_rate


def resolve_side_h2h_metrics(
    games: list[dict[str, Any]] | None,
    h2h_payload: dict[str, Any] | None,
    side: str,
    opponent_abbreviation: str | None,
) -> tuple[int, int | None, float | None]:
    """
    Resolve side-aware H2H metrics from both sources and prefer the larger
    valid sample size so we don't undercount to tiny windows like 1/1 when
    the analyzed H2H payload has a fuller sample (e.g., 1/3).
    """
    from_games = compute_side_h2h_metrics_from_games(games, side, opponent_abbreviation)
    from_payload = compute_side_h2h_metrics(h2h_payload or {}, side)

    games_count_from_games = int(from_games[0] or 0)
    games_count_from_payload = int(from_payload[0] or 0)

    if games_count_from_payload > games_count_from_games:
        return from_payload
    if games_count_from_games > 0:
        return from_games
    return from_payload


MIN_PARLAY_H2H_GAMES_FOR_RANKING = 2
MIN_PARLAY_PLAYOFF_H2H_GAMES_FOR_PRIMARY = 4
MAX_PARLAY_PLAYOFF_H2H_WEIGHT = 0.35


def playoff_h2h_weight_for_sample(games_count: int | None) -> float:
    games = max(0, int(games_count or 0))
    if games <= 0:
        return 0.0
    if games == 1:
        return 0.08
    if games == 2:
        return 0.15
    if games == 3:
        return 0.24
    if games == 4:
        return 0.32
    return MAX_PARLAY_PLAYOFF_H2H_WEIGHT


def _score_from_pct(value: Any, default: float = 50.0) -> float:
    try:
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return clamp(number, 0.0, 100.0)
    except Exception:
        return default


def build_parlay_ranking_context(
    *,
    season_type: str | None,
    side: str,
    stat: str,
    side_hit_rate: float,
    games_count: int,
    h2h_games_count: int,
    h2h_side_hit_rate: float | None,
    model_probability: float | None,
    fair_probability: float | None,
    edge_pct: float | None,
    ev_decimal: float | None,
    confidence_score: int | float | None,
    engine_ranking_score: int | float | None,
    line: float | None,
    average: float | None,
) -> dict[str, Any]:
    normalized_season_type = normalize_requested_season_type(season_type)
    side_upper = str(side or "").upper()
    stat_upper = str(stat or "").upper()

    recent_score = _score_from_pct(side_hit_rate)
    h2h_count = max(0, int(h2h_games_count or 0))
    h2h_score = _score_from_pct(h2h_side_hit_rate, default=recent_score) if h2h_side_hit_rate is not None else None
    confidence_component = _score_from_pct(
        engine_ranking_score if engine_ranking_score is not None else confidence_score,
        default=50.0,
    )

    model_raw = safe_float_or_none(model_probability)
    if model_raw is None:
        model_component = recent_score
    else:
        model_component = clamp(model_raw * 100.0 if model_raw <= 1.0 else model_raw, 0.0, 100.0)

    edge_component = 50.0 + (clamp(float(edge_pct or 0.0), -24.0, 24.0) * 1.25)
    ev_component = 50.0 + (clamp(float(ev_decimal or 0.0), -0.25, 0.25) * 110.0)
    fair_raw = safe_float_or_none(fair_probability)
    fair_component = 50.0
    if fair_raw is not None:
        fair_pct = fair_raw * 100.0 if fair_raw <= 1.0 else fair_raw
        fair_component = 50.0 + clamp(model_component - fair_pct, -18.0, 18.0) * 1.1

    line_component = 50.0
    avg_value = safe_float_or_none(average)
    line_value = safe_float_or_none(line)
    if avg_value is not None and line_value is not None:
        raw_cushion = avg_value - line_value
        side_cushion = -raw_cushion if side_upper == "UNDER" else raw_cushion
        scale = max(1.0, abs(line_value) * 0.16)
        line_component = 50.0 + clamp((side_cushion / scale) * 18.0, -18.0, 18.0)

    base_blend = (
        model_component * 0.27
        + confidence_component * 0.23
        + recent_score * 0.18
        + edge_component * 0.13
        + ev_component * 0.08
        + fair_component * 0.06
        + line_component * 0.05
    )
    if int(games_count or 0) <= 4:
        base_blend -= max(0, 5 - int(games_count or 0)) * 1.3
    base_blend = clamp(base_blend, 0.0, 100.0)

    notes: list[str] = []
    tips: list[str] = []
    h2h_weight = 0.0
    ranking_source = "recent"
    ranking_hit_rate = recent_score
    ranking_sort_score = recent_score
    ranking_blend_score = recent_score

    if normalized_season_type == SEASON_TYPE_PLAYOFFS:
        h2h_weight = playoff_h2h_weight_for_sample(h2h_count) if h2h_score is not None else 0.0
        ranking_blend_score = base_blend
        if h2h_score is not None:
            ranking_blend_score = (base_blend * (1.0 - h2h_weight)) + (h2h_score * h2h_weight)
        ranking_blend_score = clamp(ranking_blend_score, 0.0, 100.0)
        ranking_source = "playoff_blend"
        ranking_hit_rate = ranking_blend_score
        ranking_sort_score = ranking_blend_score

        if h2h_count <= 0:
            notes.append("New playoff matchup")
            tips.append("H2H is parked at 0% until this series creates real matchup data.")
        elif h2h_count < MIN_PARLAY_PLAYOFF_H2H_GAMES_FOR_PRIMARY:
            notes.append("Tiny H2H sample capped")
            tips.append(f"H2H is capped at {round(h2h_weight * 100)}% because the series sample is still small.")
        else:
            notes.append("H2H weighted, not absolute")
            tips.append(f"H2H contributes {round(h2h_weight * 100)}%; model edge, role, and price still drive the rank.")

        if int(games_count or 0) <= 4:
            notes.append("Small playoff sample")
            tips.append("Keep early-series slips short until minutes and coverage stabilize.")
    else:
        has_reliable_h2h = h2h_score is not None and h2h_count >= MIN_PARLAY_H2H_GAMES_FOR_RANKING
        if has_reliable_h2h:
            ranking_source = "h2h"
            ranking_hit_rate = h2h_score
            ranking_sort_score = h2h_score
            ranking_blend_score = h2h_score
        else:
            ranking_source = "recent"
            ranking_hit_rate = recent_score
            ranking_sort_score = recent_score
            ranking_blend_score = recent_score

    if stat_upper in {"STL", "BLK"}:
        tips.append("Stocks are high-variance props; use them as leans, not parlay anchors.")
    elif stat_upper == "3PM":
        tips.append("For threes, trust stable attempts more than one hot shooting result.")
    elif stat_upper in {"PRA", "PR", "PA", "RA"}:
        tips.append("Combo props need role security; minutes volatility matters more than one box score.")

    if float(edge_pct or 0.0) >= 4.0:
        tips.append("Model price is ahead of the fair market number.")
    elif float(edge_pct or 0.0) <= -2.0:
        tips.append("Market edge is thin; require strong role or matchup support.")

    unique_tips: list[str] = []
    for tip in tips:
        if tip and tip not in unique_tips:
            unique_tips.append(tip)

    return {
        "ranking_source": ranking_source,
        "ranking_hit_rate": round(ranking_hit_rate, 1),
        "ranking_sort_score": round(ranking_sort_score, 1),
        "ranking_blend_score": round(ranking_blend_score, 1),
        "h2h_weight_pct": round(h2h_weight * 100.0, 1),
        "ranking_profile": {
            "source": ranking_source,
            "sort_score": round(ranking_sort_score, 1),
            "blend_score": round(ranking_blend_score, 1),
            "h2h_weight_pct": round(h2h_weight * 100.0, 1),
            "notes": notes[:4],
            "tips": unique_tips[:4],
            "components": {
                "recent": round(recent_score, 1),
                "model": round(model_component, 1),
                "confidence": round(confidence_component, 1),
                "edge": round(edge_component, 1),
                "ev": round(ev_component, 1),
                "line_value": round(line_component, 1),
                "h2h": round(h2h_score, 1) if h2h_score is not None else None,
            },
        },
        "playoff_strategy_tips": unique_tips[:4],
    }


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


def convert_game_status_text_to_pht(status_text: str, game_date: str | None = None) -> str:
    raw = str(status_text or "").strip()
    if not raw:
        return "TBD"
    # Scheduled NBA scoreboard text is typically like "7:00 pm ET".
    match = re.match(r"^\s*(\d{1,2}):(\d{2})\s*([AaPp][Mm])\s*ET\s*$", raw)
    if not match:
        return raw
    try:
        hour = int(match.group(1))
        minute = int(match.group(2))
        ampm = match.group(3).upper()
        if ampm == "PM" and hour != 12:
            hour += 12
        elif ampm == "AM" and hour == 12:
            hour = 0

        if game_date:
            base_date = datetime.strptime(str(game_date), "%Y-%m-%d").date()
        else:
            base_date = app_now().date()

        et_dt = datetime(
            base_date.year,
            base_date.month,
            base_date.day,
            hour,
            minute,
            tzinfo=ZoneInfo("America/New_York"),
        )
        pht_dt = et_dt.astimezone(APP_ZONEINFO)
        hh = pht_dt.strftime("%I").lstrip("0") or "0"
        mm = pht_dt.strftime("%M")
        ampm_pht = pht_dt.strftime("%p").lower()
        return f"{hh}:{mm} {ampm_pht} PHT"
    except Exception:
        return raw


def normalize_requested_season_type(season_type: str | None) -> str:
    raw = str(season_type or "").strip().lower()
    if not raw:
        return DEFAULT_SEASON_TYPE
    if raw in {"combined", "all", "all games", "regular+playoffs", "regular season + playoffs", "regular season and playoffs"}:
        return SEASON_TYPE_COMBINED
    if raw in {"playoff", "playoffs", "postseason"}:
        return SEASON_TYPE_PLAYOFFS
    if raw in {"regular", "regular season"}:
        return SEASON_TYPE_REGULAR
    return SEASON_TYPE_REGULAR


def season_types_for_analysis(season_type: str | None) -> list[str]:
    normalized = normalize_requested_season_type(season_type)
    if normalized == SEASON_TYPE_COMBINED:
        return [SEASON_TYPE_REGULAR, SEASON_TYPE_PLAYOFFS]
    return [normalized]


def merge_game_log_rows(rows_source: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [dict(row) for row in (rows_source or []) if isinstance(row, dict)]
    deduped = dedupe_game_log_rows(rows)
    deduped.sort(
        key=lambda row: (
            parse_game_date_any(row.get("GAME_DATE")) or datetime.min,
            str(row.get("GAME_ID") or row.get("Game_ID") or ""),
        ),
        reverse=True,
    )
    return deduped


@timed_call("team_game_log")
def fetch_team_game_log(team_id: int, season: str, season_type: str) -> list[dict[str, Any]]:
    normalized_season_type = normalize_requested_season_type(season_type)
    cache_key = (int(team_id), str(season), str(normalized_season_type))
    cached = TEAM_GAME_LOG_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < TEAM_CONTEXT_TTL_SECONDS:
        return [dict(row) for row in (cached.get("rows") or [])]

    season_parts = season_types_for_analysis(normalized_season_type)
    if len(season_parts) > 1:
        merged_rows: list[dict[str, Any]] = []
        for part in season_parts:
            merged_rows.extend(fetch_team_game_log(team_id=team_id, season=season, season_type=part))
        merged_rows = merge_game_log_rows(merged_rows)
        TEAM_GAME_LOG_CACHE[cache_key] = {"timestamp": now_ts, "rows": merged_rows}
        return [dict(row) for row in merged_rows]

    source_season_type = season_parts[0]
    try:
        response = call_nba_with_retries(
            lambda: TeamGameLog(
                team_id=team_id,
                season=season,
                season_type_all_star=source_season_type,
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
            payload["spread_label"] = f"Blowout risk  est. -{abs(projected_spread):.1f}"
        elif projected_spread >= 5:
            payload["spread_bucket"] = "favorite"
            payload["spread_label"] = f"Favorite script  est. -{abs(projected_spread):.1f}"
        elif projected_spread <= -13:
            payload["spread_bucket"] = "blowout"
            payload["spread_label"] = f"Blowout risk  est. +{abs(projected_spread):.1f}"
        elif projected_spread <= -5:
            payload["spread_bucket"] = "underdog"
            payload["spread_label"] = f"Underdog script  est. +{abs(projected_spread):.1f}"
        else:
            payload["spread_bucket"] = "close"
            payload["spread_label"] = f"Close spread setup  est. {projected_spread:+.1f}"
    return payload

def derive_market_environment_signal(stat: str | None, environment: dict[str, Any] | None = None) -> dict[str, Any]:
    env = environment or {}
    normalized_stat = str(stat or "").upper().strip()

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

        if normalized_stat in SCORING_STATS:
            over_adjustment += clamp((team_total - 112.0) * 0.006, -0.04, 0.04)
        elif normalized_stat in PLAYMAKING_STATS:
            over_adjustment += clamp((team_total - 112.0) * 0.0045, -0.03, 0.03)
        elif normalized_stat in REBOUND_STATS:
            over_adjustment += clamp((team_total - 112.0) * 0.002, -0.015, 0.015)

    if game_total is not None:
        if game_total >= 232:
            support_tags.append("High game total")
            summary_bits.append(f"game total {game_total:.1f}")
        elif game_total <= 220:
            caution_tags.append("Low game total")
            summary_bits.append(f"game total {game_total:.1f}")

        if normalized_stat in SCORING_STATS:
            over_adjustment += clamp((game_total - 226.0) * 0.003, -0.025, 0.025)
        elif normalized_stat in PLAYMAKING_STATS:
            over_adjustment += clamp((game_total - 226.0) * 0.0025, -0.02, 0.02)
        elif normalized_stat in REBOUND_STATS:
            over_adjustment += clamp((game_total - 226.0) * 0.0015, -0.015, 0.015)

    if spread is not None:
        abs_spread = abs(spread)
        if abs_spread <= 3.5:
            support_tags.append("Tight spread")
            summary_bits.append(f"close spread {spread:+.1f}")
        elif abs_spread >= 10:
            caution_tags.append("Blowout risk")
            summary_bits.append(f"spread {spread:+.1f}")

        if abs_spread <= 3.5 and normalized_stat in SCORING_STATS.union(PLAYMAKING_STATS):
            over_adjustment += 0.01
        elif abs_spread >= 10 and normalized_stat in SCORING_STATS.union(PLAYMAKING_STATS):
            over_adjustment -= 0.02
        elif abs_spread >= 10 and normalized_stat in REBOUND_STATS:
            over_adjustment -= 0.01

    return {
        "over_adjustment": round(clamp(over_adjustment, -0.08, 0.08), 4),
        "support_tags": support_tags,
        "caution_tags": caution_tags,
        "summary": "  ".join(summary_bits),
    }


MARKET_CONTEXT_OVERLAY_CACHE: dict[tuple[Any, ...], dict[str, Any]] = {}
MARKET_CONTEXT_OVERLAY_CACHE_TTL_SECONDS = 15 * 60
MARKET_CONTEXT_OVERLAY_CACHE_MAX_SIZE = 4096


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

    game_total = safe_float_or_none(row.get("market_game_total"))
    home_spread = safe_float_or_none(row.get("market_home_spread"))
    away_spread = safe_float_or_none(row.get("market_away_spread"))
    home_total = safe_float_or_none(row.get("market_home_implied_total"))
    away_total = safe_float_or_none(row.get("market_away_implied_total"))

    cache_key = (
        normalize_name(team_text),
        normalize_name(home_team_text),
        normalize_name(away_team_text),
        round(float(game_total), 2) if game_total is not None else None,
        round(float(home_spread), 2) if home_spread is not None else None,
        round(float(away_spread), 2) if away_spread is not None else None,
        round(float(home_total), 2) if home_total is not None else None,
        round(float(away_total), 2) if away_total is not None else None,
    )
    now_ts = time.time()
    overlay: dict[str, Any] | None = None
    cached = MARKET_CONTEXT_OVERLAY_CACHE.get(cache_key)
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < MARKET_CONTEXT_OVERLAY_CACHE_TTL_SECONDS:
        overlay = copy.deepcopy(cached.get("overlay") or {})
    if overlay is None:
        overlay = {}
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
            overlay["market_game_total"] = round(game_total, 1)
        if team_total is not None:
            overlay["market_team_total"] = round(team_total, 1)
        if opponent_total is not None:
            overlay["market_opponent_total"] = round(opponent_total, 1)
        if team_spread is not None:
            team_spread = round(team_spread, 1)
            overlay["market_spread"] = team_spread
            overlay["projected_spread"] = team_spread
            if abs(team_spread) >= 10:
                overlay["spread_bucket"] = "blowout"
                overlay["spread_label"] = f"Market blowout risk  {team_spread:+.1f}"
            elif abs(team_spread) <= 3.5:
                overlay["spread_bucket"] = "close"
                overlay["spread_label"] = f"Market close spread  {team_spread:+.1f}"
            elif team_spread < 0:
                overlay["spread_bucket"] = "favorite"
                overlay["spread_label"] = f"Market favorite  {team_spread:+.1f}"
            else:
                overlay["spread_bucket"] = "underdog"
                overlay["spread_label"] = f"Market underdog  {team_spread:+.1f}"

        market_bits: list[str] = []
        if team_total is not None:
            market_bits.append(f"team total {team_total:.1f}")
        if game_total is not None:
            market_bits.append(f"game total {game_total:.1f}")
        if team_spread is not None:
            market_bits.append(f"spread {team_spread:+.1f}")
        if market_bits:
            overlay["market_summary"] = "Market context: " + "  ".join(market_bits) + "."

        overlay_signal_seed = dict(overlay)
        overlay["market_context"] = derive_market_environment_signal(None, overlay_signal_seed)

        if len(MARKET_CONTEXT_OVERLAY_CACHE) >= MARKET_CONTEXT_OVERLAY_CACHE_MAX_SIZE:
            MARKET_CONTEXT_OVERLAY_CACHE.clear()
        MARKET_CONTEXT_OVERLAY_CACHE[cache_key] = {
            "timestamp": now_ts,
            "overlay": copy.deepcopy(overlay),
        }

    market_summary = str(overlay.get("market_summary") or "").strip()
    for key, value in overlay.items():
        if key == "market_summary":
            continue
        payload[key] = copy.deepcopy(value)
    if market_summary:
        payload["market_summary"] = market_summary
        existing_summary = str(payload.get("summary") or "").strip()
        payload["summary"] = f"{existing_summary} {market_summary}".strip() if existing_summary else market_summary
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

    opponent_rank = None
    opponent_label = ""
    try:
        opp_id = int((next_game or {}).get("opponent_team_id") or 0)
        if opp_id:
            rank_map = get_cached_team_rank_map(
                season=season or current_nba_season(),
                season_type=normalize_requested_season_type(season_type),
            )
            opponent_rank = rank_map.get(opp_id)
            if opponent_rank:
                opponent_label = f"Opp rank {int(opponent_rank)}"
    except Exception:
        opponent_rank = None
        opponent_label = ""

    return {
        "headline": headline,
        "tone": tone,
        "summary": summary,
        "rest_days": rest_days,
        "is_back_to_back": is_back_to_back,
        "games_last7": games_last7,
        "is_home": (next_game or {}).get("is_home") if next_game else None,
        "venue_label": "Home" if (next_game or {}).get("is_home") else ("Away" if next_game else "TBD"),
        "next_opponent": (next_game or {}).get("opponent_abbreviation") or "",
        "opponent_rank": opponent_rank,
        "opponent_rank_label": opponent_label,
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


def matchup_includes_opponent(matchup: str | None, opponent_abbreviation: str | None) -> bool:
    opponent = str(opponent_abbreviation or "").upper().strip()
    if not opponent:
        return False
    raw_matchup = str(matchup or "").strip()
    if not raw_matchup:
        return False

    normalized_matchup = re.sub(r"\s+VS\.?\s+", " vs. ", raw_matchup, flags=re.IGNORECASE)
    normalized_matchup = re.sub(r"\s+@\s+", " @ ", normalized_matchup)
    parsed = parse_matchup_descriptor(normalized_matchup)
    parsed_opponent = str(parsed.get("opponent_abbreviation") or "").upper().strip()
    if parsed_opponent:
        return parsed_opponent == opponent

    tokens = [token for token in re.split(r"[^A-Z0-9]+", raw_matchup.upper()) if token]
    return opponent in tokens


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
        if h2h_only and not matchup_includes_opponent(row.get("MATCHUP"), opponent_abbreviation):
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
            chips.append(f'Margin  {margin_min:g}')
        else:
            chips.append(f'Margin  {margin_max:g}')
    if min_minutes is not None or max_minutes is not None:
        if min_minutes is not None and max_minutes is not None:
            chips.append(f'MIN {min_minutes:g}-{max_minutes:g}')
        elif min_minutes is not None:
            chips.append(f'MIN  {min_minutes:g}')
        else:
            chips.append(f'MIN  {max_minutes:g}')
    if min_fga is not None or max_fga is not None:
        if min_fga is not None and max_fga is not None:
            chips.append(f'FGA {min_fga:g}-{max_fga:g}')
        elif min_fga is not None:
            chips.append(f'FGA  {min_fga:g}')
        else:
            chips.append(f'FGA  {max_fga:g}')
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
        'label': '  '.join(chips) if chips else 'All games',
        'has_filters': bool(chips),
    }


TEAM_RECORDS_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
TEAM_RECORDS_CACHE_TTL_SECONDS = 6 * 60 * 60
TEAMMATE_ABSENCE_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
TEAMMATE_ABSENCE_CACHE_TTL_SECONDS = 6 * 60 * 60
TEAMMATE_IMPACT_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}
TEAMMATE_IMPACT_CACHE_TTL_SECONDS = 6 * 60 * 60


def get_cached_team_rank_map(season: str, season_type: str = DEFAULT_SEASON_TYPE) -> dict[int, int]:
    normalized_season_type = normalize_requested_season_type(season_type)
    cache_key = (str(season), str(normalized_season_type))
    cached = TEAM_RECORDS_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get('timestamp') or 0.0) < TEAM_RECORDS_CACHE_TTL_SECONDS:
        return dict(cached.get('rank_map') or {})
    return {}


def build_team_rank_map(season: str, season_type: str = DEFAULT_SEASON_TYPE) -> dict[int, int]:
    normalized_season_type = normalize_requested_season_type(season_type)
    cache_key = (str(season), str(normalized_season_type))
    cached = TEAM_RECORDS_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get('timestamp') or 0.0) < TEAM_RECORDS_CACHE_TTL_SECONDS:
        return dict(cached.get('rank_map') or {})

    rank_map: dict[int, int] = {}
    rows: list[dict[str, Any]] = []
    try:
        dash = call_nba_with_retries(
            lambda: LeagueDashTeamStats(
                season=season,
                season_type_all_star=normalized_season_type,
                per_mode_detailed="PerGame",
                timeout=12,
            ),
            label=f"league_dash_team_stats:{season}:{normalized_season_type}",
            attempts=2,
            base_delay=0.6,
        )
        frame = dash.get_data_frames()[0] if dash.get_data_frames() else None
        source_rows = frame.to_dict(orient="records") if frame is not None and not frame.empty else []
        for row in source_rows:
            team_id = row.get("TEAM_ID")
            try:
                team_id_int = int(team_id)
            except Exception:
                continue
            def_rating = row.get("DEF_RATING")
            opp_pts = row.get("OPP_PTS")
            metric = None
            try:
                metric = float(def_rating) if def_rating not in (None, "") else None
            except Exception:
                metric = None
            if metric is None:
                try:
                    metric = float(opp_pts) if opp_pts not in (None, "") else None
                except Exception:
                    metric = None
            if metric is None:
                continue
            rows.append({"team_id": team_id_int, "metric": metric})
        rows.sort(
            key=lambda item: (
                float(item.get("metric") or 0.0),
                TEAM_LOOKUP.get(int(item.get("team_id") or 0), {}).get("full_name", ""),
            )
        )
        rank_map = {int(item["team_id"]): idx + 1 for idx, item in enumerate(rows)}
    except Exception as exc:
        LOGGER.warning("Team rank map defensive stats fallback triggered: %s", exc)
        # Fallback keeps behavior available if LeagueDashTeamStats is temporarily unavailable.
        team_ids = [int(team.get('id') or 0) for team in TEAM_POOL if int(team.get('id') or 0)]
        def _fetch_team_record(team_id: int) -> dict[str, Any] | None:
            try:
                logs = fetch_team_game_log(team_id=team_id, season=season, season_type=normalized_season_type)
            except Exception:
                return None
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
            return {'team_id': team_id, 'wins': wins, 'losses': losses, 'win_pct': pct}
        if len(team_ids) <= 1:
            for team_id in team_ids:
                record = _fetch_team_record(team_id)
                if record:
                    rows.append(record)
        else:
            futures = [_submit_shared_network_task(_fetch_team_record, team_id) for team_id in team_ids]
            for future in as_completed(futures):
                record = future.result()
                if record:
                    rows.append(record)
        rows.sort(key=lambda item: (-float(item.get('win_pct') or 0.0), -int(item.get('wins') or 0), TEAM_LOOKUP.get(int(item.get('team_id') or 0), {}).get('full_name', '')))
        rank_map = {int(item['team_id']): idx + 1 for idx, item in enumerate(rows)}

    TEAM_RECORDS_CACHE[cache_key] = {'timestamp': now_ts, 'rank_map': rank_map}
    return dict(rank_map)


def teammate_absence_game_ids(player_id: int, season: str, season_type: str = DEFAULT_SEASON_TYPE) -> set[str]:
    normalized_season_type = normalize_requested_season_type(season_type)
    cache_key = (int(player_id), str(season), str(normalized_season_type))
    cached = TEAMMATE_ABSENCE_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get('timestamp') or 0.0) < TEAMMATE_ABSENCE_CACHE_TTL_SECONDS:
        return set(cached.get('game_ids') or [])
    try:
        rows = fetch_player_game_log(player_id=player_id, season=season, season_type=normalized_season_type)
    except Exception:
        rows = []
    game_ids = {str(row.get('GAME_ID') or row.get('Game_ID') or '').strip() for row in rows if str(row.get('GAME_ID') or row.get('Game_ID') or '').strip()}
    TEAMMATE_ABSENCE_CACHE[cache_key] = {'timestamp': now_ts, 'game_ids': sorted(game_ids)}
    return set(game_ids)


def teammate_impact_score(player_id: int, season: str, season_type: str = DEFAULT_SEASON_TYPE) -> float:
    normalized_season_type = normalize_requested_season_type(season_type)
    cache_key = (int(player_id), str(season), str(normalized_season_type))
    cached = TEAMMATE_IMPACT_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get('timestamp') or 0.0) < TEAMMATE_IMPACT_CACHE_TTL_SECONDS:
        return float(cached.get('score') or 0.0)

    score = 0.0
    try:
        rows = fetch_recent_player_game_log(
            player_id=player_id,
            season=season,
            season_type=normalized_season_type,
            last_n=10,
        )
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


def build_without_player_union_game_ids(player_ids: list[int], season: str, season_type: str = DEFAULT_SEASON_TYPE) -> set[str] | None:
    normalized_ids = normalize_without_player_ids(player_ids)
    if not normalized_ids:
        return None
    game_ids: set[str] = set()
    if len(normalized_ids) == 1:
        game_ids.update(teammate_absence_game_ids(int(normalized_ids[0]), season=season, season_type=season_type))
        return game_ids
    futures = [
        _submit_shared_network_task(teammate_absence_game_ids, int(teammate_id), season, season_type)
        for teammate_id in normalized_ids
    ]
    for future in as_completed(futures):
        try:
            game_ids.update(future.result() or set())
        except Exception:
            pass
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
    short_rows = recent_rows[: max(1, min(5, len(recent_rows)))]
    older_rows = recent_rows[5:10] if len(recent_rows) >= 10 else recent_rows[len(short_rows):]

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
        'summary': f'{minutes_label}  {mins_last5:.1f} MIN lately. {volume_label}  {fga_last5:.1f} FGA lately.',
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
    payload = get_cached_injury_report_payload()
    team_name = canonicalize_team_name(team_name, team_id=team_id)
    report_label = str(payload.get('report_label') or '')
    cache_key = (
        normalize_compact_text(team_name),
        normalize_report_person_name(player_name),
        stat,
        str(player_position or ''),
        int(team_id or 0),
        str(season or ''),
        report_label,
    )
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

    rows = INJURY_SERVICE.get_team_rows(payload, team_name)
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
        angle = 'Same-team absences can change the players role and path to the line.'

    headline_parts = []
    if out_count:
        headline_parts.append(f'{out_count} out')
    if risky_count:
        headline_parts.append(f'{risky_count} questionable/doubtful')
    headline = '  '.join(headline_parts)

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
        'team_name': team_name,
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


def _coerce_cache_timestamp(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _cache_timestamp_is_fresh(timestamp: Any, ttl_seconds: float) -> bool:
    cache_ts = _coerce_cache_timestamp(timestamp)
    return bool(cache_ts and (time.time() - cache_ts) < ttl_seconds)


def _store_game_log_cache_rows(
    cache_key: tuple[Any, ...],
    rows: list[dict[str, Any]],
    *,
    timestamp: Any,
    source: str,
) -> tuple[list[dict[str, Any]], float]:
    cached_ts = _coerce_cache_timestamp(timestamp)
    normalized_rows = dedupe_game_log_rows(rows or [])
    GAME_LOG_CACHE[cache_key] = {
        "timestamp": cached_ts,
        "rows": normalized_rows,
        "source": source,
    }
    return normalized_rows, cached_ts


def _get_cached_game_log_rows(cache_key: tuple[Any, ...], *, fresh_only: bool = False) -> list[dict[str, Any]] | None:
    cached = GAME_LOG_CACHE.get(cache_key)
    if not cached or not isinstance(cached.get("rows"), list) or not cached.get("rows"):
        return None
    cached_ts = _coerce_cache_timestamp(cached.get("timestamp"))
    if fresh_only:
        if not _cache_timestamp_is_fresh(cached_ts, CACHE_TTL_SECONDS):
            return None
    elif not _game_log_cache_is_reliable_stale(cached, cached_ts):
        return None
    return cached.get("rows") or []


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
    # Include per-row stat content in the cache key so summaries do not collide
    # across different players who share the same game IDs/dates.
    row_signature = tuple(
        (
            str(row.get("Game_ID") or row.get("GAME_ID") or ""),
            str(row.get("GAME_DATE") or ""),
            str(row.get("MATCHUP") or ""),
            round(safe_stat_number(row, "PTS"), 3),
            round(safe_stat_number(row, "REB"), 3),
            round(safe_stat_number(row, "AST"), 3),
            round(safe_stat_number(row, "FGA"), 3),
            round(safe_stat_number(row, "FG3A"), 3),
            round(safe_stat_number(row, "FTA"), 3),
            str(row.get("MIN") or "").strip(),
        )
        for row in rows
    )
    cache_key = (row_signature, str(stat).upper().strip(), float(line))
    cached = STAT_SUMMARY_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < STAT_SUMMARY_CACHE_TTL_SECONDS:
        return copy.deepcopy(cached["payload"])

    games: list[dict[str, Any]] = []
    hit_count = 0
    values: list[float] = []
    weighted_hits = 0.0
    weight_sum = 0.0
    now_dt = _utc_now_naive()
    for row in rows:
        game_entry = build_game_log_entry(row, stat, line)
        games.append(game_entry)
        values.append(game_entry["value"])
        if game_entry["hit"]:
            hit_count += 1
        game_dt = parse_game_date_any(game_entry.get("game_date"))
        if game_dt is None:
            game_dt = parse_game_date_any(row.get("GAME_DATE"))
        days_ago = (now_dt - game_dt).days if game_dt is not None else None
        weight = 1.0 if days_ago is None else pow(0.92, (days_ago / 7.0))
        weight_sum += weight
        weighted_hits += weight * (1.0 if game_entry["hit"] else 0.0)

    avg = round(sum(values) / len(values), 2) if values else 0
    raw_hit_rate = round((hit_count / len(values)) * 100, 1) if values else 0
    weighted_hit_rate = round((weighted_hits / weight_sum) * 100, 1) if weight_sum > 0 else raw_hit_rate
    hit_rate = weighted_hit_rate

    # Variance / distribution metrics
    variance_metrics: dict[str, Any] = {}
    if len(values) >= 3:
        import statistics as _stats
        std_dev = round(_stats.stdev(values), 2)
        median = round(_stats.median(values), 2)
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        if n >= 4:
            _quartiles = _stats.quantiles(values, n=4)
            p25 = round(_quartiles[0], 2)
            p75 = round(_quartiles[2], 2)
        else:
            p25 = round(sorted_vals[0], 2)
            p75 = round(sorted_vals[-1], 2)
        # Consistency score: 0-100, higher = more consistent
        # Use a floor for low-volume stats so BLK/STL/3PM aren't unfairly penalized
        # when the raw average is near zero (e.g. avg=0.3 blks  CV explodes otherwise).
        _LOW_VOL_STATS_CV = {"BLK", "STL", "3PM"}
        _cv_floor = 0.5 if str(stat or "").upper() in _LOW_VOL_STATS_CV else 0.1
        _cv_avg = max(float(avg), _cv_floor)
        cv = round(std_dev / _cv_avg, 3)
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
        "hit_rate_raw": raw_hit_rate,
        "hit_rate_weighted": weighted_hit_rate,
        "variance": variance_metrics,
    }
    STAT_SUMMARY_CACHE[cache_key] = {"timestamp": now_ts, "payload": copy.deepcopy(payload)}
    return payload


def build_debug_metadata(*, cache_status: dict[str, Any], freshness: dict[str, Any], timings_enabled: bool) -> dict[str, Any]:
    return {
        "cache_status": copy.deepcopy(cache_status),
        "freshness": copy.deepcopy(freshness),
        "timing_enabled": bool(timings_enabled),
        "generated_at": _utc_iso_z(),
    }


def mask_api_key_for_display(api_key: str) -> str:
    raw = str(api_key or "").strip()
    if len(raw) <= 8:
        return raw
    return f"{raw[:4]}{raw[-4:]}"


def normalize_odds_api_key(api_key: Any) -> str:
    raw = str(api_key or "").strip()
    if not raw:
        return ""

    key_text = raw.strip().strip(",;")
    if key_text.lower().startswith("bearer "):
        key_text = key_text[7:].strip()

    env_match = re.match(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_-]*)\s*[:=]\s*(.+)$", key_text)
    if env_match:
        lhs, rhs = env_match.group(1), env_match.group(2)
        if "KEY" in lhs.upper():
            key_text = rhs.strip()

    query_source = key_text
    if "?" in key_text or "://" in key_text:
        try:
            parsed = urlparse(key_text)
            query_source = parsed.query or key_text
        except Exception:
            query_source = key_text
    parsed_query = parse_qs(query_source, keep_blank_values=True)
    for field_name in ("apiKey", "apikey", "api_key"):
        values = parsed_query.get(field_name) or []
        if values:
            key_text = str(values[-1] or "").strip()
            break
    else:
        query_match = re.search(r"(?i)(?:^|[?&,\s;])(api[_-]?key|apikey)\s*=\s*([^&#,\s;]+)", key_text)
        if query_match:
            key_text = str(query_match.group(2) or "").strip()

    while len(key_text) >= 2 and key_text[0] == key_text[-1] and key_text[0] in {"'", '"', "`"}:
        key_text = key_text[1:-1].strip()

    return key_text.strip().strip(",;")


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


def odds_api_fetch(
    endpoint: str,
    api_key: str,
    params: dict[str, Any] | None = None,
    *,
    allow_query_auth_fallback: bool | None = None,
) -> dict[str, Any]:
    key = normalize_odds_api_key(api_key)
    if not key:
        raise HTTPException(status_code=400, detail="Missing Odds API key.")

    query = dict(params or {})

    def _request_with(current_query: dict[str, Any], headers: dict[str, str]) -> requests.Response:
        url = f"{ODDS_API_BASE_URL}{endpoint}?{odds_api_build_query(current_query)}"
        attempts = 0
        last_exc: Exception | None = None
        response: requests.Response | None = None
        while True:
            try:
                response = requests.get(url, timeout=(5, 30), headers=headers)
                if response.ok:
                    break
                if response.status_code in {408, 429} or response.status_code >= 500:
                    attempts += 1
                    if attempts > ODDS_API_MAX_RETRIES:
                        break
                    time.sleep(ODDS_API_RETRY_BACKOFF_SECONDS * attempts)
                    continue
                break
            except requests.RequestException as exc:
                last_exc = exc
                attempts += 1
                if attempts > ODDS_API_MAX_RETRIES:
                    break
                time.sleep(ODDS_API_RETRY_BACKOFF_SECONDS * attempts)
        if response is None:
            raise HTTPException(status_code=503, detail=f"Odds API request failed: {last_exc}") from last_exc
        return response

    header_auth_headers = {
        "User-Agent": NBA_USER_AGENT,
        "Accept": "application/json",
        "X-API-Key": key,
    }
    fallback_enabled = ODDS_API_QUERY_AUTH_FALLBACK_ENABLED if allow_query_auth_fallback is None else bool(allow_query_auth_fallback)
    response = _request_with(query, header_auth_headers)
    response_text = (response.text or "").strip()
    missing_key_error = "MISSING_KEY" in response_text or "API key is missing" in response_text
    should_try_query_fallback = (
        (fallback_enabled and response.status_code in {400, 401, 403})
        or missing_key_error
    )
    if should_try_query_fallback and "apiKey" not in query:
        # Optional compatibility fallback for providers that reject header auth.
        fallback_query = dict(query)
        fallback_query["apiKey"] = key
        response = _request_with(
            fallback_query,
            {"User-Agent": NBA_USER_AGENT, "Accept": "application/json"},
        )

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

    payload = {
        "data": data,
        "quota": {"remaining": remaining, "used": used, "last": last},
    }
    safe_params = dict(params or {})
    _submit_pg_write(_pg_write_odds_snapshot, endpoint, safe_params, payload)
    return payload


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
    normalized_season_type = normalize_requested_season_type(season_type)
    cache_key = (player_id, season, normalized_season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
    fresh_cached_rows = _get_cached_game_log_rows(cache_key, fresh_only=True)
    if fresh_cached_rows is not None:
        return fresh_cached_rows

    season_parts = season_types_for_analysis(normalized_season_type)

    def _read_postgres_rows(pg_season_type: str, target_cache_key: tuple[Any, ...]) -> tuple[list[dict[str, Any]] | None, float]:
        pg_rows, pg_ts = _pg_read_game_log(player_id, season, pg_season_type)
        if isinstance(pg_rows, list) and pg_rows:
            return _store_game_log_cache_rows(
                target_cache_key,
                pg_rows,
                timestamp=pg_ts,
                source="postgres",
            )
        return None, 0.0

    def _write_live_rows_if_fresh(pg_season_type: str, rows: list[dict[str, Any]]) -> None:
        source_cache_key = (player_id, season, pg_season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
        source_cached = GAME_LOG_CACHE.get(source_cache_key)
        source_ts = _coerce_cache_timestamp((source_cached or {}).get("timestamp"))
        if _cache_timestamp_is_fresh(source_ts, CACHE_TTL_SECONDS):
            _submit_pg_write(_pg_write_game_log, player_id, season, pg_season_type, GAME_LOG_CACHE_SCHEMA_VERSION, rows)

    if len(season_parts) > 1:
        pg_rows, pg_ts = _read_postgres_rows(normalized_season_type, cache_key)
        if pg_rows is not None and _cache_timestamp_is_fresh(pg_ts, CACHE_TTL_SECONDS):
            return pg_rows

        merged_rows: list[dict[str, Any]] = []
        part_cache_timestamps: list[float] = []
        try:
            for part in season_parts:
                try:
                    part_rows = fetch_player_game_log(player_id=player_id, season=season, season_type=part)
                    merged_rows.extend(part_rows)
                    part_cache = GAME_LOG_CACHE.get((player_id, season, part, GAME_LOG_CACHE_SCHEMA_VERSION))
                    part_cache_timestamps.append(_coerce_cache_timestamp((part_cache or {}).get("timestamp")))
                except HTTPException as exc:
                    if exc.status_code != 404:
                        raise
        except Exception:
            reliable_stale_rows = _get_cached_game_log_rows(cache_key, fresh_only=False)
            if reliable_stale_rows is not None:
                return reliable_stale_rows
            raise

        merged_rows = merge_game_log_rows(merged_rows)
        if not merged_rows:
            reliable_stale_rows = _get_cached_game_log_rows(cache_key, fresh_only=False)
            if reliable_stale_rows is not None:
                return reliable_stale_rows
            raise HTTPException(status_code=404, detail="No game logs found for this player and season.")
        merged_ts = min([ts for ts in part_cache_timestamps if ts] or [time.time()])
        all_parts_fresh = bool(part_cache_timestamps) and all(
            _cache_timestamp_is_fresh(ts, CACHE_TTL_SECONDS) for ts in part_cache_timestamps
        )
        _store_game_log_cache_rows(
            cache_key,
            merged_rows,
            timestamp=time.time() if all_parts_fresh else merged_ts,
            source="live-merged" if all_parts_fresh else "stale-merged",
        )
        if all_parts_fresh:
            _submit_pg_write(_pg_write_game_log, player_id, season, normalized_season_type, GAME_LOG_CACHE_SCHEMA_VERSION, merged_rows)
        return merged_rows

    source_season_type = season_parts[0]
    source_cache_key = (player_id, season, source_season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
    if source_cache_key != cache_key:
        fresh_source_rows = _get_cached_game_log_rows(source_cache_key, fresh_only=True)
        if fresh_source_rows is not None:
            return fresh_source_rows

    pg_rows, pg_ts = _read_postgres_rows(source_season_type, source_cache_key)
    if pg_rows is not None and _cache_timestamp_is_fresh(pg_ts, CACHE_TTL_SECONDS):
        return pg_rows

    try:
        rows = PLAYER_DATA_SERVICE.fetch_player_game_log(player_id, season, source_season_type)
    except Exception:
        reliable_stale_rows = _get_cached_game_log_rows(source_cache_key, fresh_only=False)
        if reliable_stale_rows is not None:
            return reliable_stale_rows
        raise

    _write_live_rows_if_fresh(source_season_type, rows)
    return rows


def fetch_recent_player_game_log(player_id: int, season: str, season_type: str, last_n: int) -> list[dict[str, Any]]:
    normalized_season_type = normalize_requested_season_type(season_type)
    cache_key = (player_id, season, normalized_season_type, int(last_n), GAME_LOG_CACHE_SCHEMA_VERSION)
    cached = RECENT_GAME_LOG_CACHE.get(cache_key)
    now_ts = time.time()
    if cached and now_ts - float(cached.get("timestamp") or 0.0) < CACHE_TTL_SECONDS:
        return cached.get("rows") or []
    full_rows = fetch_player_game_log(player_id=player_id, season=season, season_type=normalized_season_type)
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
    return PLAYER_DATA_SERVICE.fetch_team_roster(team_id, season)


@timed_call("fetch_common_player_info")
def fetch_common_player_info(player_id: int) -> dict[str, Any]:
    cached = PLAYER_INFO_CACHE.get(player_id)
    if cached and isinstance(cached.get("row"), dict):
        cached_ts = float(cached.get("timestamp") or 0.0)
        if cached_ts and (time.time() - cached_ts) < PROFILE_TTL_SECONDS:
            return cached["row"]
    if POSTGRES_SOURCE_OF_TRUTH:
        pg_row, pg_ts = _pg_read_player_info(player_id)
        if pg_row and isinstance(pg_row, dict):
            PLAYER_INFO_CACHE[player_id] = {"timestamp": float(pg_ts or time.time()), "row": pg_row, "source": "postgres"}
            return pg_row
        if cached and isinstance(cached.get("row"), dict):
            return cached["row"]
    else:
        if cached and isinstance(cached.get("row"), dict):
            return cached["row"]
        pg_row, pg_ts = _pg_read_player_info(player_id)
        if pg_row and isinstance(pg_row, dict):
            PLAYER_INFO_CACHE[player_id] = {"timestamp": float(pg_ts or time.time()), "row": pg_row, "source": "postgres"}
            return pg_row
    row = PLAYER_DATA_SERVICE.fetch_common_player_info(player_id)
    _submit_pg_write(_pg_write_player_info, player_id, row)
    return row


def fetch_next_game(player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
    normalized_season_type = normalize_requested_season_type(season_type)
    season_parts = season_types_for_analysis(normalized_season_type)
    if len(season_parts) == 1:
        return PLAYER_DATA_SERVICE.fetch_next_game(player_id, season, season_parts[0])
    next_games: list[dict[str, Any]] = []
    for part in season_parts:
        row = PLAYER_DATA_SERVICE.fetch_next_game(player_id, season, part)
        if isinstance(row, dict) and row:
            next_games.append(row)
    if not next_games:
        return None

    def _next_game_sort_key(row: dict[str, Any]) -> tuple[datetime, str]:
        game_dt = parse_game_date_any(row.get("GAME_DATE")) or datetime.max
        game_time = str(row.get("GAME_TIME") or "")
        return game_dt, game_time

    next_games.sort(key=_next_game_sort_key)
    return dict(next_games[0])



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


def _extract_series_context_from_scoreboard_row(row: dict[str, Any]) -> tuple[str | None, int | None]:
    return SCHEDULE_SERVICE._extract_playoff_series_context(row)


def enrich_next_game_with_series_context(next_game: dict[str, Any] | None, player_team_id: int | None) -> dict[str, Any] | None:
    if not isinstance(next_game, dict):
        return next_game
    if next_game.get("series_summary") and next_game.get("playoff_game_number"):
        return next_game
    team_id = int(player_team_id or 0)
    opp_id = int(next_game.get("opponent_team_id") or 0)
    if not team_id:
        return next_game
    probe_date = str(next_game.get("game_date") or current_nba_game_date()).strip() or current_nba_game_date()
    rows = fetch_scoreboard_games(probe_date)
    if not rows:
        return next_game
    for row in rows:
        home_id = int(row.get("HOME_TEAM_ID") or 0)
        away_id = int(row.get("VISITOR_TEAM_ID") or 0)
        if team_id not in {home_id, away_id}:
            continue
        if opp_id and opp_id not in {home_id, away_id}:
            continue
        series_text, playoff_game_number = _extract_series_context_from_scoreboard_row(row)
        if not series_text and not playoff_game_number:
            return next_game
        enriched = dict(next_game)
        if series_text:
            enriched["series_text"] = series_text
            enriched["series_summary"] = series_text
        if playoff_game_number is not None:
            enriched["playoff_game_number"] = playoff_game_number
        return enriched
    return next_game


def build_scoreboard_next_game_payload(game_row: dict[str, Any], player_team_id: int | None) -> dict[str, Any] | None:
    return SCHEDULE_SERVICE.build_scoreboard_next_game_payload(game_row, player_team_id)


def find_team_next_game_via_scoreboard(team_id: int | None, lookahead_days: int = 10) -> dict[str, Any] | None:
    return SCHEDULE_SERVICE.find_team_next_game_via_scoreboard(team_id, lookahead_days=lookahead_days)


def build_next_game_payload(next_game_row: dict[str, Any] | None, player_team_id: int | None) -> dict[str, Any] | None:
    return SCHEDULE_SERVICE.build_next_game_payload(next_game_row, player_team_id)


def resolve_team_next_game(team_id: int | None, primary_player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
    def _is_next_game_stale(row: dict[str, Any] | None) -> bool:
        if not row:
            return False
        raw_date = row.get("game_date") or row.get("GAME_DATE") or row.get("date") or row.get("GAME_DATE_EST")
        dt = parse_game_date_any(raw_date)
        if not dt:
            return False
        return dt.date() < nba_now().date()

    if team_id:
        cache_key = (team_id, season, season_type)
        cached = TEAM_NEXT_GAME_CACHE.get(cache_key)
        cached_row = cached.get("row") if cached else None
        cached_ts = float((cached or {}).get("timestamp") or 0.0)
        if cached_row and isinstance(cached_row, dict) and not _is_next_game_stale(cached_row):
            if cached_ts and (time.time() - cached_ts) < NEXT_GAME_TTL_SECONDS:
                return cached_row
        if POSTGRES_SOURCE_OF_TRUTH:
            pg_row, pg_ts = _pg_read_team_next_game(int(team_id), season, season_type)
            if pg_row and isinstance(pg_row, dict) and not _is_next_game_stale(pg_row):
                TEAM_NEXT_GAME_CACHE[cache_key] = {"timestamp": float(pg_ts or time.time()), "row": pg_row, "source": "postgres"}
                return pg_row
            if cached_row and isinstance(cached_row, dict) and not _is_next_game_stale(cached_row):
                return cached_row
        else:
            if cached_row and isinstance(cached_row, dict) and not _is_next_game_stale(cached_row):
                return cached_row
            pg_row, pg_ts = _pg_read_team_next_game(int(team_id), season, season_type)
            if pg_row and isinstance(pg_row, dict) and not _is_next_game_stale(pg_row):
                TEAM_NEXT_GAME_CACHE[cache_key] = {"timestamp": float(pg_ts or time.time()), "row": pg_row, "source": "postgres"}
                return pg_row
    row = SCHEDULE_SERVICE.resolve_team_next_game(team_id, primary_player_id, season, season_type)
    if _is_next_game_stale(row) and team_id:
        fallback_row = find_team_next_game_via_scoreboard(team_id, lookahead_days=10)
        fallback_payload = build_next_game_payload(fallback_row, team_id)
        if fallback_payload:
            row = fallback_payload
    if team_id:
        _submit_pg_write(_pg_write_team_next_game, team_id, season, season_type, row)
    return row


def _rate_limit_identity(request: Request) -> str:
    forwarded_for = str(request.headers.get("x-forwarded-for") or "").strip()
    if forwarded_for:
        first = forwarded_for.split(",")[0].strip()
        if first:
            return first
    if request.client and request.client.host:
        return str(request.client.host)
    return "unknown"


def enforce_rate_limit(request: Request, scope: str, *, max_requests: int, window_seconds: int | None = None) -> None:
    if not RATE_LIMIT_ENABLED:
        return
    window = max(1, int(window_seconds or RATE_LIMIT_WINDOW_SECONDS))
    identity = _rate_limit_identity(request)
    key = f"{scope}:{identity}"
    now_ts = time.time()
    with _RATE_LIMIT_LOCK:
        timestamps = _RATE_LIMIT_BUCKETS.get(key) or []
        threshold = now_ts - window
        timestamps = [ts for ts in timestamps if ts >= threshold]
        if len(timestamps) >= max_requests:
            retry_after = max(1, int(window - (now_ts - min(timestamps))))
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded for {scope}. Retry in {retry_after}s.",
                headers={"Retry-After": str(retry_after)},
            )
        timestamps.append(now_ts)
        _RATE_LIMIT_BUCKETS[key] = timestamps


def enforce_heavy_rate_limit(request: Request, scope: str) -> None:
    enforce_rate_limit(
        request,
        scope,
        max_requests=RATE_LIMIT_HEAVY_MAX_REQUESTS,
        window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    )


def enforce_read_rate_limit(request: Request, scope: str) -> None:
    enforce_rate_limit(
        request,
        scope,
        max_requests=RATE_LIMIT_READ_MAX_REQUESTS,
        window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    )


@app.api_route("/", methods=["GET", "HEAD"])
def root() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.api_route("/health", methods=["GET", "HEAD"])
def health() -> JSONResponse:
    return JSONResponse({"status": "ok"})


@app.get("/api/teams")
def get_teams(request: Request) -> dict[str, Any]:
    enforce_read_rate_limit(request, "teams")
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
def get_team_roster(request: Request, team_id: int, season: str | None = None) -> dict[str, Any]:
    enforce_read_rate_limit(request, "team_roster")
    selected_season = season or current_nba_season()
    team = TEAM_LOOKUP.get(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")

    rows = fetch_team_roster(team_id=team_id, season=selected_season)

    # Fetch injury data for this specific team only  filter by team_name so
    # players with the same normalized name on different teams don't bleed across.
    team_name = str(team.get("full_name") or "")
    report_payload = get_cached_injury_report_payload()
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
def get_team_injury_report(request: Request, team_id: int) -> dict[str, Any]:
    enforce_read_rate_limit(request, "team_injury_report")
    """Return injury report rows for a specific team, from the latest official NBA PDF."""
    team = TEAM_LOOKUP.get(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")

    team_name = str(team.get("full_name") or "")
    summary = build_team_availability_summary(team_name)
    payload = get_cached_injury_report_payload()

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
def search_players(request: Request, q: str = Query(..., min_length=1, max_length=50)) -> dict[str, Any]:
    enforce_read_rate_limit(request, "player_search")
    return {"results": PLAYER_SEARCH_INDEX.search(q, limit=15)}




@app.get("/api/bet-finder")
def bet_finder(
    team_id: int,
    stat: str = Query(..., pattern="^(PTS|REB|AST|3PM|STL|BLK|PRA|PR|PA|RA)$"),
    line: float = Query(..., ge=0),
    last_n: int = Query(10, ge=3, le=30),
    season: str | None = None,
    season_type: str = Query(DEFAULT_SEASON_TYPE),
    min_games: int = Query(5, ge=1, le=30),
    limit: int = Query(8, ge=1, le=20),
) -> dict[str, Any]:
    selected_season = season or current_nba_season()
    season_type = normalize_requested_season_type(season_type)
    stat = stat.upper()

    team = TEAM_LOOKUP.get(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found.")

    roster_rows = fetch_team_roster(team_id=team_id, season=selected_season)
    results: list[dict[str, Any]] = []

    def _analyze_row(row: dict[str, Any]) -> dict[str, Any] | None:
        raw_player_id = row.get("PLAYER_ID")
        if raw_player_id in (None, ""):
            return None

        player_id = int(raw_player_id)
        analysis_payload: dict[str, Any] | None = None
        try:
            analysis_payload = build_prop_analysis_payload(
                player_id=player_id,
                stat=stat,
                line=line,
                last_n=last_n,
                season=selected_season,
                season_type=season_type,
                team_id=int(team["id"]),
                player_position=str(row.get("POSITION", "")).strip() or None,
            )
        except Exception:
            analysis_payload = None

        if isinstance(analysis_payload, dict) and int(analysis_payload.get("games_count") or 0) >= min_games:
            games_count = int(analysis_payload.get("games_count") or 0)
            over_hit_count = max(0, min(games_count, int(analysis_payload.get("hit_count") or 0)))
            average = round(float(analysis_payload.get("average") or 0.0), 2)
            side = str(analysis_payload.get("recommended_side") or ("OVER" if average >= line else "UNDER")).upper()
            if side not in {"OVER", "UNDER"}:
                side = "OVER" if average >= line else "UNDER"
            side_hit_count = over_hit_count if side == "OVER" else max(0, games_count - over_hit_count)
            side_hit_rate = round(side_hit_rate_from_over_hit_rate(analysis_payload.get("hit_rate"), side), 1)
            avg_edge = round((average - line) if side == "OVER" else (line - average), 2)
            analysis_games = list(analysis_payload.get("games") or [])
            recent_game = analysis_games[-1] if analysis_games else {}
            last_value = safe_float_or_none((recent_game or {}).get("value"))
            if last_value is None and analysis_games:
                last_value = safe_float_or_none((analysis_games[0] or {}).get("value"))
            h2h_payload = analysis_payload.get("h2h") or {}
            h2h_games_count = int(h2h_payload.get("games_count") or 0)
            h2h_side_hit_count = analysis_payload.get("h2h_side_hit_count")
            h2h_side_hit_rate = analysis_payload.get("h2h_side_hit_rate")

            return {
                "player": {
                    "id": player_id,
                    "full_name": str(row.get("PLAYER", "")).strip() or str((analysis_payload.get("player") or {}).get("full_name") or ""),
                    "team_id": team["id"],
                    "team_name": team["full_name"],
                    "team_abbreviation": team["abbreviation"],
                    "position": str(row.get("POSITION", "")).strip() or str((analysis_payload.get("player") or {}).get("position") or ""),
                    "jersey": str(row.get("NUM", "")).strip() or str((analysis_payload.get("player") or {}).get("jersey") or ""),
                    "is_active": True,
                },
                "stat": stat,
                "line": line,
                "side": side,
                "games_count": games_count,
                "hit_count": side_hit_count,
                "hit_rate": side_hit_rate,
                "over_hit_count": over_hit_count,
                "over_hit_rate": round(float(analysis_payload.get("hit_rate") or 0.0), 1),
                "average": average,
                "avg_edge": avg_edge,
                "last_value": round(float(last_value or 0.0), 1),
                "hit_streak": compute_recent_hit_streak([bool(game.get("hit")) for game in analysis_games]),
                "season_type": season_type,
                "ranking_source": analysis_payload.get("ranking_source"),
                "ranking_hit_rate": analysis_payload.get("ranking_hit_rate"),
                "ranking_sort_score": analysis_payload.get("ranking_sort_score"),
                "ranking_blend_score": analysis_payload.get("ranking_blend_score"),
                "ranking_profile": analysis_payload.get("ranking_profile") or {},
                "playoff_strategy_tips": analysis_payload.get("playoff_strategy_tips") or [],
                "h2h_weight_pct": analysis_payload.get("h2h_weight_pct"),
                "h2h_games_count": h2h_games_count,
                "h2h_hit_count": h2h_side_hit_count,
                "h2h_hit_rate": h2h_side_hit_rate,
                "confidence": analysis_payload.get("confidence") or {},
                "edge": analysis_payload.get("edge"),
                "ev": analysis_payload.get("ev"),
                "model_probability": (analysis_payload.get("confidence") or {}).get("model_probability"),
                "matchup": analysis_payload.get("matchup") or {},
                "opportunity": analysis_payload.get("opportunity") or {},
                "team_context": analysis_payload.get("team_context") or {},
                "environment": analysis_payload.get("environment") or {},
            }

        try:
            game_rows = fetch_recent_player_game_log(
                player_id=player_id,
                season=selected_season,
                season_type=season_type,
                last_n=last_n,
            )
        except HTTPException:
            return None

        sample_rows = game_rows
        if len(sample_rows) < min_games:
            return None

        values: list[float] = []
        hit_flags: list[bool] = []
        for game_row in sample_rows:
            value = round(compute_stat_value(game_row, stat), 1)
            hit = value >= line
            values.append(value)
            hit_flags.append(hit)

        if not values:
            return None

        hit_count = sum(1 for hit in hit_flags if hit)
        games_count = len(values)
        hit_rate = round((hit_count / games_count) * 100, 1)
        average = round(sum(values) / games_count, 2)
        avg_edge = round(average - line, 2)
        hit_streak = compute_recent_hit_streak(hit_flags)
        last_value = values[0]

        return {
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
            "side": "OVER",
            "games_count": games_count,
            "hit_count": hit_count,
            "hit_rate": hit_rate,
            "over_hit_count": hit_count,
            "over_hit_rate": hit_rate,
            "average": average,
            "avg_edge": avg_edge,
            "last_value": last_value,
            "hit_streak": hit_streak,
            "season_type": season_type,
            "ranking_source": "recent",
            "ranking_hit_rate": hit_rate,
            "ranking_sort_score": hit_rate,
        }

    if not roster_rows:
        results = []
    elif len(roster_rows) == 1:
        candidate = _analyze_row(roster_rows[0])
        if candidate:
            results.append(candidate)
    else:
        futures = [_submit_shared_analysis_task(_analyze_row, row) for row in roster_rows]
        for future in as_completed(futures):
            try:
                candidate = future.result()
            except Exception:
                candidate = None
            if candidate:
                results.append(candidate)

    results.sort(
        key=lambda item: (
            item.get("ranking_sort_score", item["hit_rate"]),
            item["avg_edge"],
            item["hit_count"],
            item["average"],
        ),
        reverse=True,
    )

    payload_out = {
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
    payload_store = {
        "team_id": team["id"],
        "team_name": team["full_name"],
        "team_abbreviation": team["abbreviation"],
        "season": selected_season,
        "season_type": season_type,
        "stat": stat,
        "line": line,
        "last_n": last_n,
        "min_games": min_games,
        "limit": limit,
        "results": results[:limit],
    }
    _submit_pg_write(_pg_write_bet_finder_run, payload_store)
    return payload_out


MARKET_SCAN_SERVICE = MarketScanService(
    submit_async_job=submit_async_job,
)
PARLAY_SERVICE = ParlayService(
    submit_async_job=submit_async_job,
)
PROGRESS_STREAM_SERVICE = ProgressStreamService()
MARKET_SCAN_CORE_SERVICE = MarketScanCoreService(
    current_nba_season=current_nba_season,
    normalize_requested_season_type=normalize_requested_season_type,
    request_hash=_request_hash,
    read_market_scan_cache=lambda request_payload: _pg_read_market_scan_cache(request_payload, cache_scope="market_scan"),
    warm_injury_cache=get_cached_injury_report_payload,
    resolve_team_from_text=resolve_team_from_text,
    find_player_by_name=find_player_by_name,
    team_lookup=TEAM_LOOKUP,
    stat_map=STAT_MAP,
    bulk_analysis_max_workers=lambda: BULK_ANALYSIS_MAX_WORKERS,
    prefetch_bulk_analysis_context=lambda **kwargs: prefetch_bulk_analysis_context(**kwargs),
    submit_analysis_task=lambda func, *args, **kwargs: _submit_shared_analysis_task(func, *args, **kwargs),
    http_exception_cls=HTTPException,
    max_rows=MARKET_SCAN_MAX_ROWS,
    max_last_n=PROP_ANALYSIS_MAX_LAST_N,
)


def _emit_progress(progress_cb, stage: str, **extra: Any) -> None:
    PROGRESS_STREAM_SERVICE.emit_progress(progress_cb, stage, **extra)


def _stream_with_progress(run_func, payload: dict[str, Any]) -> StreamingResponse:
    return PROGRESS_STREAM_SERVICE.stream_with_progress(run_func, payload)


def _apply_parlay_event_context_to_bulk_row(
    bulk_row: dict[str, Any],
    orig_row: dict[str, Any],
) -> dict[str, Any]:
    updated = dict(bulk_row or {})
    player_id = int(updated.get("player_id") or 0)
    player_team_id = _resolve_team_id_for_player(player_id) if player_id else 0
    if player_team_id:
        updated["team_id"] = player_team_id

    event_home = resolve_team_from_text(str(orig_row.get("home_team") or updated.get("home_team") or "").strip())
    event_away = resolve_team_from_text(str(orig_row.get("away_team") or updated.get("away_team") or "").strip())
    if not (event_home and event_away and player_team_id):
        return updated

    home_id = int(event_home.get("id") or 0)
    away_id = int(event_away.get("id") or 0)
    home_abbr = str(event_home.get("abbreviation") or "").strip()
    away_abbr = str(event_away.get("abbreviation") or "").strip()
    if home_id == player_team_id and away_id:
        updated["team_id"] = home_id
        updated["override_opponent_id"] = away_id
        if home_abbr and away_abbr:
            updated["game_label"] = f"{home_abbr} vs {away_abbr}"
    elif away_id == player_team_id and home_id:
        updated["team_id"] = away_id
        updated["override_opponent_id"] = home_id
        if away_abbr and home_abbr:
            updated["game_label"] = f"{away_abbr} @ {home_abbr}"
    return updated


def _market_scan_core(payload: dict[str, Any], progress_cb=None) -> dict[str, Any]:
    prepared_payload = MARKET_SCAN_CORE_SERVICE.prepare_request(payload)
    cached_run = prepared_payload["cached_run"]
    if cached_run:
        return cached_run

    rows = prepared_payload["rows"]
    default_last_n = int(prepared_payload["default_last_n"])
    selected_season = str(prepared_payload["selected_season"])
    season_type = str(prepared_payload["season_type"])
    injury_aware = bool(prepared_payload["injury_aware"])
    request_hash_value = str(prepared_payload["request_hash_value"])
    errors: list[dict[str, Any]] = list(prepared_payload["errors"] or [])
    prepared_rows: list[tuple[int, dict[str, Any], float, float, str, str, dict[str, Any] | None, dict[str, Any] | None, int | None, str]] = list(prepared_payload["prepared_rows"] or [])

    _emit_progress(progress_cb, "start", total_rows=len(rows))

    _emit_progress(progress_cb, "prepared", prepared=len(prepared_rows), errors=len(errors))

    analysis_context = MARKET_SCAN_CORE_SERVICE.prepare_analysis_context(
        payload=payload,
        prepared_rows=prepared_rows,
        default_last_n=default_last_n,
        selected_season=selected_season,
        season_type=season_type,
    )
    defaults = analysis_context["defaults"]
    max_workers = int(analysis_context["max_workers"])
    local_cache: dict[tuple[Any, ...], dict[str, Any]] = analysis_context["local_cache"]
    analysis_by_row: dict[int, dict[str, Any]] = analysis_context["analysis_by_row"]
    analysis_key_by_row: dict[int, tuple[Any, ...]] = analysis_context["analysis_key_by_row"]
    unique_analysis_jobs: list[tuple[int, dict[str, Any], tuple[Any, ...]]] = analysis_context["unique_analysis_jobs"]

    injury_report_identity = ""
    boosted_analysis_cache: dict[tuple[Any, ...], dict[str, Any] | None] = {}
    player_lookup_cache: dict[tuple[str, int | None], dict[str, Any] | None] = {}
    without_player_names_cache: dict[tuple[int, ...], list[str]] = {}
    teammate_impact_cache: dict[int, float] = {}
    team_injury_context_cache: dict[str, dict[str, Any]] = {}
    boosted_analysis_lock = Lock()
    player_lookup_lock = Lock()
    without_player_names_lock = Lock()
    teammate_impact_lock = Lock()
    team_injury_context_lock = Lock()
    inj_report: dict[str, Any] = {"ok": False, "rows": []}

    def cached_find_player_by_name(player_name: str, team_id: int | None = None) -> dict[str, Any] | None:
        cache_key = (str(player_name or "").strip().lower(), team_id)
        with player_lookup_lock:
            if cache_key in player_lookup_cache:
                return player_lookup_cache[cache_key]
        resolved_player = find_player_by_name(player_name, team_id=team_id)
        with player_lookup_lock:
            player_lookup_cache.setdefault(cache_key, resolved_player)
            return player_lookup_cache[cache_key]

    def cached_without_player_names(player_ids_value: list[int]) -> list[str]:
        cache_key = tuple(normalize_without_player_ids(player_ids_value))
        with without_player_names_lock:
            cached_names = without_player_names_cache.get(cache_key)
        if cached_names is None:
            resolved_names = resolve_without_player_names(list(cache_key))
            with without_player_names_lock:
                without_player_names_cache.setdefault(cache_key, resolved_names)
                cached_names = without_player_names_cache.get(cache_key) or []
        return list(cached_names)

    def cached_teammate_impact(player_id_value: int) -> float:
        normalized_player_id = int(player_id_value or 0)
        if normalized_player_id <= 0:
            return 0.0
        with teammate_impact_lock:
            cached_impact = teammate_impact_cache.get(normalized_player_id)
        if cached_impact is None:
            resolved_impact = teammate_impact_score(
                normalized_player_id,
                season=selected_season,
                season_type=season_type,
            )
            with teammate_impact_lock:
                teammate_impact_cache.setdefault(normalized_player_id, resolved_impact)
                cached_impact = teammate_impact_cache.get(normalized_player_id)
        return float(cached_impact or 0.0)

    def get_injured_context_for_team(team_name_value: str, team_id_value: int | None = None) -> dict[str, Any]:
        cache_key = canonicalize_team_name(team_name_value, team_id=team_id_value)
        with team_injury_context_lock:
            cached_payload = team_injury_context_cache.get(cache_key)
        if cached_payload is not None:
            return copy.deepcopy(cached_payload)

        rows_inj = INJURY_SERVICE.get_team_rows(inj_report, team_name_value)
        ids: list[int] = []
        names: list[str] = []
        for row_inj in rows_inj:
            status = str(row_inj.get("status") or "")
            if status not in UNAVAILABLE_STATUSES and status not in RISKY_STATUSES:
                continue
            raw_display = re.sub(r",(?!\s)", ", ", str(row_inj.get("player_display") or "").strip())
            if raw_display:
                names.append(raw_display)
            parts = [p.strip() for p in raw_display.split(",")]
            lookup_name = f"{parts[1]} {parts[0]}" if len(parts) == 2 else raw_display
            player_row = (
                cached_find_player_by_name(lookup_name, team_id=team_id_value)
                or cached_find_player_by_name(raw_display, team_id=team_id_value)
                or cached_find_player_by_name(lookup_name)
                or cached_find_player_by_name(raw_display)
            )
            if player_row:
                ids.append(int(player_row["id"]))

        deduped_ids = sorted(set(ids), key=lambda teammate_id: (-cached_teammate_impact(teammate_id), teammate_id))
        deduped_names: list[str] = []
        seen_names: set[str] = set()
        for name in names:
            normalized_name = normalize_name(name)
            if normalized_name in seen_names:
                continue
            seen_names.add(normalized_name)
            deduped_names.append(name)
        context_payload = {"ids": deduped_ids, "names": deduped_names}
        with team_injury_context_lock:
            team_injury_context_cache.setdefault(cache_key, copy.deepcopy(context_payload))
            context_payload = copy.deepcopy(team_injury_context_cache[cache_key])
        return context_payload

    if injury_aware:
        try:
            inj_report = get_cached_injury_report_payload()
        except Exception:
            inj_report = {"ok": False, "rows": []}
        injury_report_identity = str(
            inj_report.get("report_url")
            or inj_report.get("report_label")
            or inj_report.get("report_timestamp")
            or ""
        )

    _emit_progress(
        progress_cb,
        "analysis_start",
        total=len(prepared_rows),
        unique=len(unique_analysis_jobs),
        workers=max_workers,
    )

    analysis_by_key, analysis_errors = MARKET_SCAN_CORE_SERVICE.run_analysis_jobs(
        unique_analysis_jobs=unique_analysis_jobs,
        defaults=defaults,
        local_cache=local_cache,
        max_workers=max_workers,
        build_bulk_prop_item=_build_bulk_prop_item,
        emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
    )
    if analysis_errors:
        errors.extend(analysis_errors)

    for row_index, analysis_key in analysis_key_by_row.items():
        item = analysis_by_key.get(analysis_key)
        if item:
            analysis_by_row[row_index] = item

    _emit_progress(
        progress_cb,
        "analysis_done",
        total=len(analysis_by_row),
        unique=len(analysis_by_key),
        deduped=max(0, len(prepared_rows) - len(unique_analysis_jobs)),
    )

    pass1_metrics_by_row: dict[int, dict[str, Any]] = {}
    pass1_ranked: list[tuple[float, int]] = []
    for index, bulk_row, over_odds, under_odds, *_ in prepared_rows:
        bulk_item = analysis_by_row.get(index)
        if not bulk_item:
            continue
        analysis_pass1 = bulk_item.get("analysis") or {}
        if not analysis_pass1:
            continue
        matchup_pass1 = analysis_pass1.get("matchup") or {}
        vs_position_pass1 = matchup_pass1.get("vs_position") or {}
        matchup_delta_pass1 = vs_position_pass1.get("delta_pct") if isinstance(vs_position_pass1, dict) else None
        pricing_pass1 = build_shared_market_pricing_snapshot(
            market_row=bulk_row,
            over_odds=over_odds,
            under_odds=under_odds,
            hit_rate_pct=float(analysis_pass1.get("hit_rate") or 0.0),
            average=float(analysis_pass1.get("average") or 0.0),
            line=float(bulk_row["line"]),
            stat=str(bulk_row["stat"]),
            matchup_delta_pct=float(matchup_delta_pass1) if matchup_delta_pass1 is not None else None,
            opportunity=analysis_pass1.get("opportunity") or {},
            team_context=analysis_pass1.get("team_context") or {},
            environment=analysis_pass1.get("environment") or {},
            variance=analysis_pass1.get("variance") or {},
            games_count=int(analysis_pass1.get("games_count") or 0),
            h2h_games_count=int((analysis_pass1.get("h2h") or {}).get("games_count") or 0),
        )
        model_over_pass1 = float(pricing_pass1["raw"]["over_probability"])
        model_under_pass1 = float(pricing_pass1["raw"]["under_probability"])
        calibrated_over_pass1 = float(pricing_pass1["calibrated"]["over_probability"])
        calibrated_under_pass1 = float(pricing_pass1["calibrated"]["under_probability"])
        implied_over_pass1 = pricing_pass1["market"].get("implied_over")
        implied_under_pass1 = pricing_pass1["market"].get("implied_under")
        fair_over_pass1 = float(pricing_pass1["market"]["fair_over"])
        fair_under_pass1 = float(pricing_pass1["market"]["fair_under"])
        over_edge_pass1 = float(pricing_pass1["calibrated"]["over_edge_pct"])
        under_edge_pass1 = float(pricing_pass1["calibrated"]["under_edge_pct"])
        over_ev_raw_pass1 = float(pricing_pass1["raw"]["over_ev"])
        under_ev_raw_pass1 = float(pricing_pass1["raw"]["under_ev"])
        over_ev_calibrated_pass1 = float(pricing_pass1["calibrated"]["over_ev"])
        under_ev_calibrated_pass1 = float(pricing_pass1["calibrated"]["under_ev"])
        over_ev_adjusted_pass1 = float(pricing_pass1["adjusted"]["over_ev"])
        under_ev_adjusted_pass1 = float(pricing_pass1["adjusted"]["under_ev"])
        if under_ev_calibrated_pass1 > over_ev_calibrated_pass1:
            best_side_pass1 = "UNDER"
            best_edge_pass1 = under_edge_pass1
            best_ev_pass1 = under_ev_adjusted_pass1
            best_model_pass1 = calibrated_under_pass1
            best_implied_pass1 = implied_under_pass1
            best_market_odds_pass1 = under_odds
            best_ev_raw_pass1 = under_ev_raw_pass1
            best_ev_calibrated_pass1 = under_ev_calibrated_pass1
            best_model_raw_pass1 = model_under_pass1
        else:
            best_side_pass1 = "OVER"
            best_edge_pass1 = over_edge_pass1
            best_ev_pass1 = over_ev_adjusted_pass1
            best_model_pass1 = calibrated_over_pass1
            best_implied_pass1 = implied_over_pass1
            best_market_odds_pass1 = over_odds
            best_ev_raw_pass1 = over_ev_raw_pass1
            best_ev_calibrated_pass1 = over_ev_calibrated_pass1
            best_model_raw_pass1 = model_over_pass1

        availability_pass1 = analysis_pass1.get("availability") or {}
        games_count_pass1 = int(analysis_pass1.get("games_count") or 0)
        hit_rate_pass1 = float(analysis_pass1.get("hit_rate") or 0.0)
        availability_penalty = -30.0 if availability_pass1.get("is_unavailable") else (-8.0 if availability_pass1.get("is_risky") else 0.0)
        pre_rank_score = (
            (best_ev_pass1 * 130.0)
            + (best_edge_pass1 * 1.15)
            + (hit_rate_pass1 * 0.15)
            + (min(games_count_pass1, 20) * 0.55)
            + availability_penalty
        )
        pass1_metrics_by_row[index] = {
            "model_over": model_over_pass1,
            "model_under": model_under_pass1,
            "calibrated_over": calibrated_over_pass1,
            "calibrated_under": calibrated_under_pass1,
            "implied_over": implied_over_pass1,
            "implied_under": implied_under_pass1,
            "fair_over": fair_over_pass1,
            "fair_under": fair_under_pass1,
            "over_edge_pct": over_edge_pass1,
            "under_edge_pct": under_edge_pass1,
            "over_ev": over_ev_adjusted_pass1,
            "under_ev": under_ev_adjusted_pass1,
            "over_ev_raw": over_ev_raw_pass1,
            "under_ev_raw": under_ev_raw_pass1,
            "over_ev_calibrated": over_ev_calibrated_pass1,
            "under_ev_calibrated": under_ev_calibrated_pass1,
            "best_side": best_side_pass1,
            "best_edge_pct": best_edge_pass1,
            "best_ev": best_ev_pass1,
            "best_ev_raw": best_ev_raw_pass1,
            "best_ev_calibrated": best_ev_calibrated_pass1,
            "best_model": best_model_pass1,
            "best_model_raw": best_model_raw_pass1,
            "best_implied": best_implied_pass1,
            "market_odds": best_market_odds_pass1,
            "matchup_delta_pct": matchup_delta_pass1,
            "calibration_reliability": float(pricing_pass1["reliability"]["reliability"]),
            "calibration_shrink": float(pricing_pass1["reliability"]["shrink_strength"]),
        }
        pass1_ranked.append((pre_rank_score, index))

    full_scoring_targets: set[int] = set(pass1_metrics_by_row.keys())
    top_k_full_scoring = len(full_scoring_targets)
    if injury_aware and pass1_ranked:
        default_top_k = min(len(pass1_ranked), max(40, int(math.ceil(len(pass1_ranked) * 0.35))))
        requested_top_k = payload.get("scoring_top_k")
        top_k_full_scoring = default_top_k
        try:
            if requested_top_k not in (None, ""):
                top_k_full_scoring = max(1, min(len(pass1_ranked), int(requested_top_k)))
        except (TypeError, ValueError):
            top_k_full_scoring = default_top_k
        pass1_ranked.sort(key=lambda item: item[0], reverse=True)
        full_scoring_targets = {row_index for _, row_index in pass1_ranked[:top_k_full_scoring]}

    pass1_rank_by_row: dict[int, int] = {}
    if pass1_ranked:
        sorted_pass1_ranked = sorted(pass1_ranked, key=lambda item: item[0], reverse=True)
        for rank_position, (_, row_index) in enumerate(sorted_pass1_ranked, start=1):
            pass1_rank_by_row[row_index] = rank_position

    _emit_progress(
        progress_cb,
        "scoring_pass1_done",
        candidates=len(pass1_ranked),
        full_scoring=top_k_full_scoring,
        injury_aware=injury_aware,
    )

    def _score_prepared_row(
        prepared_row: tuple[int, dict[str, Any], float, float, str, str, dict[str, Any] | None, dict[str, Any] | None, int | None, str]
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        index, bulk_row, over_odds, under_odds, team_text, opponent_text, team, opponent, team_id, input_game_label = prepared_row
        bulk_item = analysis_by_row.get(index)
        if not bulk_item:
            return None, None
        try:
            analysis = copy.deepcopy(bulk_item.get("analysis") or {})
            base_hit_rate = float(analysis.get("hit_rate") or 0.0)
            injury_boost = False
            injury_filter_player_ids: list[int] = []
            injury_filter_player_names: list[str] = []
            team_injury_player_names: list[str] = []

            if injury_aware and index in full_scoring_targets and analysis:
                player_info_for_injury = analysis.get("player") or {}
                player_id_for_injury = int(player_info_for_injury.get("id") or bulk_item.get("player_id") or 0)
                team_id_for_injury = int(player_info_for_injury.get("team_id") or 0) or None
                priority_rank = pass1_rank_by_row.get(index)
                priority_pool = len(pass1_ranked)
                combo_plan = _build_injury_combo_plan(
                    base_hit_rate=base_hit_rate,
                    priority_rank=priority_rank,
                    priority_pool=priority_pool,
                )
                team_name_for_injury = canonicalize_team_name(
                    str(player_info_for_injury.get("team_name") or ""),
                    team_id=team_id_for_injury,
                )
                if team_name_for_injury and player_id_for_injury:
                    injury_context = get_injured_context_for_team(team_name_for_injury, team_id_value=team_id_for_injury)
                    injury_ids = list(injury_context.get("ids") or [])
                    team_injury_player_names = (
                        cached_without_player_names(injury_ids)
                        if injury_ids
                        else list(injury_context.get("names") or [])
                    )
                    best_usable_combo: tuple[list[int], dict[str, Any], float, int] | None = None
                    _cache_sentinel = object()
                    for max_filters in combo_plan:
                        candidate_ids = injury_ids[:max_filters]
                        if not candidate_ids:
                            break
                        boost_cache_key = (
                            player_id_for_injury,
                            str(bulk_row["stat"]),
                            float(bulk_row["line"]),
                            int(default_last_n),
                            str(selected_season),
                            str(season_type),
                            tuple(candidate_ids),
                            int(bulk_row.get("team_id") or 0),
                            int(bulk_row.get("override_opponent_id") or 0),
                            injury_report_identity,
                        )
                        with boosted_analysis_lock:
                            boosted_analysis = boosted_analysis_cache.get(boost_cache_key, _cache_sentinel)
                        if boosted_analysis is _cache_sentinel:
                            cached_boosted_analysis = _get_injury_aware_boost_cache(boost_cache_key)
                            if cached_boosted_analysis is not None:
                                boosted_analysis = cached_boosted_analysis
                            else:
                                boosted_analysis = build_prop_analysis_payload(
                                    player_id=player_id_for_injury,
                                    stat=str(bulk_row["stat"]),
                                    line=float(bulk_row["line"]),
                                    last_n=default_last_n,
                                    season=selected_season,
                                    season_type=season_type,
                                    without_player_ids=candidate_ids,
                                    team_id=int(bulk_row.get("team_id") or 0) or None,
                                    override_opponent_id=int(bulk_row.get("override_opponent_id") or 0) or None,
                                )
                                _set_injury_aware_boost_cache(boost_cache_key, boosted_analysis)
                            with boosted_analysis_lock:
                                boosted_analysis_cache[boost_cache_key] = boosted_analysis if isinstance(boosted_analysis, dict) else None
                        if not isinstance(boosted_analysis, dict):
                            continue
                        boosted_games = int(boosted_analysis.get("games_count") or 0)
                        boosted_hit_rate = float(boosted_analysis.get("hit_rate") or 0.0)
                        if boosted_games < 5:
                            continue
                        if (
                            best_usable_combo is None
                            or boosted_hit_rate > best_usable_combo[2]
                            or (boosted_hit_rate == best_usable_combo[2] and boosted_games > best_usable_combo[3])
                        ):
                            best_usable_combo = (list(candidate_ids), copy.deepcopy(boosted_analysis), boosted_hit_rate, boosted_games)
                        if _should_stop_injury_combo_search(
                            base_hit_rate=base_hit_rate,
                            boosted_hit_rate=boosted_hit_rate,
                            boosted_games=boosted_games,
                            combo_size=max_filters,
                            combo_plan=combo_plan,
                            priority_rank=priority_rank,
                            priority_pool=priority_pool,
                        ):
                            break
                    if best_usable_combo is not None and float(best_usable_combo[2]) > base_hit_rate:
                        injury_filter_player_ids = list(best_usable_combo[0])
                        injury_filter_player_names = cached_without_player_names(injury_filter_player_ids)
                        analysis = copy.deepcopy(best_usable_combo[1])
                        injury_boost = True

            matchup = analysis.get("matchup", {})
            next_game = matchup.get("next_game") or {}
            vs_position = matchup.get("vs_position") or {}
            availability = analysis.get("availability") or {}
            cached_pass1_metrics = pass1_metrics_by_row.get(index)
            if cached_pass1_metrics and not injury_boost:
                matchup_delta_pct = cached_pass1_metrics.get("matchup_delta_pct")
                model_over = float(cached_pass1_metrics.get("model_over") or 0.0)
                model_under = float(cached_pass1_metrics.get("model_under") or 0.0)
                implied_over = cached_pass1_metrics.get("implied_over")
                implied_under = cached_pass1_metrics.get("implied_under")
                fair_over = cached_pass1_metrics.get("fair_over")
                fair_under = cached_pass1_metrics.get("fair_under")
                over_edge_pct = float(cached_pass1_metrics.get("over_edge_pct") or 0.0)
                under_edge_pct = float(cached_pass1_metrics.get("under_edge_pct") or 0.0)
                over_ev = float(cached_pass1_metrics.get("over_ev") or 0.0)
                under_ev = float(cached_pass1_metrics.get("under_ev") or 0.0)
                over_ev_raw = float(cached_pass1_metrics.get("over_ev_raw") or over_ev)
                under_ev_raw = float(cached_pass1_metrics.get("under_ev_raw") or under_ev)
                over_ev_calibrated = float(cached_pass1_metrics.get("over_ev_calibrated") or over_ev)
                under_ev_calibrated = float(cached_pass1_metrics.get("under_ev_calibrated") or under_ev)
                best_side = str(cached_pass1_metrics.get("best_side") or "OVER")
                best_edge_pct = float(cached_pass1_metrics.get("best_edge_pct") or 0.0)
                best_ev = float(cached_pass1_metrics.get("best_ev") or 0.0)
                best_ev_raw = float(cached_pass1_metrics.get("best_ev_raw") or best_ev)
                best_ev_calibrated = float(cached_pass1_metrics.get("best_ev_calibrated") or best_ev)
                best_model = float(cached_pass1_metrics.get("best_model") or 0.0)
                best_model_raw = float(cached_pass1_metrics.get("best_model_raw") or best_model)
                best_implied = cached_pass1_metrics.get("best_implied")
                market_odds = float(cached_pass1_metrics.get("market_odds") or (over_odds if best_side != "UNDER" else under_odds))
                calibrated_over = float(cached_pass1_metrics.get("calibrated_over") or model_over)
                calibrated_under = float(cached_pass1_metrics.get("calibrated_under") or model_under)
                calibration_reliability = float(cached_pass1_metrics.get("calibration_reliability") or 0.5)
                calibration_shrink = float(cached_pass1_metrics.get("calibration_shrink") or 0.0)
            else:
                matchup_delta_pct = vs_position.get("delta_pct") if isinstance(vs_position, dict) else None
                pricing_snapshot = build_shared_market_pricing_snapshot(
                    market_row=bulk_row,
                    over_odds=over_odds,
                    under_odds=under_odds,
                    hit_rate_pct=float(analysis["hit_rate"]),
                    average=float(analysis["average"]),
                    line=float(bulk_row["line"]),
                    stat=str(bulk_row["stat"]),
                    matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
                    opportunity=analysis.get("opportunity") or {},
                    team_context=analysis.get("team_context") or {},
                    environment=analysis.get("environment") or {},
                    variance=analysis.get("variance") or {},
                    games_count=int(analysis.get("games_count") or 0),
                    h2h_games_count=int((analysis.get("h2h") or {}).get("games_count") or 0),
                )
                implied_over = pricing_snapshot["market"].get("implied_over")
                implied_under = pricing_snapshot["market"].get("implied_under")
                fair_over = float(pricing_snapshot["market"]["fair_over"])
                fair_under = float(pricing_snapshot["market"]["fair_under"])
                model_over = float(pricing_snapshot["raw"]["over_probability"])
                model_under = float(pricing_snapshot["raw"]["under_probability"])
                calibrated_over = float(pricing_snapshot["calibrated"]["over_probability"])
                calibrated_under = float(pricing_snapshot["calibrated"]["under_probability"])
                over_edge_pct = float(pricing_snapshot["calibrated"]["over_edge_pct"])
                under_edge_pct = float(pricing_snapshot["calibrated"]["under_edge_pct"])
                over_ev_raw = float(pricing_snapshot["raw"]["over_ev"])
                under_ev_raw = float(pricing_snapshot["raw"]["under_ev"])
                over_ev_calibrated = float(pricing_snapshot["calibrated"]["over_ev"])
                under_ev_calibrated = float(pricing_snapshot["calibrated"]["under_ev"])
                over_ev = float(pricing_snapshot["adjusted"]["over_ev"])
                under_ev = float(pricing_snapshot["adjusted"]["under_ev"])
                calibration_reliability = float(pricing_snapshot["reliability"]["reliability"])
                calibration_shrink = float(pricing_snapshot["reliability"]["shrink_strength"])

                if under_ev_calibrated > over_ev_calibrated:
                    best_side = "UNDER"
                    best_edge_pct = under_edge_pct
                    best_ev = under_ev
                    best_ev_raw = under_ev_raw
                    best_ev_calibrated = under_ev_calibrated
                    best_model = calibrated_under
                    best_model_raw = model_under
                    best_implied = implied_under
                    market_odds = under_odds
                else:
                    best_side = "OVER"
                    best_edge_pct = over_edge_pct
                    best_ev = over_ev
                    best_ev_raw = over_ev_raw
                    best_ev_calibrated = over_ev_calibrated
                    best_model = calibrated_over
                    best_model_raw = model_over
                    best_implied = implied_over
                    market_odds = over_odds

            confidence_hit_rate = side_hit_rate_from_over_hit_rate(analysis.get("hit_rate"), best_side)
            confidence_engine = build_confidence_engine(
                side=best_side,
                hit_rate=float(confidence_hit_rate),
                games_count=int(analysis["games_count"]),
                edge=best_edge_pct,
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
                season_type=analysis.get("season_type"),
                playoff_game_number=int(((analysis.get("matchup") or {}).get("next_game") or {}).get("playoff_game_number") or 0) or None,
            )
            confidence_engine = apply_backtest_ranking_adjustment(
                confidence_engine,
                stat=str(bulk_row["stat"]),
                side=best_side,
            )

            display_side = best_side
            if availability.get("is_unavailable"):
                display_side = "AVOID"
            elif availability.get("is_risky"):
                display_side = f"{best_side}?"

            resolved_team_id = analysis["player"].get("team_id") or team_id
            resolved_team = TEAM_LOOKUP.get(int(resolved_team_id)) if resolved_team_id else None
            resolved_team_abbreviation = (
                (team.get("abbreviation") if team else None)
                or (resolved_team or {}).get("abbreviation")
                or (team_text.strip() if team_text else "")
            )
            resolved_opponent_abbreviation = (
                (opponent.get("abbreviation") if opponent else None)
                or (opponent_text.strip() if opponent_text else "")
                or next_game.get("opponent_abbreviation")
            )
            resolved_matchup_label = input_game_label
            if not resolved_matchup_label and resolved_team_abbreviation and resolved_opponent_abbreviation:
                resolved_matchup_label = f"{resolved_team_abbreviation} vs {resolved_opponent_abbreviation}"

            result_payload = {
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
                "game_label": resolved_matchup_label,
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
                    "games": analysis.get("games") or [],
                    # games[0] is most recent across analyzer payloads.
                    "over_streak": compute_recent_hit_streak([game.get("hit") for game in (analysis.get("games") or [])]),
                    "last_value": (analysis.get("games") or [{}])[0].get("value") if (analysis.get("games") or []) else None,
                    "availability": availability,
                    "player": analysis.get("player") or {},
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
                    "over_probability": round(calibrated_over * 100, 1),
                    "under_probability": round(calibrated_under * 100, 1),
                    "over_probability_raw": round(model_over * 100, 1),
                    "under_probability_raw": round(model_under * 100, 1),
                    "over_probability_calibrated": round(calibrated_over * 100, 1),
                    "under_probability_calibrated": round(calibrated_under * 100, 1),
                    "over_implied": round(implied_over * 100, 1) if implied_over is not None else None,
                    "under_implied": round(implied_under * 100, 1) if implied_under is not None else None,
                    "over_fair_probability": round(fair_over * 100, 1) if fair_over is not None else None,
                    "under_fair_probability": round(fair_under * 100, 1) if fair_under is not None else None,
                    "over_edge": over_edge_pct,
                    "under_edge": under_edge_pct,
                    "over_edge_pct": over_edge_pct,
                    "under_edge_pct": under_edge_pct,
                    "edge_definition": EDGE_DEFINITION_MODEL_FAIR,
                    "over_ev": round(over_ev * 100, 1),
                    "under_ev": round(under_ev * 100, 1),
                    "over_ev_raw": round(over_ev_raw * 100, 1),
                    "under_ev_raw": round(under_ev_raw * 100, 1),
                    "over_ev_calibrated": round(over_ev_calibrated * 100, 1),
                    "under_ev_calibrated": round(under_ev_calibrated * 100, 1),
                },
                "best_bet": {
                    "side": best_side,
                    "display_side": display_side,
                    "edge": round(best_edge_pct, 1),
                    "edge_pct": round(best_edge_pct, 1),
                    "edge_definition": EDGE_DEFINITION_MODEL_FAIR,
                    "ev": round(best_ev * 100, 1),
                    "ev_raw": round(best_ev_raw * 100, 1),
                    "ev_calibrated": round(best_ev_calibrated * 100, 1),
                    "model_probability": round(best_model * 100, 1),
                    "model_probability_raw": round(best_model_raw * 100, 1),
                    "model_probability_calibrated": round(
                        (calibrated_under if best_side == "UNDER" else calibrated_over) * 100, 1
                    ),
                    "implied_probability": round(best_implied * 100, 1) if best_implied is not None else None,
                    "odds": market_odds,
                    "calibration_reliability": round(calibration_reliability * 100, 1),
                    "calibration_shrink": round(calibration_shrink * 100, 1),
                    "confidence": confidence_engine["grade"],
                    "confidence_score": confidence_engine["score"],
                    "confidence_summary": confidence_engine["summary"],
                    "confidence_tone": confidence_engine["tone"],
                    "confidence_tier": confidence_engine.get("tier"),
                    "confidence_tags": confidence_engine.get("tags") or [],
                    "confidence_components": confidence_engine.get("components") or {},
                    "market_side": confidence_engine.get("market_side"),
                    "market_disagrees": confidence_engine.get("market_disagrees"),
                    "market_penalty": confidence_engine.get("market_penalty"),
                    "market_support_pct": confidence_engine.get("market_support_pct"),
                    "ranking_score": confidence_engine.get("ranking_score"),
                    "playable": not availability.get("is_unavailable", False),
                    "user_read": analysis.get("interpretation", {}).get("market_takeaway") or confidence_engine["summary"],
                },
                "availability": availability,
                "matchup": {
                    "next_game": next_game,
                    "vs_position": vs_position,
                },
                "injury_boost": injury_boost,
                "base_hit_rate": round(base_hit_rate, 1),
                "injury_filter_player_ids": injury_filter_player_ids,
                "injury_filter_player_names": injury_filter_player_names,
                "team_injury_player_names": team_injury_player_names,
            }
            return result_payload, None
        except HTTPException as exc:
            return None, {"row": index, "player_name": str(bulk_row.get("player_name") or ""), "reason": str(exc.detail)}
        except Exception as exc:
            return None, {"row": index, "player_name": str(bulk_row.get("player_name") or ""), "reason": str(exc)}

    results: list[dict[str, Any]] = []
    scoring_jobs = [row for row in prepared_rows if analysis_by_row.get(row[0])]
    requested_scoring_workers = payload.get("scoring_workers")
    scoring_workers = max_workers
    try:
        if requested_scoring_workers not in (None, ""):
            scoring_workers = max(1, min(BULK_ANALYSIS_MAX_WORKERS, int(requested_scoring_workers)))
    except (TypeError, ValueError):
        scoring_workers = max_workers
    scoring_workers = min(scoring_workers, max(1, len(scoring_jobs)))
    scoring_step = max(1, len(scoring_jobs) // 10) if scoring_jobs else 1

    _emit_progress(progress_cb, "scoring_start", total=len(scoring_jobs), workers=scoring_workers)

    scored_count = 0
    if scoring_workers <= 1:
        for prepared_row in scoring_jobs:
            result_item, error_item = _score_prepared_row(prepared_row)
            if result_item:
                results.append(result_item)
            if error_item:
                errors.append(error_item)
            scored_count += 1
            if scored_count % scoring_step == 0 or scored_count == len(scoring_jobs):
                _emit_progress(progress_cb, "scoring_progress", done=scored_count, total=len(scoring_jobs))
    else:
        for batch_start in range(0, len(scoring_jobs), scoring_workers):
            batch = scoring_jobs[batch_start : batch_start + scoring_workers]
            futures = [_submit_shared_analysis_task(_score_prepared_row, prepared_row) for prepared_row in batch]
            for future in as_completed(futures):
                try:
                    result_item, error_item = future.result()
                except Exception as exc:
                    result_item, error_item = None, {"row": 0, "reason": f"Scoring worker failed: {exc}"}
                if result_item:
                    results.append(result_item)
                if error_item:
                    errors.append(error_item)
                scored_count += 1
                if scored_count % scoring_step == 0 or scored_count == len(scoring_jobs):
                    _emit_progress(progress_cb, "scoring_progress", done=scored_count, total=len(scoring_jobs))

    results.sort(
        key=lambda item: (
            item["best_bet"].get("ranking_score") if item["best_bet"].get("ranking_score") is not None else float("-inf"),
            item["best_bet"].get("ev") if item["best_bet"].get("ev") is not None else float("-inf"),
            item["best_bet"].get("edge_pct") if item["best_bet"].get("edge_pct") is not None else item["best_bet"].get("edge") if item["best_bet"].get("edge") is not None else float("-inf"),
            item["analysis"].get("hit_rate") if item["analysis"].get("hit_rate") is not None else float("-inf"),
            item["best_bet"].get("confidence_score", 0),
            -1 * int(item.get("availability", {}).get("sort_rank", 3) or 3),
        ),
        reverse=True,
    )

    errors.sort(key=lambda item: int(item.get("row") or 0))

    payload_out = {
        "season": selected_season,
        "season_type": season_type,
        "last_n": default_last_n,
        "injury_aware": injury_aware,
        "template": "player_name,stat,line,over_odds,under_odds",
        "results": results,
        "errors": errors,
    }
    _submit_pg_write(_pg_write_market_scan_run, payload_out, request_hash_value)
    _emit_progress(progress_cb, "done", results=len(results), errors=len(errors))
    return payload_out


def _offset_market_scan_row_numbers(items: list[dict[str, Any]] | None, row_offset: int) -> list[dict[str, Any]]:
    adjusted: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        next_item = copy.deepcopy(item)
        try:
            row_number = int(next_item.get("row") or 0)
        except (TypeError, ValueError):
            row_number = 0
        if row_number > 0:
            next_item["row"] = row_number + row_offset
        adjusted.append(next_item)
    return adjusted


def _sort_market_scan_results(results: list[dict[str, Any]]) -> None:
    results.sort(
        key=lambda item: (
            item.get("best_bet", {}).get("ranking_score") if item.get("best_bet", {}).get("ranking_score") is not None else float("-inf"),
            item.get("best_bet", {}).get("ev") if item.get("best_bet", {}).get("ev") is not None else float("-inf"),
            item.get("best_bet", {}).get("edge_pct")
            if item.get("best_bet", {}).get("edge_pct") is not None
            else item.get("best_bet", {}).get("edge")
            if item.get("best_bet", {}).get("edge") is not None
            else float("-inf"),
            item.get("analysis", {}).get("hit_rate") if item.get("analysis", {}).get("hit_rate") is not None else float("-inf"),
            item.get("best_bet", {}).get("confidence_score", 0),
            -1 * int(item.get("availability", {}).get("sort_rank", 3) or 3),
        ),
        reverse=True,
    )


def _market_scan_request(payload: dict[str, Any], progress_cb=None) -> dict[str, Any]:
    rows = payload.get("rows") or []
    if not isinstance(rows, list) or len(rows) <= int(MARKET_SCAN_CORE_SERVICE.max_rows):
        return _market_scan_core(payload, progress_cb)

    batch_size = max(1, int(MARKET_SCAN_CORE_SERVICE.max_rows))
    total_batches = int(math.ceil(len(rows) / batch_size))
    merged_results: list[dict[str, Any]] = []
    merged_errors: list[dict[str, Any]] = []
    merged_base: dict[str, Any] = {
        "season": str(payload.get("season") or current_nba_season()),
        "season_type": normalize_requested_season_type(payload.get("season_type")),
        "last_n": payload.get("last_n"),
        "injury_aware": bool(payload.get("injury_aware")),
        "template": "player_name,stat,line,over_odds,under_odds",
    }

    for batch_index, batch_start in enumerate(range(0, len(rows), batch_size), start=1):
        batch_rows = rows[batch_start : batch_start + batch_size]

        def batch_progress(update: dict[str, Any]) -> None:
            if not progress_cb:
                return
            batch_update = dict(update or {})
            original_stage = str(batch_update.get("stage") or "")
            if original_stage == "done":
                batch_update["stage"] = "batch_done"
            batch_update.update(
                {
                    "batch": batch_index,
                    "batches": total_batches,
                    "batch_rows": len(batch_rows),
                    "total_rows": len(rows),
                }
            )
            progress_cb(batch_update)

        _emit_progress(
            progress_cb,
            "batch_start",
            batch=batch_index,
            batches=total_batches,
            batch_rows=len(batch_rows),
            total_rows=len(rows),
        )
        batch_payload = {**payload, "rows": batch_rows}
        batch_result = _market_scan_core(batch_payload, batch_progress)
        if batch_index == 1 and isinstance(batch_result, dict):
            merged_base.update({k: v for k, v in batch_result.items() if k not in {"results", "errors"}})
        merged_results.extend(_offset_market_scan_row_numbers(batch_result.get("results") if isinstance(batch_result, dict) else [], batch_start))
        merged_errors.extend(_offset_market_scan_row_numbers(batch_result.get("errors") if isinstance(batch_result, dict) else [], batch_start))

    _sort_market_scan_results(merged_results)
    merged_errors.sort(key=lambda item: int(item.get("row") or 0))
    payload_out = {
        **merged_base,
        "results": merged_results,
        "errors": merged_errors,
        "rows_submitted": len(rows),
        "batches_scanned": total_batches,
        "batch_size": batch_size,
    }
    _submit_pg_write(_pg_write_market_scan_run, payload_out, _request_hash("market_scan", payload))
    _emit_progress(progress_cb, "done", results=len(merged_results), errors=len(merged_errors), batches=total_batches)
    return payload_out


def _build_parlay_odds_source_payload(orig_row: dict[str, Any], side: str, odds: float) -> dict[str, Any]:
    side_key = "under" if str(side or "").upper() == "UNDER" else "over"
    best_odds = safe_float_or_none(orig_row.get(f"best_{side_key}_odds"))
    selected_odds = safe_float_or_none(odds)
    selected_bookmaker = str(orig_row.get("bookmaker_title") or orig_row.get("bookmaker_key") or "N/A")
    best_bookmaker = str(orig_row.get(f"best_{side_key}_bookmaker") or selected_bookmaker)
    if best_odds is None:
        best_odds = selected_odds
    is_best_available = bool(
        best_odds is not None
        and selected_odds is not None
        and abs(float(best_odds) - float(selected_odds)) <= 1e-9
    )
    return {
        "bookmaker": selected_bookmaker,
        "bookmaker_key": orig_row.get("bookmaker_key"),
        "odds_source": selected_bookmaker,
        "odds_source_type": "selected_bookmaker",
        "odds_last_update": orig_row.get("market_last_update"),
        "odds_market_key": orig_row.get("market_key") or "",
        "odds_side": side_key.upper(),
        "odds_is_best_available": is_best_available,
        "best_available_odds": best_odds,
        "best_available_bookmaker": best_bookmaker,
        "books_count": orig_row.get("books_count") or 1,
        "hold_percent": orig_row.get("hold_percent"),
    }


@app.post("/api/market-scan")
def market_scan(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    enforce_heavy_rate_limit(request, "market_scan")
    return MARKET_SCAN_SERVICE.run_sync(_market_scan_request, payload)

@app.post("/api/market-scan/async")
def market_scan_async(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    enforce_heavy_rate_limit(request, "market_scan_async")
    return MARKET_SCAN_SERVICE.run_async(_market_scan_request, payload)

@app.get("/api/jobs/{job_id}")
def async_job_status(job_id: str) -> dict[str, Any]:
    return get_async_job(job_id)


@app.post("/api/market-scan/stream")
def market_scan_stream(request: Request, payload: dict[str, Any] = Body(...)) -> StreamingResponse:
    enforce_heavy_rate_limit(request, "market_scan_stream")
    return _stream_with_progress(_market_scan_request, payload)


@app.post("/api/odds/events")
def odds_events(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    api_key = normalize_odds_api_key(payload.get("api_key") or "")
    sport = str(payload.get("sport") or "basketball_nba")
    result = odds_api_fetch(
        f"/sports/{sport}/events",
        api_key,
        {"dateFormat": "iso"},
    )
    return {
        "events": result["data"],
        "quota": result["quota"],
        "api_key_used": api_key,
        "api_key_masked": mask_api_key_for_display(api_key),
        "sport": sport,
    }


@app.post("/api/odds/player-props-import")
def odds_player_props_import(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    api_key = normalize_odds_api_key(payload.get("api_key") or "")
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


def _parlay_builder_core(payload: dict[str, Any], progress_cb=None) -> dict[str, Any]:
    """
    Scrape selected NBA events + all markets in batches using rotating API keys,
    analyze every prop via the existing bulk engine, rank by hit rate, and build
    the best N-leg parlay the user requested.

    Payload:
      api_keys      list[str]    one or more Odds API keys (rotated round-robin)
      event_ids     list[str]    specific event IDs to scrape (from /api/odds/events)
      legs          int          26 (number of parlay legs)
      sport         str          default "basketball_nba"
      regions       str          default "us"
      odds_format   str          "decimal" | "american"
      last_n        int          recent-game sample size for analysis (default 10)
      season        str          e.g. "2024-25"
      season_type   str          "Combined" (Regular Season + Playoffs)
      batch_size    int          events per batch (default 3, keeps credit bursts small)
    """
    #  Validate inputs 
    raw_keys = payload.get("api_keys") or []
    if isinstance(raw_keys, str):
        raw_keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
    api_keys: list[str] = [
        normalized for k in raw_keys
        if (normalized := normalize_odds_api_key(k))
    ]
    if not api_keys:
        raise HTTPException(status_code=400, detail="Provide at least one Odds API key in 'api_keys'.")

    season_type  = normalize_requested_season_type(payload.get("season_type"))
    legs = _parse_bounded_int(
        payload.get("legs"),
        default=3,
        min_value=2,
        max_value=6,
        field_name="legs",
    )

    sport        = str(payload.get("sport") or "basketball_nba")
    regions      = str(payload.get("regions") or "us")
    odds_format  = str(payload.get("odds_format") or "decimal")
    last_n       = _parse_bounded_int(
        payload.get("last_n"),
        default=10,
        min_value=1,
        max_value=PROP_ANALYSIS_MAX_LAST_N,
        field_name="last_n",
    )
    season       = str(payload.get("season") or current_nba_season())
    batch_size   = _parse_bounded_int(
        payload.get("batch_size"),
        default=3,
        min_value=1,
        max_value=PARLAY_BUILDER_MAX_BATCH_SIZE,
        field_name="batch_size",
    )
    requested_bookmakers = parse_requested_bookmakers(payload.get("bookmakers") or payload.get("bookmaker") or ODDS_DEFAULT_BOOKMAKERS)
    markets      = ",".join(ODDS_PARLAY_MARKETS)

    # event_ids  if provided, skip fetching the events list entirely (saves 1 credit)
    requested_event_ids: set[str] = set()
    raw_event_ids = payload.get("event_ids") or []
    if isinstance(raw_event_ids, list):
        requested_event_ids = {str(e).strip() for e in raw_event_ids if str(e).strip()}
    canonical_request_payload = {
        "event_ids": sorted(requested_event_ids),
        "legs": legs,
        "sport": sport,
        "regions": regions,
        "odds_format": odds_format,
        "last_n": last_n,
        "season": season,
        "season_type": season_type,
        "batch_size": batch_size,
        "bookmakers": requested_bookmakers,
        "markets": list(ODDS_PARLAY_MARKETS),
    }
    request_hash_value = _request_hash("parlay_builder", canonical_request_payload)
    bypass_cache = bool(payload.get("bypass_cache")) or season_type == SEASON_TYPE_PLAYOFFS
    if not bypass_cache:
        cached_run = _pg_read_parlay_builder_cache(canonical_request_payload, cache_scope="parlay_builder")
        if cached_run:
            return cached_run

    _emit_progress(progress_cb, "start", events_requested=len(requested_event_ids))

    #  Phase 1: Resolve events 
    key_index = 0
    quota_log: list[dict[str, Any]] = []

    def next_key() -> str:
        nonlocal key_index
        key = api_keys[key_index % len(api_keys)]
        key_index += 1
        return key

    if requested_event_ids:
        # User already selected specific events  build stub dicts directly
        # so we don't spend a credit on the events list
        events: list[dict[str, Any]] = [{"id": eid} for eid in sorted(requested_event_ids)]
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

    _emit_progress(progress_cb, "events_resolved", events=len(events))

    if not events:
        payload = {
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
        _submit_pg_write(_pg_write_parlay_builder_run, payload, request_hash_value)
        return payload

    #  Phase 2: Fetch odds per event in batches 
    scrape_payload = PARLAY_SERVICE.fetch_event_odds_batches(
        events=events,
        batch_size=batch_size,
        api_keys=api_keys,
        sport=sport,
        regions=regions,
        markets=markets,
        odds_format=odds_format,
        requested_bookmakers=requested_bookmakers,
        fetch_event_odds_payload=_fetch_event_odds_payload,
        submit_network_task=_submit_shared_network_task,
        emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
    )
    all_import_rows: list[dict[str, Any]] = list(scrape_payload.get("all_import_rows") or [])
    scrape_errors: list[dict[str, Any]] = [
        {
            "event_id": str(item.get("event_id") or ""),
            "reason": item.get("reason"),
            "status_code": item.get("status_code"),
        }
        for item in (scrape_payload.get("scrape_errors") or [])
    ]
    quota_log.extend(scrape_payload.get("quota_log") or [])

    if not all_import_rows:
        payload = {
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
        _submit_pg_write(_pg_write_parlay_builder_run, payload, request_hash_value)
        return payload

    #  Phase 3a: Pre-warm game-log cache with one bulk LeagueDash call 
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
        # (LeagueDash gives season averages, not per-game logs  we can't fully
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

    #  Phase 3: Bulk-analyze all props in one shot (free, parallel) 
    defaults = {"last_n": last_n, "season": season, "season_type": season_type}
    local_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    analysis_prep = PARLAY_SERVICE.prepare_analysis_jobs(
        all_import_rows=all_import_rows,
        resolve_player=lambda player_name: find_player_by_name(player_name),
    )
    deduped_prepared: list[tuple[dict[str, Any], dict[str, Any]]] = list(analysis_prep["deduped_prepared"] or [])
    deduped_prepared = [
        (_apply_parlay_event_context_to_bulk_row(bulk_row, orig_row), orig_row)
        for bulk_row, orig_row in deduped_prepared
    ]
    analysis_errors: list[dict[str, Any]] = list(analysis_prep["analysis_errors"] or [])
    unique_player_ids: set[int] = set(analysis_prep["unique_player_ids"] or set())
    primary_by_team: dict[int, int] = {}
    for bulk_row, _ in deduped_prepared:
        team_id = int(bulk_row.get("team_id") or 0) or _resolve_team_id_for_player(int(bulk_row["player_id"]))
        if team_id and team_id not in primary_by_team:
            primary_by_team[team_id] = int(bulk_row["player_id"])
    prefetch_bulk_analysis_context(
        player_ids=unique_player_ids,
        season=season,
        season_type=season_type,
        team_ids=set(primary_by_team.keys()),
        primary_player_by_team=primary_by_team,
        max_workers=BULK_ANALYSIS_MAX_WORKERS,
        label="parlay_builder",
    )

    max_workers = min(BULK_ANALYSIS_MAX_WORKERS, max(1, len(deduped_prepared)))
    analysis_rows, analysis_errors_run = PARLAY_SERVICE.run_bulk_analysis(
        deduped_prepared=deduped_prepared,
        defaults=defaults,
        local_cache=local_cache,
        max_workers=max_workers,
        build_bulk_prop_item=_build_bulk_prop_item,
        submit_analysis_task=_submit_shared_analysis_task,
        emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
    )
    if analysis_errors_run:
        analysis_errors.extend(analysis_errors_run)

    #  Phase 4: Score every prop by hit rate, pick best N legs 
    playoff_live_refresh_cache: dict[tuple[int, str, float], dict[str, Any]] = {}

    def _refresh_playoff_low_sample_analysis(
        result_row: dict[str, Any],
        market_row: dict[str, Any],
        analysis_payload: dict[str, Any],
    ) -> dict[str, Any]:
        # In playoffs, stale cached analysis can occasionally collapse to a 1-game sample.
        # Hard-refresh low samples so parlay ranking uses current series logs.
        if season_type != SEASON_TYPE_PLAYOFFS:
            return analysis_payload
        current_games = int((analysis_payload or {}).get("games_count") or 0)
        if current_games >= 3:
            return analysis_payload
        player_id_val = int(result_row.get("player_id") or market_row.get("player_id") or 0)
        stat_val = str(result_row.get("stat") or market_row.get("stat") or "").strip().upper()
        line_val = float(result_row.get("line") or market_row.get("line") or 0.0)
        if not player_id_val or not stat_val:
            return analysis_payload
        team_id_val = int(result_row.get("team_id") or market_row.get("team_id") or 0) or None
        override_opponent_id_val = int(result_row.get("override_opponent_id") or market_row.get("override_opponent_id") or 0) or None
        refresh_key = (
            player_id_val,
            stat_val,
            round(line_val, 4),
            int(team_id_val or 0),
            int(override_opponent_id_val or 0),
        )
        cached_refresh = playoff_live_refresh_cache.get(refresh_key)
        if cached_refresh:
            return cached_refresh
        try:
            for cache_key in list(ANALYSIS_CACHE.keys()):
                try:
                    key_player_id = int(cache_key[1])
                    key_stat = str(cache_key[2] or "").strip().upper()
                    key_line = float(cache_key[3] or 0.0)
                    key_season_type = str(cache_key[6] or "")
                except Exception:
                    continue
                if (
                    key_player_id == player_id_val
                    and key_stat == stat_val
                    and abs(key_line - line_val) <= 1e-6
                    and key_season_type == SEASON_TYPE_PLAYOFFS
                ):
                    ANALYSIS_CACHE.pop(cache_key, None)
            game_log_key = (player_id_val, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
            GAME_LOG_CACHE.pop(game_log_key, None)
            GAME_LOG_FAILURE_META.pop(game_log_key, None)
            RECENT_GAME_LOG_CACHE.pop((player_id_val, season, season_type, 20, GAME_LOG_CACHE_SCHEMA_VERSION), None)
            refreshed = build_prop_analysis_payload(
                player_id=player_id_val,
                stat=stat_val,
                line=line_val,
                last_n=last_n,
                season=season,
                season_type=season_type,
                team_id=team_id_val,
                override_opponent_id=override_opponent_id_val,
            )
            if int((refreshed or {}).get("games_count") or 0) > current_games:
                playoff_live_refresh_cache[refresh_key] = refreshed
                return refreshed
        except Exception:
            return analysis_payload
        playoff_live_refresh_cache[refresh_key] = analysis_payload
        return analysis_payload

    def _score_regular_row(
        result: dict[str, Any],
        orig_row: dict[str, Any],
        *,
        strict_playoff_rules: bool = True,
    ) -> dict[str, Any] | None:
        analysis = result.get("analysis") or {}
        analysis = _refresh_playoff_low_sample_analysis(result, orig_row, analysis)
        hit_rate  = float(analysis.get("hit_rate") or 0)
        avg       = float(analysis.get("average") or 0)
        line      = float(result.get("line") or orig_row.get("line") or 0)
        stat      = str(result.get("stat") or orig_row.get("stat") or "")

        # Prefer the analyzer's recommended side to keep parlay cards consistent
        # with the Player Analyzer. Fall back to hit-rate if unavailable.
        recommended_side = str(analysis.get("recommended_side") or "").upper()
        if recommended_side in {"OVER", "UNDER"}:
            side = recommended_side
            if side == "OVER":
                odds = float(orig_row.get("over_odds") or 1.91)
            else:
                odds = float(orig_row.get("under_odds") or 1.91)
            side_hit_rate = side_hit_rate_from_over_hit_rate(hit_rate, side)
        else:
            # Over hit_rate > 50  OVER is the stronger side
            if hit_rate >= 50:
                side = "OVER"
                odds = float(orig_row.get("over_odds") or 1.91)
            else:
                side = "UNDER"
                odds = float(orig_row.get("under_odds") or 1.91)
            side_hit_rate = side_hit_rate_from_over_hit_rate(hit_rate, side)

        # Skip unavailable players
        availability = analysis.get("availability") or {}
        if availability.get("is_unavailable"):
            return None

        # Skip props with odds below 1.40  too low to be meaningful in a parlay
        if odds < 1.40:
            return None

        matchup = analysis.get("matchup") or {}
        next_game_info = copy.deepcopy(matchup.get("next_game") or {})
        vs_position = matchup.get("vs_position") or {}
        matchup_delta_pct = vs_position.get("delta_pct") if isinstance(vs_position, dict) else None
        player_team_id_int = int((analysis.get("player") or {}).get("team_id") or 0)
        player_team_abbr = str((analysis.get("player") or {}).get("team_abbreviation") or next_game_info.get("player_team_abbreviation") or "").strip()
        next_game_info = PARLAY_SERVICE.apply_event_matchup_override(
            next_game_info=next_game_info,
            orig_row=orig_row,
            player_team_id=player_team_id_int,
            player_team_abbreviation=player_team_abbr,
            resolve_team_from_text=resolve_team_from_text,
        )
        pricing_snapshot = build_shared_market_pricing_snapshot(
            market_row=orig_row,
            over_odds=float(orig_row.get("over_odds") or 0.0),
            under_odds=float(orig_row.get("under_odds") or 0.0),
            hit_rate_pct=float(analysis.get("hit_rate") or 0.0),
            average=float(analysis.get("average") or 0.0),
            line=float(line),
            stat=stat,
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {},
            environment=analysis.get("environment") or {},
            variance=analysis.get("variance") or {},
            games_count=int(analysis.get("games_count") or 0),
            h2h_games_count=int((analysis.get("h2h") or {}).get("games_count") or 0),
        )
        fair_over_prob = pricing_snapshot["market"].get("fair_over")
        fair_under_prob = pricing_snapshot["market"].get("fair_under")
        model_over = float(pricing_snapshot["raw"]["over_probability"])
        model_under = float(pricing_snapshot["raw"]["under_probability"])
        calibrated_over = float(pricing_snapshot["calibrated"]["over_probability"])
        calibrated_under = float(pricing_snapshot["calibrated"]["under_probability"])

        if side == "OVER":
            model_prob = calibrated_over
            fair_prob = float(fair_over_prob or 0.5)
            edge_pct = float(pricing_snapshot["calibrated"]["over_edge_pct"])
            edge_pct_raw = float(pricing_snapshot["raw"]["over_edge_pct"])
            ev_decimal = float(pricing_snapshot["calibrated"]["over_ev"])
            ev_decimal_raw = float(pricing_snapshot["raw"]["over_ev"])
            ev_decimal_adjusted = float(pricing_snapshot["adjusted"]["over_ev"])
            calibrated_model_prob = calibrated_over
            calibrated_edge_pct = float(pricing_snapshot["calibrated"]["over_edge_pct"])
            calibrated_ev_decimal = float(pricing_snapshot["calibrated"]["over_ev"])
        else:
            model_prob = calibrated_under
            fair_prob = float(fair_under_prob or 0.5)
            edge_pct = float(pricing_snapshot["calibrated"]["under_edge_pct"])
            edge_pct_raw = float(pricing_snapshot["raw"]["under_edge_pct"])
            ev_decimal = float(pricing_snapshot["calibrated"]["under_ev"])
            ev_decimal_raw = float(pricing_snapshot["raw"]["under_ev"])
            ev_decimal_adjusted = float(pricing_snapshot["adjusted"]["under_ev"])
            calibrated_model_prob = calibrated_under
            calibrated_edge_pct = float(pricing_snapshot["calibrated"]["under_edge_pct"])
            calibrated_ev_decimal = float(pricing_snapshot["calibrated"]["under_ev"])
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
            edge=edge_pct,
            ev=ev_decimal,
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            availability=availability,
            opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {},
            environment=enriched_environment,
            stat=stat,
            player_position=(analysis.get("player") or {}).get("position") or '',
            line=line,
            average=avg,
            season_type=analysis.get("season_type"),
            playoff_game_number=int((next_game_info or {}).get("playoff_game_number") or 0) or None,
        )
        confidence_engine = apply_market_confidence_adjustment(
            confidence_engine,
            side=side,
            over_probability=fair_over_prob,
            under_probability=fair_under_prob,
            odds=odds,
        )
        confidence_engine = apply_backtest_ranking_adjustment(
            confidence_engine,
            stat=stat,
            side=side,
        )

        scoring_season_type = str(analysis.get("season_type") or season_type or "")
        is_playoffs_mode = scoring_season_type == SEASON_TYPE_PLAYOFFS
        confidence_score_value = int(float(confidence_engine.get("score") or 0))
        if is_playoffs_mode and strict_playoff_rules and confidence_score_value < 75:
            return None

        # Resolve team + opponent IDs from the analysis payload so the frontend
        # can auto-populate the analyzer without the user having to re-select manually.
        player_info    = analysis.get("player") or {}
        resolved_team_id_scored    = player_info.get("team_id")
        resolved_opp_info = PARLAY_SERVICE.fill_missing_opponent_from_event(
            opponent_info={
                "opponent_team_id": next_game_info.get("opponent_team_id"),
                "opponent_abbreviation": str(next_game_info.get("opponent_abbreviation") or "").strip(),
                "opponent_name": str(next_game_info.get("opponent_name") or "").strip(),
            },
            orig_row=orig_row,
            player_team_id=int(resolved_team_id_scored or 0),
            resolve_team_from_text=resolve_team_from_text,
        )
        resolved_opponent_team_id = resolved_opp_info.get("opponent_team_id")
        resolved_opponent_abbr = str(resolved_opp_info.get("opponent_abbreviation") or "").strip()
        h2h_games_count, h2h_side_hit_count, h2h_side_hit_rate = resolve_side_h2h_metrics(
            games=analysis.get("games") or [],
            h2h_payload=analysis.get("h2h") or {},
            side=side,
            opponent_abbreviation=resolved_opponent_abbr,
        )
        h2h_from_games = compute_side_h2h_metrics_from_games(
            games=analysis.get("games") or [],
            side=side,
            opponent_abbreviation=resolved_opponent_abbr,
        )
        h2h_from_payload = compute_side_h2h_metrics(
            h2h_payload=analysis.get("h2h") or {},
            side=side,
        )
        ranking_context = build_parlay_ranking_context(
            season_type=scoring_season_type,
            side=side,
            stat=stat,
            side_hit_rate=float(side_hit_rate),
            games_count=int(analysis.get("games_count") or 0),
            h2h_games_count=int(h2h_games_count or 0),
            h2h_side_hit_rate=float(h2h_side_hit_rate) if h2h_side_hit_rate is not None else None,
            model_probability=model_prob,
            fair_probability=fair_prob,
            edge_pct=edge_pct,
            ev_decimal=ev_decimal_adjusted,
            confidence_score=confidence_score_value,
            engine_ranking_score=confidence_engine.get("ranking_score"),
            line=line,
            average=avg,
        )
        ranking_hit_rate = float(ranking_context.get("ranking_hit_rate") or side_hit_rate)
        ranking_source = str(ranking_context.get("ranking_source") or "recent")

        scored_games_count = int(analysis.get("games_count") or 0)
        side_hit_count = max(0, min(scored_games_count, int(round((float(side_hit_rate) / 100.0) * scored_games_count))))
        matchup_payload = copy.deepcopy(analysis.get("matchup") or {})
        if next_game_info:
            matchup_payload["next_game"] = copy.deepcopy(next_game_info)
        return {
            "player_name": result.get("player_name") or "",
            "player_id": result.get("player_id"),
            "season_type": analysis.get("season_type") or season_type,
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
            **_build_parlay_odds_source_payload(orig_row, side, odds),
            "hit_rate": round(side_hit_rate, 1),
            "ranking_hit_rate": round(ranking_hit_rate, 1),
            "ranking_source": ranking_source,
            "ranking_sort_score": ranking_context.get("ranking_sort_score"),
            "ranking_blend_score": ranking_context.get("ranking_blend_score"),
            "ranking_profile": ranking_context.get("ranking_profile") or {},
            "playoff_strategy_tips": ranking_context.get("playoff_strategy_tips") or [],
            "h2h_weight_pct": ranking_context.get("h2h_weight_pct"),
            "h2h_games_count": h2h_games_count,
            "h2h_hit_count": h2h_side_hit_count,
            "h2h_hit_rate": round(h2h_side_hit_rate, 1) if h2h_side_hit_rate is not None else None,
            "h2h_debug": {
                "opponent_abbreviation": resolved_opponent_abbr,
                "from_games_count": int(h2h_from_games[0] or 0),
                "from_games_hits": int(h2h_from_games[1] or 0) if h2h_from_games[1] is not None else None,
                "from_payload_count": int(h2h_from_payload[0] or 0),
                "from_payload_hits": int(h2h_from_payload[1] or 0) if h2h_from_payload[1] is not None else None,
                "resolved_count": int(h2h_games_count or 0),
                "resolved_hits": int(h2h_side_hit_count or 0) if h2h_side_hit_count is not None else None,
                "season_type": scoring_season_type,
                "game_log_source": str(((analysis.get("debug") or {}).get("freshness") or {}).get("game_log_source") or ""),
                "filtered_pool_count": int(analysis.get("filtered_pool_count") or 0),
                "season_pool_count": int(analysis.get("season_pool_count") or 0),
            },
            "average": round(avg, 2),
            "games_count": scored_games_count,
            "hit_count": side_hit_count,
            "last_n": int(analysis.get("last_n") or last_n),
            "model_probability": round(model_prob * 100.0, 1),
            "model_probability_raw": round((model_under if side == "UNDER" else model_over) * 100.0, 1),
            "model_probability_calibrated": round(calibrated_model_prob * 100.0, 1),
            "implied_probability": round(fair_prob * 100.0, 1),
            "bookmaker": orig_row.get("bookmaker_title") or "N/A",
            "market_key": orig_row.get("market_key") or "",
            "availability_label": availability.get("label") or "Active",
            "availability": copy.deepcopy(availability),
            "matchup": matchup_payload,
            "environment": copy.deepcopy(enriched_environment),
            "confidence": confidence_engine.get("grade"),
            "confidence_score": confidence_score_value,
            "confidence_tone": confidence_engine.get("tone"),
            "confidence_tier": confidence_engine.get("tier"),
            "confidence_summary": confidence_engine.get("summary"),
            "confidence_tags": confidence_engine.get("tags") or [],
            "market_side": confidence_engine.get("market_side"),
            "market_disagrees": confidence_engine.get("market_disagrees"),
            "market_penalty": confidence_engine.get("market_penalty"),
            "ranking_score": confidence_engine.get("ranking_score"),
            "edge_pct": edge_pct,
            "edge_definition": EDGE_DEFINITION_MODEL_FAIR,
            "edge": edge_pct,
            "edge_raw": edge_pct_raw,
            "ev": ev_decimal_adjusted,
            "ev_raw": ev_decimal_raw,
            "calibrated_edge_pct": calibrated_edge_pct,
            "calibrated_ev": calibrated_ev_decimal,
            "calibration_reliability": round(float(pricing_snapshot["reliability"]["reliability"]) * 100.0, 1),
            "calibration_shrink": round(float(pricing_snapshot["reliability"]["shrink_strength"]) * 100.0, 1),
            "event_id": orig_row.get("event_id") or "",
            "game_label": orig_row.get("game_label") or "",
            "home_team": orig_row.get("home_team") or "",
            "away_team": orig_row.get("away_team") or "",
            "books_count": orig_row.get("books_count") or 1,
            "hold_percent": orig_row.get("hold_percent"),
            "fair_probability": round(fair_prob * 100.0, 1),
            "best_over_odds": orig_row.get("best_over_odds"),
            "best_under_odds": orig_row.get("best_under_odds"),
            "best_over_bookmaker": orig_row.get("best_over_bookmaker"),
            "best_under_bookmaker": orig_row.get("best_under_bookmaker"),
        }

    scored = PARLAY_SERVICE.run_scoring_rows(
        analysis_rows=analysis_rows,
        score_row=lambda result, orig_row: _score_regular_row(result, orig_row, strict_playoff_rules=True),
        emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
    )
    playoff_relaxed_fallback_applied = False
    if season_type == SEASON_TYPE_PLAYOFFS and not scored and analysis_rows:
        _emit_progress(
            progress_cb,
            "scoring_relaxed_fallback",
            total=len(analysis_rows),
            reason="strict_playoff_filters_removed_all",
        )
        scored = PARLAY_SERVICE.run_scoring_rows(
            analysis_rows=analysis_rows,
            score_row=lambda result, orig_row: _score_regular_row(result, orig_row, strict_playoff_rules=False),
            emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
        )
        playoff_relaxed_fallback_applied = bool(scored)

    # Primary rank: playoff blend in postseason, H2H/recent side hit rate otherwise.
    # The playoff blend caps tiny H2H samples so a new series does not overfit one game.
    scored.sort(
        key=lambda x: (
            x.get("ranking_sort_score", x.get("ranking_hit_rate", x.get("hit_rate", 0))),
            x.get("calibrated_edge_pct", x.get("edge", 0)),
            x.get("model_probability", 0),
            x.get("h2h_games_count", 0),
            x.get("ranking_score", x["confidence_score"]),
            x["odds"],
        ),
        reverse=True,
    )

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

    _emit_progress(
        progress_cb,
        "done",
        events_scraped=len(events),
        props_found=len(all_import_rows),
        props_analyzed=len(scored),
        errors=len(all_errors),
    )

    payload = {
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
        "playoff_relaxed_fallback_applied": playoff_relaxed_fallback_applied,
    }
    _submit_pg_write(_pg_write_parlay_builder_run, payload, request_hash_value)
    return payload


@app.post("/api/parlay-builder")
def parlay_builder(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    enforce_heavy_rate_limit(request, "parlay_builder")
    return PARLAY_SERVICE.run_sync(_parlay_builder_core, payload)


@app.post("/api/parlay-builder/async")
def parlay_builder_async(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    enforce_heavy_rate_limit(request, "parlay_builder_async")
    return PARLAY_SERVICE.run_async(_parlay_builder_core, payload)


@app.post("/api/parlay-builder/stream")
def parlay_builder_stream(request: Request, payload: dict[str, Any] = Body(...)) -> StreamingResponse:
    enforce_heavy_rate_limit(request, "parlay_builder_stream")
    return _stream_with_progress(_parlay_builder_core, payload)


@app.get("/api/team-injuries")
def get_team_injuries(team_name: str = Query(..., min_length=1)) -> dict[str, Any]:
    """
    Return injured/risky players for a team with their resolved NBA player IDs.
    Used by the injury-aware parlay builder and Player Analyzer lineup-context UI.
    """
    payload = get_cached_injury_report_payload()
    rows = [row for row in (payload.get("rows") or []) if row.get("team_name") == team_name]

    results: list[dict[str, Any]] = []
    for row in rows:
        status = str(row.get("status") or "").strip()
        if status not in UNAVAILABLE_STATUSES and status not in RISKY_STATUSES:
            continue
        raw_display = re.sub(r",(?!\s)", ", ", str(row.get("player_display") or "").strip())
        # Convert "Last, First"  "First Last" for player lookup
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


def _parlay_builder_injury_aware_core(payload: dict[str, Any], progress_cb=None) -> dict[str, Any]:
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
    api_keys: list[str] = [
        normalized for k in raw_keys
        if (normalized := normalize_odds_api_key(k))
    ]
    if not api_keys:
        raise HTTPException(status_code=400, detail="Provide at least one Odds API key in 'api_keys'.")

    season_type = normalize_requested_season_type(payload.get("season_type"))
    legs = _parse_bounded_int(
        payload.get("legs"),
        default=3,
        min_value=2,
        max_value=6,
        field_name="legs",
    )

    sport       = str(payload.get("sport") or "basketball_nba")
    regions     = str(payload.get("regions") or "us")
    odds_format = str(payload.get("odds_format") or "decimal")
    last_n      = _parse_bounded_int(
        payload.get("last_n"),
        default=10,
        min_value=1,
        max_value=PROP_ANALYSIS_MAX_LAST_N,
        field_name="last_n",
    )
    season      = str(payload.get("season") or current_nba_season())
    batch_size  = _parse_bounded_int(
        payload.get("batch_size"),
        default=3,
        min_value=1,
        max_value=PARLAY_BUILDER_MAX_BATCH_SIZE,
        field_name="batch_size",
    )
    requested_bookmakers = parse_requested_bookmakers(payload.get("bookmakers") or payload.get("bookmaker") or ODDS_DEFAULT_BOOKMAKERS)
    markets     = ",".join(ODDS_PARLAY_MARKETS)

    requested_event_ids: set[str] = set()
    raw_event_ids = payload.get("event_ids") or []
    if isinstance(raw_event_ids, list):
        requested_event_ids = {str(e).strip() for e in raw_event_ids if str(e).strip()}
    canonical_request_payload = {
        "event_ids": sorted(requested_event_ids),
        "legs": legs,
        "sport": sport,
        "regions": regions,
        "odds_format": odds_format,
        "last_n": last_n,
        "season": season,
        "season_type": season_type,
        "batch_size": batch_size,
        "bookmakers": requested_bookmakers,
        "markets": list(ODDS_PARLAY_MARKETS),
    }
    request_hash_value = _request_hash("parlay_builder_injury_aware", canonical_request_payload)
    bypass_cache = bool(payload.get("bypass_cache")) or season_type == SEASON_TYPE_PLAYOFFS
    if not bypass_cache:
        cached_run = _pg_read_parlay_builder_cache(canonical_request_payload, cache_scope="parlay_builder_injury_aware")
        if cached_run:
            return cached_run

    key_index = 0
    quota_log: list[dict[str, Any]] = []

    def next_key() -> str:
        nonlocal key_index
        key = api_keys[key_index % len(api_keys)]
        key_index += 1
        return key

    if requested_event_ids:
        events: list[dict[str, Any]] = [{"id": eid} for eid in sorted(requested_event_ids)]
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

    _emit_progress(progress_cb, "events_resolved", events=len(events))

    if not events:
        payload_out = {"legs": legs, "parlay": [], "parlay_odds": None, "all_props_scored": [],
                       "events_scraped": 0, "props_found": 0, "errors": [], "quota_log": quota_log,
                       "bookmakers": requested_bookmakers, "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers),
                       "injury_summary": [], "message": "No events found for today."}
        _submit_pg_write(_pg_write_parlay_builder_run, payload_out, request_hash_value)
        return payload_out

    scrape_payload = PARLAY_SERVICE.fetch_event_odds_batches(
        events=events,
        batch_size=batch_size,
        api_keys=api_keys,
        sport=sport,
        regions=regions,
        markets=markets,
        odds_format=odds_format,
        requested_bookmakers=requested_bookmakers,
        fetch_event_odds_payload=_fetch_event_odds_payload,
        submit_network_task=_submit_shared_network_task,
        emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
    )
    all_import_rows: list[dict[str, Any]] = list(scrape_payload.get("all_import_rows") or [])
    scrape_errors: list[dict[str, Any]] = list(scrape_payload.get("scrape_errors") or [])
    quota_log.extend(scrape_payload.get("quota_log") or [])

    if not all_import_rows:
        payload_out = {"legs": legs, "parlay": [], "parlay_odds": None, "all_props_scored": [],
                       "events_scraped": len(events), "props_found": 0, "errors": scrape_errors,
                       "quota_log": quota_log, "bookmakers": requested_bookmakers,
                       "cost_hint": build_odds_api_cost_hint(markets, requested_bookmakers), "injury_summary": [],
                       "message": "No props found across today's events. Check your API keys or try again later."}
        _submit_pg_write(_pg_write_parlay_builder_run, payload_out, request_hash_value)
        return payload_out

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

    #  Pre-fetch injury report once 
    try:
        inj_report = get_cached_injury_report_payload()
    except Exception:
        inj_report = {"ok": False, "rows": []}

    injury_report_identity = str(
        inj_report.get("report_url")
        or inj_report.get("report_label")
        or inj_report.get("report_timestamp")
        or ""
    )

    player_lookup_cache: dict[tuple[str, int | None], dict[str, Any] | None] = {}
    without_player_names_cache: dict[tuple[int, ...], list[str]] = {}
    boosted_analysis_cache: dict[tuple[Any, ...], dict[str, Any] | None] = {}
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

    # Build per-team injured player IDs cache: team_name  list[int]
    _team_injury_context_cache: dict[str, dict[str, Any]] = {}

    def get_injured_context_for_team(team_name: str, team_id: int | None = None) -> dict[str, Any]:
        cache_key = canonicalize_team_name(team_name, team_id=team_id)
        if cache_key in _team_injury_context_cache:
            return copy.deepcopy(_team_injury_context_cache[cache_key])
        rows_inj = INJURY_SERVICE.get_team_rows(inj_report, team_name)
        ids: list[int] = []
        names: list[str] = []
        for row in rows_inj:
            status = str(row.get("status") or "")
            if status not in UNAVAILABLE_STATUSES and status not in RISKY_STATUSES:
                continue
            raw_disp = re.sub(r",(?!\s)", ", ", str(row.get("player_display") or "").strip())
            if raw_disp:
                names.append(raw_disp)
            parts = [p.strip() for p in raw_disp.split(",")]
            name_lk = f"{parts[1]} {parts[0]}" if len(parts) == 2 else raw_disp
            player = (
                cached_find_player_by_name(name_lk, team_id=team_id)
                or cached_find_player_by_name(raw_disp, team_id=team_id)
                or cached_find_player_by_name(name_lk)
                or cached_find_player_by_name(raw_disp)
            )
            if player:
                ids.append(int(player["id"]))
        deduped_ids = sorted(set(ids), key=lambda teammate_id: (-cached_teammate_impact(teammate_id), teammate_id))
        deduped_names = []
        seen_names: set[str] = set()
        for name in names:
            normalized_name = normalize_name(name)
            if normalized_name in seen_names:
                continue
            seen_names.add(normalized_name)
            deduped_names.append(name)
        payload = {
            "ids": deduped_ids,
            "names": deduped_names,
        }
        _team_injury_context_cache[cache_key] = copy.deepcopy(payload)
        return payload

    defaults = {"last_n": last_n, "season": season, "season_type": season_type}
    local_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
    local_cache_lock = Lock()
    analysis_prep = PARLAY_SERVICE.prepare_analysis_jobs(
        all_import_rows=all_import_rows,
        resolve_player=lambda player_name: cached_find_player_by_name(player_name),
    )
    deduped_prepared: list[tuple[dict[str, Any], dict[str, Any]]] = list(analysis_prep["deduped_prepared"] or [])
    deduped_prepared = [
        (_apply_parlay_event_context_to_bulk_row(bulk_row, orig_row), orig_row)
        for bulk_row, orig_row in deduped_prepared
    ]
    analysis_errors: list[dict[str, Any]] = list(analysis_prep["analysis_errors"] or [])
    unique_player_ids: set[int] = set(analysis_prep["unique_player_ids"] or set())
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
        ids_list = list(ids_to_fetch)
        for batch_start in range(0, len(ids_list), prewarm_workers):
            chunk = ids_list[batch_start : batch_start + prewarm_workers]
            futures = [_submit_shared_analysis_task(_prefetch_log, pid) for pid in chunk]
            for future in futures:
                try:
                    future.result()
                except Exception:
                    pass

    max_workers = min(BULK_ANALYSIS_MAX_WORKERS, max(1, len(deduped_prepared)))
    analysis_rows, analysis_errors_run = PARLAY_SERVICE.run_bulk_analysis(
        deduped_prepared=deduped_prepared,
        defaults=defaults,
        local_cache=local_cache,
        max_workers=max_workers,
        build_bulk_prop_item=_build_bulk_prop_item,
        submit_analysis_task=_submit_shared_analysis_task,
        emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
    )
    if analysis_errors_run:
        analysis_errors.extend(analysis_errors_run)

    #  Injury-aware scoring 
    playoff_live_refresh_cache: dict[tuple[int, str, float], dict[str, Any]] = {}

    def _refresh_playoff_low_sample_analysis(
        result_row: dict[str, Any],
        market_row: dict[str, Any],
        analysis_payload: dict[str, Any],
    ) -> dict[str, Any]:
        if season_type != SEASON_TYPE_PLAYOFFS:
            return analysis_payload
        current_games = int((analysis_payload or {}).get("games_count") or 0)
        if current_games >= 3:
            return analysis_payload
        player_id_val = int(result_row.get("player_id") or market_row.get("player_id") or 0)
        stat_val = str(result_row.get("stat") or market_row.get("stat") or "").strip().upper()
        line_val = float(result_row.get("line") or market_row.get("line") or 0.0)
        if not player_id_val or not stat_val:
            return analysis_payload
        team_id_val = int(result_row.get("team_id") or market_row.get("team_id") or 0) or None
        override_opponent_id_val = int(result_row.get("override_opponent_id") or market_row.get("override_opponent_id") or 0) or None
        refresh_key = (
            player_id_val,
            stat_val,
            round(line_val, 4),
            int(team_id_val or 0),
            int(override_opponent_id_val or 0),
        )
        cached_refresh = playoff_live_refresh_cache.get(refresh_key)
        if cached_refresh:
            return cached_refresh
        try:
            for cache_key in list(ANALYSIS_CACHE.keys()):
                try:
                    key_player_id = int(cache_key[1])
                    key_stat = str(cache_key[2] or "").strip().upper()
                    key_line = float(cache_key[3] or 0.0)
                    key_season_type = str(cache_key[6] or "")
                except Exception:
                    continue
                if (
                    key_player_id == player_id_val
                    and key_stat == stat_val
                    and abs(key_line - line_val) <= 1e-6
                    and key_season_type == SEASON_TYPE_PLAYOFFS
                ):
                    ANALYSIS_CACHE.pop(cache_key, None)
            game_log_key = (player_id_val, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
            GAME_LOG_CACHE.pop(game_log_key, None)
            GAME_LOG_FAILURE_META.pop(game_log_key, None)
            RECENT_GAME_LOG_CACHE.pop((player_id_val, season, season_type, 20, GAME_LOG_CACHE_SCHEMA_VERSION), None)
            refreshed = build_prop_analysis_payload(
                player_id=player_id_val,
                stat=stat_val,
                line=line_val,
                last_n=last_n,
                season=season,
                season_type=season_type,
                team_id=team_id_val,
                override_opponent_id=override_opponent_id_val,
            )
            if int((refreshed or {}).get("games_count") or 0) > current_games:
                playoff_live_refresh_cache[refresh_key] = refreshed
                return refreshed
        except Exception:
            return analysis_payload
        playoff_live_refresh_cache[refresh_key] = analysis_payload
        return analysis_payload

    def injury_aware_score(
        result: dict[str, Any],
        orig_row: dict[str, Any],
        *,
        strict_playoff_rules: bool = True,
    ) -> dict[str, Any] | None:
        """Score a prop with injury-aware without_player boosting."""
        analysis = result.get("analysis") or {}
        analysis = _refresh_playoff_low_sample_analysis(result, orig_row, analysis)
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
        # player dict carries team_id but often no team_name  resolve via TEAM_LOOKUP.
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
        team_name = canonicalize_team_name(team_name, team_id=int(team_id_raw) if team_id_raw else None)
        LOGGER.debug(
            "injury_aware_score team resolution: player=%s team_id=%s  team_name=%r",
            result.get("player_name"), team_id_raw, team_name
        )

        # Try injury-aware re-analysis
        injury_boost = False
        injury_filter_player_ids: list[int] = []
        injury_filter_player_names: list[str] = []
        team_injury_player_names: list[str] = []
        base_hit_rate = hit_rate
        base_games_count = int(analysis.get("games_count") or 0)
        base_over_hit_count = int(analysis.get("hit_count") or 0)
        active_games_count = base_games_count
        active_over_hit_count = base_over_hit_count
        active_h2h_payload: dict[str, Any] = copy.deepcopy(analysis.get("h2h") or {})
        active_h2h_games: list[dict[str, Any]] = list(analysis.get("games") or [])

        if team_name and player_id:
            injury_context = get_injured_context_for_team(team_name, team_id=int(team_id_raw) if team_id_raw else None)
            inj_ids = list(injury_context.get("ids") or [])
            # Collect names for display
            team_injury_player_names = cached_without_player_names(inj_ids) if inj_ids else list(injury_context.get("names") or [])
            best_usable_combo: tuple[list[int], float, float, int, int, dict[str, Any]] | None = None
            min_boost_games_required = 1 if str(season_type or "") == SEASON_TYPE_PLAYOFFS else 5
            combo_plan = _build_injury_combo_plan(base_hit_rate=base_hit_rate)
            # Try top 1, top 2, then top 3 impacted absences.
            for max_filters in combo_plan:
                candidate_ids = inj_ids[:max_filters]
                if not candidate_ids:
                    break
                try:
                    boost_team_id = int(result.get("team_id") or team_id_raw or 0) or None
                    boost_override_opponent_id = int(result.get("override_opponent_id") or orig_row.get("override_opponent_id") or 0) or None
                    boost_cache_key = (
                        player_id,
                        stat,
                        float(line),
                        int(last_n),
                        str(season),
                        str(season_type),
                        tuple(candidate_ids),
                        int(boost_team_id or 0),
                        int(boost_override_opponent_id or 0),
                        injury_report_identity,
                    )
                    if boost_cache_key not in boosted_analysis_cache:
                        cached_boosted_analysis = _get_injury_aware_boost_cache(boost_cache_key)
                        if cached_boosted_analysis is not None:
                            boosted_analysis_cache[boost_cache_key] = cached_boosted_analysis
                        else:
                            boosted_analysis_cache[boost_cache_key] = build_prop_analysis_payload(
                                player_id=player_id,
                                stat=stat,
                                line=line,
                                last_n=last_n,
                                season=season,
                                season_type=season_type,
                                without_player_ids=candidate_ids,
                                team_id=boost_team_id,
                                override_opponent_id=boost_override_opponent_id,
                            )
                            _set_injury_aware_boost_cache(boost_cache_key, boosted_analysis_cache[boost_cache_key])
                    boosted_analysis = boosted_analysis_cache[boost_cache_key]
                    if not boosted_analysis:
                        break
                    boosted_games = int(boosted_analysis.get("games_count") or 0)
                    boosted_hr    = float(boosted_analysis.get("hit_rate") or 0)
                    # Playoff "without player" windows are naturally small (series-level).
                    # Allow those contexts instead of forcing regular-season sized samples.
                    if boosted_games >= min_boost_games_required:
                        boosted_avg = float(boosted_analysis.get("average") or avg)
                        boosted_over_hit_count = int(boosted_analysis.get("hit_count") or 0)
                        if best_usable_combo is None or boosted_hr > best_usable_combo[1] or (
                            boosted_hr == best_usable_combo[1] and boosted_games > best_usable_combo[3]
                        ):
                            best_usable_combo = (
                                list(candidate_ids),
                                boosted_hr,
                                boosted_avg,
                                boosted_games,
                                boosted_over_hit_count,
                                copy.deepcopy(boosted_analysis.get("h2h") or {}),
                                list(boosted_analysis.get("games") or []),
                            )
                        if _should_stop_injury_combo_search(
                            base_hit_rate=base_hit_rate,
                            boosted_hit_rate=boosted_hr,
                            boosted_games=boosted_games,
                            combo_size=max_filters,
                            combo_plan=combo_plan,
                        ):
                            break
                except Exception:
                    break
            if best_usable_combo is not None:
                injury_filter_player_ids = list(best_usable_combo[0])
                injury_filter_player_names = cached_without_player_names(injury_filter_player_ids)
                base_games_count = int(best_usable_combo[3])
                active_games_count = int(best_usable_combo[3])
                active_over_hit_count = int(best_usable_combo[4])
                active_h2h_payload = copy.deepcopy(best_usable_combo[5] or {})
                active_h2h_games = list(best_usable_combo[6] or [])
                if float(best_usable_combo[1]) > hit_rate:
                    hit_rate = float(best_usable_combo[1])
                    avg = float(best_usable_combo[2])
                    injury_boost = True

        recommended_side = str(analysis.get("recommended_side") or "").upper()
        if recommended_side in {"OVER", "UNDER"}:
            side = recommended_side
            if side == "OVER":
                odds = float(orig_row.get("over_odds") or 1.91)
            else:
                odds = float(orig_row.get("under_odds") or 1.91)
        else:
            if hit_rate >= 50:
                side = "OVER"
                odds = float(orig_row.get("over_odds") or 1.91)
            else:
                side = "UNDER"
                odds = float(orig_row.get("under_odds") or 1.91)

        # Always compute displayed sample hits/rate from real filtered rows, not from rounded percentages.
        clamped_games_count = max(0, int(active_games_count))
        clamped_over_hits = max(0, min(clamped_games_count, int(active_over_hit_count)))
        if side == "OVER":
            side_hit_count = clamped_over_hits
        else:
            side_hit_count = max(0, clamped_games_count - clamped_over_hits)
        side_hit_rate = round((side_hit_count / clamped_games_count) * 100.0, 1) if clamped_games_count > 0 else 0.0

        if odds < 1.40:
            return None

        matchup = analysis.get("matchup") or {}
        next_game_info = copy.deepcopy(matchup.get("next_game") or {})
        vs_position = matchup.get("vs_position") or {}
        matchup_delta_pct = vs_position.get("delta_pct") if isinstance(vs_position, dict) else None
        player_team_id_int = int((analysis.get("player") or {}).get("team_id") or 0)
        player_team_abbr = str((analysis.get("player") or {}).get("team_abbreviation") or next_game_info.get("player_team_abbreviation") or "").strip()
        next_game_info = PARLAY_SERVICE.apply_event_matchup_override(
            next_game_info=next_game_info,
            orig_row=orig_row,
            player_team_id=player_team_id_int,
            player_team_abbreviation=player_team_abbr,
            resolve_team_from_text=resolve_team_from_text,
        )
        pricing_snapshot = build_shared_market_pricing_snapshot(
            market_row=orig_row,
            over_odds=float(orig_row.get("over_odds") or 0.0),
            under_odds=float(orig_row.get("under_odds") or 0.0),
            hit_rate_pct=float(hit_rate),
            average=float(avg),
            line=float(line),
            stat=stat,
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {},
            environment=analysis.get("environment") or {},
            variance=analysis.get("variance") or {},
            games_count=int(active_games_count or 0),
            h2h_games_count=int((active_h2h_payload or {}).get("games_count") or 0),
        )
        fair_over_prob = pricing_snapshot["market"].get("fair_over")
        fair_under_prob = pricing_snapshot["market"].get("fair_under")
        model_over = float(pricing_snapshot["raw"]["over_probability"])
        model_under = float(pricing_snapshot["raw"]["under_probability"])
        calibrated_over = float(pricing_snapshot["calibrated"]["over_probability"])
        calibrated_under = float(pricing_snapshot["calibrated"]["under_probability"])
        if side == "OVER":
            model_prob = calibrated_over
            fair_prob = float(fair_over_prob or 0.5)
            _computed_edge = float(pricing_snapshot["calibrated"]["over_edge_pct"])
            _computed_edge_raw = float(pricing_snapshot["raw"]["over_edge_pct"])
            _computed_ev = float(pricing_snapshot["adjusted"]["over_ev"])
            _computed_ev_raw = float(pricing_snapshot["raw"]["over_ev"])
            calibrated_model_prob = calibrated_over
            calibrated_edge_pct = float(pricing_snapshot["calibrated"]["over_edge_pct"])
            calibrated_ev = float(pricing_snapshot["calibrated"]["over_ev"])
        else:
            model_prob = calibrated_under
            fair_prob = float(fair_under_prob or 0.5)
            _computed_edge = float(pricing_snapshot["calibrated"]["under_edge_pct"])
            _computed_edge_raw = float(pricing_snapshot["raw"]["under_edge_pct"])
            _computed_ev = float(pricing_snapshot["adjusted"]["under_ev"])
            _computed_ev_raw = float(pricing_snapshot["raw"]["under_ev"])
            calibrated_model_prob = calibrated_under
            calibrated_edge_pct = float(pricing_snapshot["calibrated"]["under_edge_pct"])
            calibrated_ev = float(pricing_snapshot["calibrated"]["under_ev"])
        enriched_environment = enrich_environment_with_market_context(
            analysis.get("environment") or {},
            orig_row,
            player_team_name=team_name,
            player_team_abbreviation=player_info.get("team_abbreviation") or "",
        )
        confidence_engine = build_confidence_engine(
            side=side, hit_rate=float(side_hit_rate), games_count=int(active_games_count or 0),
            edge=_computed_edge,
            ev=_computed_ev,
            matchup_delta_pct=float(matchup_delta_pct) if matchup_delta_pct is not None else None,
            availability=availability, opportunity=analysis.get("opportunity") or {},
            team_context=analysis.get("team_context") or {}, environment=enriched_environment,
            stat=stat, player_position=(analysis.get("player") or {}).get("position") or "",
            line=line, average=avg,
            season_type=analysis.get("season_type"),
            playoff_game_number=int((next_game_info or {}).get("playoff_game_number") or 0) or None,
        )
        confidence_engine = apply_market_confidence_adjustment(
            confidence_engine,
            side=side,
            over_probability=fair_over_prob,
            under_probability=fair_under_prob,
            odds=odds,
        )
        confidence_engine = apply_backtest_ranking_adjustment(
            confidence_engine,
            stat=stat,
            side=side,
        )

        scoring_season_type = str(analysis.get("season_type") or season_type or "")
        is_playoffs_mode = scoring_season_type == SEASON_TYPE_PLAYOFFS
        confidence_score_value = int(float(confidence_engine.get("score") or 0))
        if is_playoffs_mode and strict_playoff_rules and confidence_score_value < 75:
            return None

        resolved_team_id = player_info.get("team_id")
        matchup_payload = copy.deepcopy(analysis.get("matchup") or {})
        if next_game_info:
            matchup_payload["next_game"] = copy.deepcopy(next_game_info)
        opponent_info = PARLAY_SERVICE.fill_missing_opponent_from_event(
            opponent_info=copy.deepcopy(next_game_info),
            orig_row=orig_row,
            player_team_id=int(resolved_team_id or 0),
            resolve_team_from_text=resolve_team_from_text,
        )
        h2h_games_count, h2h_side_hit_count, h2h_side_hit_rate = resolve_side_h2h_metrics(
            games=active_h2h_games,
            h2h_payload=active_h2h_payload or {},
            side=side,
            opponent_abbreviation=str(opponent_info.get("opponent_abbreviation") or ""),
        )
        h2h_from_games = compute_side_h2h_metrics_from_games(
            games=active_h2h_games,
            side=side,
            opponent_abbreviation=str(opponent_info.get("opponent_abbreviation") or ""),
        )
        h2h_from_payload = compute_side_h2h_metrics(
            h2h_payload=active_h2h_payload or {},
            side=side,
        )
        # Keep injury-aware matchup context, but never downsample playoff H2H ranking/sample
        # versus the base analyzer payload. Prefer the larger valid H2H sample so parlay cards
        # stay consistent with the player context modal.
        base_h2h_games_count, base_h2h_side_hit_count, base_h2h_side_hit_rate = resolve_side_h2h_metrics(
            games=analysis.get("games") or [],
            h2h_payload=analysis.get("h2h") or {},
            side=side,
            opponent_abbreviation=str(opponent_info.get("opponent_abbreviation") or ""),
        )
        if int(base_h2h_games_count or 0) > int(h2h_games_count or 0):
            h2h_games_count = int(base_h2h_games_count or 0)
            h2h_side_hit_count = int(base_h2h_side_hit_count or 0)
            h2h_side_hit_rate = float(base_h2h_side_hit_rate) if base_h2h_side_hit_rate is not None else None
        ranking_context = build_parlay_ranking_context(
            season_type=scoring_season_type,
            side=side,
            stat=stat,
            side_hit_rate=float(side_hit_rate),
            games_count=int(active_games_count or 0),
            h2h_games_count=int(h2h_games_count or 0),
            h2h_side_hit_rate=float(h2h_side_hit_rate) if h2h_side_hit_rate is not None else None,
            model_probability=model_prob,
            fair_probability=fair_prob,
            edge_pct=_computed_edge,
            ev_decimal=_computed_ev,
            confidence_score=confidence_score_value,
            engine_ranking_score=confidence_engine.get("ranking_score"),
            line=line,
            average=avg,
        )
        ranking_hit_rate = float(ranking_context.get("ranking_hit_rate") or side_hit_rate)
        ranking_source = str(ranking_context.get("ranking_source") or "recent")

        return {
            "player_name": result.get("player_name") or "",
            "player_id": player_id,
            "season_type": analysis.get("season_type") or season_type,
            "team_id": resolved_team_id,
            "team_name": team_name,
            "team_abbreviation": player_info.get("team_abbreviation") or "",
            "player_position": player_info.get("position") or "",
            "player_jersey": player_info.get("jersey") or "",
            "opponent_team_id": opponent_info.get("opponent_team_id"),
            "opponent_abbreviation": str(opponent_info.get("opponent_abbreviation") or ""),
            "stat": stat, "line": line, "side": side, "odds": odds,
            **_build_parlay_odds_source_payload(orig_row, side, odds),
            "hit_rate": round(side_hit_rate, 1),
            "ranking_hit_rate": round(ranking_hit_rate, 1),
            "ranking_source": ranking_source,
            "ranking_sort_score": ranking_context.get("ranking_sort_score"),
            "ranking_blend_score": ranking_context.get("ranking_blend_score"),
            "ranking_profile": ranking_context.get("ranking_profile") or {},
            "playoff_strategy_tips": ranking_context.get("playoff_strategy_tips") or [],
            "h2h_weight_pct": ranking_context.get("h2h_weight_pct"),
            "h2h_games_count": h2h_games_count,
            "h2h_hit_count": h2h_side_hit_count,
            "h2h_hit_rate": round(h2h_side_hit_rate, 1) if h2h_side_hit_rate is not None else None,
            "h2h_debug": {
                "opponent_abbreviation": str(opponent_info.get("opponent_abbreviation") or ""),
                "from_games_count": int(h2h_from_games[0] or 0),
                "from_games_hits": int(h2h_from_games[1] or 0) if h2h_from_games[1] is not None else None,
                "from_payload_count": int(h2h_from_payload[0] or 0),
                "from_payload_hits": int(h2h_from_payload[1] or 0) if h2h_from_payload[1] is not None else None,
                "resolved_count": int(h2h_games_count or 0),
                "resolved_hits": int(h2h_side_hit_count or 0) if h2h_side_hit_count is not None else None,
                "season_type": scoring_season_type,
                "game_log_source": str(((analysis.get("debug") or {}).get("freshness") or {}).get("game_log_source") or ""),
                "filtered_pool_count": int(analysis.get("filtered_pool_count") or 0),
                "season_pool_count": int(analysis.get("season_pool_count") or 0),
            },
            "base_hit_rate": round(base_hit_rate, 1),
            "average": round(avg, 2),
            "games_count": clamped_games_count,
            "hit_count": side_hit_count,
            "ev": _computed_ev,
            "ev_raw": _computed_ev_raw,
            "edge": _computed_edge,
            "edge_raw": _computed_edge_raw,
            "edge_pct": _computed_edge,
            "edge_definition": EDGE_DEFINITION_MODEL_FAIR,
            "model_probability": round(model_prob * 100.0, 1),
            "model_probability_raw": round((model_under if side == "UNDER" else model_over) * 100.0, 1),
            "model_probability_calibrated": round(calibrated_model_prob * 100.0, 1),
            "implied_probability": round(fair_prob * 100.0, 1),
            "calibrated_edge_pct": calibrated_edge_pct,
            "calibrated_ev": calibrated_ev,
            "calibration_reliability": round(float(pricing_snapshot["reliability"]["reliability"]) * 100.0, 1),
            "calibration_shrink": round(float(pricing_snapshot["reliability"]["shrink_strength"]) * 100.0, 1),
            "last_n": last_n,
            "bookmaker": orig_row.get("bookmaker_title") or "N/A",
            "market_key": orig_row.get("market_key") or "",
            "availability_label": availability.get("label") or "Active",
            "availability": copy.deepcopy(availability),
            "matchup": matchup_payload,
            "environment": copy.deepcopy(enriched_environment),
            "confidence": confidence_engine.get("grade"),
            "confidence_score": confidence_score_value,
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
            "injury_filter_count": len(injury_filter_player_ids),
            "injury_filter_mode": (
                "combo" if len(injury_filter_player_ids) > 1 else ("single" if len(injury_filter_player_ids) == 1 else "none")
            ),
            "team_injury_player_names": team_injury_player_names,
            "books_count": orig_row.get("books_count") or 1,
            "hold_percent": orig_row.get("hold_percent"),
            "fair_probability": round(fair_prob * 100.0, 1) if fair_prob is not None else None,
            "best_over_odds": orig_row.get("best_over_odds"),
            "best_under_odds": orig_row.get("best_under_odds"),
            "best_over_bookmaker": orig_row.get("best_over_bookmaker"),
            "best_under_bookmaker": orig_row.get("best_under_bookmaker"),
        }

    scored = PARLAY_SERVICE.run_scoring_rows(
        analysis_rows=analysis_rows,
        score_row=lambda result, orig_row: injury_aware_score(result, orig_row, strict_playoff_rules=True),
        emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
    )
    playoff_relaxed_fallback_applied = False
    if season_type == SEASON_TYPE_PLAYOFFS and not scored and analysis_rows:
        _emit_progress(
            progress_cb,
            "scoring_relaxed_fallback",
            total=len(analysis_rows),
            reason="strict_playoff_filters_removed_all",
        )
        scored = PARLAY_SERVICE.run_scoring_rows(
            analysis_rows=analysis_rows,
            score_row=lambda result, orig_row: injury_aware_score(result, orig_row, strict_playoff_rules=False),
            emit_progress=lambda stage, meta: _emit_progress(progress_cb, stage, **meta),
        )
        playoff_relaxed_fallback_applied = bool(scored)

    scored.sort(
        key=lambda x: (
            x.get("ranking_sort_score", x.get("ranking_hit_rate", x.get("hit_rate", 0))),
            x.get("calibrated_edge_pct", x.get("edge", 0)),
            x.get("model_probability", 0),
            x.get("h2h_games_count", 0),
            x.get("ranking_score", x["confidence_score"]),
            x["odds"],
        ),
        reverse=True,
    )

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
        injury_context = get_injured_context_for_team(tn)
        inj_ids = list(injury_context.get("ids") or [])
        if inj_ids:
            injury_summary.append({
                "team_name": tn,
                "injured_player_names": cached_without_player_names(inj_ids),
                "count": len(inj_ids),
            })
        elif injury_context.get("names"):
            injury_summary.append({
                "team_name": tn,
                "injured_player_names": list(injury_context.get("names") or []),
                "count": len(injury_context.get("names") or []),
            })

    all_errors = scrape_errors + analysis_errors
    _emit_progress(
        progress_cb,
        "done",
        events_scraped=len(events),
        props_found=len(all_import_rows),
        props_analyzed=len(scored),
        errors=len(all_errors),
    )
    payload = {
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
        "playoff_relaxed_fallback_applied": playoff_relaxed_fallback_applied,
    }
    _submit_pg_write(_pg_write_parlay_builder_run, payload, request_hash_value)
    return payload


@app.post("/api/parlay-builder-injury-aware")
def parlay_builder_injury_aware(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    enforce_heavy_rate_limit(request, "parlay_builder_injury_aware")
    return PARLAY_SERVICE.run_sync(_parlay_builder_injury_aware_core, payload)


@app.post("/api/parlay-builder-injury-aware/async")
def parlay_builder_injury_aware_async(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    enforce_heavy_rate_limit(request, "parlay_builder_injury_aware_async")
    return PARLAY_SERVICE.run_async(_parlay_builder_injury_aware_core, payload)


@app.post("/api/parlay-builder-injury-aware/stream")
def parlay_builder_injury_aware_stream(request: Request, payload: dict[str, Any] = Body(...)) -> StreamingResponse:
    enforce_heavy_rate_limit(request, "parlay_builder_injury_aware_stream")
    return _stream_with_progress(_parlay_builder_injury_aware_core, payload)


@app.get("/api/todays-games")
@timed_call("todays_games_endpoint")
def todays_games(game_date: str | None = None) -> dict[str, Any]:
    requested_date = game_date or current_nba_game_date()
    resolved_date = requested_date
    rows = fetch_scoreboard_games(requested_date)
    fallback_used = False
    live_scoreboard: dict[str, Any] | None = None

    if not rows:
        base_date = datetime.strptime(requested_date, "%Y-%m-%d").date()
        # If no explicit date is requested, search around "today" to absorb timezone
        # offsets (e.g., Asia/Manila vs ET) and still show the active slate.
        probe_offsets = [1, 2, 3] if game_date else [-1, 1, 2]
        for offset in probe_offsets:
            probe_date = (base_date + timedelta(days=offset)).strftime("%Y-%m-%d")
            probe_rows = fetch_scoreboard_games(probe_date)
            if probe_rows:
                rows = probe_rows
                resolved_date = probe_date
                fallback_used = True
                break

    if not rows:
        live_scoreboard = _fetch_nba_live_scoreboard()
        live_scoreboard_payload = (live_scoreboard or {}).get("scoreboard") or {}
        live_scoreboard_date = str(live_scoreboard_payload.get("gameDate") or "").strip()
        live_games = [game for game in (live_scoreboard_payload.get("games") or []) if isinstance(game, dict)]
        if live_games and (not game_date or live_scoreboard_date == requested_date):
            rows = []
            resolved_date = live_scoreboard_date or resolved_date
            fallback_used = True
            for live_game in live_games:
                live_home = (live_game or {}).get("homeTeam") or {}
                live_away = (live_game or {}).get("awayTeam") or {}
                rows.append({
                    "GAME_ID": str((live_game or {}).get("gameId") or "").strip(),
                    "GAME_DATE_EST": resolved_date,
                    "HOME_TEAM_ID": safe_int_score(live_home.get("teamId"), 0),
                    "VISITOR_TEAM_ID": safe_int_score(live_away.get("teamId"), 0),
                    "GAME_STATUS_TEXT": str((live_game or {}).get("gameStatusText") or "").strip(),
                    "PTS_HOME": safe_int_score(live_home.get("score"), 0),
                    "PTS_AWAY": safe_int_score(live_away.get("score"), 0),
                })

    report_payload = get_cached_injury_report_payload_fast()
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
    live_scoreboard = live_scoreboard or _fetch_nba_live_scoreboard()
    live_games_by_id: dict[str, dict[str, Any]] = {}
    live_scoreboard_date = str(((live_scoreboard or {}).get("scoreboard") or {}).get("gameDate") or "").strip()
    if live_scoreboard_date == str(resolved_date):
        for live_game in (((live_scoreboard or {}).get("scoreboard") or {}).get("games") or []):
            live_game_id = str((live_game or {}).get("gameId") or "").strip()
            if live_game_id:
                live_games_by_id[live_game_id] = live_game
    needs_schedule_context = any(
        not str(row.get("GAME_STATUS_TEXT") or "").strip()
        or str(row.get("GAME_ID") or "").strip()[:3] in {"001", "004", "005"}
        for row in rows
    )
    schedule_payload = _fetch_nba_schedule_league() if needs_schedule_context else None
    schedule_games_by_id: dict[str, dict[str, Any]] = {}
    schedule_games_by_matchup: dict[str, dict[str, Any]] = {}
    for date_group in (((schedule_payload or {}).get("leagueSchedule") or {}).get("gameDates") or []):
        schedule_date = str((date_group or {}).get("gameDate") or (date_group or {}).get("gameDateEst") or "").strip()
        for schedule_game in ((date_group or {}).get("games") or []):
            if not isinstance(schedule_game, dict):
                continue
            schedule_game_id = str(schedule_game.get("gameId") or "").strip()
            if schedule_game_id:
                schedule_games_by_id[schedule_game_id] = schedule_game
            schedule_home = schedule_game.get("homeTeam") or {}
            schedule_away = schedule_game.get("awayTeam") or {}
            schedule_home_id = safe_int_score(schedule_home.get("teamId"), 0)
            schedule_away_id = safe_int_score(schedule_away.get("teamId"), 0)
            if schedule_date and schedule_home_id and schedule_away_id:
                schedule_games_by_matchup[f"{schedule_date}|{schedule_away_id}@{schedule_home_id}"] = schedule_game

    def _team_record_from_live(live_team: dict[str, Any] | None) -> str:
        live_team = live_team or {}
        wins = live_team.get("wins")
        losses = live_team.get("losses")
        if wins in (None, "") or losses in (None, ""):
            return ""
        try:
            return f"{int(wins)}-{int(losses)}"
        except Exception:
            return ""

    def _team_record_for_team(team_id: int, *games_to_scan: dict[str, Any] | None) -> str:
        for game_to_scan in games_to_scan:
            for team_key in ("homeTeam", "awayTeam"):
                team_payload = (game_to_scan or {}).get(team_key) or {}
                if safe_int_score(team_payload.get("teamId"), 0) == team_id:
                    record = _team_record_from_live(team_payload)
                    if record:
                        return record
        return ""

    def _team_seed_for_team(team_id: int, *games_to_scan: dict[str, Any] | None) -> Any:
        for game_to_scan in games_to_scan:
            for team_key in ("homeTeam", "awayTeam"):
                team_payload = (game_to_scan or {}).get(team_key) or {}
                if safe_int_score(team_payload.get("teamId"), 0) == team_id and team_payload.get("seed") is not None:
                    return team_payload.get("seed")
        return None

    def _format_live_tipoff_pht(live_game: dict[str, Any] | None) -> str:
        raw_utc = str((live_game or {}).get("gameTimeUTC") or (live_game or {}).get("gameDateTimeUTC") or "").strip()
        if not raw_utc:
            return ""
        try:
            parsed = datetime.fromisoformat(raw_utc.replace("Z", "+00:00"))
            pht_dt = parsed.astimezone(APP_ZONEINFO)
            hh = pht_dt.strftime("%I").lstrip("0") or "0"
            return f"{hh}:{pht_dt.strftime('%M')} {pht_dt.strftime('%p').lower()} PHT"
        except Exception:
            return ""

    def _infer_scoreboard_season_type(
        row: dict[str, Any],
        live_game: dict[str, Any] | None,
        schedule_game: dict[str, Any] | None,
    ) -> str:
        game_id_prefix = str(row.get("GAME_ID") or (live_game or {}).get("gameId") or (schedule_game or {}).get("gameId") or "")[:3]
        live_label = " ".join(
            str((live_game or {}).get(key) or (schedule_game or {}).get(key) or "")
            for key in ("gameLabel", "gameSubLabel", "seriesText", "poRoundDesc")
        ).lower()
        if game_id_prefix == "004" or "round" in live_label or "series" in live_label:
            return SEASON_TYPE_PLAYOFFS
        if game_id_prefix == "005" or "play-in" in live_label:
            return "Play-In"
        if game_id_prefix == "001" or "preseason" in live_label:
            return "Preseason"
        return SEASON_TYPE_REGULAR

    def _live_game_number(live_game: dict[str, Any] | None) -> int | None:
        raw = str(
            (live_game or {}).get("seriesGameNumber")
            or (live_game or {}).get("gameSubLabel")
            or (live_game or {}).get("gameLabel")
            or ""
        ).strip()
        match = re.search(r"\bGame\s*(\d+)\b", raw, flags=re.IGNORECASE)
        if not match:
            return None
        try:
            value = int(match.group(1))
            return value if 1 <= value <= 7 else None
        except Exception:
            return None

    def _infer_playoff_game_number_from_game_id(game_id: Any) -> int | None:
        raw = str(game_id or "").strip()
        if len(raw) < 4 or raw[:3] != "004":
            return None
        try:
            value = int(raw[-1])
            return value if 1 <= value <= 7 else None
        except Exception:
            return None

    def _previous_schedule_series_game(game_id: Any) -> dict[str, Any] | None:
        raw = str(game_id or "").strip()
        current_game_number = _infer_playoff_game_number_from_game_id(raw)
        if not current_game_number or len(raw) < 4:
            return None
        prefix = raw[:-1]
        candidates: list[tuple[int, dict[str, Any]]] = []
        for candidate_id, candidate_game in schedule_games_by_id.items():
            if not candidate_id.startswith(prefix) or candidate_id == raw:
                continue
            candidate_game_number = _infer_playoff_game_number_from_game_id(candidate_id)
            if candidate_game_number is None or candidate_game_number >= current_game_number:
                continue
            candidates.append((candidate_game_number, candidate_game))
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _series_text_from_team_records(
        away_abbreviation: str,
        home_abbreviation: str,
        away_record: str,
        home_record: str,
    ) -> str:
        def _wins(record: str) -> int | None:
            match = re.match(r"^\s*(\d+)\s*-\s*\d+\s*$", str(record or ""))
            if not match:
                return None
            try:
                return int(match.group(1))
            except Exception:
                return None

        away_wins = _wins(away_record)
        home_wins = _wins(home_record)
        if away_wins is None or home_wins is None:
            return ""
        if away_wins == home_wins:
            return f"Series tied {away_wins}-{home_wins}"
        if home_wins > away_wins:
            leader = home_abbreviation or "Home"
            return f"{leader} leads {home_wins}-{away_wins}"
        leader = away_abbreviation or "Away"
        return f"{leader} leads {away_wins}-{home_wins}"

    for row in rows:
        home_team_id = int(row.get("HOME_TEAM_ID") or 0)
        away_team_id = int(row.get("VISITOR_TEAM_ID") or 0)
        home_team = TEAM_LOOKUP.get(home_team_id, {})
        away_team = TEAM_LOOKUP.get(away_team_id, {})
        game_id = str(row.get("GAME_ID") or "").strip()
        live_game = live_games_by_id.get(game_id)
        schedule_game = schedule_games_by_id.get(game_id) or schedule_games_by_matchup.get(f"{resolved_date}|{away_team_id}@{home_team_id}")
        previous_series_game = _previous_schedule_series_game(game_id) if not schedule_game else None
        live_home = (live_game or {}).get("homeTeam") or {}
        live_away = (live_game or {}).get("awayTeam") or {}
        schedule_home = (schedule_game or {}).get("homeTeam") or {}
        schedule_away = (schedule_game or {}).get("awayTeam") or {}
        context_game = live_game or schedule_game or {}
        series_context_game = live_game or schedule_game or previous_series_game or {}
        game_status = str(
            (live_game or {}).get("gameStatusText")
            or row.get("GAME_STATUS_TEXT")
            or (schedule_game or {}).get("gameStatusText")
            or ""
        ).strip()
        status_text_display = convert_game_status_text_to_pht(game_status, resolved_date)
        if status_text_display == "TBD":
            status_text_display = _format_live_tipoff_pht(context_game) or status_text_display
        live_status_code = safe_int_score((live_game or {}).get("gameStatus"), (schedule_game or {}).get("gameStatus"), 0)
        if live_status_code == 3:
            status_category = "final"
        elif live_status_code == 2:
            status_category = "live"
        elif live_status_code == 1:
            status_category = "scheduled"
        else:
            status_category = "final" if "Final" in game_status else ("live" if "Q" in game_status or "Halftime" in game_status else "scheduled")
        if status_text_display == "TBD" and status_category == "scheduled":
            status_text_display = "Scheduled"
        home_score = safe_int_score(live_home.get("score"), schedule_home.get("score"), row.get("PTS_HOME"), 0)
        away_score = safe_int_score(live_away.get("score"), schedule_away.get("score"), row.get("PTS_AWAY"), 0)
        home_summary = build_team_availability_summary(str(home_team.get("full_name") or ""), report_payload, game_date=resolved_date)
        away_summary = build_team_availability_summary(str(away_team.get("full_name") or ""), report_payload, game_date=resolved_date)
        series_text, scoreboard_game_number = _extract_series_context_from_scoreboard_row(row)
        live_game_number = _live_game_number(live_game)
        schedule_game_number = _live_game_number(schedule_game)
        series_text = str(
            (live_game or {}).get("seriesText")
            or (schedule_game or {}).get("seriesText")
            or series_text
            or (previous_series_game or {}).get("seriesText")
            or ""
        ).strip()
        playoff_game_number = live_game_number or schedule_game_number or scoreboard_game_number or _infer_playoff_game_number_from_game_id(game_id)
        scoreboard_season_type = _infer_scoreboard_season_type(row, live_game, series_context_game)
        competition_label = str((live_game or {}).get("gameLabel") or (schedule_game or {}).get("gameLabel") or (previous_series_game or {}).get("gameLabel") or "").strip()
        game_sub_label = str((live_game or {}).get("gameSubLabel") or (schedule_game or {}).get("gameSubLabel") or "").strip()
        if not game_sub_label and scoreboard_season_type == SEASON_TYPE_PLAYOFFS and playoff_game_number:
            game_sub_label = f"Game {playoff_game_number}"
        playoff_round = str((live_game or {}).get("poRoundDesc") or (schedule_game or {}).get("poRoundDesc") or (previous_series_game or {}).get("poRoundDesc") or "").strip()
        series_conference = str((live_game or {}).get("seriesConference") or (schedule_game or {}).get("seriesConference") or (previous_series_game or {}).get("seriesConference") or "").strip()
        away_record = _team_record_for_team(away_team_id, live_game, schedule_game, previous_series_game)
        home_record = _team_record_for_team(home_team_id, live_game, schedule_game, previous_series_game)
        if scoreboard_season_type == SEASON_TYPE_PLAYOFFS and not series_text:
            series_text = _series_text_from_team_records(
                str(away_team.get("abbreviation") or ""),
                str(home_team.get("abbreviation") or ""),
                away_record,
                home_record,
            )
        standings_summary = ""
        if scoreboard_season_type == SEASON_TYPE_PLAYOFFS:
            standings_summary = series_text or "Series standing unavailable"
        elif away_record or home_record:
            standings_summary = " | ".join(
                part for part in (
                    f"{away_team.get('abbreviation', '')} {away_record}".strip(),
                    f"{home_team.get('abbreviation', '')} {home_record}".strip(),
                )
                if part
            )
        series_is_over = scoreboard_season_type == SEASON_TYPE_PLAYOFFS and bool(
            re.search(r"\bwins\s+4\s*-\s*\d\b", series_text, flags=re.IGNORECASE)
        )
        if series_is_over and status_category == "scheduled" and status_text_display in {"TBD", "Scheduled"}:
            status_text_display = "Series over"

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
            "status_text": status_text_display,
            "status_category": status_category,
            "game_label": f"{away_team.get('abbreviation', '')} @ {home_team.get('abbreviation', '')}",
            "season_type": scoreboard_season_type,
            "competition_label": competition_label,
            "game_sub_label": game_sub_label,
            "series_text": series_text,
            "series_summary": series_text,
            "series_conference": series_conference,
            "playoff_round": playoff_round,
            "playoff_game_number": playoff_game_number,
            "standings_summary": standings_summary,
            "home": {
                "team_id": home_team_id,
                "full_name": str(home_team.get("full_name") or "").strip(),
                "abbreviation": str(home_team.get("abbreviation") or "").strip(),
                "score": home_score,
                "record": home_record,
                "seed": _team_seed_for_team(home_team_id, live_game, schedule_game, previous_series_game),
                "availability": home_summary,
                "injury_players": _inj_players(str(home_team.get("full_name") or "")),
            },
            "away": {
                "team_id": away_team_id,
                "full_name": str(away_team.get("full_name") or "").strip(),
                "abbreviation": str(away_team.get("abbreviation") or "").strip(),
                "score": away_score,
                "record": away_record,
                "seed": _team_seed_for_team(away_team_id, live_game, schedule_game, previous_series_game),
                "availability": away_summary,
                "injury_players": _inj_players(str(away_team.get("full_name") or "")),
            },
        })

    return {
        "requested_date": requested_date,
        "resolved_date": resolved_date,
        "fallback_used": fallback_used,
        "playoff_mode_active": is_playoff_season(),
        "playoff_mode_start_date": f"{nba_now().year}-{NBA_PLAYOFF_MODE_START_MONTH:02d}-{NBA_PLAYOFF_MODE_START_DAY:02d}",
        "report_label": report_payload.get("report_label") or "",
        "games": games,
    }


#  Live boxscore stat for tracker 
_LIVE_BOX_CACHE: dict[str, dict[str, Any]] = {}
_LIVE_BOX_CACHE_TTL = 30  # seconds

# NBA CDN live scoreboard  much faster than ScoreboardV2
_LIVE_SCOREBOARD_CACHE: dict[str, Any] = {}
_LIVE_SCOREBOARD_TTL = 20  # seconds
_NBA_SCHEDULE_CACHE: dict[str, Any] = {}
_NBA_SCHEDULE_TTL = 1800  # seconds

_NBA_CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
}


def _fetch_nba_live_scoreboard() -> dict[str, Any] | None:
    """Fetch NBA CDN live scoreboard  real-time game states and IDs."""
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


def _fetch_nba_schedule_league() -> dict[str, Any] | None:
    """Fetch NBA CDN season schedule for exact game-id playoff metadata."""
    cached = _NBA_SCHEDULE_CACHE.get("data")
    if cached and time.time() - _NBA_SCHEDULE_CACHE.get("ts", 0) < _NBA_SCHEDULE_TTL:
        return cached
    urls = (
        "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json",
        "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json",
    )
    for url in urls:
        try:
            resp = requests.get(url, timeout=10, headers=_NBA_CDN_HEADERS)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not isinstance((data or {}).get("leagueSchedule"), dict):
                continue
            _NBA_SCHEDULE_CACHE["data"] = data
            _NBA_SCHEDULE_CACHE["ts"] = time.time()
            return data
        except Exception:
            continue
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

    #  1. NBA CDN live scoreboard (20s cache) 
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

    #  2. Pull live boxscore from NBA CDN 
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
                else:
                    if game_status == "final":
                        game_status = "no_game"
            except (KeyError, TypeError, StopIteration):
                pass

    #  3. Fallback to last game log if no live data 
    fallback_val: float | None = None
    fallback_date: str | None = None
    try:
        season = current_nba_season()
        rows = fetch_player_game_log(player_id=player_id, season=season, season_type=DEFAULT_SEASON_TYPE)
        if rows:
            r = rows[0]
            fallback_val = compute_stat_value(r, stat)
            fallback_date = str(r.get("GAME_DATE") or "")
    except Exception:
        pass

    #  4. Injury status check 
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
    over_odds: float | None = Query(None, ge=1.01),
    under_odds: float | None = Query(None, ge=1.01),
    season: str | None = None,
    season_type: str = Query(DEFAULT_SEASON_TYPE),
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
    forced_side: str | None = Query(None, pattern="^(OVER|UNDER)$"),
    debug: bool = Query(False),
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
        over_odds=over_odds,
        under_odds=under_odds,
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
        forced_side=forced_side,
        debug=debug,
        populate_player_info_cache=True,
    )

BULK_ANALYSIS_ENABLED = os.getenv("NBA_BULK_ANALYSIS_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
BULK_ANALYSIS_MAX_WORKERS = max(1, int(os.getenv("NBA_BULK_ANALYSIS_MAX_WORKERS", "4")))
BULK_ANALYSIS_MAX_ROWS = max(1, int(os.getenv("NBA_BULK_ANALYSIS_MAX_ROWS", "100")))
BULK_PREFETCH_MAX_WORKERS = max(1, int(os.getenv("NBA_BULK_PREFETCH_MAX_WORKERS", "6")))
SHARED_ANALYSIS_MAX_WORKERS = max(BULK_ANALYSIS_MAX_WORKERS, int(os.getenv("NBA_SHARED_ANALYSIS_MAX_WORKERS", str(BULK_ANALYSIS_MAX_WORKERS))))
SHARED_NETWORK_MAX_WORKERS = max(4, int(os.getenv("NBA_SHARED_NETWORK_MAX_WORKERS", "8")))
_SHARED_EXECUTOR_LOCK = Lock()
_SHARED_ANALYSIS_EXECUTOR: ThreadPoolExecutor | None = None
_SHARED_NETWORK_EXECUTOR: ThreadPoolExecutor | None = None


def _get_shared_analysis_executor() -> ThreadPoolExecutor:
    global _SHARED_ANALYSIS_EXECUTOR
    with _SHARED_EXECUTOR_LOCK:
        if _SHARED_ANALYSIS_EXECUTOR is None:
            _SHARED_ANALYSIS_EXECUTOR = ThreadPoolExecutor(max_workers=SHARED_ANALYSIS_MAX_WORKERS)
    return _SHARED_ANALYSIS_EXECUTOR


def _get_shared_network_executor() -> ThreadPoolExecutor:
    global _SHARED_NETWORK_EXECUTOR
    with _SHARED_EXECUTOR_LOCK:
        if _SHARED_NETWORK_EXECUTOR is None:
            _SHARED_NETWORK_EXECUTOR = ThreadPoolExecutor(max_workers=SHARED_NETWORK_MAX_WORKERS)
    return _SHARED_NETWORK_EXECUTOR


def _submit_shared_analysis_task(func, *args, **kwargs):
    return _get_shared_analysis_executor().submit(func, *args, **kwargs)


def _submit_shared_network_task(func, *args, **kwargs):
    return _get_shared_network_executor().submit(func, *args, **kwargs)


def _shutdown_shared_executors() -> None:
    global _SHARED_ANALYSIS_EXECUTOR, _SHARED_NETWORK_EXECUTOR
    with _SHARED_EXECUTOR_LOCK:
        analysis_executor = _SHARED_ANALYSIS_EXECUTOR
        network_executor = _SHARED_NETWORK_EXECUTOR
        _SHARED_ANALYSIS_EXECUTOR = None
        _SHARED_NETWORK_EXECUTOR = None
    for executor in (analysis_executor, network_executor):
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)


def _cache_is_fresh(entry: dict[str, Any] | None, ttl_seconds: float) -> bool:
    age_seconds = _cache_age_seconds(entry)
    return bool(entry) and age_seconds is not None and age_seconds < ttl_seconds


def _resolve_team_id_for_player(player_id: int, fallback_team_id: int | None = None) -> int | None:
    if fallback_team_id not in (None, "", 0):
        return int(fallback_team_id)
    cached = PLAYER_INFO_CACHE.get(player_id)
    if cached and isinstance(cached.get("row"), dict):
        team_id = cached["row"].get("TEAM_ID")
        if team_id not in (None, "", 0):
            return int(team_id)
    player = PLAYER_LOOKUP.get(player_id)
    if player:
        team_id = player.get("team_id")
        if team_id not in (None, "", 0):
            return int(team_id)
    return None


def prefetch_bulk_analysis_context(
    player_ids: set[int],
    season: str,
    season_type: str,
    *,
    team_ids: set[int] | None = None,
    primary_player_by_team: dict[int, int] | None = None,
    max_workers: int | None = None,
    label: str = "bulk",
    include_game_logs: bool = True,
    include_player_info: bool = True,
    include_team_next_game: bool = True,
) -> dict[str, int]:
    if not player_ids:
        return {"game_logs": 0, "player_info": 0, "team_next_games": 0}

    team_ids = set(team_ids or [])
    primary_player_by_team = dict(primary_player_by_team or {})

    for pid in player_ids:
        team_id = _resolve_team_id_for_player(pid)
        if team_id:
            team_ids.add(team_id)
            primary_player_by_team.setdefault(team_id, pid)

    game_log_jobs = []
    if include_game_logs:
        for pid in player_ids:
            cache_key = (pid, season, season_type, GAME_LOG_CACHE_SCHEMA_VERSION)
            cached = GAME_LOG_CACHE.get(cache_key)
            if not _cache_is_fresh(cached, CACHE_TTL_SECONDS):
                game_log_jobs.append(pid)

    player_info_jobs = []
    if include_player_info:
        for pid in player_ids:
            cached = PLAYER_INFO_CACHE.get(pid)
            if not _cache_is_fresh(cached, PROFILE_TTL_SECONDS):
                player_info_jobs.append(pid)

    team_jobs: list[tuple[int, int]] = []
    if include_team_next_game:
        for team_id in team_ids:
            cache_key = (team_id, season, season_type)
            cached = TEAM_NEXT_GAME_CACHE.get(cache_key)
            if not _cache_is_fresh(cached, NEXT_GAME_TTL_SECONDS):
                primary_player_id = primary_player_by_team.get(team_id)
                if primary_player_id:
                    team_jobs.append((team_id, primary_player_id))

    total_jobs = len(game_log_jobs) + len(player_info_jobs) + len(team_jobs)
    if total_jobs == 0:
        return {"game_logs": 0, "player_info": 0, "team_next_games": 0}

    workers = max(1, min(BULK_PREFETCH_MAX_WORKERS, max_workers or BULK_PREFETCH_MAX_WORKERS, total_jobs))

    def _prefetch_log(pid: int) -> None:
        try:
            fetch_player_game_log(player_id=pid, season=season, season_type=season_type)
        except Exception:
            pass

    def _prefetch_info(pid: int) -> None:
        try:
            fetch_common_player_info(pid)
        except Exception:
            pass

    def _prefetch_team_next_game(team_id: int, primary_player_id: int) -> None:
        try:
            resolve_team_next_game(team_id=team_id, primary_player_id=primary_player_id, season=season, season_type=season_type)
        except Exception:
            pass

    LOGGER.info(
        "Bulk prefetch (%s): %d game logs, %d player info, %d team next-game (workers=%d)",
        label,
        len(game_log_jobs),
        len(player_info_jobs),
        len(team_jobs),
        workers,
    )

    futures = []
    futures.extend(_submit_shared_network_task(_prefetch_log, pid) for pid in game_log_jobs)
    futures.extend(_submit_shared_network_task(_prefetch_info, pid) for pid in player_info_jobs)
    futures.extend(
        _submit_shared_network_task(_prefetch_team_next_game, team_id, primary_pid)
        for team_id, primary_pid in team_jobs
    )
    for future in as_completed(futures):
        try:
            future.result()
        except Exception:
            pass

    return {
        "game_logs": len(game_log_jobs),
        "player_info": len(player_info_jobs),
        "team_next_games": len(team_jobs),
    }


def _build_bulk_prop_item(
    row_index: int,
    row: dict[str, Any],
    defaults: dict[str, Any],
    local_cache: dict[tuple[Any, ...], dict[str, Any]],
    local_cache_lock: Lock | None = None,
) -> dict[str, Any]:
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
    season_type = normalize_requested_season_type(row.get("season_type") or defaults.get("season_type"))
    last_n = _parse_bounded_int(
        row.get("last_n", defaults.get("last_n")),
        default=10,
        min_value=1,
        max_value=PROP_ANALYSIS_MAX_LAST_N,
        field_name="last_n",
    )
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
    override_opponent_id = row.get("override_opponent_id", defaults.get("override_opponent_id"))
    override_opponent_id = int(override_opponent_id) if override_opponent_id not in (None, "") else None

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
        override_opponent_id,
    )

    cached_analysis: dict[str, Any] | None = None
    if local_cache_lock is not None:
        with local_cache_lock:
            cached_analysis = local_cache.get(cache_key)
    else:
        cached_analysis = local_cache.get(cache_key)

    if cached_analysis is not None:
        analysis = copy.deepcopy(cached_analysis)
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
            override_opponent_id=override_opponent_id,
        )
        if local_cache_lock is not None:
            with local_cache_lock:
                local_cache.setdefault(cache_key, copy.deepcopy(analysis))
        else:
            local_cache.setdefault(cache_key, copy.deepcopy(analysis))

    return {
        "row": row_index,
        "player_id": resolved_player_id,
        "player_name": analysis.get("player", {}).get("full_name") or player_name,
        "team_id": resolved_team_id,
        "player_position": player_position,
        "override_opponent_id": override_opponent_id,
        "event_id": row.get("event_id") or "",
        "game_label": row.get("game_label") or "",
        "home_team": row.get("home_team") or "",
        "away_team": row.get("away_team") or "",
        "stat": stat,
        "line": float(line),
        "analysis": analysis,
    }


def _bulk_player_props_core(payload: dict[str, Any], request: Request | None = None) -> dict[str, Any]:
    if request is not None:
        enforce_heavy_rate_limit(request, "player_props_bulk")
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
        get_cached_injury_report_payload()
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
    local_cache_lock = Lock()
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    bulk_player_ids: set[int] = set()
    bulk_team_ids: set[int] = set()
    primary_by_team: dict[int, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        team_id = row.get("team_id")
        try:
            team_id = int(team_id) if team_id not in (None, "") else None
        except (TypeError, ValueError):
            team_id = None
        player_id = row.get("player_id")
        player = None
        if player_id not in (None, ""):
            try:
                player_id = int(player_id)
                player = PLAYER_LOOKUP.get(player_id)
            except (TypeError, ValueError):
                player_id = None
        if not player_id:
            player_name = str(row.get("player_name") or "").strip()
            if player_name:
                player = find_player_by_name(player_name, team_id=team_id)
                if player:
                    player_id = int(player["id"])
        if player_id:
            bulk_player_ids.add(int(player_id))
            resolved_team_id = _resolve_team_id_for_player(int(player_id), fallback_team_id=team_id)
            if resolved_team_id:
                bulk_team_ids.add(int(resolved_team_id))
                primary_by_team.setdefault(int(resolved_team_id), int(player_id))

    prefetch_bulk_analysis_context(
        player_ids=bulk_player_ids,
        season=str(defaults.get("season") or current_nba_season()),
        season_type=normalize_requested_season_type(defaults.get("season_type")),
        team_ids=bulk_team_ids,
        primary_player_by_team=primary_by_team,
        max_workers=max_workers,
        label="bulk_props",
    )

    if max_workers <= 1:
        for row_index, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                errors.append({"row": row_index, "reason": "Invalid row format."})
                continue
            try:
                results.append(_build_bulk_prop_item(row_index, row, defaults, local_cache, local_cache_lock))
            except HTTPException as exc:
                errors.append({"row": row_index, "reason": exc.detail})
            except Exception as exc:
                errors.append({"row": row_index, "reason": str(exc)})
    else:
        futures: list[tuple[int, Any]] = []
        for row_index, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                errors.append({"row": row_index, "reason": "Invalid row format."})
                continue
            future = _submit_shared_analysis_task(
                _build_bulk_prop_item,
                row_index,
                row,
                defaults,
                local_cache,
                local_cache_lock,
            )
            futures.append((row_index, future))
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


@app.post("/api/player-props/bulk")
def bulk_player_props(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    return _bulk_player_props_core(payload, request=request)


@app.post("/api/player-props/bulk/async")
def bulk_player_props_async(request: Request, payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    enforce_heavy_rate_limit(request, "player_props_bulk_async")
    return submit_async_job("bulk_player_props", _bulk_player_props_core, payload)


@app.get("/api/debug/injury-report-raw")
def debug_injury_report_raw() -> dict[str, Any]:
    """Debug endpoint: returns raw extracted PDF text and parsed rows so the
    team-assignment logic can be inspected without guessing."""
    payload = get_cached_injury_report_payload()
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


# 
# BACKTEST ENGINE
# Logs predictions and actual results, computes ROI / hit-rate by bucket.
# In-memory store (resets on restart). Frontend can persist via localStorage.
# 

_BACKTEST_LOCK = threading.RLock()
_BACKTEST_LOG: list[dict[str, Any]] = []   # [{id, player, stat, line, side, confidence_score, confidence_tier, model_prob, result, hit, odds, ev, logged_at, resolved_at}]
_KEY_VAULT_LOCK = threading.Lock()
_KEY_VAULT_STATE: dict[str, Any] = {"entries": [], "active_id": ""}
_FAVORITES_LOCK = threading.Lock()
_FAVORITES_STATE: list[dict[str, Any]] = []
_TRACKER_LOCK = threading.RLock()
_TRACKER_STATE: list[dict[str, Any]] = []


def _save_backtest_log() -> None:
    _invalidate_backtest_ranking_cache()
    def _payload_factory() -> dict[str, Any]:
        with _BACKTEST_LOCK:
            return {"entries": list(_BACKTEST_LOG)}

    save_json_snapshot(
        enabled=PERSISTENT_CACHE_ENABLED,
        path=BACKTEST_PERSIST_PATH,
        payload_factory=_payload_factory,
        on_error=lambda exc: LOGGER.warning("Backtest log save failed: %s", exc),
    )


def _load_backtest_log() -> None:
    _invalidate_backtest_ranking_cache()
    def _extract_entries(payload: dict[str, Any]) -> Any:
        entries = payload.get("entries") or []
        return entries if isinstance(entries, list) else []

    def _apply_entries(entries: Any) -> None:
        with _BACKTEST_LOCK:
            _BACKTEST_LOG.clear()
            for raw in entries:
                if not isinstance(raw, dict):
                    continue
                normalized_tier = _normalize_backtest_confidence_tier(raw.get("confidence_tier"), raw.get("confidence_score"))
                normalized_score = _normalize_backtest_confidence_score(raw.get("confidence_score"), normalized_tier)
                entry = {
                    "id": str(raw.get("id") or _backtest_new_id()),
                    "player": str(raw.get("player") or ""),
                    "stat": str(raw.get("stat") or ""),
                    "line": float(raw.get("line") or 0),
                    "side": str(raw.get("side") or ""),
                    "confidence_score": normalized_score,
                    "confidence_tier": normalized_tier,
                    "model_prob": float(raw.get("model_prob") or 0.5),
                    "odds": raw.get("odds"),
                    "result": str(raw.get("result") or "pending"),
                    "actual_value": raw.get("actual_value"),
                    "logged_at": str(raw.get("logged_at") or ""),
                    "resolved_at": raw.get("resolved_at"),
                    "event_date": str(raw.get("event_date") or ""),
                    "source": str(raw.get("source") or ""),
                    "market_side": str(raw.get("market_side") or ""),
                    "market_disagrees": bool(raw.get("market_disagrees")) if raw.get("market_disagrees") is not None else False,
                    "notes": str(raw.get("notes") or ""),
                }
                _BACKTEST_LOG.append(entry)

    load_json_snapshot(
        enabled=PERSISTENT_CACHE_ENABLED,
        path=BACKTEST_PERSIST_PATH,
        extract_entries=_extract_entries,
        apply_entries=_apply_entries,
        on_error=lambda exc: LOGGER.warning("Backtest log load failed: %s", exc),
    )


def _sanitize_key_vault_entries(entries: Any) -> list[dict[str, Any]]:
    if not isinstance(entries, list):
        return []
    merged_by_key: dict[str, dict[str, Any]] = {}
    for raw in entries:
        if not isinstance(raw, dict):
            continue
        key_text = normalize_odds_api_key(raw.get("key") or raw.get("api_key") or "")
        if not key_text:
            continue
        previous = merged_by_key.get(key_text) or {}
        normalized: dict[str, Any] = {**previous, **copy.deepcopy(raw)}
        normalized["key"] = key_text
        provider = str(normalized.get("provider") or "odds_api").strip() or "odds_api"
        normalized["provider"] = provider
        entry_id = str(normalized.get("id") or "").strip()
        if not entry_id:
            entry_id = uuid4().hex[:12]
        normalized["id"] = entry_id
        label = str(normalized.get("label") or "").strip()
        if not label:
            label = "Odds API Key"
        normalized["label"] = label
        merged_by_key[key_text] = normalized
    return list(merged_by_key.values())


def _read_local_key_vault_backup_state() -> dict[str, Any] | None:
    if not PERSISTENT_CACHE_ENABLED or not KEY_VAULT_PERSIST_PATH.exists():
        return None
    try:
        payload = json.loads(KEY_VAULT_PERSIST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    entries = _sanitize_key_vault_entries(payload.get("entries") or [])
    active_id = str(payload.get("active_id") or "")
    valid_ids = {str(entry.get("id") or "") for entry in entries}
    if active_id and active_id not in valid_ids:
        active_id = ""
    return {"entries": entries, "active_id": active_id}


def _save_key_vault_state() -> None:
    state: dict[str, Any]
    with _KEY_VAULT_LOCK:
        state = {
            "entries": list(_sanitize_key_vault_entries(_KEY_VAULT_STATE.get("entries") or [])),
            "active_id": str(_KEY_VAULT_STATE.get("active_id") or ""),
        }

    if postgres_available() and POSTGRES_CACHE_WRITE_ENABLED:
        try:
            _pg_write_key_vault_state(state)
        except Exception as exc:
            LOGGER.warning("Key vault Postgres save failed: %s", exc)

    def _payload_factory() -> dict[str, Any]:
        return state

    save_json_snapshot(
        enabled=PERSISTENT_CACHE_ENABLED,
        path=KEY_VAULT_PERSIST_PATH,
        payload_factory=_payload_factory,
        on_error=lambda exc: LOGGER.warning("Key vault save failed: %s", exc),
    )


def _save_favorites_state() -> None:
    def _payload_factory() -> dict[str, Any]:
        with _FAVORITES_LOCK:
            return {"entries": list(_FAVORITES_STATE[:24])}

    save_json_snapshot(
        enabled=PERSISTENT_CACHE_ENABLED,
        path=FAVORITES_PERSIST_PATH,
        payload_factory=_payload_factory,
        on_error=lambda exc: LOGGER.warning("Favorites save failed: %s", exc),
    )


def _save_tracker_state() -> None:
    def _payload_factory() -> dict[str, Any]:
        with _TRACKER_LOCK:
            return {"entries": list(_TRACKER_STATE)}

    save_json_snapshot(
        enabled=PERSISTENT_CACHE_ENABLED,
        path=TRACKER_PERSIST_PATH,
        payload_factory=_payload_factory,
        on_error=lambda exc: LOGGER.warning("Tracker save failed: %s", exc),
    )


def _load_key_vault_state() -> None:
    if postgres_available():
        pg_state = _pg_read_key_vault_state()
        if isinstance(pg_state, dict):
            entries = _sanitize_key_vault_entries(pg_state.get("entries") or [])
            active_id = str(pg_state.get("active_id") or "")
            valid_ids = {str(entry.get("id") or "") for entry in entries}
            if active_id and active_id not in valid_ids:
                active_id = ""
            with _KEY_VAULT_LOCK:
                _KEY_VAULT_STATE["entries"] = entries
                _KEY_VAULT_STATE["active_id"] = active_id
            LOGGER.info("Loaded %s key vault entrie(s) from Postgres", len(entries))
            return

    def _extract_entries(payload: dict[str, Any]) -> Any:
        entries = _sanitize_key_vault_entries(payload.get("entries") or [])
        active_id = str(payload.get("active_id") or "")
        valid_ids = {str(entry.get("id") or "") for entry in entries}
        if active_id and active_id not in valid_ids:
            active_id = ""
        return {"entries": entries, "active_id": active_id}

    def _apply_entries(state: Any) -> None:
        entries = list((state or {}).get("entries") or [])
        active_id = str((state or {}).get("active_id") or "")
        with _KEY_VAULT_LOCK:
            _KEY_VAULT_STATE["entries"] = entries
            _KEY_VAULT_STATE["active_id"] = active_id
        LOGGER.info("Loaded %s key vault entrie(s) from persistent storage", len(entries))

    load_json_snapshot(
        enabled=PERSISTENT_CACHE_ENABLED,
        path=KEY_VAULT_PERSIST_PATH,
        extract_entries=_extract_entries,
        apply_entries=_apply_entries,
        on_error=lambda exc: LOGGER.warning("Key vault load failed: %s", exc),
    )


def _load_favorites_state() -> None:
    def _extract_entries(payload: dict[str, Any]) -> Any:
        entries = payload.get("entries") or []
        return entries if isinstance(entries, list) else []

    def _apply_entries(entries: Any) -> None:
        with _FAVORITES_LOCK:
            _FAVORITES_STATE.clear()
            for entry in entries[:24]:
                if isinstance(entry, dict):
                    _FAVORITES_STATE.append(entry)

    load_json_snapshot(
        enabled=PERSISTENT_CACHE_ENABLED,
        path=FAVORITES_PERSIST_PATH,
        extract_entries=_extract_entries,
        apply_entries=_apply_entries,
        on_error=lambda exc: LOGGER.warning("Favorites load failed: %s", exc),
    )


def _load_tracker_state() -> None:
    def _extract_entries(payload: dict[str, Any]) -> Any:
        entries = payload.get("entries") or []
        return entries if isinstance(entries, list) else []

    def _apply_entries(entries: Any) -> None:
        with _TRACKER_LOCK:
            _TRACKER_STATE.clear()
            for entry in entries[:250]:
                if isinstance(entry, dict):
                    _TRACKER_STATE.append(entry)

    load_json_snapshot(
        enabled=PERSISTENT_CACHE_ENABLED,
        path=TRACKER_PERSIST_PATH,
        extract_entries=_extract_entries,
        apply_entries=_apply_entries,
        on_error=lambda exc: LOGGER.warning("Tracker load failed: %s", exc),
    )


def _backtest_new_id() -> str:
    import uuid
    return str(uuid.uuid4())[:8]


def _normalize_backtest_confidence_tier(raw_tier: Any, confidence_score: Any = None) -> str:
    tier_raw = str(raw_tier or "").strip()
    tier_key = tier_raw.lower()
    tier_map = {
        "a": "Elite",
        "elite": "Elite",
        "b": "High",
        "high": "High",
        "c": "Medium",
        "medium": "Medium",
        "d": "Low",
        "low": "Low",
        "f": "Low",
        "very low": "Low",
        "verylow": "Low",
        "x": "Low",
        "out": "Low",
    }
    if tier_key in tier_map:
        return tier_map[tier_key]

    try:
        score = int(float(confidence_score))
    except Exception:
        score = None

    if score is not None:
        if score >= CONFIDENCE_TIER_ELITE_MIN:
            return "Elite"
        if score >= CONFIDENCE_TIER_HIGH_MIN:
            return "High"
        if score >= CONFIDENCE_TIER_MEDIUM_MIN:
            return "Medium"
        return "Low"

    return "Medium"


def _normalize_backtest_confidence_score(raw_score: Any, fallback_tier: str) -> int:
    try:
        score = int(float(raw_score))
        return max(0, min(100, score))
    except Exception:
        pass
    tier = str(fallback_tier or "").strip().lower()
    if tier == "elite":
        return 90
    if tier == "high":
        return 79
    if tier == "medium":
        return 66
    return 54


def _merge_backtest_entries(raw_entries: list[Any]) -> tuple[int, int, int, list[dict[str, Any]]]:
    added = 0
    updated = 0
    skipped = 0
    imported: list[dict[str, Any]] = []
    with _BACKTEST_LOCK:
        existing_entries_by_id: dict[str, dict[str, Any]] = {}
        existing_entries_by_key: dict[tuple[str, str, float, str, str], dict[str, Any]] = {}
        for existing in _BACKTEST_LOG:
            if not isinstance(existing, dict):
                continue
            existing_id = str(existing.get("id") or "").strip()
            if existing_id:
                existing_entries_by_id[existing_id] = existing
            dedupe_key = (
                str(existing.get("player") or "").strip().lower(),
                str(existing.get("stat") or "").strip().upper(),
                float(existing.get("line") or 0),
                str(existing.get("side") or "").strip().upper(),
                str(existing.get("logged_at") or "").strip(),
            )
            existing_entries_by_key[dedupe_key] = existing

        for raw in raw_entries:
            if not isinstance(raw, dict):
                skipped += 1
                continue
            player = str(raw.get("player") or "").strip()
            stat = str(raw.get("stat") or "").strip().upper()
            side = str(raw.get("side") or "").strip().upper()
            if not player or not stat or not side:
                skipped += 1
                continue
            try:
                line = float(raw.get("line") or 0)
            except Exception:
                skipped += 1
                continue
            normalized_tier = _normalize_backtest_confidence_tier(raw.get("confidence_tier"), raw.get("confidence_score"))
            normalized_score = _normalize_backtest_confidence_score(raw.get("confidence_score"), normalized_tier)
            logged_at = str(raw.get("logged_at") or _utc_iso_z()).strip()
            incoming_id = str(raw.get("id") or _backtest_new_id()).strip()
            dedupe_key = (player.lower(), stat, line, side, logged_at)
            actual_raw = raw.get("actual_value")
            try:
                actual_value = float(actual_raw) if actual_raw not in (None, "", "null") else None
            except Exception:
                actual_value = None
            entry = {
                "id": incoming_id,
                "player": player,
                "stat": stat,
                "line": line,
                "side": side,
                "confidence_score": normalized_score,
                "confidence_tier": normalized_tier,
                "model_prob": float(raw.get("model_prob") or 0.5),
                "odds": raw.get("odds"),
                "result": str(raw.get("result") or "pending"),
                "actual_value": actual_value,
                "logged_at": logged_at,
                "resolved_at": raw.get("resolved_at"),
                "event_date": str(raw.get("event_date") or ""),
                "source": str(raw.get("source") or ""),
                "market_side": str(raw.get("market_side") or ""),
                "market_disagrees": bool(raw.get("market_disagrees")) if raw.get("market_disagrees") is not None else False,
                "notes": str(raw.get("notes") or ""),
            }

            existing_by_id = existing_entries_by_id.get(incoming_id)
            if isinstance(existing_by_id, dict):
                if existing_by_id != entry:
                    existing_by_id.clear()
                    existing_by_id.update(entry)
                    updated += 1
                    imported.append(entry)
                    existing_entries_by_key[dedupe_key] = existing_by_id
                else:
                    skipped += 1
                continue

            existing_by_key = existing_entries_by_key.get(dedupe_key)
            if isinstance(existing_by_key, dict):
                # Prefer the incoming row when it has a resolved outcome while local is still pending.
                existing_result = str(existing_by_key.get("result") or "pending").lower()
                incoming_result = str(entry.get("result") or "pending").lower()
                should_update = existing_result == "pending" and incoming_result in {"hit", "miss"}
                if should_update:
                    preserved_id = str(existing_by_key.get("id") or "").strip()
                    if preserved_id and not str(entry.get("id") or "").strip():
                        entry["id"] = preserved_id
                    existing_by_key.clear()
                    existing_by_key.update(entry)
                    updated += 1
                    imported.append(entry)
                    entry_id = str(existing_by_key.get("id") or "").strip()
                    if entry_id:
                        existing_entries_by_id[entry_id] = existing_by_key
                else:
                    skipped += 1
                continue

            _BACKTEST_LOG.append(entry)
            existing_entries_by_key[dedupe_key] = entry
            if incoming_id:
                existing_entries_by_id[incoming_id] = entry
            imported.append(entry)
            added += 1
        if added or updated:
            _save_backtest_log()
    return added, updated, skipped, imported


def _backtest_entry_datetime(entry: dict[str, Any]) -> datetime | None:
    if not isinstance(entry, dict):
        return None
    return _coerce_datetime(
        entry.get("resolved_at")
        or entry.get("logged_at")
        or entry.get("event_date")
    )


def _backtest_utc_now() -> datetime:
    return datetime.now(dt.timezone.utc)


def _backtest_normalize_datetime(value: datetime | None) -> datetime | None:
    if not value:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _backtest_current_season_start(reference: datetime | None = None) -> datetime:
    now = _backtest_normalize_datetime(reference) or _backtest_utc_now()
    season_year = now.year if now.month >= 10 else now.year - 1
    return datetime(season_year, 10, 1, tzinfo=dt.timezone.utc)


def _backtest_matches_filters(
    entry: dict[str, Any],
    *,
    search: str = "",
    result_filter: str = "all",
    stat_filter: str = "all",
    tier_filter: str = "all",
    side_filter: str = "all",
    date_range: str = "all",
    view_mode: str = "active",
) -> bool:
    haystack = " ".join(
        [
            str(entry.get("player") or ""),
            str(entry.get("stat") or ""),
            str(entry.get("confidence_tier") or ""),
            str(entry.get("notes") or ""),
            str(entry.get("source") or ""),
        ]
    ).lower()
    normalized_search = str(search or "").strip().lower()
    if normalized_search and normalized_search not in haystack:
        return False

    result_value = str(entry.get("result") or "").strip().lower()
    if view_mode == "pending" and result_value != "pending":
        return False
    if view_mode == "resolved" and result_value not in {"hit", "miss"}:
        return False
    if view_mode == "active":
        entry_dt = _backtest_normalize_datetime(_backtest_entry_datetime(entry))
        if result_value != "pending" and entry_dt:
            if entry_dt < _backtest_utc_now() - timedelta(days=7):
                return False

    if result_filter != "all" and result_value != str(result_filter).strip().lower():
        return False
    if stat_filter != "all" and str(entry.get("stat") or "").strip().upper() != str(stat_filter).strip().upper():
        return False
    if tier_filter != "all" and str(entry.get("confidence_tier") or "").strip().lower() != str(tier_filter).strip().lower():
        return False
    if side_filter != "all" and str(entry.get("side") or "").strip().upper() != str(side_filter).strip().upper():
        return False

    if date_range != "all":
        entry_dt = _backtest_normalize_datetime(_backtest_entry_datetime(entry))
        if not entry_dt:
            return False
        now = _backtest_utc_now()
        normalized_range = str(date_range).strip().lower()
        if normalized_range == "7d" and entry_dt < now - timedelta(days=7):
            return False
        if normalized_range == "30d" and entry_dt < now - timedelta(days=30):
            return False
        if normalized_range == "season" and entry_dt < _backtest_current_season_start(now):
            return False
    return True


def _backtest_sort_key(entry: dict[str, Any]) -> float:
    entry_dt = _backtest_normalize_datetime(_backtest_entry_datetime(entry))
    if not entry_dt:
        return 0.0
    try:
        return entry_dt.timestamp()
    except Exception:
        return 0.0


def _filter_backtest_entries(
    entries: list[dict[str, Any]],
    *,
    search: str = "",
    result_filter: str = "all",
    stat_filter: str = "all",
    tier_filter: str = "all",
    side_filter: str = "all",
    date_range: str = "all",
    view_mode: str = "active",
) -> list[dict[str, Any]]:
    filtered = [
        copy.deepcopy(entry)
        for entry in entries
        if _backtest_matches_filters(
            entry,
            search=search,
            result_filter=result_filter,
            stat_filter=stat_filter,
            tier_filter=tier_filter,
            side_filter=side_filter,
            date_range=date_range,
            view_mode=view_mode,
        )
    ]
    filtered.sort(key=_backtest_sort_key, reverse=True)
    return filtered


def _backtest_group_entries(entries: list[dict[str, Any]], group_by: str = "none") -> list[dict[str, Any]]:
    normalized_group = str(group_by or "none").strip().lower()
    if normalized_group not in {"date", "player"}:
        return [{"label": "All entries", "entries": entries}]
    buckets: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        if normalized_group == "player":
            label = str(entry.get("player") or "Unknown player").strip() or "Unknown player"
        else:
            entry_dt = _backtest_entry_datetime(entry)
            label = entry_dt.strftime("%b %d, %Y") if entry_dt else "Unknown date"
        buckets.setdefault(label, []).append(entry)
    grouped: list[dict[str, Any]] = []
    for label, bucket_entries in buckets.items():
        grouped.append({"label": label, "entries": bucket_entries})
    grouped.sort(key=lambda group: _backtest_sort_key(group["entries"][0]) if group["entries"] else 0.0, reverse=True)
    return grouped


def _build_backtest_csv(entries: list[dict[str, Any]]) -> str:
    headers = [
        "id",
        "player",
        "stat",
        "line",
        "side",
        "confidence_tier",
        "confidence_score",
        "model_prob",
        "odds",
        "result",
        "actual_value",
        "logged_at",
        "resolved_at",
        "event_date",
        "source",
        "market_side",
        "market_disagrees",
        "notes",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers)
    writer.writeheader()
    for entry in entries:
        writer.writerow({key: entry.get(key) for key in headers})
    return buffer.getvalue()


def _compute_backtest_stats(entries: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate hit-rate, ROI, EV accuracy by confidence tier, stat, and odds band."""
    resolved = [e for e in entries if e.get("result") in ("hit", "miss")]
    total = len(resolved)
    if total == 0:
        pending_only = [e for e in entries if e.get("result") == "pending"]
        return {
            "total": 0,
            "pending": len(entries) - total,
            "logged_total": len(entries),
            "recent_form": [],
            "by_result": {},
            "by_side": {},
            "by_stat": {},
            "by_tier": {},
            "by_market_alignment": {},
            "roi_assumed_odds_count": 0,
            "top_players": [
                {"player": player, "total": count}
                for player, count in sorted(
                    ((str(p or "Unknown"), sum(1 for e in pending_only if str(e.get("player") or "Unknown") == str(p or "Unknown"))) for p in {e.get("player") for e in pending_only}),
                    key=lambda item: item[1],
                    reverse=True,
                )[:5]
                if count > 0
            ],
        }

    hits = sum(1 for e in resolved if e["result"] == "hit")
    win_rate = round(hits / total * 100, 1)

    # ROI: assumes -110 odds (decimal 1.909) unless odds supplied
    roi_list = []
    roi_assumed_odds_count = 0
    for e in resolved:
        stored_odds = e.get("odds")
        if stored_odds in (None, ""):
            roi_assumed_odds_count += 1
        decimal_odds = float(stored_odds or 1.909)
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

    alignment_stats: dict[str, dict[str, int]] = {}
    for e in resolved:
        alignment = "Against market" if e.get("market_disagrees") else ("Market aligned" if e.get("market_side") else "No market context")
        alignment_stats.setdefault(alignment, {"hits": 0, "total": 0})
        alignment_stats[alignment]["total"] += 1
        if e["result"] == "hit":
            alignment_stats[alignment]["hits"] += 1
    alignment_summary = {
        key: {
            "win_rate": round(value["hits"] / value["total"] * 100, 1),
            "total": value["total"],
        }
        for key, value in alignment_stats.items()
    }

    player_stats: dict[str, dict[str, int]] = {}
    for e in resolved:
        player = str(e.get("player") or "Unknown")
        player_stats.setdefault(player, {"hits": 0, "total": 0})
        player_stats[player]["total"] += 1
        if e["result"] == "hit":
            player_stats[player]["hits"] += 1
    top_players = [
        {
            "player": player,
            "win_rate": round(values["hits"] / values["total"] * 100, 1),
            "total": values["total"],
        }
        for player, values in sorted(player_stats.items(), key=lambda item: (item[1]["total"], item[1]["hits"]), reverse=True)[:6]
    ]

    recent_form = [
        1 if e.get("result") == "hit" else 0
        for e in sorted(resolved, key=lambda entry: str(entry.get("resolved_at") or entry.get("logged_at") or ""))[-10:]
    ]

    # --- Calibration by confidence grade (A/B/C/D/F) ---
    # This tells you whether the letter grade actually predicts hit rate.
    # After 50+ resolved entries, A should hit more than B, B more than C, etc.
    grade_buckets: dict[str, dict[str, int]] = {}
    for e in resolved:
        grade = str(e.get("grade") or e.get("confidence") or "?").strip().upper()
        if not grade or grade not in {"A", "B", "C", "D", "F"}:
            grade = "?"
        if grade not in grade_buckets:
            grade_buckets[grade] = {"hits": 0, "total": 0}
        grade_buckets[grade]["total"] += 1
        if e["result"] == "hit":
            grade_buckets[grade]["hits"] += 1
    calibration_by_grade = {
        grade: {
            "total": v["total"],
            "hits": v["hits"],
            "hit_rate": round(v["hits"] / v["total"] * 100, 1) if v["total"] else None,
        }
        for grade, v in sorted(grade_buckets.items())
    }

    # --- Calibration by confidence score band ---
    # Splits 0-99 score into bands so you can see if score 80-89 hits at a
    # meaningfully different rate than 70-79 or 90-99.
    score_band_buckets: dict[str, dict[str, int]] = {}
    for e in resolved:
        try:
            score = int(float(e.get("confidence_score") or 0))
        except (TypeError, ValueError):
            score = 0
        if score >= 90:
            band = "90-99"
        elif score >= 80:
            band = "80-89"
        elif score >= 70:
            band = "70-79"
        elif score >= 60:
            band = "60-69"
        elif score >= 50:
            band = "50-59"
        else:
            band = "0-49"
        if band not in score_band_buckets:
            score_band_buckets[band] = {"hits": 0, "total": 0}
        score_band_buckets[band]["total"] += 1
        if e["result"] == "hit":
            score_band_buckets[band]["hits"] += 1
    calibration_by_score_band = {
        band: {
            "total": v["total"],
            "hits": v["hits"],
            "hit_rate": round(v["hits"] / v["total"] * 100, 1) if v["total"] else None,
        }
        for band, v in sorted(score_band_buckets.items(), reverse=True)
    }

    return {
        "total": total,
        "logged_total": len(entries),
        "pending": len(entries) - total,
        "hits": hits,
        "misses": total - hits,
        "win_rate": win_rate,
        "roi_pct": roi,
        "roi_assumed_odds_count": roi_assumed_odds_count,
        "by_tier": tier_summary,
        "by_stat": stat_summary,
        "by_side": side_summary,
        "by_result": {
            "hit": hits,
            "miss": total - hits,
            "pending": len(entries) - total,
        },
        "by_market_alignment": alignment_summary,
        "top_players": top_players,
        "recent_form": recent_form,
        "calibration_by_grade": calibration_by_grade,
        "calibration_by_score_band": calibration_by_score_band,
    }


def _nba_season_for_datetime(value: datetime | None) -> str:
    dt_value = value or nba_now()
    start_year = dt_value.year if dt_value.month >= 10 else dt_value.year - 1
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def _backtest_parlay_filter_thresholds(mode: str) -> tuple[int, int]:
    normalized = str(mode or "action").strip().lower()
    if normalized == "selected":
        return 0, 0
    if normalized == "broad":
        return 60, 4
    if normalized == "playoff_high":
        return 75, 0
    return 72, 5


def _backtest_parlay_prop_passes_action_filter(prop: dict[str, Any], mode: str) -> bool:
    if not isinstance(prop, dict):
        return False
    normalized = str(mode or "action").strip().lower()
    if normalized == "selected":
        return str(prop.get("selection_status") or "").strip().lower() == "selected"
    min_score, min_games = _backtest_parlay_filter_thresholds(normalized)
    try:
        score = int(float(prop.get("confidence_score") or 0))
    except Exception:
        score = 0
    games_count = int(prop.get("games_count") or 0)
    odds = float(prop.get("odds") or 0.0)
    return score >= min_score and games_count >= min_games and odds >= 1.40


def _backtest_parlay_event_date(prop: dict[str, Any]) -> str:
    matchup = prop.get("matchup") or {}
    next_game = matchup.get("next_game") if isinstance(matchup, dict) else {}
    if isinstance(next_game, dict):
        game_date = str(next_game.get("game_date") or "").strip()
        if game_date:
            return game_date
    return str(prop.get("event_date") or "").strip()


def _backtest_parlay_unique_key(prop: dict[str, Any], event_date: str) -> str:
    player_id = int(prop.get("player_id") or 0)
    stat = str(prop.get("stat") or "").strip().upper()
    line = round(float(prop.get("line") or 0.0), 3)
    side = str(prop.get("side") or "").strip().upper()
    event_id = str(prop.get("event_id") or "").strip()
    game_label = str(prop.get("game_label") or "").strip()
    opponent = str(prop.get("opponent_abbreviation") or "").strip().upper()
    return "|".join(
        [
            str(player_id),
            stat,
            f"{line:.3f}",
            side,
            event_id,
            game_label,
            event_date,
            opponent,
        ]
    )


def _extract_opponent_abbreviation_from_entry(entry: dict[str, Any]) -> str:
    explicit = str(entry.get("opponent_abbreviation") or "").strip().upper()
    if explicit:
        return explicit
    game_label = str(entry.get("game_label") or "").strip().upper()
    if game_label:
        match = re.search(r"(?:@|VS)\s*([A-Z]{2,3})\b", game_label)
        if match:
            return str(match.group(1) or "").strip().upper()
    return ""


def _normalize_backtest_compare_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None:
        try:
            return value.astimezone(NBA_GAME_DATE_ZONEINFO).replace(tzinfo=None)
        except Exception:
            return value.replace(tzinfo=None)
    return value


def _build_today_scoreboard_status() -> tuple[set[tuple[str, str]], dict[str, bool], bool]:
    try:
        rows = fetch_scoreboard_games(_today_scoreboard_date())
    except Exception:
        return set(), {}, False
    pairs: set[tuple[str, str]] = set()
    team_final_map: dict[str, bool] = {}
    for row in rows:
        game_status = str(row.get("GAME_STATUS_TEXT") or "").strip()
        is_final = "Final" in game_status
        try:
            home_team_id = int(row.get("HOME_TEAM_ID") or 0)
            away_team_id = int(row.get("VISITOR_TEAM_ID") or 0)
        except Exception:
            continue
        home_abbr = str(TEAM_LOOKUP.get(home_team_id, {}).get("abbreviation") or "").strip().upper()
        away_abbr = str(TEAM_LOOKUP.get(away_team_id, {}).get("abbreviation") or "").strip().upper()
        if not home_abbr or not away_abbr:
            continue
        team_final_map[home_abbr] = is_final
        team_final_map[away_abbr] = is_final
        if is_final:
            pairs.add(tuple(sorted((home_abbr, away_abbr))))
    return pairs, team_final_map, True


def _pick_game_log_row_for_backtest_entry(
    entry: dict[str, Any],
    rows: list[dict[str, Any]],
    now_local: datetime | None = None,
    today_final_matchup_pairs: set[tuple[str, str]] | None = None,
    today_team_final_map: dict[str, bool] | None = None,
    today_scoreboard_available: bool = False,
) -> dict[str, Any] | None:
    if not rows:
        return None
    now_dt = _normalize_backtest_compare_datetime(now_local or nba_now())
    if not now_dt:
        return None
    today_date = now_dt.date()

    target_date_raw = parse_game_date_any(entry.get("event_date"))
    logged_at_raw = _coerce_datetime(entry.get("logged_at"))
    opponent_abbr = _extract_opponent_abbreviation_from_entry(entry)
    has_game_label = bool(str(entry.get("game_label") or "").strip())

    target_date = _normalize_backtest_compare_datetime(target_date_raw)
    logged_at = _normalize_backtest_compare_datetime(logged_at_raw)

    # Some logged rows may carry stale event_date (older than the log time),
    # so don't hard-fail date matching in that case.
    if target_date and logged_at:
        if target_date.date() < (logged_at.date() - timedelta(days=1)):
            target_date = None

    if target_date and target_date.date() > today_date:
        return None

    candidates: list[tuple[datetime, dict[str, Any], str, str]] = []
    for row in rows:
        game_dt_raw = parse_game_date_any(row.get("GAME_DATE"))
        game_dt = _normalize_backtest_compare_datetime(game_dt_raw)
        if not game_dt:
            continue
        matchup_info = parse_matchup_descriptor(str(row.get("MATCHUP") or ""))
        row_team = str(matchup_info.get("team_abbreviation") or "").strip().upper()
        row_opp = str(matchup_info.get("opponent_abbreviation") or "").strip().upper()
        candidates.append((game_dt, row, row_team, row_opp))
    if not candidates:
        return None

    if target_date:
        same_date = [item for item in candidates if item[0].date() == target_date.date()]
        if same_date:
            candidates = same_date
        elif logged_at:
            cutoff = logged_at - timedelta(hours=12)
            filtered = [item for item in candidates if item[0] >= cutoff]
            if filtered:
                candidates = filtered
            else:
                return None
        else:
            return None
    elif logged_at:
        cutoff = logged_at - timedelta(hours=12)
        filtered = [item for item in candidates if item[0] >= cutoff]
        if filtered:
            candidates = filtered

    if opponent_abbr:
        opp_filtered = [
            item
            for item in candidates
            if opponent_abbr and (opponent_abbr == item[3] or opponent_abbr in str(item[1].get("MATCHUP") or "").upper())
        ]
        if opp_filtered:
            candidates = opp_filtered
        elif target_date:
            return None

    candidates.sort(key=lambda item: item[0], reverse=True)
    chosen = candidates[0]
    chosen_dt = chosen[0]
    chosen_row = chosen[1]
    chosen_team_abbr = chosen[2]
    chosen_opp_abbr = chosen[3]

    weak_context = not target_date and not opponent_abbr and not has_game_label
    if weak_context and logged_at and chosen_dt.date() < today_date:
        # If team is currently playing and game is not final, don't grade from an older row.
        if today_scoreboard_available and today_team_final_map and chosen_team_abbr:
            team_final = today_team_final_map.get(chosen_team_abbr)
            recent_log = (today_date - logged_at.date()).days <= 1
            if recent_log and team_final is False:
                return None

    if chosen_dt.date() == today_date:
        if not today_scoreboard_available:
            return None
        if not chosen_team_abbr or not chosen_opp_abbr:
            return None
        pair = tuple(sorted((chosen_team_abbr, chosen_opp_abbr)))
        if not today_final_matchup_pairs or pair not in today_final_matchup_pairs:
            return None
    elif chosen_dt.date() > today_date:
        # NBA game logs can sometimes carry a next-day date stamp relative to ET.
        # Allow grading only if today's scoreboard confirms this matchup finished.
        if not today_scoreboard_available:
            return None
        if not chosen_team_abbr or not chosen_opp_abbr:
            return None
        pair = tuple(sorted((chosen_team_abbr, chosen_opp_abbr)))
        if not today_final_matchup_pairs or pair not in today_final_matchup_pairs:
            return None

    return chosen_row


def _backtest_get_live_final_stat_value(player_id: int, stat: str) -> tuple[float | None, str]:
    try:
        payload = tracker_live_stat(player_id=player_id, stat=stat)
    except Exception:
        return None, "no_game"
    game_status = str(payload.get("game_status") or "").strip().lower()
    live_val = payload.get("live_val")
    if game_status == "final" and live_val is not None:
        try:
            return round(float(live_val), 1), game_status
        except Exception:
            return None, game_status
    return None, game_status


@app.post("/api/backtest/log")
def backtest_log_prediction(payload: dict = Body(...)) -> dict[str, Any]:
    """
    Log a prediction before a game.
    Body: { player, stat, line, side, confidence_score, confidence_tier, model_prob, odds? }
    """
    normalized_tier = _normalize_backtest_confidence_tier(payload.get("confidence_tier"), payload.get("confidence_score"))
    normalized_score = _normalize_backtest_confidence_score(payload.get("confidence_score"), normalized_tier)
    player_id_raw = payload.get("player_id")
    try:
        player_id = int(player_id_raw) if player_id_raw not in (None, "", "null") else None
    except Exception:
        player_id = None
    entry = {
        "id": _backtest_new_id(),
        "player": str(payload.get("player") or ""),
        "player_id": player_id,
        "stat": str(payload.get("stat") or ""),
        "line": float(payload.get("line") or 0),
        "side": str(payload.get("side") or ""),
        "confidence_score": normalized_score,
        "confidence_tier": normalized_tier,
        "model_prob": float(payload.get("model_prob") or 0.5),
        "odds": payload.get("odds"),  # decimal odds, optional
        "result": "pending",
        "actual_value": None,
        "logged_at": _utc_iso_z(),
        "resolved_at": None,
        "event_date": str(payload.get("event_date") or ""),
        "season": str(payload.get("season") or ""),
        "season_type": str(payload.get("season_type") or ""),
        "event_id": str(payload.get("event_id") or ""),
        "game_label": str(payload.get("game_label") or ""),
        "opponent_abbreviation": str(payload.get("opponent_abbreviation") or ""),
        "batch_id": str(payload.get("batch_id") or ""),
        "parlay_prop_key": str(payload.get("parlay_prop_key") or ""),
        "source": str(payload.get("source") or ""),
        "market_side": str(payload.get("market_side") or ""),
        "market_disagrees": bool(payload.get("market_disagrees")) if payload.get("market_disagrees") is not None else False,
        "notes": str(payload.get("notes") or ""),
    }
    with _BACKTEST_LOCK:
        _BACKTEST_LOG.append(entry)
        _save_backtest_log()
    _require_pg_backtest_write(_pg_write_backtest_entries, [entry])
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
                    actual_value >= entry["line"] if entry["side"] == "OVER"
                    else actual_value < entry["line"]
                )
                entry["result"] = "hit" if hit else "miss"
                entry["actual_value"] = actual_value
                entry["resolved_at"] = _utc_iso_z()
                _save_backtest_log()
                _require_pg_backtest_write(_pg_write_backtest_entries, [entry])
                return {"ok": True, "entry": entry}
    raise HTTPException(status_code=404, detail=f"No prediction found with id={pred_id}")


@app.post("/api/backtest/parlay/log-batch")
def backtest_log_parlay_batch(payload: dict = Body(...)) -> dict[str, Any]:
    raw_props = payload.get("props")
    if raw_props is None:
        raw_props = payload.get("all_props_scored")
    if not isinstance(raw_props, list) or not raw_props:
        raise HTTPException(status_code=400, detail="props list is required")

    filter_mode = str(payload.get("filter_mode") or "action").strip().lower()
    if filter_mode not in {"action", "selected", "broad", "playoff_high"}:
        filter_mode = "action"
    source_label = str(payload.get("source") or "parlay_builder").strip() or "parlay_builder"
    season_input = str(payload.get("season") or "").strip()
    season_type_input = normalize_requested_season_type(payload.get("season_type"))
    batch_id = str(payload.get("batch_id") or uuid4().hex[:10]).strip()
    now_iso = _utc_iso_z()

    with _BACKTEST_LOCK:
        existing_prop_keys = {
            str(entry.get("parlay_prop_key") or "").strip()
            for entry in _BACKTEST_LOG
            if isinstance(entry, dict) and str(entry.get("parlay_prop_key") or "").strip()
        }

    logged_entries: list[dict[str, Any]] = []
    skipped_invalid = 0
    skipped_filter = 0
    skipped_duplicate = 0

    for raw_prop in raw_props:
        if not isinstance(raw_prop, dict):
            skipped_invalid += 1
            continue
        if not _backtest_parlay_prop_passes_action_filter(raw_prop, filter_mode):
            skipped_filter += 1
            continue
        player_name = str(raw_prop.get("player_name") or "").strip()
        stat = str(raw_prop.get("stat") or "").strip().upper()
        side = str(raw_prop.get("side") or "").strip().upper()
        if not player_name or not stat or side not in {"OVER", "UNDER"}:
            skipped_invalid += 1
            continue
        try:
            line = float(raw_prop.get("line"))
        except Exception:
            skipped_invalid += 1
            continue
        try:
            model_prob = float(raw_prop.get("model_probability") or 50.0) / 100.0
        except Exception:
            model_prob = 0.5
        event_date = _backtest_parlay_event_date(raw_prop)
        event_dt = parse_game_date_any(event_date)
        season = season_input or _nba_season_for_datetime(event_dt)
        season_type = normalize_requested_season_type(raw_prop.get("season_type") or season_type_input)
        unique_key = _backtest_parlay_unique_key(raw_prop, event_date)
        if unique_key in existing_prop_keys:
            skipped_duplicate += 1
            continue
        existing_prop_keys.add(unique_key)

        confidence_score = _normalize_backtest_confidence_score(
            raw_prop.get("confidence_score"),
            _normalize_backtest_confidence_tier(raw_prop.get("confidence_tier"), raw_prop.get("confidence_score")),
        )
        confidence_tier = _normalize_backtest_confidence_tier(raw_prop.get("confidence_tier"), confidence_score)
        try:
            player_id = int(raw_prop.get("player_id")) if raw_prop.get("player_id") not in (None, "", "null") else None
        except Exception:
            player_id = None

        entry = {
            "id": _backtest_new_id(),
            "player": player_name,
            "player_id": player_id,
            "stat": stat,
            "line": line,
            "side": side,
            "confidence_score": confidence_score,
            "confidence_tier": confidence_tier,
            "model_prob": max(0.0, min(1.0, model_prob)),
            "odds": raw_prop.get("odds"),
            "result": "pending",
            "actual_value": None,
            "logged_at": now_iso,
            "resolved_at": None,
            "event_date": event_date,
            "season": season,
            "season_type": season_type,
            "event_id": str(raw_prop.get("event_id") or ""),
            "game_label": str(raw_prop.get("game_label") or ""),
            "opponent_abbreviation": str(raw_prop.get("opponent_abbreviation") or ""),
            "batch_id": batch_id,
            "parlay_prop_key": unique_key,
            "source": source_label,
            "market_side": str(raw_prop.get("market_side") or ""),
            "market_disagrees": bool(raw_prop.get("market_disagrees")) if raw_prop.get("market_disagrees") is not None else False,
            "notes": str(raw_prop.get("selection_reason") or ""),
        }
        logged_entries.append(entry)

    if logged_entries:
        with _BACKTEST_LOCK:
            _BACKTEST_LOG.extend(logged_entries)
            _save_backtest_log()
        _require_pg_backtest_write(_pg_write_backtest_entries, logged_entries)

    return {
        "ok": True,
        "batch_id": batch_id,
        "filter_mode": filter_mode,
        "logged": len(logged_entries),
        "skipped_filter": skipped_filter,
        "skipped_invalid": skipped_invalid,
        "skipped_duplicate": skipped_duplicate,
        "total_props": len(raw_props),
    }


@app.post("/api/backtest/parlay/grade-batch")
def backtest_grade_parlay_batch(payload: dict = Body(default={})) -> dict[str, Any]:
    batch_id = str(payload.get("batch_id") or "").strip()
    all_sources = bool(payload.get("all_sources")) if payload.get("all_sources") is not None else False
    source_prefix = "" if all_sources else str(payload.get("source_prefix") or "parlay_builder").strip().lower()
    max_entries = max(1, min(int(payload.get("max_entries") or 500), 2000))
    run_id = str(payload.get("run_id") or uuid4().hex[:12]).strip()
    now_local = nba_now()
    now_local_normalized = _normalize_backtest_compare_datetime(now_local) or now_local
    today_final_matchup_pairs, today_team_final_map, today_scoreboard_available = _build_today_scoreboard_status()

    with _BACKTEST_LOCK:
        candidates = [
            copy.deepcopy(entry)
            for entry in _BACKTEST_LOG
            if isinstance(entry, dict)
            and str(entry.get("result") or "").strip().lower() == "pending"
            and (not batch_id or str(entry.get("batch_id") or "").strip() == batch_id)
            and (all_sources or str(entry.get("source") or "").strip().lower().startswith(source_prefix))
        ][:max_entries]

    checked = 0
    graded = 0
    skipped_no_player = 0
    skipped_no_game = 0
    skipped_not_finished = 0
    errors: list[str] = []
    updates_by_id: dict[str, dict[str, Any]] = {}

    for entry in candidates:
        checked += 1
        entry_id = str(entry.get("id") or "").strip()
        if not entry_id:
            continue

        player_id_raw = entry.get("player_id")
        player_id: int | None = None
        try:
            player_id = int(player_id_raw) if player_id_raw not in (None, "", "null") else None
        except Exception:
            player_id = None
        if not player_id:
            player_name = str(entry.get("player") or "").strip()
            player = find_player_by_name(player_name) if player_name else None
            player_id = int(player["id"]) if player else None
        if not player_id:
            skipped_no_player += 1
            continue

        event_dt = parse_game_date_any(entry.get("event_date")) or _coerce_datetime(entry.get("logged_at"))
        season = str(entry.get("season") or "").strip() or _nba_season_for_datetime(event_dt)
        season_type = normalize_requested_season_type(entry.get("season_type") or DEFAULT_SEASON_TYPE)
        stat = str(entry.get("stat") or "").strip().upper()
        side = str(entry.get("side") or "").strip().upper()
        try:
            line = float(entry.get("line"))
        except Exception:
            line = None
        if not stat or side not in {"OVER", "UNDER"} or line is None:
            skipped_no_game += 1
            continue
        if not event_dt:
            skipped_no_game += 1
            continue

        # Prefer live/final today-game feed so completed games grade immediately,
        # and live games are safely skipped.
        live_actual, live_status = _backtest_get_live_final_stat_value(player_id=player_id, stat=stat)
        if live_status in {"live", "scheduled"}:
            skipped_not_finished += 1
            continue
        if live_status == "final" and live_actual is not None:
            hit = live_actual >= line if side == "OVER" else live_actual < line
            updates_by_id[entry_id] = {
                "actual_value": live_actual,
                "result": "hit" if hit else "miss",
                "resolved_at": _utc_iso_z(),
                "resolved_game_date": _today_scoreboard_date(),
                "resolved_matchup": "",
                "auto_grade_run_id": run_id,
                "auto_graded_at": _utc_iso_z(),
            }
            continue

        try:
            game_rows = fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
        except Exception as exc:
            errors.append(f"{entry_id}: game log fetch failed ({exc})")
            continue

        picked_row = _pick_game_log_row_for_backtest_entry(
            entry=entry,
            rows=game_rows,
            now_local=now_local_normalized,
            today_final_matchup_pairs=today_final_matchup_pairs,
            today_team_final_map=today_team_final_map,
            today_scoreboard_available=today_scoreboard_available,
        )
        if not picked_row:
            entry_target_date = _normalize_backtest_compare_datetime(event_dt)
            if entry_target_date and entry_target_date.date() >= now_local_normalized.date():
                skipped_not_finished += 1
            else:
                skipped_no_game += 1
            continue

        game_dt = parse_game_date_any(picked_row.get("GAME_DATE"))
        game_dt = _normalize_backtest_compare_datetime(game_dt)
        if game_dt and game_dt.date() > now_local_normalized.date():
            matchup_info = parse_matchup_descriptor(str(picked_row.get("MATCHUP") or ""))
            row_team = str(matchup_info.get("team_abbreviation") or "").strip().upper()
            row_opp = str(matchup_info.get("opponent_abbreviation") or "").strip().upper()
            pair = tuple(sorted((row_team, row_opp))) if row_team and row_opp else None
            if (not today_scoreboard_available) or (not pair) or (pair not in today_final_matchup_pairs):
                skipped_not_finished += 1
                continue

        try:
            actual_value = round(float(compute_stat_value(picked_row, stat)), 1)
        except Exception as exc:
            errors.append(f"{entry_id}: stat compute failed ({exc})")
            continue

        hit = actual_value >= line if side == "OVER" else actual_value < line
        updates_by_id[entry_id] = {
            "actual_value": actual_value,
            "result": "hit" if hit else "miss",
            "resolved_at": _utc_iso_z(),
            "resolved_game_date": str(picked_row.get("GAME_DATE") or ""),
            "resolved_matchup": str(picked_row.get("MATCHUP") or ""),
            "auto_grade_run_id": run_id,
            "auto_graded_at": _utc_iso_z(),
        }

    if updates_by_id:
        updated_rows: list[dict[str, Any]] = []
        with _BACKTEST_LOCK:
            for entry in _BACKTEST_LOG:
                if not isinstance(entry, dict):
                    continue
                update = updates_by_id.get(str(entry.get("id") or "").strip())
                if not update:
                    continue
                entry.update(update)
                updated_rows.append(copy.deepcopy(entry))
            if updated_rows:
                _save_backtest_log()
        if updated_rows:
            graded = len(updated_rows)
            _require_pg_backtest_write(_pg_write_backtest_entries, updated_rows)

    return {
        "ok": True,
        "batch_id": batch_id,
        "run_id": run_id,
        "all_sources": all_sources,
        "checked": checked,
        "graded": graded,
        "pending_remaining": max(0, checked - graded - skipped_no_player - skipped_no_game - skipped_not_finished),
        "skipped_no_player": skipped_no_player,
        "skipped_no_game": skipped_no_game,
        "skipped_not_finished": skipped_not_finished,
        "errors": errors[:20],
    }


@app.post("/api/backtest/parlay/undo-grade")
def backtest_undo_parlay_grade(payload: dict = Body(...)) -> dict[str, Any]:
    run_id = str(payload.get("run_id") or "").strip()
    reverted_rows: list[dict[str, Any]] = []
    reverted = 0
    fallback_latest_ts: float | None = None
    fallback_candidate_ids: set[str] = set()

    if not run_id:
        with _BACKTEST_LOCK:
            for entry in _BACKTEST_LOG:
                if not isinstance(entry, dict):
                    continue
                if str(entry.get("result") or "").strip().lower() not in {"hit", "miss"}:
                    continue
                if not (str(entry.get("resolved_game_date") or "").strip() or str(entry.get("resolved_matchup") or "").strip()):
                    continue
                ts = _datetime_to_ts(entry.get("resolved_at"))
                if ts is None:
                    continue
                if fallback_latest_ts is None or ts > fallback_latest_ts:
                    fallback_latest_ts = ts
        if fallback_latest_ts is not None:
            cutoff = fallback_latest_ts - 20 * 60
            with _BACKTEST_LOCK:
                for entry in _BACKTEST_LOG:
                    if not isinstance(entry, dict):
                        continue
                    if str(entry.get("result") or "").strip().lower() not in {"hit", "miss"}:
                        continue
                    if not (str(entry.get("resolved_game_date") or "").strip() or str(entry.get("resolved_matchup") or "").strip()):
                        continue
                    ts = _datetime_to_ts(entry.get("resolved_at"))
                    if ts is None or ts < cutoff:
                        continue
                    fallback_candidate_ids.add(str(entry.get("id") or "").strip())

    with _BACKTEST_LOCK:
        for entry in _BACKTEST_LOG:
            if not isinstance(entry, dict):
                continue
            if run_id:
                if str(entry.get("auto_grade_run_id") or "").strip() != run_id:
                    continue
            else:
                if str(entry.get("id") or "").strip() not in fallback_candidate_ids:
                    continue
            if str(entry.get("result") or "").strip().lower() not in {"hit", "miss"}:
                continue
            entry["result"] = "pending"
            entry["actual_value"] = None
            entry["resolved_at"] = None
            entry["resolved_game_date"] = ""
            entry["resolved_matchup"] = ""
            entry["auto_grade_run_id"] = ""
            entry["auto_graded_at"] = ""
            reverted += 1
            reverted_rows.append(copy.deepcopy(entry))
        if reverted_rows:
            _save_backtest_log()

    if reverted_rows:
        _require_pg_backtest_write(_pg_write_backtest_entries, reverted_rows)

    return {
        "ok": True,
        "run_id": run_id,
        "fallback_used": not bool(run_id),
        "reverted": reverted,
    }


@app.post("/api/backtest/parlay/undo-batch")
def backtest_undo_parlay_batch(payload: dict = Body(...)) -> dict[str, Any]:
    batch_id = str(payload.get("batch_id") or "").strip()
    if not batch_id:
        raise HTTPException(status_code=400, detail="batch_id is required")

    source_prefix = str(payload.get("source_prefix") or "parlay_builder").strip().lower()
    only_pending = bool(payload.get("only_pending")) if payload.get("only_pending") is not None else True

    deleted_ids: list[str] = []
    with _BACKTEST_LOCK:
        keep_entries: list[dict[str, Any]] = []
        for entry in _BACKTEST_LOG:
            if not isinstance(entry, dict):
                keep_entries.append(entry)
                continue
            entry_batch = str(entry.get("batch_id") or "").strip()
            entry_source = str(entry.get("source") or "").strip().lower()
            entry_result = str(entry.get("result") or "").strip().lower()
            matches_batch = entry_batch == batch_id
            matches_source = entry_source.startswith(source_prefix)
            matches_pending = (entry_result == "pending") if only_pending else True
            if matches_batch and matches_source and matches_pending:
                deleted_ids.append(str(entry.get("id") or ""))
                continue
            keep_entries.append(entry)
        if deleted_ids:
            _BACKTEST_LOG[:] = keep_entries
            _save_backtest_log()

    if deleted_ids:
        _require_pg_backtest_write(_pg_delete_backtest_entries, deleted_ids)

    return {
        "ok": True,
        "batch_id": batch_id,
        "deleted": len(deleted_ids),
        "only_pending": only_pending,
    }


@app.get("/api/backtest/log")
def backtest_get_log(
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0, le=5000),
    search: str = Query("", max_length=120),
    result_filter: str = Query("all"),
    stat_filter: str = Query("all"),
    tier_filter: str = Query("all"),
    side_filter: str = Query("all"),
    date_range: str = Query("all"),
    view_mode: str = Query("active"),
    group_by: str = Query("none"),
) -> dict[str, Any]:
    """Return the backtest log with server-side filtering and pagination."""
    with _BACKTEST_LOCK:
        all_entries = [copy.deepcopy(entry) for entry in _BACKTEST_LOG]
    stats = _compute_backtest_stats(all_entries)
    filtered_entries = _filter_backtest_entries(
        all_entries,
        search=search,
        result_filter=result_filter,
        stat_filter=stat_filter,
        tier_filter=tier_filter,
        side_filter=side_filter,
        date_range=date_range,
        view_mode=view_mode,
    )
    total_filtered = len(filtered_entries)
    page_entries = filtered_entries[offset: offset + limit]
    grouped_entries = _backtest_group_entries(page_entries, group_by=group_by)
    return {
        "stats": stats,
        "entries": page_entries,
        "groups": grouped_entries,
        "meta": {
            "offset": offset,
            "limit": limit,
            "returned": len(page_entries),
            "total_filtered": total_filtered,
            "has_next": offset + limit < total_filtered,
            "has_prev": offset > 0,
            "group_by": group_by,
            "view_mode": view_mode,
            "date_range": date_range,
        },
    }


@app.get("/api/history/injury-reports")
def history_injury_reports(limit: int = Query(25, ge=1, le=200)) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "entries": [], "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_url, report_timestamp, report_label, payload, fetched_at
                FROM injury_reports
                ORDER BY report_timestamp DESC NULLS LAST, fetched_at DESC
                LIMIT %s;
                """,
                (int(limit),),
            )
            entries = []
            for report_url, report_timestamp, report_label, payload, fetched_at in cur.fetchall():
                entries.append({
                    "report_url": report_url,
                    "report_timestamp": str(report_timestamp) if report_timestamp else "",
                    "report_label": report_label or "",
                    "fetched_at": str(fetched_at) if fetched_at else "",
                    "rows_count": len((payload or {}).get("rows") or []) if isinstance(payload, dict) else 0,
                })
        return {"ok": True, "entries": entries}
    except Exception as exc:
        return {"ok": False, "entries": [], "error": str(exc)}


@app.get("/api/history/injury-reports/{report_url:path}")
def history_injury_report_payload(report_url: str) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "payload": None, "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT payload FROM injury_reports WHERE report_url = %s;",
                (str(report_url),),
            )
            row = cur.fetchone()
            if not row:
                return {"ok": False, "payload": None, "error": "Report not found."}
            payload = row[0] if isinstance(row[0], dict) else {}
        return {"ok": True, "payload": payload}
    except Exception as exc:
        return {"ok": False, "payload": None, "error": str(exc)}


def _extract_player_display_name(payload: dict[str, Any] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("DISPLAY_FIRST_LAST", "DISPLAY_LAST_COMMA_FIRST", "PLAYER_NAME", "full_name", "name"):
        value = payload.get(key)
        if value:
            return str(value)
    return ""


@app.get("/api/history/player-info")
def history_player_info(limit: int = Query(25, ge=1, le=200)) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "entries": [], "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT player_id, payload, updated_at
                FROM player_info_cache
                ORDER BY updated_at DESC
                LIMIT %s;
                """,
                (int(limit),),
            )
            entries = []
            for player_id, payload, updated_at in cur.fetchall():
                display_name = _extract_player_display_name(payload)
                team_abbr = ""
                if isinstance(payload, dict):
                    team_abbr = str(payload.get("TEAM_ABBREVIATION") or payload.get("team_abbreviation") or "").strip()
                entries.append({
                    "player_id": int(player_id),
                    "player_name": display_name or f"Player {player_id}",
                    "team_abbreviation": team_abbr,
                    "updated_at": str(updated_at) if updated_at else "",
                })
        return {"ok": True, "entries": entries}
    except Exception as exc:
        return {"ok": False, "entries": [], "error": str(exc)}


@app.get("/api/history/player-info/{player_id}")
def history_player_info_payload(player_id: int) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "payload": None, "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM player_info_cache WHERE player_id = %s;", (int(player_id),))
            row = cur.fetchone()
            if not row:
                return {"ok": False, "payload": None, "error": "Player info not found."}
            payload = row[0] if isinstance(row[0], dict) else {}
        return {"ok": True, "payload": payload}
    except Exception as exc:
        return {"ok": False, "payload": None, "error": str(exc)}


@app.get("/api/history/odds-snapshots")
def history_odds_snapshots(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "entries": [], "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, endpoint, params, fetched_at
                FROM odds_snapshots
                ORDER BY fetched_at DESC
                LIMIT %s;
                """,
                (int(limit),),
            )
            entries = [
                {
                    "id": row_id,
                    "endpoint": endpoint,
                    "params": params or {},
                    "fetched_at": str(fetched_at) if fetched_at else "",
                }
                for row_id, endpoint, params, fetched_at in cur.fetchall()
            ]
        return {"ok": True, "entries": entries}
    except Exception as exc:
        return {"ok": False, "entries": [], "error": str(exc)}


@app.get("/api/history/odds-snapshots/{snapshot_id}")
def history_odds_snapshot_payload(snapshot_id: int) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "payload": None, "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM odds_snapshots WHERE id = %s;", (int(snapshot_id),))
            row = cur.fetchone()
            if not row:
                return {"ok": False, "payload": None, "error": "Snapshot not found."}
            payload = row[0] if isinstance(row[0], dict) else {}
        return {"ok": True, "payload": payload}
    except Exception as exc:
        return {"ok": False, "payload": None, "error": str(exc)}


@app.get("/api/history/market-scans")
def history_market_scans(limit: int = Query(25, ge=1, le=200)) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "entries": [], "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, payload, requested_at
                FROM market_scan_runs
                ORDER BY requested_at DESC
                LIMIT %s;
                """,
                (int(limit),),
            )
            entries = []
            for row_id, payload, requested_at in cur.fetchall():
                entries.append({
                    "id": row_id,
                    "requested_at": str(requested_at) if requested_at else "",
                    "count": len((payload or {}).get("results") or []) if isinstance(payload, dict) else 0,
                    "errors": len((payload or {}).get("errors") or []) if isinstance(payload, dict) else 0,
                })
        return {"ok": True, "entries": entries}
    except Exception as exc:
        return {"ok": False, "entries": [], "error": str(exc)}


@app.get("/api/history/market-scans/{scan_id}")
def history_market_scan_payload(scan_id: int) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "payload": None, "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM market_scan_runs WHERE id = %s;", (int(scan_id),))
            row = cur.fetchone()
            if not row:
                return {"ok": False, "payload": None, "error": "Scan not found."}
            payload = row[0] if isinstance(row[0], dict) else {}
        return {"ok": True, "payload": payload}
    except Exception as exc:
        return {"ok": False, "payload": None, "error": str(exc)}


@app.get("/api/history/parlay-runs")
def history_parlay_runs(limit: int = Query(25, ge=1, le=200)) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "entries": [], "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, payload, requested_at
                FROM parlay_builder_runs
                ORDER BY requested_at DESC
                LIMIT %s;
                """,
                (int(limit),),
            )
            entries = []
            for row_id, payload, requested_at in cur.fetchall():
                entries.append({
                    "id": row_id,
                    "requested_at": str(requested_at) if requested_at else "",
                    "legs": (payload or {}).get("legs") if isinstance(payload, dict) else None,
                    "props_found": (payload or {}).get("props_found") if isinstance(payload, dict) else None,
                    "errors": len((payload or {}).get("errors") or []) if isinstance(payload, dict) else 0,
                })
        return {"ok": True, "entries": entries}
    except Exception as exc:
        return {"ok": False, "entries": [], "error": str(exc)}


@app.get("/api/history/parlay-runs/{run_id}")
def history_parlay_run_payload(run_id: int) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "payload": None, "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM parlay_builder_runs WHERE id = %s;", (int(run_id),))
            row = cur.fetchone()
            if not row:
                return {"ok": False, "payload": None, "error": "Parlay run not found."}
            payload = row[0] if isinstance(row[0], dict) else {}
        return {"ok": True, "payload": payload}
    except Exception as exc:
        return {"ok": False, "payload": None, "error": str(exc)}


@app.get("/api/history/backtest-entries")
def history_backtest_entries(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "entries": [], "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT entry_id, payload, updated_at
                FROM backtest_log_entries
                ORDER BY updated_at DESC
                LIMIT %s;
                """,
                (int(limit),),
            )
            entries = []
            for entry_id, payload, updated_at in cur.fetchall():
                entries.append({
                    "id": entry_id,
                    "player": (payload or {}).get("player") if isinstance(payload, dict) else "",
                    "stat": (payload or {}).get("stat") if isinstance(payload, dict) else "",
                    "side": (payload or {}).get("side") if isinstance(payload, dict) else "",
                    "line": (payload or {}).get("line") if isinstance(payload, dict) else None,
                    "result": (payload or {}).get("result") if isinstance(payload, dict) else "",
                    "updated_at": str(updated_at) if updated_at else "",
                })
        return {"ok": True, "entries": entries}
    except Exception as exc:
        return {"ok": False, "entries": [], "error": str(exc)}


@app.get("/api/history/backtest-entries/{entry_id}")
def history_backtest_entry_payload(entry_id: str) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "payload": None, "error": "Postgres not configured."}
    try:
        with postgres_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM backtest_log_entries WHERE entry_id = %s;", (str(entry_id),))
            row = cur.fetchone()
            if not row:
                return {"ok": False, "payload": None, "error": "Backtest entry not found."}
            payload = row[0] if isinstance(row[0], dict) else {}
        return {"ok": True, "payload": payload}
    except Exception as exc:
        return {"ok": False, "payload": None, "error": str(exc)}


@app.post("/api/backtest/import")
def backtest_import_entries(payload: dict = Body(...)) -> dict[str, Any]:
    raw_entries = payload.get("entries") or []
    if not isinstance(raw_entries, list) or not raw_entries:
        raise HTTPException(status_code=400, detail="entries list is required")
    added, updated, skipped, imported = _merge_backtest_entries(raw_entries)
    if imported:
        _require_pg_backtest_write(_pg_write_backtest_entries, imported)
    return {"ok": True, "added": added, "updated": updated, "skipped": skipped, "entries": imported}


@app.post("/api/backtest/sync-postgres")
def backtest_sync_from_postgres(payload: dict = Body(default={})) -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "added": 0, "skipped": 0, "entries": [], "error": "Postgres not configured."}
    limit = max(1, min(int(payload.get("limit") or 5000), 20000))
    pg_entries = _pg_fetch_backtest_entries(limit=limit)
    if not pg_entries:
        return {"ok": True, "added": 0, "updated": 0, "skipped": 0, "entries": [], "fetched": 0}
    added, updated, skipped, imported = _merge_backtest_entries(pg_entries)
    return {
        "ok": True,
        "added": added,
        "updated": updated,
        "skipped": skipped,
        "entries": imported,
        "fetched": len(pg_entries),
    }


@app.post("/api/backtest/push-postgres")
def backtest_push_to_postgres() -> dict[str, Any]:
    if not postgres_available():
        return {"ok": False, "pushed": 0, "error": "Postgres not configured."}
    with _BACKTEST_LOCK:
        entries = [copy.deepcopy(entry) for entry in _BACKTEST_LOG if isinstance(entry, dict)]
    if not entries:
        return {"ok": True, "pushed": 0}
    _require_pg_backtest_write(_pg_write_backtest_entries, entries)
    return {"ok": True, "pushed": len(entries)}


@app.get("/api/backtest/export")
def backtest_export_csv(
    search: str = Query("", max_length=120),
    result_filter: str = Query("all"),
    stat_filter: str = Query("all"),
    tier_filter: str = Query("all"),
    side_filter: str = Query("all"),
    date_range: str = Query("all"),
    view_mode: str = Query("active"),
) -> StreamingResponse:
    with _BACKTEST_LOCK:
        all_entries = [copy.deepcopy(entry) for entry in _BACKTEST_LOG]
    filtered_entries = _filter_backtest_entries(
        all_entries,
        search=search,
        result_filter=result_filter,
        stat_filter=stat_filter,
        tier_filter=tier_filter,
        side_filter=side_filter,
        date_range=date_range,
        view_mode=view_mode,
    )
    filename_suffix = str(date_range or view_mode or "backtest").replace("/", "-")
    csv_payload = _build_backtest_csv(filtered_entries)
    response = StreamingResponse(iter([csv_payload]), media_type="text/csv")
    response.headers["Content-Disposition"] = f'attachment; filename="backtest-{filename_suffix}.csv"'
    return response


@app.post("/api/backtest/archive")
def backtest_archive_entries(payload: dict = Body(default={})) -> dict[str, Any]:
    older_than_days = max(1, min(int(payload.get("older_than_days") or 90), 3650))
    archive_pending = bool(payload.get("archive_pending")) if payload.get("archive_pending") is not None else False
    cutoff = _utc_now_naive() - timedelta(days=older_than_days)
    archived: list[dict[str, Any]] = []
    archived_ids: list[str] = []
    with _BACKTEST_LOCK:
        keep_entries: list[dict[str, Any]] = []
        for entry in _BACKTEST_LOG:
            entry_dt = _backtest_entry_datetime(entry)
            result_value = str(entry.get("result") or "").strip().lower()
            eligible = bool(entry_dt and entry_dt < cutoff and (archive_pending or result_value in {"hit", "miss"}))
            if eligible:
                archived.append(copy.deepcopy(entry))
                archived_ids.append(str(entry.get("id") or ""))
            else:
                keep_entries.append(entry)
        if archived:
            _BACKTEST_LOG[:] = keep_entries
            _save_backtest_log()
    for entry_id in archived_ids:
        if entry_id:
            _require_pg_backtest_write(_pg_delete_backtest_entry, entry_id)
    return {
        "ok": True,
        "archived_count": len(archived),
        "older_than_days": older_than_days,
        "csv": _build_backtest_csv(archived) if archived else "",
    }


@app.delete("/api/backtest/log/{entry_id}")
def backtest_delete_entry(entry_id: str) -> dict[str, Any]:
    """Delete a single backtest entry by ID."""
    with _BACKTEST_LOCK:
        before = len(_BACKTEST_LOG)
        _BACKTEST_LOG[:] = [e for e in _BACKTEST_LOG if e["id"] != entry_id]
        after = len(_BACKTEST_LOG)
        if before != after:
            _save_backtest_log()
    if before == after:
        raise HTTPException(status_code=404, detail=f"No entry found with id={entry_id}")
    _require_pg_backtest_write(_pg_delete_backtest_entry, entry_id)
    return {"ok": True, "deleted": entry_id}


@app.delete("/api/backtest/log")
def backtest_clear_log() -> dict[str, Any]:
    """Clear all backtest entries."""
    with _BACKTEST_LOCK:
        count = len(_BACKTEST_LOG)
        _BACKTEST_LOG.clear()
        _save_backtest_log()
    _require_pg_backtest_write(_pg_clear_backtest_entries)
    return {"ok": True, "cleared": count}


@app.post("/api/odds/check-quota")
def odds_check_quota(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Check remaining Odds API quota without loading events (uses a lightweight sports list call)."""
    api_key = normalize_odds_api_key(payload.get("api_key") or "")
    if not api_key:
        raise HTTPException(status_code=400, detail="Missing API key.")
    # Hit the /sports endpoint  it's the cheapest call (costs 0 credits on free tier)
    try:
        result = odds_api_fetch(
            "/sports",
            api_key,
            {"all": "false"},
        )
    except Exception as exc:
        detail = f"Odds API check failed: {exc}"
        return {
            "ok": False,
            "quota": None,
            "api_key_masked": mask_api_key_for_display(api_key),
            "error": detail,
        }
    return {
        "ok": True,
        "quota": result["quota"],
        "api_key_masked": mask_api_key_for_display(api_key),
    }


@app.get("/api/key-vault")
def key_vault_get() -> dict[str, Any]:
    if postgres_available():
        pg_state = _pg_read_key_vault_state()
        if isinstance(pg_state, dict):
            raw_entries = pg_state.get("entries") or []
            entries = _sanitize_key_vault_entries(raw_entries)
            active_id = str(pg_state.get("active_id") or "")
            valid_ids = {str(entry.get("id") or "") for entry in entries}
            if active_id and active_id not in valid_ids:
                active_id = ""
            if len(entries) != len(raw_entries) and POSTGRES_CACHE_WRITE_ENABLED:
                try:
                    _pg_write_key_vault_state({"entries": list(entries), "active_id": active_id})
                except Exception:
                    pass
            with _KEY_VAULT_LOCK:
                _KEY_VAULT_STATE["entries"] = entries
                _KEY_VAULT_STATE["active_id"] = active_id
            return {"entries": entries, "active_id": active_id}
    with _KEY_VAULT_LOCK:
        return {
            "entries": list(_sanitize_key_vault_entries(_KEY_VAULT_STATE.get("entries") or [])),
            "active_id": str(_KEY_VAULT_STATE.get("active_id") or ""),
        }


@app.put("/api/key-vault")
def key_vault_put(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    entries = payload.get("entries") or []
    active_id = str(payload.get("active_id") or "")
    mode = str(payload.get("mode") or "merge").strip().lower()
    if mode not in {"merge", "replace"}:
        mode = "merge"
    if not isinstance(entries, list):
        raise HTTPException(status_code=400, detail="entries must be a list")
    incoming = _sanitize_key_vault_entries(entries)

    existing_entries: list[dict[str, Any]] = []
    existing_active_id = ""
    if postgres_available():
        pg_state = _pg_read_key_vault_state()
        if isinstance(pg_state, dict):
            existing_entries = _sanitize_key_vault_entries(pg_state.get("entries") or [])
            existing_active_id = str(pg_state.get("active_id") or "")
    if not existing_entries:
        with _KEY_VAULT_LOCK:
            existing_entries = _sanitize_key_vault_entries(_KEY_VAULT_STATE.get("entries") or [])
            existing_active_id = str(_KEY_VAULT_STATE.get("active_id") or "")
    if not existing_entries:
        local_backup = _read_local_key_vault_backup_state()
        if isinstance(local_backup, dict):
            existing_entries = list(local_backup.get("entries") or [])
            existing_active_id = str(local_backup.get("active_id") or "")

    effective_mode = mode
    shrink_guard_applied = False
    if mode == "replace":
        sanitized = incoming
        existing_count = len(existing_entries)
        incoming_count = len(sanitized)
        allow_shrink = bool(payload.get("allow_shrink")) if payload.get("allow_shrink") is not None else False
        if (
            not allow_shrink
            and existing_count >= KEY_VAULT_SHRINK_GUARD_MIN_EXISTING
            and incoming_count < existing_count
            and (incoming_count / float(max(1, existing_count))) < KEY_VAULT_SHRINK_GUARD_RATIO
        ):
            # Defensive fallback: avoid destructive replace from stale/partial clients.
            merged_by_key: dict[str, dict[str, Any]] = {}
            for entry in existing_entries:
                key_text = str(entry.get("key") or "").strip()
                if key_text:
                    merged_by_key[key_text] = copy.deepcopy(entry)
            for entry in incoming:
                key_text = str(entry.get("key") or "").strip()
                if key_text:
                    prev = merged_by_key.get(key_text) or {}
                    merged_by_key[key_text] = {**prev, **copy.deepcopy(entry)}
            sanitized = list(merged_by_key.values())
            effective_mode = "merge_guarded"
            shrink_guard_applied = True
            LOGGER.warning(
                "Key vault shrink guard prevented destructive replace (incoming=%s existing=%s)",
                incoming_count,
                existing_count,
            )
    else:
        merged_by_key: dict[str, dict[str, Any]] = {}
        for entry in existing_entries:
            key_text = str(entry.get("key") or "").strip()
            if key_text:
                merged_by_key[key_text] = copy.deepcopy(entry)
        for entry in incoming:
            key_text = str(entry.get("key") or "").strip()
            if not key_text:
                continue
            prev = merged_by_key.get(key_text) or {}
            merged_by_key[key_text] = {**prev, **copy.deepcopy(entry)}
        sanitized = list(merged_by_key.values())

    chosen_active_id = active_id or existing_active_id
    if chosen_active_id:
        valid_ids = {str(entry.get("id") or "") for entry in sanitized}
        if chosen_active_id not in valid_ids:
            chosen_active_id = ""

    with _KEY_VAULT_LOCK:
        _KEY_VAULT_STATE["entries"] = sanitized
        _KEY_VAULT_STATE["active_id"] = chosen_active_id
    if postgres_available() and POSTGRES_CACHE_WRITE_ENABLED:
        try:
            _pg_write_key_vault_state({"entries": list(sanitized), "active_id": chosen_active_id})
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Key vault Postgres write failed: {exc}")
    _save_key_vault_state()
    return {
        "ok": True,
        "count": len(sanitized),
        "active_id": chosen_active_id,
        "mode": effective_mode,
        "shrink_guard_applied": shrink_guard_applied,
    }


@app.get("/api/favorites")
def favorites_get() -> dict[str, Any]:
    with _FAVORITES_LOCK:
        entries = list(_FAVORITES_STATE)
    return {"ok": True, "entries": entries}


@app.put("/api/favorites")
def favorites_put(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    entries = payload.get("entries") or []
    if not isinstance(entries, list):
        raise HTTPException(status_code=400, detail="entries must be a list")
    sanitized: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for raw in entries[:24]:
        if not isinstance(raw, dict):
            continue
        item_type = str(raw.get("type") or "").strip()
        item_key = str(raw.get("key") or "").strip()
        if not item_type or not item_key:
            continue
        dedupe = f"{item_type}:{item_key}"
        if dedupe in seen_keys:
            continue
        seen_keys.add(dedupe)
        sanitized.append(raw)
    with _FAVORITES_LOCK:
        _FAVORITES_STATE.clear()
        _FAVORITES_STATE.extend(sanitized)
    _save_favorites_state()
    return {"ok": True, "count": len(sanitized), "entries": sanitized}


@app.get("/api/tracker/props")
def tracker_props_get() -> dict[str, Any]:
    with _TRACKER_LOCK:
        entries = list(_TRACKER_STATE)
    return {"ok": True, "entries": entries}


@app.put("/api/tracker/props")
def tracker_props_put(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    entries = payload.get("entries") or []
    if not isinstance(entries, list):
        raise HTTPException(status_code=400, detail="entries must be a list")
    sanitized: list[dict[str, Any]] = []
    for raw in entries[:250]:
        if not isinstance(raw, dict):
            continue
        sanitized.append(copy.deepcopy(raw))
    with _TRACKER_LOCK:
        _TRACKER_STATE.clear()
        _TRACKER_STATE.extend(sanitized)
    _save_tracker_state()
    return {"ok": True, "count": len(sanitized), "entries": sanitized}


@app.post("/api/odds/game-context")
def odds_game_context(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    api_key = normalize_odds_api_key(payload.get("api_key") or "")
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

    events_result = odds_api_fetch(
        f"/sports/{sport}/events",
        api_key,
        {"dateFormat": "iso"},
    )
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

