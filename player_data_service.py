from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class PlayerDataService:
    game_log_cache_schema_version: str
    cache_ttl_seconds: int
    profile_ttl_seconds: int
    team_roster_cache_ttl_seconds: int
    game_log_cache: dict[tuple[int, str, str, str], dict[str, Any]]
    recent_game_log_cache: dict[tuple[int, str, str, int, str], dict[str, Any]]
    game_log_failure_meta: dict[tuple[int, str, str, str], dict[str, float]]
    roster_cache: dict[tuple[int, str], dict[str, Any]]
    team_roster_failure_meta: dict[tuple[int, str], dict[str, float]]
    player_info_cache: dict[int, dict[str, Any]]
    player_info_failure_meta: dict[int, dict[str, float]]
    next_game_cache: dict[tuple[int, str, str], dict[str, Any]]
    next_game_failure_meta: dict[tuple[int, str, str], dict[str, float]]
    player_game_log_cls: Any
    common_team_roster_cls: Any
    common_player_info_cls: Any
    player_next_n_games_cls: Any
    dedupe_game_log_rows: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    throttle_request: Callable[[], None]
    call_nba_with_retries: Callable[..., Any]
    save_persistent_caches: Callable[[], None]
    is_transient_nba_error: Callable[[Exception], bool]
    game_log_cache_is_reliable_stale: Callable[[dict[str, Any] | None, float], bool]
    player_info_cache_is_reliable_stale: Callable[[dict[str, Any] | None, float], bool]
    next_game_cache_is_reliable_stale: Callable[[dict[str, Any] | None, float], bool]
    team_roster_cache_is_reliable_stale: Callable[[dict[str, Any] | None, float | None], bool]
    game_log_failure_cooldown_seconds: int
    game_log_fetch_budget_with_reliable_stale_seconds: int
    game_log_fetch_budget_no_reliable_stale_seconds: int
    game_log_fetch_attempts_with_reliable_stale: int
    game_log_fetch_attempts_no_reliable_stale: int
    game_log_fetch_timeout_cap_seconds: int
    game_log_fetch_timeout_floor_seconds: int
    nba_backoff_factor: float
    team_roster_failure_cooldown_seconds: int
    team_roster_fetch_budget_with_reliable_stale_seconds: int
    team_roster_fetch_budget_no_reliable_stale_seconds: int
    team_roster_fetch_attempts_with_reliable_stale: int
    team_roster_fetch_attempts_no_reliable_stale: int
    team_roster_fetch_timeout_cap_seconds: int
    team_roster_fetch_timeout_floor_seconds: int
    team_roster_retry_base_delay: float
    player_info_failure_cooldown_seconds: int
    player_info_fetch_budget_with_reliable_stale_seconds: int
    player_info_fetch_budget_no_reliable_stale_seconds: int
    player_info_fetch_attempts_with_reliable_stale: int
    player_info_fetch_attempts_no_reliable_stale: int
    player_info_fetch_timeout_cap_seconds: int
    player_info_fetch_timeout_floor_seconds: int
    player_info_retry_base_delay: float
    next_game_ttl_seconds: int
    next_game_failure_cooldown_seconds: int
    next_game_fetch_budget_with_reliable_stale_seconds: int
    next_game_fetch_budget_no_reliable_stale_seconds: int
    next_game_fetch_attempts_with_reliable_stale: int
    next_game_fetch_attempts_no_reliable_stale: int
    next_game_fetch_timeout_cap_seconds: int
    next_game_fetch_timeout_floor_seconds: int
    next_game_retry_base_delay: float
    http_exception_cls: Any

    def fetch_player_game_log(self, player_id: int, season: str, season_type: str) -> list[dict[str, Any]]:
        cache_key = (player_id, season, season_type, self.game_log_cache_schema_version)
        cached = self.game_log_cache.get(cache_key)
        cached_ts = float((cached or {}).get("timestamp") or 0.0)
        now_ts = time.time()

        if cached and now_ts - cached_ts < self.cache_ttl_seconds:
            sanitized_cached_rows = self.dedupe_game_log_rows(cached["rows"])
            if sanitized_cached_rows != cached.get("rows"):
                self.game_log_cache[cache_key] = {"timestamp": cached_ts or time.time(), "rows": sanitized_cached_rows}
            return sanitized_cached_rows

        has_reliable_stale = bool(cached and self.game_log_cache_is_reliable_stale(cached, cached_ts))
        failure_meta = self.game_log_failure_meta.get(cache_key) or {}
        last_failure = float(failure_meta.get("last_failure") or 0.0)
        if has_reliable_stale and now_ts - last_failure < self.game_log_failure_cooldown_seconds:
            return self.dedupe_game_log_rows(cached["rows"])

        total_budget_seconds = (
            self.game_log_fetch_budget_with_reliable_stale_seconds
            if has_reliable_stale
            else self.game_log_fetch_budget_no_reliable_stale_seconds
        )
        max_attempts = (
            self.game_log_fetch_attempts_with_reliable_stale
            if has_reliable_stale
            else self.game_log_fetch_attempts_no_reliable_stale
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
                self.game_log_fetch_timeout_floor_seconds,
                min(self.game_log_fetch_timeout_cap_seconds, int(remaining_budget)),
            )

            self.throttle_request()
            try:
                response = self.player_game_log_cls(
                    player_id=player_id,
                    season=season,
                    season_type_all_star=season_type,
                    timeout=timeout_seconds,
                )
                df = response.get_data_frames()[0]
                break
            except Exception as exc:
                last_exc = exc
                if not self.is_transient_nba_error(exc):
                    break
                elapsed = time.monotonic() - started_at
                remaining_budget = total_budget_seconds - elapsed
                if attempt >= max_attempts - 1 or remaining_budget <= 0:
                    break
                sleep_for = min(self.nba_backoff_factor * (2 ** attempt), max(0.0, remaining_budget))
                if sleep_for > 0:
                    time.sleep(sleep_for)

        if df is None:
            self.game_log_failure_meta[cache_key] = {"last_failure": time.time()}
            if has_reliable_stale:
                return self.dedupe_game_log_rows(cached["rows"])
            detail_suffix = f" Details: {last_exc}" if last_exc else ""
            raise self.http_exception_cls(
                status_code=502,
                detail=(
                    "NBA data request failed. This can happen when NBA stats throttles or times out."
                    f" Total live fetch budget: {int(total_budget_seconds)}s."
                    f"{detail_suffix}"
                ),
            ) from last_exc

        if df.empty:
            raise self.http_exception_cls(status_code=404, detail="No game logs found for this player and season.")

        df["GAME_DATE"] = df["GAME_DATE"].astype(str)
        rows = self.dedupe_game_log_rows(df.to_dict(orient="records"))
        self.game_log_failure_meta.pop(cache_key, None)
        self.game_log_cache[cache_key] = {"timestamp": time.time(), "rows": rows}
        return rows

    def fetch_recent_player_game_log(self, player_id: int, season: str, season_type: str, last_n: int) -> list[dict[str, Any]]:
        cache_key = (player_id, season, season_type, int(last_n), self.game_log_cache_schema_version)
        cached = self.recent_game_log_cache.get(cache_key)
        now_ts = time.time()
        if cached and now_ts - cached["timestamp"] < self.cache_ttl_seconds:
            return cached["rows"]
        full_rows = self.fetch_player_game_log(player_id=player_id, season=season, season_type=season_type)
        recent_rows = full_rows[:last_n]
        self.recent_game_log_cache[cache_key] = {"timestamp": time.time(), "rows": recent_rows}
        return recent_rows

    def fetch_team_roster(self, team_id: int, season: str) -> list[dict[str, Any]]:
        cache_key = (team_id, season)
        cached = self.roster_cache.get(cache_key)
        now_ts = time.time()
        cached_ts = float((cached or {}).get("timestamp") or 0.0)

        if cached and now_ts - cached_ts < self.team_roster_cache_ttl_seconds:
            return cached["rows"]

        reliable_stale_exists = self.team_roster_cache_is_reliable_stale(cached, cached_ts)
        failure_meta = self.team_roster_failure_meta.get(cache_key) or {}
        last_failure_ts = float(failure_meta.get("timestamp") or 0.0)
        if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < self.team_roster_failure_cooldown_seconds:
            return cached["rows"]

        budget_seconds = self.team_roster_fetch_budget_with_reliable_stale_seconds if reliable_stale_exists else self.team_roster_fetch_budget_no_reliable_stale_seconds
        attempts = self.team_roster_fetch_attempts_with_reliable_stale if reliable_stale_exists else self.team_roster_fetch_attempts_no_reliable_stale
        started_at = time.monotonic()
        last_exc: Exception | None = None

        for _attempt_idx in range(max(1, attempts)):
            elapsed = time.monotonic() - started_at
            remaining = budget_seconds - elapsed
            if remaining <= 0:
                break
            timeout_seconds = max(self.team_roster_fetch_timeout_floor_seconds, min(self.team_roster_fetch_timeout_cap_seconds, int(math.ceil(remaining))))
            try:
                response = self.call_nba_with_retries(
                    lambda timeout_seconds=timeout_seconds: self.common_team_roster_cls(team_id=team_id, season=season, timeout=timeout_seconds),
                    label="team roster request",
                    attempts=1,
                    base_delay=self.team_roster_retry_base_delay,
                )
                df = response.get_data_frames()[0]
                if df.empty:
                    raise self.http_exception_cls(status_code=404, detail="No roster found for this team and season.")
                rows = df.to_dict(orient="records")

                def jersey_sort_key(row: dict[str, Any]) -> tuple[int, str]:
                    raw_num = str(row.get("NUM", "")).strip()
                    jersey_num = int(raw_num) if raw_num.isdigit() else 999
                    return jersey_num, str(row.get("PLAYER", "")).lower()

                rows.sort(key=jersey_sort_key)
                self.roster_cache[cache_key] = {"timestamp": time.time(), "rows": rows}
                self.team_roster_failure_meta.pop(cache_key, None)
                self.save_persistent_caches()
                return rows
            except self.http_exception_cls:
                raise
            except Exception as exc:
                last_exc = exc
                self.team_roster_failure_meta[cache_key] = {"timestamp": time.time()}

        if cached and cached.get("rows"):
            return cached["rows"]
        detail = "Team roster request failed. This can happen when NBA stats throttles or times out."
        if last_exc is not None:
            detail = f"{detail} Details: {last_exc}"
        raise self.http_exception_cls(status_code=502, detail=detail)

    def fetch_common_player_info(self, player_id: int) -> dict[str, Any]:
        cached = self.player_info_cache.get(player_id)
        now_ts = time.time()
        cached_ts = float((cached or {}).get("timestamp") or 0.0)

        if cached and now_ts - cached_ts < self.profile_ttl_seconds:
            return cached["row"]

        reliable_stale_exists = self.player_info_cache_is_reliable_stale(cached, cached_ts)
        failure_meta = self.player_info_failure_meta.get(player_id) or {}
        last_failure_ts = float(failure_meta.get("timestamp") or 0.0)
        if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < self.player_info_failure_cooldown_seconds:
            return cached["row"]

        budget_seconds = self.player_info_fetch_budget_with_reliable_stale_seconds if reliable_stale_exists else self.player_info_fetch_budget_no_reliable_stale_seconds
        attempts = self.player_info_fetch_attempts_with_reliable_stale if reliable_stale_exists else self.player_info_fetch_attempts_no_reliable_stale
        started_at = time.monotonic()
        last_exc: Exception | None = None

        for attempt_idx in range(max(1, attempts)):
            elapsed = time.monotonic() - started_at
            remaining = budget_seconds - elapsed
            if remaining <= 0:
                break
            timeout_seconds = max(self.player_info_fetch_timeout_floor_seconds, min(self.player_info_fetch_timeout_cap_seconds, int(math.ceil(remaining))))
            try:
                response = self.call_nba_with_retries(
                    lambda timeout_seconds=timeout_seconds: self.common_player_info_cls(player_id=player_id, timeout=timeout_seconds),
                    label="player info request",
                    attempts=1,
                    base_delay=self.player_info_retry_base_delay,
                )
                df = response.get_data_frames()[0]
                if df.empty:
                    raise self.http_exception_cls(status_code=404, detail="Player info not found.")
                row = df.to_dict(orient="records")[0]
                self.player_info_cache[player_id] = {"timestamp": time.time(), "row": row}
                self.player_info_failure_meta.pop(player_id, None)
                self.save_persistent_caches()
                return row
            except self.http_exception_cls:
                raise
            except Exception as exc:
                last_exc = exc
                self.player_info_failure_meta[player_id] = {"timestamp": time.time()}
                if reliable_stale_exists:
                    return cached["row"]
                if attempt_idx < max(1, attempts) - 1:
                    time.sleep(min(self.player_info_retry_base_delay * (attempt_idx + 1), max(0.0, max(remaining - 1, 0.0))))

        if reliable_stale_exists and cached and cached.get("row"):
            return cached["row"]
        if cached and cached.get("row") and self.player_info_cache_is_reliable_stale(cached, cached_ts):
            return cached["row"]
        raise self.http_exception_cls(
            status_code=502,
            detail=("Player info request failed. This can happen when NBA stats throttles or times out. " f"Details: {last_exc}"),
        ) from last_exc

    def fetch_next_game(self, player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
        cache_key = (player_id, season, season_type)
        cached = self.next_game_cache.get(cache_key)
        now_ts = time.time()
        cached_ts = float((cached or {}).get("timestamp") or 0.0)

        if cached and now_ts - cached_ts < self.next_game_ttl_seconds:
            return cached.get("row")

        reliable_stale_exists = self.next_game_cache_is_reliable_stale(cached, cached_ts)
        failure_meta = self.next_game_failure_meta.get(cache_key) or {}
        last_failure_ts = float(failure_meta.get("timestamp") or 0.0)
        if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < self.next_game_failure_cooldown_seconds:
            return cached.get("row")

        budget_seconds = self.next_game_fetch_budget_with_reliable_stale_seconds if reliable_stale_exists else self.next_game_fetch_budget_no_reliable_stale_seconds
        attempts = self.next_game_fetch_attempts_with_reliable_stale if reliable_stale_exists else self.next_game_fetch_attempts_no_reliable_stale
        started_at = time.monotonic()
        last_exc: Exception | None = None

        for attempt_idx in range(max(1, attempts)):
            elapsed = time.monotonic() - started_at
            remaining = budget_seconds - elapsed
            if remaining <= 0:
                break
            timeout_seconds = max(self.next_game_fetch_timeout_floor_seconds, min(self.next_game_fetch_timeout_cap_seconds, int(math.ceil(remaining))))
            try:
                response = self.call_nba_with_retries(
                    lambda timeout_seconds=timeout_seconds: self.player_next_n_games_cls(
                        player_id=player_id,
                        number_of_games=1,
                        season=season,
                        season_type_all_star=season_type,
                        timeout=timeout_seconds,
                    ),
                    label="next game request",
                    attempts=1,
                    base_delay=self.next_game_retry_base_delay,
                )
                df = response.get_data_frames()[0]
                row = None if df.empty else df.to_dict(orient="records")[0]
                self.next_game_cache[cache_key] = {"timestamp": time.time(), "row": row}
                self.next_game_failure_meta.pop(cache_key, None)
                return row
            except Exception as exc:
                last_exc = exc
                self.next_game_failure_meta[cache_key] = {"timestamp": time.time()}
                if reliable_stale_exists:
                    return cached.get("row")
                if attempt_idx < max(1, attempts) - 1:
                    time.sleep(min(self.next_game_retry_base_delay * (attempt_idx + 1), max(0.0, max(remaining - 1, 0.0))))

        if cached:
            return cached.get("row")
        return None
