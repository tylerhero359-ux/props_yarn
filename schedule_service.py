from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

from zoneinfo import ZoneInfo


@dataclass
class ScheduleDataService:
    team_lookup: dict[int, dict[str, Any]]
    scoreboard_cache: dict[str, dict[str, Any]]
    next_game_cache: dict[tuple[int, str, str], dict[str, Any]]
    next_game_failure_meta: dict[tuple[int, str, str], dict[str, float]]
    scoreboard_v2_cls: Any
    call_with_retries: Callable[..., Any]
    fetch_next_game: Callable[[int, str, str], dict[str, Any] | None]
    save_persistent_caches: Callable[[], None]
    safe_int_score: Callable[..., int]
    current_nba_game_date: Callable[[], str]
    timed_call: Callable[[str], Callable]
    next_game_ttl_seconds: int
    next_game_failure_cooldown_seconds: int
    scoreboard_cache_ttl_seconds: int
    scoreboard_max_stale_seconds: int
    scoreboard_failure_cooldown_seconds: int
    scoreboard_fetch_budget_with_reliable_stale_seconds: int
    scoreboard_fetch_budget_no_reliable_stale_seconds: int
    scoreboard_fetch_attempts_with_reliable_stale: int
    scoreboard_fetch_attempts_no_reliable_stale: int
    scoreboard_fetch_timeout_cap_seconds: int
    scoreboard_fetch_timeout_floor_seconds: int
    scoreboard_retry_base_delay: float
    next_game_cache_is_reliable_stale: Callable[[dict[str, Any] | None, float], bool]

    def _scoreboard_cache_is_reliable_stale(self, game_date: str, cached: dict[str, Any] | None, cached_ts: float) -> bool:
        if not cached:
            return False
        rows = cached.get("rows") or []
        if not rows:
            return False
        age_seconds = time.time() - float(cached_ts or 0.0)
        if age_seconds > self.scoreboard_max_stale_seconds:
            return False
        current_date = self.current_nba_game_date()
        return str(game_date or "").strip() == str(current_date or "").strip()

    def _get_scoreboard_failure_meta(self, game_date: str) -> dict[str, Any]:
        return self.scoreboard_cache.setdefault(f"__failure__::{game_date}", {})

    def fetch_scoreboard_games(self, game_date: str) -> list[dict[str, Any]]:
        game_date = str(game_date or "").strip()
        cached = self.scoreboard_cache.get(game_date)
        now_ts = time.time()

        if cached and now_ts - float(cached.get("timestamp") or 0.0) < self.scoreboard_cache_ttl_seconds:
            return cached.get("rows") or []

        cached_ts = float(cached.get("timestamp") or 0.0) if cached else 0.0
        reliable_stale_exists = self._scoreboard_cache_is_reliable_stale(game_date, cached, cached_ts)
        failure_meta = self._get_scoreboard_failure_meta(game_date)
        last_failure_ts = float(failure_meta.get("timestamp") or 0.0)

        if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < self.scoreboard_failure_cooldown_seconds:
            return cached.get("rows") or []

        total_budget = (
            self.scoreboard_fetch_budget_with_reliable_stale_seconds
            if reliable_stale_exists
            else self.scoreboard_fetch_budget_no_reliable_stale_seconds
        )
        max_attempts = (
            self.scoreboard_fetch_attempts_with_reliable_stale
            if reliable_stale_exists
            else self.scoreboard_fetch_attempts_no_reliable_stale
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
                self.scoreboard_fetch_timeout_floor_seconds,
                min(self.scoreboard_fetch_timeout_cap_seconds, int(remaining_budget)),
            )

            try:
                response = self.call_with_retries(
                    lambda timeout_seconds=timeout_seconds: self.scoreboard_v2_cls(
                        game_date=game_date,
                        day_offset=0,
                        league_id="00",
                        timeout=timeout_seconds,
                    ),
                    label="scoreboard request",
                    attempts=1,
                    base_delay=self.scoreboard_retry_base_delay,
                )
                header_df = response.game_header.get_data_frame()
                try:
                    line_score_df = response.line_score.get_data_frame()
                except Exception:
                    line_score_df = None

                rows = header_df.to_dict(orient="records") if not header_df.empty else []
                if not rows:
                    self.scoreboard_cache[game_date] = {"timestamp": time.time(), "rows": []}
                    self.scoreboard_cache.pop(f"__failure__::{game_date}", None)
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
                    enriched_row["PTS_HOME"] = self.safe_int_score(home_score_row.get("PTS"), row.get("PTS_HOME"), 0)
                    enriched_row["PTS_AWAY"] = self.safe_int_score(away_score_row.get("PTS"), row.get("PTS_AWAY"), 0)
                    enriched_rows.append(enriched_row)

                self.scoreboard_cache[game_date] = {"timestamp": time.time(), "rows": enriched_rows}
                self.scoreboard_cache.pop(f"__failure__::{game_date}", None)
                return enriched_rows
            except Exception as exc:
                attempts_made += 1
                last_exc = exc
                if reliable_stale_exists:
                    self.scoreboard_cache[f"__failure__::{game_date}"] = {"timestamp": time.time(), "error": str(exc)}
                    return cached.get("rows") or []
                if attempts_made >= max_attempts:
                    break

        if reliable_stale_exists:
            self.scoreboard_cache[f"__failure__::{game_date}"] = {"timestamp": time.time(), "error": str(last_exc) if last_exc else "timeout"}
            return cached.get("rows") or []

        if cached:
            self.scoreboard_cache[f"__failure__::{game_date}"] = {"timestamp": time.time(), "error": str(last_exc) if last_exc else "timeout"}
            return cached.get("rows") or []

        return []

    def build_scoreboard_next_game_payload(self, game_row: dict[str, Any], player_team_id: int | None) -> dict[str, Any] | None:
        if not player_team_id:
            return None

        home_team_id = int(game_row.get("HOME_TEAM_ID") or 0)
        visitor_team_id = int(game_row.get("VISITOR_TEAM_ID") or 0)

        if player_team_id == home_team_id:
            is_home = True
            opponent_team_id = visitor_team_id
            opponent = self.team_lookup.get(visitor_team_id, {})
            player_team = self.team_lookup.get(home_team_id, {})
            opponent_abbreviation = str(opponent.get("abbreviation") or "").strip()
            matchup_label = f"vs {opponent_abbreviation}" if opponent_abbreviation else "vs Opponent"
        elif player_team_id == visitor_team_id:
            is_home = False
            opponent_team_id = home_team_id
            opponent = self.team_lookup.get(home_team_id, {})
            player_team = self.team_lookup.get(visitor_team_id, {})
            opponent_abbreviation = str(opponent.get("abbreviation") or "").strip()
            matchup_label = f"@ {opponent_abbreviation}" if opponent_abbreviation else "@ Opponent"
        else:
            return None

        return {
            "game_date": str(game_row.get("GAME_DATE_EST") or "").strip(),
            "game_time": "",
            "is_home": is_home,
            "matchup_label": matchup_label,
            "opponent_team_id": opponent_team_id,
            "opponent_name": str(opponent.get("full_name") or "").strip(),
            "opponent_abbreviation": opponent_abbreviation,
            "player_team_abbreviation": str(player_team.get("abbreviation") or "").strip(),
        }

    def find_team_next_game_via_scoreboard(self, team_id: int | None, lookahead_days: int = 10) -> dict[str, Any] | None:
        if not team_id:
            return None

        start_date = datetime.now(ZoneInfo("America/New_York")).date()
        for offset in range(lookahead_days + 1):
            game_date = (start_date + timedelta(days=offset)).strftime("%Y-%m-%d")
            rows = self.fetch_scoreboard_games(game_date)
            for row in rows:
                payload = self.build_scoreboard_next_game_payload(row, team_id)
                if payload:
                    return payload
        return None

    def build_next_game_payload(self, next_game_row: dict[str, Any] | None, player_team_id: int | None) -> dict[str, Any] | None:
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

    def _resolve_team_next_game_impl(self, team_id: int | None, primary_player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
        if not team_id:
            return None

        cache_key = (team_id, season, season_type)
        cached = self.next_game_cache.get(cache_key)
        now_ts = time.time()
        cached_ts = float((cached or {}).get("timestamp") or 0.0)

        if cached and now_ts - cached_ts < self.next_game_ttl_seconds:
            return cached["row"]

        reliable_stale_exists = self.next_game_cache_is_reliable_stale(cached, cached_ts)
        failure_meta = self.next_game_failure_meta.get(cache_key) or {}
        last_failure_ts = float(failure_meta.get("timestamp") or 0.0)
        if reliable_stale_exists and last_failure_ts and (now_ts - last_failure_ts) < self.next_game_failure_cooldown_seconds:
            return cached.get("row")

        scoreboard_next_game = self.find_team_next_game_via_scoreboard(team_id=team_id, lookahead_days=10)
        if scoreboard_next_game:
            self.next_game_cache[cache_key] = {"timestamp": time.time(), "row": scoreboard_next_game}
            self.next_game_failure_meta.pop(cache_key, None)
            self.save_persistent_caches()
            return scoreboard_next_game

        next_game_row = self.fetch_next_game(primary_player_id, season, season_type)
        next_game = self.build_next_game_payload(next_game_row, team_id)
        if next_game:
            self.next_game_cache[cache_key] = {"timestamp": time.time(), "row": next_game}
            self.next_game_failure_meta.pop(cache_key, None)
            self.save_persistent_caches()
            return next_game

        self.next_game_failure_meta[cache_key] = {"timestamp": time.time()}
        if reliable_stale_exists and cached:
            return cached.get("row")

        self.next_game_cache[cache_key] = {"timestamp": time.time(), "row": None}
        self.save_persistent_caches()
        return None

    def resolve_team_next_game(self, team_id: int | None, primary_player_id: int, season: str, season_type: str) -> dict[str, Any] | None:
        return self.timed_call("resolve_team_next_game")(self._resolve_team_next_game_impl)(team_id, primary_player_id, season, season_type)
