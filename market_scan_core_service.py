from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


PreparedMarketRow = tuple[
    int,
    dict[str, Any],
    float,
    float,
    str,
    str,
    dict[str, Any] | None,
    dict[str, Any] | None,
    int | None,
    str,
]


@dataclass
class MarketScanCoreService:
    current_nba_season: Callable[[], str]
    normalize_requested_season_type: Callable[[str | None], str]
    request_hash: Callable[[str, dict[str, Any]], str]
    read_market_scan_cache: Callable[[dict[str, Any]], dict[str, Any] | None]
    warm_injury_cache: Callable[[], Any]
    resolve_team_from_text: Callable[[str | None], dict[str, Any] | None]
    find_player_by_name: Callable[[str, int | None], dict[str, Any] | None]
    team_lookup: dict[int, dict[str, Any]]
    stat_map: dict[str, str]
    bulk_analysis_max_workers: Callable[[], int]
    prefetch_bulk_analysis_context: Callable[..., None]
    submit_analysis_task: Callable[..., Any]
    http_exception_cls: Any

    def prepare_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        rows = payload.get("rows") or []
        default_last_n = int(payload.get("last_n") or 10)
        selected_season = str(payload.get("season") or self.current_nba_season())
        season_type = self.normalize_requested_season_type(payload.get("season_type"))
        injury_aware = bool(payload.get("injury_aware"))

        if not isinstance(rows, list) or not rows:
            raise self.http_exception_cls(status_code=400, detail="Please provide at least one market row.")

        request_hash_value = self.request_hash("market_scan", payload)
        cached_run = self.read_market_scan_cache(payload)
        if cached_run:
            return {
                "cached_run": cached_run,
                "request_hash_value": request_hash_value,
                "rows": rows,
                "default_last_n": default_last_n,
                "selected_season": selected_season,
                "season_type": season_type,
                "injury_aware": injury_aware,
                "errors": [],
                "prepared_rows": [],
            }

        errors: list[dict[str, Any]] = []
        prepared_rows: list[PreparedMarketRow] = []

        try:
            self.warm_injury_cache()
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
            home_team_text = str(row.get("home_team") or "").strip()
            away_team_text = str(row.get("away_team") or "").strip()
            input_game_label = str(row.get("game_label") or "").strip()
            if stat not in self.stat_map:
                errors.append({"row": index, "player_name": player_name, "reason": f"Unsupported stat: {stat}"})
                continue
            try:
                line = float(row.get("line"))
                over_odds = float(row.get("over_odds"))
                under_odds = float(row.get("under_odds"))
            except (TypeError, ValueError):
                errors.append({"row": index, "player_name": player_name, "reason": "Line and odds must be numeric."})
                continue

            team = self.resolve_team_from_text(team_text) if team_text else None
            opponent = self.resolve_team_from_text(opponent_text) if opponent_text else None
            home_team = self.resolve_team_from_text(home_team_text) if home_team_text else None
            away_team = self.resolve_team_from_text(away_team_text) if away_team_text else None
            team_id = int(team["id"]) if team else None
            player = self.find_player_by_name(player_name, team_id)
            if not player:
                errors.append({"row": index, "player_name": player_name, "reason": "Player not found."})
                continue
            player_team_id = int(player.get("team_id") or 0) if player.get("team_id") else 0
            if player_team_id:
                player_team = self.team_lookup.get(player_team_id, {})
                if not team or int(team.get("id") or 0) != player_team_id:
                    team = player_team
                    team_text = str(player_team.get("abbreviation") or player_team.get("full_name") or team_text)
            if (not team_text or not opponent_text) and (home_team or away_team):
                if home_team and player_team_id and int(home_team.get("id") or 0) == player_team_id:
                    team_text = home_team_text or (home_team.get("abbreviation") or "")
                    opponent_text = away_team_text or (away_team.get("abbreviation") if away_team else opponent_text)
                    team = home_team
                    opponent = away_team
                elif away_team and player_team_id and int(away_team.get("id") or 0) == player_team_id:
                    team_text = away_team_text or (away_team.get("abbreviation") or "")
                    opponent_text = home_team_text or (home_team.get("abbreviation") if home_team else opponent_text)
                    team = away_team
                    opponent = home_team
                elif home_team and away_team and not team:
                    team_text = away_team_text or (away_team.get("abbreviation") or "")
                    opponent_text = home_team_text or (home_team.get("abbreviation") or "")
                    team = away_team
                    opponent = home_team
            team_id = int(team["id"]) if team else None

            bulk_row = {
                "player_id": int(player["id"]),
                "player_name": player_name or str(player.get("full_name") or ""),
                "stat": stat,
                "line": line,
                "team_id": team_id or (player_team_id if player_team_id else None),
                "player_position": None,
                "override_opponent_id": int(opponent["id"]) if opponent and int(opponent.get("id") or 0) != (team_id or player_team_id) else None,
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
            prepared_rows.append((index, bulk_row, over_odds, under_odds, team_text, opponent_text, team, opponent, team_id, input_game_label))

        return {
            "cached_run": None,
            "request_hash_value": request_hash_value,
            "rows": rows,
            "default_last_n": default_last_n,
            "selected_season": selected_season,
            "season_type": season_type,
            "injury_aware": injury_aware,
            "errors": errors,
            "prepared_rows": prepared_rows,
        }

    def prepare_analysis_context(
        self,
        *,
        payload: dict[str, Any],
        prepared_rows: list[PreparedMarketRow],
        default_last_n: int,
        selected_season: str,
        season_type: str,
    ) -> dict[str, Any]:
        defaults = {
            "last_n": int(default_last_n),
            "season": str(selected_season),
            "season_type": str(season_type),
        }

        configured_max_workers = max(1, int(self.bulk_analysis_max_workers()))
        requested_max_workers = payload.get("max_workers")
        max_workers = configured_max_workers
        try:
            if requested_max_workers not in (None, ""):
                max_workers = max(1, min(configured_max_workers, int(requested_max_workers)))
        except (TypeError, ValueError):
            max_workers = configured_max_workers
        max_workers = min(max_workers, max(1, len(prepared_rows)))

        local_cache: dict[tuple[Any, ...], dict[str, Any]] = {}
        analysis_by_row: dict[int, dict[str, Any]] = {}

        player_ids: set[int] = {int(bulk_row["player_id"]) for _, bulk_row, *_ in prepared_rows}
        team_ids: set[int] = set()
        primary_by_team: dict[int, int] = {}
        for row in prepared_rows:
            team_id = row[-2]
            if team_id in (None, ""):
                continue
            try:
                team_id_int = int(team_id)
            except (TypeError, ValueError):
                continue
            team_ids.add(team_id_int)
            if team_id_int not in primary_by_team:
                bulk_row = row[1]
                primary_by_team[team_id_int] = int(bulk_row["player_id"])
        self.prefetch_bulk_analysis_context(
            player_ids=player_ids,
            season=selected_season,
            season_type=season_type,
            team_ids=team_ids,
            primary_player_by_team=primary_by_team,
            max_workers=max_workers,
            label="market_scan",
        )

        analysis_key_by_row: dict[int, tuple[Any, ...]] = {}
        seen_analysis_keys: set[tuple[Any, ...]] = set()
        unique_analysis_jobs: list[tuple[int, dict[str, Any], tuple[Any, ...]]] = []
        for row_index, bulk_row, *_ in prepared_rows:
            analysis_key: tuple[Any, ...] = (
                int(bulk_row.get("player_id") or 0),
                str(bulk_row.get("stat") or ""),
                float(bulk_row.get("line") or 0.0),
                int(default_last_n),
                str(selected_season),
                str(season_type),
                int(bulk_row.get("team_id") or 0),
                int(bulk_row.get("override_opponent_id") or 0),
            )
            analysis_key_by_row[row_index] = analysis_key
            if analysis_key not in seen_analysis_keys:
                seen_analysis_keys.add(analysis_key)
                unique_analysis_jobs.append((row_index, bulk_row, analysis_key))

        return {
            "defaults": defaults,
            "max_workers": max_workers,
            "local_cache": local_cache,
            "analysis_by_row": analysis_by_row,
            "analysis_key_by_row": analysis_key_by_row,
            "unique_analysis_jobs": unique_analysis_jobs,
        }

    def run_analysis_jobs(
        self,
        *,
        unique_analysis_jobs: list[tuple[int, dict[str, Any], tuple[Any, ...]]],
        defaults: dict[str, Any],
        local_cache: dict[tuple[Any, ...], dict[str, Any]],
        max_workers: int,
        build_bulk_prop_item: Callable[[int, dict[str, Any], dict[str, Any], dict[tuple[Any, ...], dict[str, Any]]], dict[str, Any]],
        emit_progress: Callable[[str, dict[str, Any]], None],
    ) -> tuple[dict[tuple[Any, ...], dict[str, Any]], list[dict[str, Any]]]:
        analysis_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
        analysis_errors: list[dict[str, Any]] = []

        if max_workers <= 1:
            step = max(1, len(unique_analysis_jobs) // 10) if unique_analysis_jobs else 1
            done = 0
            for row_index, bulk_row, analysis_key in unique_analysis_jobs:
                try:
                    analysis_by_key[analysis_key] = build_bulk_prop_item(row_index, bulk_row, defaults, local_cache)
                except Exception as exc:
                    reason = str(exc)
                    detail = getattr(exc, "detail", None)
                    if detail is not None:
                        reason = str(detail)
                    analysis_errors.append({"row": row_index, "player_name": bulk_row.get("player_name"), "reason": reason})
                done += 1
                if done % step == 0 or done == len(unique_analysis_jobs):
                    emit_progress("analysis_progress", {"done": done, "total": len(unique_analysis_jobs)})
            return analysis_by_key, analysis_errors

        submitted: list[tuple[int, dict[str, Any], tuple[Any, ...], Any]] = []
        for batch_start in range(0, len(unique_analysis_jobs), max_workers):
            batch = unique_analysis_jobs[batch_start : batch_start + max_workers]
            for row_index, bulk_row, analysis_key in batch:
                submitted.append(
                    (
                        row_index,
                        bulk_row,
                        analysis_key,
                        self.submit_analysis_task(build_bulk_prop_item, row_index, bulk_row, defaults, local_cache),
                    )
                )
            step = max(1, len(unique_analysis_jobs) // 10) if unique_analysis_jobs else 1
            done = batch_start
            for row_index, bulk_row, analysis_key, future in submitted[batch_start : batch_start + len(batch)]:
                try:
                    analysis_by_key[analysis_key] = future.result()
                except Exception as exc:
                    reason = str(exc)
                    detail = getattr(exc, "detail", None)
                    if detail is not None:
                        reason = str(detail)
                    analysis_errors.append({"row": row_index, "player_name": bulk_row.get("player_name"), "reason": reason})
                done += 1
                if done % step == 0 or done == len(unique_analysis_jobs):
                    emit_progress("analysis_progress", {"done": done, "total": len(unique_analysis_jobs)})

        return analysis_by_key, analysis_errors
