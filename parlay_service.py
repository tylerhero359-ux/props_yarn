from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class ParlayService:
    submit_async_job: Callable[[str, Callable[..., dict[str, Any]], dict[str, Any]], dict[str, Any]]

    @staticmethod
    def run_sync(run_func, payload: dict[str, Any]) -> dict[str, Any]:
        return run_func(payload)

    def run_async(self, run_func, payload: dict[str, Any]) -> dict[str, Any]:
        return self.submit_async_job("parlay_builder", run_func, payload)

    @staticmethod
    def prepare_analysis_jobs(
        *,
        all_import_rows: list[dict[str, Any]],
        resolve_player: Callable[[str], dict[str, Any] | None],
    ) -> dict[str, Any]:
        prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
        analysis_errors: list[dict[str, Any]] = []

        for row in all_import_rows:
            player_name = str(row.get("player_name") or "").strip()
            player = resolve_player(player_name)
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

        seen_analysis_keys: set[tuple[Any, ...]] = set()
        deduped_prepared: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for bulk_row, orig_row in prepared:
            analysis_key = (bulk_row["player_id"], bulk_row["stat"], float(bulk_row["line"]))
            if analysis_key in seen_analysis_keys:
                continue
            seen_analysis_keys.add(analysis_key)
            deduped_prepared.append((bulk_row, orig_row))

        unique_player_ids: set[int] = {int(bulk_row["player_id"]) for bulk_row, _ in deduped_prepared}
        return {
            "analysis_errors": analysis_errors,
            "deduped_prepared": deduped_prepared,
            "unique_player_ids": unique_player_ids,
        }

    @staticmethod
    def run_bulk_analysis(
        *,
        deduped_prepared: list[tuple[dict[str, Any], dict[str, Any]]],
        defaults: dict[str, Any],
        local_cache: dict[tuple[Any, ...], dict[str, Any]],
        max_workers: int,
        build_bulk_prop_item: Callable[[int, dict[str, Any], dict[str, Any], dict[tuple[Any, ...], dict[str, Any]]], dict[str, Any]],
        submit_analysis_task: Callable[..., Any],
        emit_progress: Callable[[str, dict[str, Any]], None],
    ) -> tuple[list[tuple[dict[str, Any], dict[str, Any]]], list[dict[str, Any]]]:
        analysis_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
        analysis_errors: list[dict[str, Any]] = []

        emit_progress("analysis_start", {"total": len(deduped_prepared), "workers": max_workers})

        if max_workers <= 1:
            step = max(1, len(deduped_prepared) // 10) if deduped_prepared else 1
            done = 0
            for idx, (bulk_row, orig_row) in enumerate(deduped_prepared, start=1):
                try:
                    result = build_bulk_prop_item(idx, bulk_row, defaults, local_cache)
                    analysis_rows.append((result, orig_row))
                except Exception as exc:
                    analysis_errors.append({"player_name": bulk_row["player_name"], "reason": str(exc)})
                done += 1
                if done % step == 0 or done == len(deduped_prepared):
                    emit_progress("analysis_progress", {"done": done, "total": len(deduped_prepared)})
        else:
            step = max(1, len(deduped_prepared) // 10) if deduped_prepared else 1
            done = 0
            for batch_start in range(0, len(deduped_prepared), max_workers):
                chunk = deduped_prepared[batch_start : batch_start + max_workers]
                futures_list: list[tuple[int, dict[str, Any], dict[str, Any], Any]] = []
                for idx, (bulk_row, orig_row) in enumerate(chunk, start=batch_start + 1):
                    future = submit_analysis_task(build_bulk_prop_item, idx, bulk_row, defaults, local_cache)
                    futures_list.append((idx, bulk_row, orig_row, future))
                for idx, bulk_row, orig_row, future in futures_list:
                    try:
                        result = future.result()
                        analysis_rows.append((result, orig_row))
                    except Exception as exc:
                        analysis_errors.append({"player_name": bulk_row["player_name"], "reason": str(exc)})
                    done += 1
                    if done % step == 0 or done == len(deduped_prepared):
                        emit_progress("analysis_progress", {"done": done, "total": len(deduped_prepared)})

        emit_progress("analysis_done", {"analyzed": len(analysis_rows), "errors": len(analysis_errors)})
        return analysis_rows, analysis_errors

    @staticmethod
    def run_scoring_rows(
        *,
        analysis_rows: list[tuple[dict[str, Any], dict[str, Any]]],
        score_row: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any] | None],
        emit_progress: Callable[[str, dict[str, Any]], None],
    ) -> list[dict[str, Any]]:
        scored: list[dict[str, Any]] = []
        scoring_step = max(1, len(analysis_rows) // 10) if analysis_rows else 1
        for idx, (result, orig_row) in enumerate(analysis_rows, start=1):
            item = score_row(result, orig_row)
            if item:
                scored.append(item)
            if idx % scoring_step == 0 or idx == len(analysis_rows):
                emit_progress("scoring_progress", {"done": idx, "total": len(analysis_rows)})
        return scored

    @staticmethod
    def apply_event_matchup_override(
        *,
        next_game_info: dict[str, Any],
        orig_row: dict[str, Any],
        player_team_id: int,
        player_team_abbreviation: str,
        resolve_team_from_text: Callable[[str], dict[str, Any] | None],
    ) -> dict[str, Any]:
        updated_next_game_info = dict(next_game_info or {})
        event_home = resolve_team_from_text(str(orig_row.get("home_team") or "").strip())
        event_away = resolve_team_from_text(str(orig_row.get("away_team") or "").strip())
        event_opponent = None
        is_home = None
        if player_team_id:
            if event_home and int(event_home.get("id") or 0) == player_team_id and event_away:
                event_opponent = event_away
                is_home = True
            elif event_away and int(event_away.get("id") or 0) == player_team_id and event_home:
                event_opponent = event_home
                is_home = False
        if not event_opponent and player_team_abbreviation:
            home_abbr = str(event_home.get("abbreviation") or "").upper() if event_home else ""
            away_abbr = str(event_away.get("abbreviation") or "").upper() if event_away else ""
            if home_abbr and home_abbr == player_team_abbreviation.upper() and event_away:
                event_opponent = event_away
                is_home = True
            elif away_abbr and away_abbr == player_team_abbreviation.upper() and event_home:
                event_opponent = event_home
                is_home = False
        if event_opponent and int(event_opponent.get("id") or 0) != player_team_id:
            opp_abbr = str(event_opponent.get("abbreviation") or "").strip()
            opp_name = str(event_opponent.get("full_name") or "").strip()
            team_abbr = (
                player_team_abbreviation
                or (event_home.get("abbreviation") if event_home else "")
                or (event_away.get("abbreviation") if event_away else "")
            )
            matchup_label = None
            if team_abbr and opp_abbr:
                matchup_label = f"{team_abbr} vs {opp_abbr}" if is_home else f"{team_abbr} @ {opp_abbr}"
            updated_next_game_info.update(
                {
                    "opponent_team_id": int(event_opponent.get("id") or 0),
                    "opponent_abbreviation": opp_abbr,
                    "opponent_name": opp_name,
                    "player_team_abbreviation": team_abbr,
                    "is_home": is_home,
                    "is_override": True,
                    "matchup_label": matchup_label or updated_next_game_info.get("matchup_label"),
                }
            )
        return updated_next_game_info

    @staticmethod
    def fill_missing_opponent_from_event(
        *,
        opponent_info: dict[str, Any],
        orig_row: dict[str, Any],
        player_team_id: int,
        resolve_team_from_text: Callable[[str], dict[str, Any] | None],
    ) -> dict[str, Any]:
        updated = dict(opponent_info or {})
        if updated.get("opponent_team_id"):
            return updated
        home_candidate = resolve_team_from_text(str(orig_row.get("home_team") or ""))
        away_candidate = resolve_team_from_text(str(orig_row.get("away_team") or ""))
        for candidate in [away_candidate, home_candidate]:
            if candidate and int(candidate.get("id") or 0) != int(player_team_id or 0):
                updated["opponent_team_id"] = int(candidate.get("id") or 0)
                updated["opponent_abbreviation"] = str(candidate.get("abbreviation") or "").strip()
                updated["opponent_name"] = str(candidate.get("full_name") or "").strip()
                break
        return updated

    @staticmethod
    def fetch_event_odds_batches(
        *,
        events: list[dict[str, Any]],
        batch_size: int,
        api_keys: list[str],
        sport: str,
        regions: str,
        markets: str,
        odds_format: str,
        requested_bookmakers: list[str],
        fetch_event_odds_payload: Callable[..., dict[str, Any]],
        submit_network_task: Callable[..., Any],
        emit_progress: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        all_import_rows: list[dict[str, Any]] = []
        scrape_errors: list[dict[str, Any]] = []
        quota_log: list[dict[str, Any]] = []
        total_batches = max(1, ((len(events) + batch_size - 1) // batch_size)) if events else 1
        key_index = 0
        batch_index = 0

        def next_key() -> str:
            nonlocal key_index
            key = api_keys[key_index % len(api_keys)]
            key_index += 1
            return key

        for batch_start in range(0, len(events), batch_size):
            batch_index += 1
            batch = events[batch_start : batch_start + batch_size]
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
                        fetch_event_odds_payload(
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
                batch_results: list[tuple[dict[str, Any], dict[str, Any]]] = []
                for submit_start in range(0, len(batch_jobs), batch_workers):
                    submit_chunk = batch_jobs[submit_start : submit_start + batch_workers]
                    futures = [
                        (
                            job,
                            submit_network_task(
                                fetch_event_odds_payload,
                                event_id=job["event_id"],
                                api_key=job["api_key"],
                                sport=sport,
                                regions=regions,
                                markets=markets,
                                odds_format=odds_format,
                                requested_bookmakers=requested_bookmakers,
                            ),
                        )
                        for job in submit_chunk
                    ]
                    batch_results.extend((job, future.result()) for job, future in futures)

            for job, result in batch_results:
                event_id = str(result.get("event_id") or job.get("event_id") or "")
                if result.get("error"):
                    scrape_errors.append(
                        {
                            "event_id": event_id,
                            "home_team": job.get("home_team"),
                            "away_team": job.get("away_team"),
                            "reason": result.get("error"),
                            "status_code": result.get("status_code"),
                        }
                    )
                    continue
                quota_log.append({"call": f"event_{event_id[:8]}", "quota": result.get("quota")})
                all_import_rows.extend(result.get("rows") or [])

            if emit_progress:
                emit_progress(
                    "scrape_progress",
                    {
                        "batch": batch_index,
                        "batches": total_batches,
                        "events_scraped": min(batch_index * batch_size, len(events)),
                        "props_found": len(all_import_rows),
                    },
                )

        return {
            "all_import_rows": all_import_rows,
            "scrape_errors": scrape_errors,
            "quota_log": quota_log,
        }
