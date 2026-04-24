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
