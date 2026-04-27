import json
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import main


class Phase4RegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)

    def test_backtest_ranking_adjustment_biases_ranking_score_by_stat_side_results(self) -> None:
        entries = []
        for idx in range(10):
            entries.append(
                {
                    "id": f"over-{idx}",
                    "stat": "PTS",
                    "side": "OVER",
                    "result": "hit" if idx < 8 else "miss",
                    "odds": 1.9,
                }
            )
        for idx in range(10):
            entries.append(
                {
                    "id": f"under-{idx}",
                    "stat": "PTS",
                    "side": "UNDER",
                    "result": "hit" if idx < 2 else "miss",
                    "odds": 1.9,
                }
            )

        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()
            main._BACKTEST_LOG.extend(entries)
        with main._BACKTEST_RANKING_CACHE_LOCK:
            main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
            main._BACKTEST_RANKING_CACHE["metrics"] = {}

        base_engine = {
            "score": 70,
            "ranking_score": 70,
            "tags": [],
            "components": {},
        }
        adjusted_over = main.apply_backtest_ranking_adjustment(base_engine, stat="PTS", side="OVER")
        adjusted_under = main.apply_backtest_ranking_adjustment(base_engine, stat="PTS", side="UNDER")

        self.assertGreater(adjusted_over["ranking_score"], 70)
        self.assertLess(adjusted_under["ranking_score"], 70)
        self.assertTrue(any("Backtest adj" in str(tag) for tag in (adjusted_over.get("tags") or [])))

    def test_parlay_sync_async_stream_have_matching_counts_and_payload_shape(self) -> None:
        payload = {"api_keys": ["test-key"], "event_ids": ["ev1", "ev2"], "legs": 2}
        fake_scored = [
            {
                "player_name": "Jalen Brunson",
                "player_id": 1628973,
                "event_id": "ev1",
                "stat": "PTS",
                "line": 24.5,
                "side": "OVER",
                "odds": 1.91,
                "hit_rate": 62.0,
                "hit_count": 6,
                "games_count": 10,
                "h2h_games_count": 3,
                "h2h_hit_count": 2,
                "h2h_hit_rate": 66.7,
                "ranking_hit_rate": 66.7,
                "ranking_source": "h2h",
                "confidence": "A",
                "confidence_score": 83,
                "ranking_score": 84,
                "selection_status": "selected",
                "selection_reason": "Selected for strong profile.",
                "selection_reason_parts": ["strong profile"],
                "average": 27.1,
                "ev": 0.087,
                "edge": 6.2,
            },
            {
                "player_name": "Tyrese Haliburton",
                "player_id": 1630169,
                "event_id": "ev2",
                "stat": "AST",
                "line": 9.5,
                "side": "OVER",
                "odds": 1.84,
                "hit_rate": 59.0,
                "hit_count": 6,
                "games_count": 10,
                "h2h_games_count": 2,
                "h2h_hit_count": 1,
                "h2h_hit_rate": 50.0,
                "ranking_hit_rate": 59.0,
                "ranking_source": "recent",
                "confidence": "B",
                "confidence_score": 78,
                "ranking_score": 78,
                "selection_status": "selected",
                "selection_reason": "Selected for depth.",
                "selection_reason_parts": ["depth"],
                "average": 10.2,
                "ev": 0.062,
                "edge": 4.1,
            },
        ]

        def _fake_parlay_core(_payload, progress_cb=None):
            if progress_cb:
                progress_cb({"stage": "events_resolved", "events": 2})
                progress_cb({"stage": "analysis_done", "analyzed": 2, "errors": 0})
                progress_cb({"stage": "done", "events_scraped": 2, "props_found": 2, "props_analyzed": 2, "errors": 0})
            return {
                "legs": 2,
                "parlay": list(fake_scored),
                "parlay_odds": 3.51,
                "all_props_scored": list(fake_scored),
                "events_scraped": 2,
                "props_found": 2,
                "props_analyzed": 2,
                "errors": [],
                "quota_log": [],
                "bookmakers": ["draftkings"],
                "cost_hint": {},
                "playoff_relaxed_fallback_applied": False,
            }

        with patch.object(main, "_parlay_builder_core", side_effect=_fake_parlay_core), patch.object(
            main, "enforce_heavy_rate_limit", return_value=None
        ):
            sync_resp = self.client.post("/api/parlay-builder", json=payload)
            self.assertEqual(sync_resp.status_code, 200)
            sync_payload = sync_resp.json()

            async_submit = self.client.post("/api/parlay-builder/async", json=payload)
            self.assertEqual(async_submit.status_code, 200)
            async_meta = async_submit.json()
            self.assertTrue(async_meta.get("ok"))
            job_id = str(async_meta.get("job_id") or "")
            self.assertTrue(job_id)

            async_payload = None
            deadline = time.time() + 5.0
            while time.time() < deadline:
                job_resp = self.client.get(f"/api/jobs/{job_id}")
                self.assertEqual(job_resp.status_code, 200)
                job = job_resp.json()
                if str(job.get("status")) == "done":
                    async_payload = job.get("result")
                    break
                time.sleep(0.05)
            self.assertIsNotNone(async_payload)

            stream_resp = self.client.post("/api/parlay-builder/stream", json=payload)
            self.assertEqual(stream_resp.status_code, 200)
            ndjson_lines = [line for line in stream_resp.text.splitlines() if line.strip()]
            parsed = [json.loads(line) for line in ndjson_lines]
            result_events = [item for item in parsed if item.get("type") == "result"]
            self.assertTrue(result_events)
            stream_payload = result_events[-1].get("payload") or {}

        for candidate in (sync_payload, async_payload, stream_payload):
            self.assertEqual(candidate.get("props_found"), 2)
            self.assertEqual(candidate.get("props_analyzed"), 2)
            self.assertEqual(len(candidate.get("all_props_scored") or []), 2)
            self.assertEqual(len(candidate.get("parlay") or []), 2)

        self.assertEqual(sync_payload.get("props_found"), async_payload.get("props_found"))
        self.assertEqual(sync_payload.get("props_found"), stream_payload.get("props_found"))
        self.assertEqual(sync_payload.get("props_analyzed"), async_payload.get("props_analyzed"))
        self.assertEqual(sync_payload.get("props_analyzed"), stream_payload.get("props_analyzed"))


if __name__ == "__main__":
    unittest.main()

