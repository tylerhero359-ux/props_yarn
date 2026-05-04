import json
import time
import unittest
from datetime import datetime, timedelta, timezone
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

    def test_backtest_probability_calibration_adjusts_model_probabilities(self) -> None:
        entries = []
        for idx in range(10):
            entries.append(
                {
                    "id": f"over-prob-{idx}",
                    "stat": "PTS",
                    "side": "OVER",
                    "result": "hit" if idx < 8 else "miss",
                    "odds": 1.9,
                    "model_prob": 0.50,
                }
            )
        for idx in range(10):
            entries.append(
                {
                    "id": f"under-prob-{idx}",
                    "stat": "PTS",
                    "side": "UNDER",
                    "result": "hit" if idx < 2 else "miss",
                    "odds": 1.9,
                    "model_prob": 0.50,
                }
            )

        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()
            main._BACKTEST_LOG.extend(entries)
        with main._BACKTEST_RANKING_CACHE_LOCK:
            main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
            main._BACKTEST_RANKING_CACHE["metrics"] = {}

        try:
            over_prob, under_prob, meta = main.apply_backtest_probability_calibration(
                0.55,
                0.45,
                stat="PTS",
            )
        finally:
            with main._BACKTEST_LOCK:
                main._BACKTEST_LOG.clear()
            with main._BACKTEST_RANKING_CACHE_LOCK:
                main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
                main._BACKTEST_RANKING_CACHE["metrics"] = {}

        self.assertTrue(meta["applied"])
        self.assertGreater(over_prob, 0.55)
        self.assertLess(under_prob, 0.45)
        self.assertGreater(meta["over_delta_pct"], 0)
        self.assertLess(meta["under_delta_pct"], 0)

    def test_backtest_probability_calibration_weights_recent_results(self) -> None:
        now = datetime(2026, 5, 4, tzinfo=timezone.utc)
        entries = []
        for idx in range(20):
            entries.append(
                {
                    "id": f"old-over-{idx}",
                    "stat": "PTS",
                    "side": "OVER",
                    "result": "hit" if idx < 18 else "miss",
                    "odds": 1.9,
                    "model_prob": 0.50,
                    "resolved_at": "2026-03-01T00:00:00Z",
                }
            )
        for idx in range(8):
            entries.append(
                {
                    "id": f"recent-over-{idx}",
                    "stat": "PTS",
                    "side": "OVER",
                    "result": "miss",
                    "odds": 1.9,
                    "model_prob": 0.50,
                    "resolved_at": "2026-05-04T00:00:00Z",
                }
            )

        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()
            main._BACKTEST_LOG.extend(entries)
        with main._BACKTEST_RANKING_CACHE_LOCK:
            main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
            main._BACKTEST_RANKING_CACHE["metrics"] = {}

        try:
            with patch.object(main, "_backtest_utc_now", return_value=now):
                delta, meta = main.get_backtest_probability_adjustment("PTS", "OVER")
        finally:
            with main._BACKTEST_LOCK:
                main._BACKTEST_LOG.clear()
            with main._BACKTEST_RANKING_CACHE_LOCK:
                main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
                main._BACKTEST_RANKING_CACHE["metrics"] = {}

        self.assertLess(delta, 0)
        self.assertLess(meta["actual_hit_rate_pct"], 50.0)
        self.assertEqual(meta["segment_count"], 28)
        self.assertLess(meta["segment_effective_count"], 28.0)

    def test_source_specific_backtest_calibration_keeps_scanner_and_parlay_separate(self) -> None:
        entries = []
        for source, over_hits, under_hits in [
            ("market_scanner", 8, 2),
            ("parlay_builder", 2, 8),
        ]:
            for idx in range(10):
                entries.append(
                    {
                        "id": f"{source}-over-{idx}",
                        "source": source,
                        "stat": "PTS",
                        "side": "OVER",
                        "result": "hit" if idx < over_hits else "miss",
                        "odds": 1.9,
                        "model_prob": 0.50,
                    }
                )
            for idx in range(10):
                entries.append(
                    {
                        "id": f"{source}-under-{idx}",
                        "source": source,
                        "stat": "PTS",
                        "side": "UNDER",
                        "result": "hit" if idx < under_hits else "miss",
                        "odds": 1.9,
                        "model_prob": 0.50,
                    }
                )

        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()
            main._BACKTEST_LOG.extend(entries)
        with main._BACKTEST_RANKING_CACHE_LOCK:
            main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
            main._BACKTEST_RANKING_CACHE["metrics"] = {}

        try:
            scanner_over, _scanner_under, scanner_meta = main.apply_backtest_probability_calibration(
                0.55,
                0.45,
                stat="PTS",
                source="market_scanner",
            )
            parlay_over, _parlay_under, parlay_meta = main.apply_backtest_probability_calibration(
                0.55,
                0.45,
                stat="PTS",
                source="parlay_builder",
            )
            base_engine = {"score": 70, "ranking_score": 70, "tags": [], "components": {}}
            scanner_rank = main.apply_backtest_ranking_adjustment(base_engine, stat="PTS", side="OVER", source="market_scan")
            parlay_rank = main.apply_backtest_ranking_adjustment(base_engine, stat="PTS", side="OVER", source="parlay_builder")
        finally:
            with main._BACKTEST_LOCK:
                main._BACKTEST_LOG.clear()
            with main._BACKTEST_RANKING_CACHE_LOCK:
                main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
                main._BACKTEST_RANKING_CACHE["metrics"] = {}

        self.assertGreater(scanner_over, 0.55)
        self.assertLess(parlay_over, 0.55)
        self.assertEqual(scanner_meta["source"], "market_scanner")
        self.assertEqual(parlay_meta["source"], "parlay_builder")
        self.assertGreater(scanner_rank["ranking_score"], 70)
        self.assertLess(parlay_rank["ranking_score"], 70)

    def test_source_specific_calibration_uses_global_fallback_when_source_sample_is_thin(self) -> None:
        entries = []
        for idx in range(10):
            entries.append(
                {
                    "id": f"manual-over-{idx}",
                    "source": "manual",
                    "stat": "PTS",
                    "side": "OVER",
                    "result": "hit" if idx < 8 else "miss",
                    "odds": 1.9,
                    "model_prob": 0.50,
                }
            )
        for idx in range(10):
            entries.append(
                {
                    "id": f"manual-under-{idx}",
                    "source": "manual",
                    "stat": "PTS",
                    "side": "UNDER",
                    "result": "miss" if idx < 8 else "hit",
                    "odds": 1.9,
                    "model_prob": 0.50,
                }
            )
        entries.append(
            {
                "id": "scanner-thin-over",
                "source": "market_scanner",
                "stat": "PTS",
                "side": "OVER",
                "result": "miss",
                "odds": 1.9,
                "model_prob": 0.50,
            }
        )

        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()
            main._BACKTEST_LOG.extend(entries)
        with main._BACKTEST_RANKING_CACHE_LOCK:
            main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
            main._BACKTEST_RANKING_CACHE["metrics"] = {}

        try:
            delta, meta = main.get_backtest_probability_adjustment("PTS", "OVER", source="market_scan_latest")
        finally:
            with main._BACKTEST_LOCK:
                main._BACKTEST_LOG.clear()
            with main._BACKTEST_RANKING_CACHE_LOCK:
                main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
                main._BACKTEST_RANKING_CACHE["metrics"] = {}

        self.assertGreater(delta, 0)
        self.assertEqual(meta["source"], "market_scanner")
        self.assertEqual(meta["calibration_scope"], "global_fallback")
        self.assertEqual(meta["fallback_reason"], "insufficient_source_base")
        self.assertEqual(meta["source_segment_count"], 1)

    def test_calibration_diagnostics_include_probability_and_ranking_context(self) -> None:
        diagnostics = main.build_backtest_calibration_diagnostics(
            stat="PRA",
            side="UNDER",
            probability_calibration={
                "applied": True,
                "under_delta_pct": 1.2,
                "under": {
                    "reason": "applied",
                    "actual_hit_rate_pct": 64.0,
                    "expected_hit_rate_pct": 58.0,
                    "segment_count": 12,
                    "segment_effective_count": 7.5,
                    "delta_pct": 1.4,
                },
            },
            ranking_meta={
                "reason": "applied",
                "hit_rate_delta": 8.0,
                "roi_delta_pct": 12.5,
                "segment_count": 12,
                "segment_effective_count": 7.5,
            },
            ranking_adjustment=2.1,
        )

        self.assertTrue(diagnostics["applied"])
        self.assertEqual(diagnostics["stat"], "PRA")
        self.assertEqual(diagnostics["side"], "UNDER")
        self.assertEqual(diagnostics["probability"]["final_delta_pct"], 1.2)
        self.assertEqual(diagnostics["ranking"]["adjustment"], 2.1)

    def test_parlay_selection_diversifies_stat_exposure_before_relaxing(self) -> None:
        scored = [
            {"player_id": 1, "player_name": "A", "event_id": "g1", "team_id": 10, "stat": "PTS", "side": "OVER", "hit_rate": 80, "games_count": 10, "average": 25, "line": 20, "odds": 1.9},
            {"player_id": 2, "player_name": "B", "event_id": "g2", "team_id": 11, "stat": "PR", "side": "OVER", "hit_rate": 79, "games_count": 10, "average": 31, "line": 25, "odds": 1.9},
            {"player_id": 3, "player_name": "C", "event_id": "g3", "team_id": 12, "stat": "REB", "side": "OVER", "hit_rate": 78, "games_count": 10, "average": 11, "line": 8, "odds": 1.9},
            {"player_id": 4, "player_name": "D", "event_id": "g4", "team_id": 13, "stat": "AST", "side": "OVER", "hit_rate": 77, "games_count": 10, "average": 8, "line": 5, "odds": 1.9},
        ]

        selected = main.annotate_parlay_selection(scored, 3)

        self.assertEqual([leg["player_name"] for leg in selected], ["A", "C", "D"])
        self.assertEqual(scored[1]["diversification"]["blocker"], "stat_family_cluster")
        self.assertIn("diversified ticket fit", selected[0]["selection_reason"])

    def test_parlay_risk_meter_surfaces_concentration_and_sample_risk(self) -> None:
        legs = [
            {
                "player_id": 1,
                "player_name": "A",
                "event_id": "game-1",
                "team_id": 10,
                "stat": "PRA",
                "side": "OVER",
                "odds": 2.45,
                "games_count": 3,
                "model_probability": 57.0,
                "market_disagrees": True,
                "availability": {"is_risky": True},
                "calibration_diagnostics": {"probability": {"final_delta_pct": -1.2}},
            },
            {
                "player_id": 2,
                "player_name": "B",
                "event_id": "game-1",
                "team_id": 10,
                "stat": "PTS",
                "side": "OVER",
                "odds": 2.4,
                "games_count": 5,
                "model_probability": 55.0,
                "market_disagrees": True,
                "calibration_diagnostics": {"probability": {"final_delta_pct": -0.8}},
            },
            {
                "player_id": 3,
                "player_name": "C",
                "event_id": "game-2",
                "team_id": 11,
                "stat": "3PM",
                "side": "UNDER",
                "odds": 1.95,
                "games_count": 6,
                "model_probability": 54.0,
                "calibration_diagnostics": {"probability": {"final_delta_pct": -0.5}},
            },
        ]

        risk = main.build_parlay_risk_meter(legs, legs)
        factors = {factor["label"]: factor for factor in risk["factors"]}

        self.assertGreaterEqual(risk["risk_score"], 42)
        self.assertIn(risk["tone"], {"warning", "bad"})
        self.assertEqual(risk["details"]["same_game_extra_legs"], 1)
        self.assertEqual(risk["details"]["low_sample_count"], 3)
        self.assertIn("Game overlap", factors)
        self.assertIn("Small samples", factors)

    def test_shared_market_snapshot_uses_backtest_probability_calibration(self) -> None:
        entries = []
        for idx in range(10):
            entries.append(
                {
                    "id": f"over-snapshot-{idx}",
                    "stat": "REB",
                    "side": "OVER",
                    "result": "hit" if idx < 8 else "miss",
                    "odds": 1.9,
                    "model_prob": 0.50,
                }
            )
        for idx in range(10):
            entries.append(
                {
                    "id": f"under-snapshot-{idx}",
                    "stat": "REB",
                    "side": "UNDER",
                    "result": "hit" if idx < 2 else "miss",
                    "odds": 1.9,
                    "model_prob": 0.50,
                }
            )

        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()
            main._BACKTEST_LOG.extend(entries)
        with main._BACKTEST_RANKING_CACHE_LOCK:
            main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
            main._BACKTEST_RANKING_CACHE["metrics"] = {}

        try:
            with patch.object(main, "estimate_model_probabilities", return_value=(0.56, 0.44)):
                snapshot = main.build_shared_market_pricing_snapshot(
                    market_row={},
                    over_odds=1.9,
                    under_odds=1.9,
                    hit_rate_pct=60.0,
                    average=12.0,
                    line=10.5,
                    stat="REB",
                    matchup_delta_pct=None,
                    games_count=12,
                )
        finally:
            with main._BACKTEST_LOCK:
                main._BACKTEST_LOG.clear()
            with main._BACKTEST_RANKING_CACHE_LOCK:
                main._BACKTEST_RANKING_CACHE["timestamp"] = 0.0
                main._BACKTEST_RANKING_CACHE["metrics"] = {}

        self.assertTrue(snapshot["backtest"]["applied"])
        self.assertGreater(
            snapshot["calibrated"]["over_probability"],
            snapshot["market_calibrated"]["over_probability"],
        )
        self.assertGreater(
            snapshot["calibrated"]["over_edge_pct"],
            snapshot["market_calibrated"]["over_edge_pct"],
        )

    def test_postgres_cached_run_helper_respects_ttl(self) -> None:
        fresh = main._build_postgres_cached_run(
            {"ok": True},
            datetime.now(timezone.utc) - timedelta(seconds=5),
            60,
            "postgres",
        )
        stale = main._build_postgres_cached_run(
            {"ok": True},
            datetime.now(timezone.utc) - timedelta(seconds=120),
            60,
            "postgres",
        )

        self.assertIsNotNone(fresh)
        self.assertTrue(fresh["cache_hit"])
        self.assertIsNone(stale)

    def test_postgres_game_log_source_of_truth_skips_stale_rows(self) -> None:
        player_id = 999001
        season = "2025-26"
        season_type = main.SEASON_TYPE_REGULAR
        cache_key = (player_id, season, season_type, main.GAME_LOG_CACHE_SCHEMA_VERSION)
        stale_ts = time.time() - main.CACHE_TTL_SECONDS - 60
        stale_rows = [{"GAME_ID": "stale", "GAME_DATE": "2025-10-01"}]
        live_rows = [{"GAME_ID": "fresh", "GAME_DATE": "2025-10-02"}]
        main.GAME_LOG_CACHE.pop(cache_key, None)

        try:
            with patch.object(main, "POSTGRES_SOURCE_OF_TRUTH", True), patch.object(
                main, "_pg_read_game_log", return_value=(stale_rows, stale_ts)
            ), patch.object(
                main.PLAYER_DATA_SERVICE, "fetch_player_game_log", return_value=live_rows
            ) as live_fetch, patch.object(
                main, "_submit_pg_write"
            ):
                result = main.fetch_player_game_log(player_id, season, season_type)
        finally:
            main.GAME_LOG_CACHE.pop(cache_key, None)

        self.assertEqual(result, live_rows)
        live_fetch.assert_called_once_with(player_id, season, season_type)

    def test_player_position_falls_back_to_team_roster_for_matchup_context(self) -> None:
        with patch.object(
            main,
            "fetch_team_roster",
            return_value=[{"PLAYER_ID": 202331, "POSITION": "F"}],
        ):
            position = main.resolve_player_position_from_roster(
                player_id=202331,
                team_id=1610612755,
                season="2025-26",
            )

        position_code, position_label = main.resolve_primary_position(position)
        self.assertEqual(position, "F")
        self.assertEqual(position_code, "F")
        self.assertEqual(position_label, "Forwards")

    def test_bulk_prop_item_returns_backend_resolved_player_position(self) -> None:
        analysis_payload = {
            "player": {
                "id": 202331,
                "full_name": "Paul George",
                "team_id": 1610612755,
                "position": "F",
            },
            "season_type": main.SEASON_TYPE_PLAYOFFS,
            "average": 18.2,
            "hit_rate": 100.0,
            "hit_count": 6,
            "games_count": 6,
            "last_n": 20,
            "games": [],
        }

        with patch.object(main, "build_prop_analysis_payload", return_value=analysis_payload):
            result = main._build_bulk_prop_item(
                1,
                {
                    "player_id": 202331,
                    "player_name": "Paul George",
                    "team_id": 1610612755,
                    "stat": "PTS",
                    "line": 15.5,
                    "player_position": None,
                },
                {"last_n": 20, "season": "2025-26", "season_type": main.SEASON_TYPE_PLAYOFFS},
                {},
            )

        self.assertEqual(result["player_position"], "F")

    def test_parlay_prepare_analysis_jobs_keeps_same_prop_from_distinct_events(self) -> None:
        rows = [
            {
                "player_name": "Shared Player",
                "stat": "PTS",
                "line": 24.5,
                "event_id": "event-a",
                "home_team": "Boston Celtics",
                "away_team": "New York Knicks",
                "game_label": "NYK @ BOS",
            },
            {
                "player_name": "Shared Player",
                "stat": "PTS",
                "line": 24.5,
                "event_id": "event-b",
                "home_team": "Los Angeles Lakers",
                "away_team": "New York Knicks",
                "game_label": "NYK @ LAL",
            },
        ]
        prepared = main.PARLAY_SERVICE.prepare_analysis_jobs(
            all_import_rows=rows,
            resolve_player=lambda _name: {"id": 12345},
        )
        deduped = prepared["deduped_prepared"]

        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0][0]["event_id"], "event-a")
        self.assertEqual(deduped[1][0]["event_id"], "event-b")

    def test_parlay_event_context_sets_analysis_opponent_before_bulk_run(self) -> None:
        bulk_row = {
            "player_id": 1628973,
            "player_name": "Jalen Brunson",
            "stat": "PTS",
            "line": 24.5,
        }
        import_row = {
            "home_team": "Boston Celtics",
            "away_team": "New York Knicks",
        }
        with patch.object(main, "_resolve_team_id_for_player", return_value=1610612752):
            updated = main._apply_parlay_event_context_to_bulk_row(bulk_row, import_row)

        self.assertEqual(updated["team_id"], 1610612752)
        self.assertEqual(updated["override_opponent_id"], 1610612738)
        self.assertEqual(updated["game_label"], "NYK @ BOS")

    def test_market_scan_rejects_too_many_rows(self) -> None:
        service = main.MarketScanCoreService(
            current_nba_season=lambda: "2025-26",
            normalize_requested_season_type=main.normalize_requested_season_type,
            request_hash=main._request_hash,
            read_market_scan_cache=lambda _payload: None,
            warm_injury_cache=lambda: None,
            resolve_team_from_text=lambda _text: None,
            find_player_by_name=lambda _name, _team_id=None: None,
            team_lookup={},
            stat_map=main.STAT_MAP,
            bulk_analysis_max_workers=lambda: 1,
            prefetch_bulk_analysis_context=lambda **_kwargs: None,
            submit_analysis_task=lambda *_args, **_kwargs: None,
            http_exception_cls=main.HTTPException,
            max_rows=2,
            max_last_n=82,
        )

        with self.assertRaises(main.HTTPException) as ctx:
            service.prepare_request({"rows": [{}, {}, {}]})

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("Maximum 2 market rows", str(ctx.exception.detail))

    def test_market_scan_route_batches_oversized_payloads(self) -> None:
        calls: list[int] = []

        def fake_core(payload, progress_cb=None):
            calls.append(len(payload.get("rows") or []))
            call_index = len(calls)
            if progress_cb:
                progress_cb({"stage": "done", "results": 1, "errors": 0})
            return {
                "season": "2025-26",
                "season_type": "Regular Season",
                "last_n": 10,
                "injury_aware": False,
                "template": "player_name,stat,line,over_odds,under_odds",
                "results": [
                    {
                        "row": 1,
                        "best_bet": {"ranking_score": 100 - call_index, "ev": 1.0, "edge": 1.0, "confidence_score": 50},
                        "analysis": {"hit_rate": 50},
                        "availability": {"sort_rank": 1},
                    }
                ],
                "errors": [{"row": 2, "reason": "bad row"}] if call_index == 2 else [],
            }

        rows = [{"player_name": f"Player {idx}", "stat": "PTS", "line": 10.5, "over_odds": 1.91, "under_odds": 1.91} for idx in range(5)]
        with (
            patch.object(main.MARKET_SCAN_CORE_SERVICE, "max_rows", 2),
            patch.object(main, "_market_scan_core", side_effect=fake_core),
            patch.object(main, "_submit_pg_write", return_value=None),
        ):
            payload = main._market_scan_request({"rows": rows, "last_n": 10, "season_type": "Regular Season"})

        self.assertEqual(calls, [2, 2, 1])
        self.assertEqual(payload["rows_submitted"], 5)
        self.assertEqual(payload["batches_scanned"], 3)
        self.assertEqual([item["row"] for item in payload["results"]], [1, 3, 5])
        self.assertEqual(payload["errors"][0]["row"], 4)

    def test_market_scan_rejects_invalid_decimal_odds(self) -> None:
        service = main.MarketScanCoreService(
            current_nba_season=lambda: "2025-26",
            normalize_requested_season_type=main.normalize_requested_season_type,
            request_hash=main._request_hash,
            read_market_scan_cache=lambda _payload: None,
            warm_injury_cache=lambda: None,
            resolve_team_from_text=lambda _text: None,
            find_player_by_name=lambda _name, _team_id=None: {"id": 1, "team_id": 0},
            team_lookup={},
            stat_map=main.STAT_MAP,
            bulk_analysis_max_workers=lambda: 1,
            prefetch_bulk_analysis_context=lambda **_kwargs: None,
            submit_analysis_task=lambda *_args, **_kwargs: None,
            http_exception_cls=main.HTTPException,
            max_rows=10,
            max_last_n=82,
        )
        result = service.prepare_request(
            {
                "last_n": 10,
                "rows": [
                    {
                        "player_name": "Test Player",
                        "stat": "PTS",
                        "line": 20.5,
                        "over_odds": 1.0,
                        "under_odds": 1.91,
                    }
                ],
            }
        )

        self.assertEqual(result["prepared_rows"], [])
        self.assertEqual(len(result["errors"]), 1)
        self.assertIn("over_odds must be between", result["errors"][0]["reason"])

    def test_market_scan_prepare_preserves_game_market_context(self) -> None:
        teams = {
            "PHI": {"id": 1610612755, "full_name": "Philadelphia 76ers", "abbreviation": "PHI"},
            "BOS": {"id": 1610612738, "full_name": "Boston Celtics", "abbreviation": "BOS"},
        }

        def resolve_team(text):
            return teams.get(str(text or "").upper())

        service = main.MarketScanCoreService(
            current_nba_season=lambda: "2025-26",
            normalize_requested_season_type=main.normalize_requested_season_type,
            request_hash=main._request_hash,
            read_market_scan_cache=lambda _payload: None,
            warm_injury_cache=lambda: None,
            resolve_team_from_text=resolve_team,
            find_player_by_name=lambda _name, _team_id=None: {"id": 202331, "team_id": 1610612755},
            team_lookup={team["id"]: team for team in teams.values()},
            stat_map=main.STAT_MAP,
            bulk_analysis_max_workers=lambda: 1,
            prefetch_bulk_analysis_context=lambda **_kwargs: None,
            submit_analysis_task=lambda *_args, **_kwargs: None,
            http_exception_cls=main.HTTPException,
            max_rows=10,
            max_last_n=82,
        )
        result = service.prepare_request(
            {
                "last_n": 10,
                "rows": [
                    {
                        "player_name": "Paul George",
                        "team": "PHI",
                        "opponent": "BOS",
                        "stat": "PTS",
                        "line": 15.5,
                        "over_odds": 1.91,
                        "under_odds": 1.91,
                        "event_id": "evt_123",
                        "game_label": "PHI @ BOS",
                        "home_team": "Boston Celtics",
                        "away_team": "Philadelphia 76ers",
                        "market_game_total": 214.5,
                        "market_home_spread": -4.5,
                        "market_away_spread": 4.5,
                        "market_home_implied_total": 109.5,
                        "market_away_implied_total": 105.0,
                    }
                ],
            }
        )

        bulk_row = result["prepared_rows"][0][1]
        self.assertEqual(bulk_row["event_id"], "evt_123")
        self.assertEqual(bulk_row["home_team"], "Boston Celtics")
        self.assertEqual(bulk_row["market_game_total"], 214.5)
        self.assertEqual(bulk_row["market_away_implied_total"], 105.0)

    def test_odds_game_context_can_fetch_direct_event_id(self) -> None:
        calls = []

        def fake_odds_fetch(endpoint, api_key, params):
            calls.append(endpoint)
            return {
                "data": {
                    "id": "evt_direct",
                    "home_team": "Boston Celtics",
                    "away_team": "Philadelphia 76ers",
                    "bookmakers": [
                        {
                            "markets": [
                                {
                                    "key": "spreads",
                                    "outcomes": [
                                        {"name": "Boston Celtics", "point": -4.5},
                                        {"name": "Philadelphia 76ers", "point": 4.5},
                                    ],
                                },
                                {
                                    "key": "totals",
                                    "outcomes": [
                                        {"name": "Over", "point": 214.5},
                                        {"name": "Under", "point": 214.5},
                                    ],
                                },
                            ]
                        }
                    ],
                },
                "quota": {"remaining": 99, "used": 1, "last": 1},
            }

        with patch.object(main, "odds_api_fetch", side_effect=fake_odds_fetch):
            result = main.odds_game_context(
                {
                    "api_key": "test-key",
                    "event_id": "evt_direct",
                    "team_name": "Philadelphia 76ers",
                    "team_abbreviation": "PHI",
                    "home_team": "Boston Celtics",
                    "away_team": "Philadelphia 76ers",
                }
            )

        self.assertEqual(calls, ["/sports/basketball_nba/events/evt_direct/odds"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["context"]["market_game_total"], 214.5)
        self.assertEqual(result["environment"]["market_team_total"], 105.0)
        self.assertEqual(result["environment"]["market_spread"], 4.5)

    def test_parlay_rejects_out_of_range_last_n_before_network_work(self) -> None:
        with self.assertRaises(main.HTTPException) as ctx:
            main._parlay_builder_core(
                {
                    "api_keys": ["test-key"],
                    "event_ids": ["ev1"],
                    "last_n": main.PROP_ANALYSIS_MAX_LAST_N + 1,
                }
            )

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("last_n must be between", str(ctx.exception.detail))

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
