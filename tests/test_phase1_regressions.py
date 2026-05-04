import unittest
from pathlib import Path
from unittest.mock import patch

import main


class Phase1RegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()

    def test_backtest_over_equal_line_is_hit(self) -> None:
        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.append(
                {
                    "id": "eq-over",
                    "player": "Test Player",
                    "stat": "PTS",
                    "line": 25.5,
                    "side": "OVER",
                    "confidence_score": 70,
                    "confidence_tier": "B",
                    "model_prob": 0.55,
                    "odds": 1.91,
                    "result": "pending",
                    "actual_value": None,
                    "logged_at": "2026-04-21T00:00:00Z",
                    "resolved_at": None,
                    "event_date": "2026-04-21",
                    "source": "test",
                    "market_side": "OVER",
                    "market_disagrees": False,
                    "notes": "",
                }
            )

        with patch.object(main, "_save_backtest_log", return_value=None), patch.object(
            main, "_require_pg_backtest_write", return_value=None
        ):
            result = main.backtest_resolve_prediction({"id": "eq-over", "actual_value": 25.5})

        self.assertTrue(result["ok"])
        self.assertEqual(result["entry"]["result"], "hit")
        self.assertEqual(result["entry"]["actual_value"], 25.5)

    def test_backtest_stats_follow_log_filters(self) -> None:
        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.extend(
                [
                    {
                        "id": "bt-hit-1",
                        "player": "Filter Hit One",
                        "stat": "PTS",
                        "line": 20.5,
                        "side": "OVER",
                        "confidence_score": 82,
                        "confidence_tier": "High",
                        "model_prob": 0.58,
                        "odds": 1.91,
                        "result": "hit",
                        "actual_value": 24,
                        "logged_at": "2026-04-20T00:00:00Z",
                        "resolved_at": "2026-04-21T00:00:00Z",
                    },
                    {
                        "id": "bt-hit-2",
                        "player": "Filter Hit Two",
                        "stat": "REB",
                        "line": 8.5,
                        "side": "UNDER",
                        "confidence_score": 76,
                        "confidence_tier": "High",
                        "model_prob": 0.56,
                        "odds": 1.88,
                        "result": "hit",
                        "actual_value": 6,
                        "logged_at": "2026-04-20T00:00:00Z",
                        "resolved_at": "2026-04-21T00:00:00Z",
                    },
                    {
                        "id": "bt-miss-1",
                        "player": "Filter Miss",
                        "stat": "AST",
                        "line": 5.5,
                        "side": "OVER",
                        "confidence_score": 66,
                        "confidence_tier": "Medium",
                        "model_prob": 0.52,
                        "odds": 1.91,
                        "result": "miss",
                        "actual_value": 4,
                        "logged_at": "2026-04-20T00:00:00Z",
                        "resolved_at": "2026-04-21T00:00:00Z",
                    },
                ]
            )

        payload = main.backtest_get_log(
            limit=25,
            offset=0,
            search="",
            result_filter="hit",
            stat_filter="all",
            tier_filter="all",
            side_filter="all",
            date_range="all",
            view_mode="resolved",
            group_by="none",
        )

        self.assertEqual(payload["stats"]["total"], 2)
        self.assertEqual(payload["stats"]["hits"], 2)
        self.assertEqual(payload["stats"]["misses"], 0)
        self.assertEqual(payload["stats"]["win_rate"], 100.0)
        self.assertTrue(all(entry["result"] == "hit" for entry in payload["entries"]))

    def test_backtest_batch_logs_side_specific_odds_when_odds_missing(self) -> None:
        payload = {
            "filter_mode": "action",
            "source": "market_scanner",
            "props": [
                {
                    "player_name": "Odds Hydrated",
                    "player_id": 123,
                    "stat": "AST",
                    "line": 4.5,
                    "side": "UNDER",
                    "confidence_score": 82,
                    "confidence_tier": "High",
                    "games_count": 6,
                    "over_odds": 1.95,
                    "under_odds": 1.72,
                }
            ],
        }

        with patch.object(main, "_save_backtest_log", return_value=None), patch.object(
            main, "_require_pg_backtest_write", return_value=None
        ):
            result = main.backtest_log_parlay_batch(payload)

        self.assertTrue(result["ok"])
        self.assertEqual(result["logged"], 1)
        with main._BACKTEST_LOCK:
            self.assertEqual(main._BACKTEST_LOG[0]["odds"], 1.72)

    def test_backtest_batch_backfills_missing_duplicate_odds(self) -> None:
        prop = {
            "player_name": "Duplicate Odds",
            "player_id": 456,
            "stat": "REB",
            "line": 7.5,
            "side": "OVER",
            "confidence_score": 84,
            "confidence_tier": "High",
            "games_count": 8,
            "over_odds": 1.76,
            "under_odds": 2.05,
        }
        prop_key = main._backtest_parlay_unique_key(prop, "")
        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.append(
                {
                    "id": "dup-odds",
                    "player": "Duplicate Odds",
                    "player_id": 456,
                    "stat": "REB",
                    "line": 7.5,
                    "side": "OVER",
                    "confidence_score": 84,
                    "confidence_tier": "High",
                    "odds": None,
                    "result": "pending",
                    "parlay_prop_key": prop_key,
                    "source": "market_scanner",
                }
            )

        with patch.object(main, "_save_backtest_log", return_value=None), patch.object(
            main, "_require_pg_backtest_write", return_value=None
        ):
            result = main.backtest_log_parlay_batch({"filter_mode": "action", "props": [prop]})

        self.assertEqual(result["logged"], 0)
        self.assertEqual(result["skipped_duplicate"], 1)
        self.assertEqual(result["updated_odds"], 1)
        with main._BACKTEST_LOCK:
            self.assertEqual(main._BACKTEST_LOG[0]["odds"], 1.76)

    def test_analysis_cache_ttl_seconds_is_single_capped_value(self) -> None:
        self.assertGreaterEqual(main.ANALYSIS_CACHE_TTL_SECONDS, 1)
        self.assertLessEqual(main.ANALYSIS_CACHE_TTL_SECONDS, 300)

    def test_injury_service_uses_configured_ttl(self) -> None:
        self.assertEqual(main.INJURY_SERVICE.report_ttl_seconds, main.INJURY_REPORT_TTL_SECONDS)

    def test_warm_cache_constants_are_not_redeclared(self) -> None:
        source = Path(main.__file__).read_text(encoding="utf-8")
        self.assertEqual(source.count('WARM_CACHE_ON_STARTUP_ENABLED = os.getenv("NBA_WARM_CACHE_ON_STARTUP_ENABLED"'), 1)
        self.assertEqual(source.count('WARM_CACHE_PRELOAD_TEAM_RANKS = os.getenv("NBA_WARM_CACHE_PRELOAD_TEAM_RANKS"'), 1)


if __name__ == "__main__":
    unittest.main()
