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
