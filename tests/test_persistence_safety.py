import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import main


class PersistenceSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self._stack = ExitStack()
        temp_root = Path(self._tempdir.name)

        self._stack.enter_context(patch.object(main, "PERSISTENT_CACHE_ENABLED", True))
        self._stack.enter_context(patch.object(main, "PERSISTENT_CACHE_DIR", temp_root))
        self._stack.enter_context(patch.object(main, "BACKTEST_PERSIST_PATH", temp_root / "backtest_log.json"))
        self._stack.enter_context(patch.object(main, "KEY_VAULT_PERSIST_PATH", temp_root / "key_vault.json"))
        self._stack.enter_context(patch.object(main, "FAVORITES_PERSIST_PATH", temp_root / "favorites.json"))
        self._stack.enter_context(patch.object(main, "TRACKER_PERSIST_PATH", temp_root / "tracker_props.json"))
        self._stack.enter_context(patch.object(main, "postgres_available", return_value=False))
        self._stack.enter_context(patch.object(main, "POSTGRES_CACHE_WRITE_ENABLED", False))

        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()
        with main._KEY_VAULT_LOCK:
            main._KEY_VAULT_STATE["entries"] = []
            main._KEY_VAULT_STATE["active_id"] = ""
        with main._FAVORITES_LOCK:
            main._FAVORITES_STATE.clear()
        with main._TRACKER_LOCK:
            main._TRACKER_STATE.clear()

    def tearDown(self) -> None:
        self._stack.close()
        self._tempdir.cleanup()

    def test_tracker_routes_roundtrip_and_persist(self) -> None:
        payload = {
            "entries": [
                {
                    "id": "trk-1",
                    "player_name": "LeBron James",
                    "player_id": 2544,
                    "stat": "PTS",
                    "line": 27.5,
                    "side": "OVER",
                    "odds": 1.91,
                }
            ]
        }

        result = main.tracker_props_put(payload)
        self.assertTrue(result["ok"])
        self.assertEqual(result["count"], 1)
        self.assertTrue(main.TRACKER_PERSIST_PATH.exists())

        with main._TRACKER_LOCK:
            main._TRACKER_STATE.clear()
        main._load_tracker_state()

        loaded = main.tracker_props_get()
        self.assertEqual(len(loaded["entries"]), 1)
        self.assertEqual(loaded["entries"][0]["player_name"], "LeBron James")

    def test_favorites_put_dedupes_and_persists(self) -> None:
        payload = {
            "entries": [
                {"type": "player", "key": "player:2544", "label": "LeBron James"},
                {"type": "player", "key": "player:2544", "label": "LeBron James duplicate"},
                {"type": "prop", "key": "prop:2544:PTS:27.5:OVER", "label": "LeBron PTS OVER"},
            ]
        }

        result = main.favorites_put(payload)
        self.assertEqual(result["count"], 2)
        self.assertTrue(main.FAVORITES_PERSIST_PATH.exists())

        with main._FAVORITES_LOCK:
            main._FAVORITES_STATE.clear()
        main._load_favorites_state()

        loaded = main.favorites_get()
        self.assertEqual(len(loaded["entries"]), 2)
        self.assertEqual([entry["key"] for entry in loaded["entries"]], ["player:2544", "prop:2544:PTS:27.5:OVER"])

    def test_key_vault_roundtrip_persists_active_key(self) -> None:
        payload = {
            "entries": [
                {"id": "k1", "provider": "odds_api", "api_key": "abc123", "label": "Odds API Key 1"},
                {"id": "k2", "provider": "odds_api", "api_key": "def456", "label": "Odds API Key 2"},
            ],
            "active_id": "k2",
            "mode": "replace",
        }

        result = main.key_vault_put(payload)
        self.assertTrue(result["ok"])
        self.assertEqual(result["active_id"], "k2")
        self.assertTrue(main.KEY_VAULT_PERSIST_PATH.exists())

        with main._KEY_VAULT_LOCK:
            main._KEY_VAULT_STATE["entries"] = []
            main._KEY_VAULT_STATE["active_id"] = ""
        main._load_key_vault_state()

        loaded = main.key_vault_get()
        self.assertEqual(len(loaded["entries"]), 2)
        self.assertEqual(loaded["active_id"], "k2")

    def test_backtest_save_and_load_roundtrip(self) -> None:
        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.append(
                {
                    "id": "bt-1",
                    "player": "Shai Gilgeous-Alexander",
                    "player_id": 1628983,
                    "stat": "AST",
                    "line": 8.5,
                    "side": "UNDER",
                    "confidence_score": 87,
                    "confidence_tier": "A",
                    "confidence_contract": {"score": 87, "tier": "Elite", "model_probability": 61.0},
                    "base_confidence_score": 84,
                    "market_adjusted_confidence_score": 87,
                    "ranking_score": 91,
                    "confidence_score_source": "market_adjusted",
                    "model_prob": 0.61,
                    "odds": 2.05,
                    "result": "pending",
                    "actual_value": None,
                    "logged_at": "2026-04-10T12:00:00",
                    "resolved_at": None,
                    "event_date": "2026-04-10",
                    "season": "2025-26",
                    "season_type": main.SEASON_TYPE_PLAYOFFS,
                    "event_id": "evt-123",
                    "game_label": "OKC vs DAL",
                    "opponent_abbreviation": "DAL",
                    "batch_id": "batch-1",
                    "parlay_prop_key": "1628983|AST|8.500|UNDER|evt-123|OKC vs DAL|2026-04-10|DAL",
                    "source": "scanner",
                    "market_side": "OVER",
                    "market_disagrees": True,
                    "notes": "market disagreement case",
                    "resolved_game_date": "2026-04-10",
                    "resolved_matchup": "OKC vs DAL",
                    "auto_grade_run_id": "run-1",
                    "auto_graded_at": "2026-04-10T23:00:00Z",
                }
            )
        main._save_backtest_log()
        self.assertTrue(main.BACKTEST_PERSIST_PATH.exists())

        with main._BACKTEST_LOCK:
            main._BACKTEST_LOG.clear()
        main._load_backtest_log()

        with main._BACKTEST_LOCK:
            self.assertEqual(len(main._BACKTEST_LOG), 1)
            loaded = main._BACKTEST_LOG[0]
            self.assertEqual(loaded["player"], "Shai Gilgeous-Alexander")
            self.assertEqual(loaded["player_id"], 1628983)
            self.assertEqual(loaded["season_type"], main.SEASON_TYPE_PLAYOFFS)
            self.assertEqual(loaded["game_label"], "OKC vs DAL")
            self.assertEqual(loaded["opponent_abbreviation"], "DAL")
            self.assertEqual(loaded["batch_id"], "batch-1")
            self.assertEqual(loaded["parlay_prop_key"], "1628983|AST|8.500|UNDER|evt-123|OKC vs DAL|2026-04-10|DAL")
            self.assertEqual(loaded["ranking_score"], 91)
            self.assertEqual(loaded["auto_grade_run_id"], "run-1")
            self.assertTrue(loaded["market_disagrees"])


if __name__ == "__main__":
    unittest.main()
