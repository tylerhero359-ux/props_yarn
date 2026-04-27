import unittest
from contextlib import ExitStack
from unittest.mock import patch

import main


class _FakeFrame:
    def __init__(self, rows):
        self._rows = rows
        self.empty = len(rows) == 0

    def to_dict(self, orient="records"):
        if orient != "records":
            raise ValueError("Only records orient is supported in test fake.")
        return list(self._rows)


class _FakeDash:
    def __init__(self, rows):
        self._rows = rows

    def get_data_frames(self):
        return [_FakeFrame(self._rows)]


class Phase3RegressionTests(unittest.TestCase):
    def test_build_team_rank_map_prefers_lower_def_rating(self) -> None:
        fake_rows = [
            {"TEAM_ID": 1610612747, "DEF_RATING": 109.1, "OPP_PTS": 111.0},  # Lakers
            {"TEAM_ID": 1610612744, "DEF_RATING": 112.4, "OPP_PTS": 114.2},  # Warriors
            {"TEAM_ID": 1610612738, "DEF_RATING": 107.8, "OPP_PTS": 109.5},  # Celtics
        ]
        with patch.object(main, "call_nba_with_retries", return_value=_FakeDash(fake_rows)):
            main.TEAM_RECORDS_CACHE.clear()
            rank_map = main.build_team_rank_map("2025-26", season_type=main.DEFAULT_SEASON_TYPE)
        self.assertEqual(rank_map.get(1610612738), 1)  # best defensive rating
        self.assertEqual(rank_map.get(1610612747), 2)
        self.assertEqual(rank_map.get(1610612744), 3)

    def test_build_team_rank_map_falls_back_to_opp_pts_when_def_rating_missing(self) -> None:
        fake_rows = [
            {"TEAM_ID": 1610612738, "DEF_RATING": "", "OPP_PTS": 108.2},
            {"TEAM_ID": 1610612744, "DEF_RATING": None, "OPP_PTS": 112.3},
        ]
        with patch.object(main, "call_nba_with_retries", return_value=_FakeDash(fake_rows)):
            main.TEAM_RECORDS_CACHE.clear()
            rank_map = main.build_team_rank_map("2025-26", season_type=main.DEFAULT_SEASON_TYPE)
        self.assertEqual(rank_map.get(1610612738), 1)
        self.assertEqual(rank_map.get(1610612744), 2)

    def test_decimal_ev_uses_decimal_odds_directly(self) -> None:
        metrics = main.compute_side_pricing_metrics(
            probability=0.55,
            fair_probability=0.5,
            odds=1.91,
        )
        self.assertAlmostEqual(metrics["ev"], 0.0505, places=4)

    def test_side_hit_rate_flips_for_under(self) -> None:
        self.assertEqual(main.side_hit_rate_from_over_hit_rate(62.0, "OVER"), 62.0)
        self.assertEqual(main.side_hit_rate_from_over_hit_rate(62.0, "UNDER"), 38.0)

    def test_build_opportunity_context_respects_last_n_window(self) -> None:
        rows = [
            {"MIN": "40:00", "FGA": 22, "FG3A": 8, "FTA": 6},
            {"MIN": "39:30", "FGA": 21, "FG3A": 7, "FTA": 5},
            {"MIN": "41:00", "FGA": 23, "FG3A": 9, "FTA": 7},
            {"MIN": "10:00", "FGA": 4, "FG3A": 1, "FTA": 1},
            {"MIN": "09:30", "FGA": 3, "FG3A": 1, "FTA": 1},
        ]
        context = main.build_opportunity_context(rows, last_n=3)
        self.assertAlmostEqual(context["minutes_sample"], context["minutes_last5"], places=1)
        self.assertAlmostEqual(context["fga_sample"], context["fga_last5"], places=1)

    def test_h2h_only_filter_uses_true_opponent_side(self) -> None:
        rows = [
            {"MATCHUP": "NYK vs. BOS", "_location": "home", "_result": "win", "_minutes": 35, "_fga": 15, "_margin": 6},
            {"MATCHUP": "NYK @ BOS", "_location": "away", "_result": "loss", "_minutes": 36, "_fga": 16, "_margin": -4},
        ]
        filtered = main.apply_game_log_filters(
            rows,
            h2h_only=True,
            opponent_abbreviation="NYK",
        )
        self.assertEqual(filtered, [])

    def test_parlay_playoff_relaxed_fallback_can_return_non_empty_output(self) -> None:
        payload = {
            "api_keys": ["odds-key-1"],
            "event_ids": ["evt_1"],
            "legs": 2,
            "season": "2025-26",
            "season_type": main.SEASON_TYPE_PLAYOFFS,
            "last_n": 10,
            "bypass_cache": True,
        }
        import_row = {
            "player_name": "Jalen Brunson",
            "stat": "PTS",
            "line": 24.5,
            "over_odds": 1.91,
            "under_odds": 1.91,
            "event_id": "evt_1",
            "home_team": "Boston Celtics",
            "away_team": "New York Knicks",
            "bookmaker_title": "DraftKings",
            "market_key": "player_points",
        }
        analysis_rows = [
            (
                {
                    "player_name": "Jalen Brunson",
                    "player_id": 1628973,
                    "stat": "PTS",
                    "line": 24.5,
                    "analysis": {},
                },
                import_row,
            )
        ]
        relaxed_scored_row = {
            "player_name": "Jalen Brunson",
            "player_id": 1628973,
            "event_id": "evt_1",
            "stat": "PTS",
            "line": 24.5,
            "side": "OVER",
            "odds": 1.91,
            "hit_rate": 62.5,
            "ranking_hit_rate": 62.5,
            "h2h_games_count": 0,
            "games_count": 8,
            "average": 27.2,
            "confidence_score": 74,
            "ranking_score": 74,
        }
        scoring_pass = {"count": 0}

        def _fake_run_scoring_rows(*, analysis_rows, score_row, emit_progress):
            scoring_pass["count"] += 1
            if scoring_pass["count"] == 1:
                return []
            return [dict(relaxed_scored_row)]

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "_pg_read_parlay_builder_cache", return_value=None))
            stack.enter_context(patch.object(main, "_submit_pg_write"))
            stack.enter_context(patch.object(main, "prefetch_bulk_analysis_context"))
            stack.enter_context(patch.object(main, "_resolve_team_id_for_player", return_value=0))
            stack.enter_context(patch.object(main, "LeagueDashPlayerStats", return_value=_FakeDash([])))
            stack.enter_context(
                patch.object(
                    main.PARLAY_SERVICE,
                    "fetch_event_odds_batches",
                    return_value={"all_import_rows": [import_row], "scrape_errors": [], "quota_log": []},
                )
            )
            stack.enter_context(
                patch.object(
                    main.PARLAY_SERVICE,
                    "prepare_analysis_jobs",
                    return_value={
                        "analysis_errors": [],
                        "deduped_prepared": [({"player_id": 1628973, "player_name": "Jalen Brunson", "stat": "PTS", "line": 24.5}, import_row)],
                        "unique_player_ids": set(),
                    },
                )
            )
            stack.enter_context(patch.object(main.PARLAY_SERVICE, "run_bulk_analysis", return_value=(analysis_rows, [])))
            stack.enter_context(patch.object(main.PARLAY_SERVICE, "run_scoring_rows", side_effect=_fake_run_scoring_rows))
            result = main._parlay_builder_core(payload)

        self.assertEqual(scoring_pass["count"], 2)
        self.assertTrue(result["playoff_relaxed_fallback_applied"])
        self.assertEqual(result["props_analyzed"], 1)
        self.assertEqual(len(result["all_props_scored"]), 1)
        self.assertEqual(len(result["parlay"]), 1)
        self.assertEqual(result["parlay"][0]["player_name"], "Jalen Brunson")

    def test_parlay_scored_rows_keep_recent_and_h2h_fields_consistent_for_ui(self) -> None:
        payload = {
            "api_keys": ["odds-key-1"],
            "event_ids": ["evt_2"],
            "legs": 2,
            "season": "2025-26",
            "season_type": main.DEFAULT_SEASON_TYPE,
            "last_n": 10,
            "bypass_cache": True,
        }
        import_row = {
            "player_name": "Jalen Brunson",
            "stat": "PTS",
            "line": 24.5,
            "over_odds": 1.91,
            "under_odds": 1.91,
            "event_id": "evt_2",
            "home_team": "Boston Celtics",
            "away_team": "New York Knicks",
            "bookmaker_title": "DraftKings",
            "market_key": "player_points",
            "game_label": "NYK @ BOS",
        }
        analysis_payload = {
            "hit_rate": 60.0,
            "average": 26.4,
            "recommended_side": "OVER",
            "availability": {"is_unavailable": False, "label": "Active"},
            "matchup": {
                "next_game": {
                    "opponent_team_id": 1610612738,
                    "opponent_abbreviation": "BOS",
                    "opponent_name": "Boston Celtics",
                },
                "vs_position": {"delta_pct": 1.5},
            },
            "player": {
                "team_id": 1610612752,
                "team_abbreviation": "NYK",
                "team_name": "New York Knicks",
                "position": "G",
                "jersey": "11",
            },
            "opportunity": {},
            "team_context": {},
            "environment": {},
            "variance": {},
            "games_count": 10,
            "last_n": 10,
            "h2h": {"games_count": 3, "hit_count": 2, "hit_rate": 66.7},
            "games": [
                {"matchup": "NYK vs. BOS", "hit": True},
                {"matchup": "NYK @ BOS", "hit": False},
            ],
            "season_type": main.DEFAULT_SEASON_TYPE,
            "filtered_pool_count": 10,
            "season_pool_count": 20,
            "debug": {"freshness": {"game_log_source": "live"}},
        }
        analysis_rows = [
            (
                {
                    "player_name": "Jalen Brunson",
                    "player_id": 1628973,
                    "stat": "PTS",
                    "line": 24.5,
                    "analysis": analysis_payload,
                },
                import_row,
            )
        ]
        pricing_snapshot = {
            "market": {"fair_over": 0.52, "fair_under": 0.48},
            "raw": {
                "over_probability": 0.60,
                "under_probability": 0.40,
                "over_edge_pct": 8.0,
                "under_edge_pct": -8.0,
                "over_ev": 0.14,
                "under_ev": -0.14,
            },
            "calibrated": {
                "over_probability": 0.58,
                "under_probability": 0.42,
                "over_edge_pct": 6.0,
                "under_edge_pct": -6.0,
                "over_ev": 0.10,
                "under_ev": -0.10,
            },
            "adjusted": {"over_ev": 0.09, "under_ev": -0.09},
            "reliability": {"reliability": 0.73, "shrink_strength": 0.27},
        }
        confidence_engine = {
            "grade": "A",
            "score": 84,
            "tone": "good",
            "tier": "High",
            "summary": "Strong profile",
            "tags": ["Stable minutes"],
            "market_side": "OVER",
            "market_disagrees": False,
            "market_penalty": 0.0,
            "ranking_score": 88,
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "_pg_read_parlay_builder_cache", return_value=None))
            stack.enter_context(patch.object(main, "_submit_pg_write"))
            stack.enter_context(patch.object(main, "prefetch_bulk_analysis_context"))
            stack.enter_context(patch.object(main, "_resolve_team_id_for_player", return_value=1610612752))
            stack.enter_context(patch.object(main, "LeagueDashPlayerStats", return_value=_FakeDash([])))
            stack.enter_context(
                patch.object(
                    main.PARLAY_SERVICE,
                    "fetch_event_odds_batches",
                    return_value={"all_import_rows": [import_row], "scrape_errors": [], "quota_log": []},
                )
            )
            stack.enter_context(
                patch.object(
                    main.PARLAY_SERVICE,
                    "prepare_analysis_jobs",
                    return_value={
                        "analysis_errors": [],
                        "deduped_prepared": [({"player_id": 1628973, "player_name": "Jalen Brunson", "stat": "PTS", "line": 24.5}, import_row)],
                        "unique_player_ids": set(),
                    },
                )
            )
            stack.enter_context(patch.object(main.PARLAY_SERVICE, "run_bulk_analysis", return_value=(analysis_rows, [])))
            stack.enter_context(patch.object(main, "build_shared_market_pricing_snapshot", return_value=pricing_snapshot))
            stack.enter_context(patch.object(main, "enrich_environment_with_market_context", side_effect=lambda environment, *_args, **_kwargs: environment))
            stack.enter_context(patch.object(main, "build_confidence_engine", return_value=dict(confidence_engine)))
            stack.enter_context(patch.object(main, "apply_market_confidence_adjustment", side_effect=lambda engine, **_kwargs: engine))
            result = main._parlay_builder_core(payload)

        self.assertEqual(result["props_analyzed"], 1)
        self.assertEqual(len(result["all_props_scored"]), 1)
        row = result["all_props_scored"][0]
        self.assertEqual(row["games_count"], 10)
        self.assertEqual(row["hit_count"], 6)
        self.assertEqual(row["h2h_games_count"], 3)
        self.assertEqual(row["h2h_hit_count"], 2)
        self.assertAlmostEqual(row["h2h_hit_rate"], 66.7, places=1)
        self.assertEqual(row["ranking_source"], "h2h")
        self.assertAlmostEqual(row["ranking_hit_rate"], 66.7, places=1)
        self.assertEqual(row["h2h_debug"]["from_games_count"], 2)
        self.assertEqual(row["h2h_debug"]["from_payload_count"], 3)
        self.assertEqual(row["h2h_debug"]["resolved_count"], 3)


if __name__ == "__main__":
    unittest.main()
