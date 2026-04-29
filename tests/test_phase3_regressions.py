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

    def test_parlay_playoff_zero_h2h_uses_blend_without_relaxed_fallback(self) -> None:
        payload = {
            "api_keys": ["odds-key-1"],
            "event_ids": ["evt_2"],
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
            "event_id": "evt_2",
            "home_team": "Boston Celtics",
            "away_team": "New York Knicks",
            "bookmaker_title": "DraftKings",
            "market_key": "player_points",
            "game_label": "NYK @ BOS",
        }
        analysis_payload = {
            "hit_rate": 64.0,
            "average": 28.1,
            "recommended_side": "OVER",
            "availability": {"is_unavailable": False, "label": "Active"},
            "matchup": {
                "next_game": {
                    "opponent_team_id": 1610612738,
                    "opponent_abbreviation": "BOS",
                    "opponent_name": "Boston Celtics",
                    "playoff_game_number": 1,
                },
                "vs_position": {"delta_pct": 2.0},
            },
            "player": {
                "team_id": 1610612752,
                "team_abbreviation": "NYK",
                "team_name": "New York Knicks",
                "position": "G",
                "jersey": "11",
            },
            "opportunity": {"minutes_trend": "up", "volume_trend": "up"},
            "team_context": {},
            "environment": {},
            "variance": {},
            "games_count": 6,
            "last_n": 10,
            "h2h": {"games_count": 0, "hit_count": 0, "hit_rate": 0.0},
            "games": [
                {"matchup": "NYK vs. CLE", "hit": True},
                {"matchup": "NYK @ CLE", "hit": True},
                {"matchup": "NYK vs. CLE", "hit": False},
            ],
            "season_type": main.SEASON_TYPE_PLAYOFFS,
            "filtered_pool_count": 6,
            "season_pool_count": 6,
            "debug": {"freshness": {"game_log_source": "live"}},
        }
        pricing_snapshot = {
            "market": {"fair_over": 0.54, "fair_under": 0.46},
            "raw": {
                "over_probability": 0.64,
                "under_probability": 0.36,
                "over_edge_pct": 10.0,
                "under_edge_pct": -10.0,
                "over_ev": 0.18,
                "under_ev": -0.18,
            },
            "calibrated": {
                "over_probability": 0.61,
                "under_probability": 0.39,
                "over_edge_pct": 7.0,
                "under_edge_pct": -7.0,
                "over_ev": 0.12,
                "under_ev": -0.12,
            },
            "adjusted": {"over_ev": 0.11, "under_ev": -0.11},
            "reliability": {"reliability": 0.70, "shrink_strength": 0.30},
        }
        confidence_engine = {
            "grade": "A",
            "score": 82,
            "tone": "good",
            "tier": "High",
            "summary": "Strong playoff role",
            "tags": ["Minutes rising"],
            "market_side": "OVER",
            "market_disagrees": False,
            "market_penalty": 0.0,
            "ranking_score": 84,
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
            stack.enter_context(patch.object(main, "apply_backtest_ranking_adjustment", side_effect=lambda engine, **_kwargs: engine))
            result = main._parlay_builder_core(payload)

        self.assertFalse(result["playoff_relaxed_fallback_applied"])
        self.assertEqual(len(result["all_props_scored"]), 1)
        row = result["all_props_scored"][0]
        self.assertEqual(row["ranking_source"], "playoff_blend")
        self.assertEqual(row["h2h_games_count"], 0)
        self.assertEqual(row["h2h_weight_pct"], 0.0)
        self.assertGreater(row["ranking_sort_score"], 0)
        self.assertIn("H2H is parked", " ".join(row["playoff_strategy_tips"]))

    def test_bet_finder_playoff_uses_analyzer_blend_context(self) -> None:
        analysis_payload = {
            "player": {
                "id": 1628973,
                "full_name": "Jalen Brunson",
                "team_id": 1610612752,
                "team_name": "New York Knicks",
                "position": "G",
                "jersey": "11",
            },
            "season_type": main.SEASON_TYPE_PLAYOFFS,
            "recommended_side": "OVER",
            "games_count": 6,
            "hit_count": 4,
            "hit_rate": 66.7,
            "average": 27.8,
            "games": [{"value": 31, "hit": True}, {"value": 24, "hit": False}],
            "h2h": {"games_count": 0, "hit_count": 0, "hit_rate": 0.0},
            "h2h_side_hit_count": None,
            "h2h_side_hit_rate": None,
            "ranking_source": "playoff_blend",
            "ranking_hit_rate": 72.4,
            "ranking_sort_score": 72.4,
            "ranking_blend_score": 72.4,
            "h2h_weight_pct": 0.0,
            "ranking_profile": {
                "source": "playoff_blend",
                "sort_score": 72.4,
                "blend_score": 72.4,
                "h2h_weight_pct": 0.0,
                "notes": ["New playoff matchup"],
                "tips": ["H2H is parked at 0% until this series creates real matchup data."],
                "components": {"recent": 66.7, "model": 71.0, "confidence": 80.0, "edge": 58.0, "h2h": None},
            },
            "playoff_strategy_tips": ["H2H is parked at 0% until this series creates real matchup data."],
            "confidence": {"grade": "B", "score": 80, "model_probability": 61.0},
            "edge": 7.0,
            "ev": 0.11,
            "matchup": {},
            "opportunity": {},
            "team_context": {},
            "environment": {},
        }
        roster_row = {"PLAYER_ID": 1628973, "PLAYER": "Jalen Brunson", "POSITION": "G", "NUM": "11"}

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "fetch_team_roster", return_value=[roster_row]))
            stack.enter_context(patch.object(main, "build_prop_analysis_payload", return_value=analysis_payload))
            stack.enter_context(patch.object(main, "_submit_pg_write"))
            result = main.bet_finder(
                team_id=1610612752,
                stat="PTS",
                line=24.5,
                last_n=10,
                season="2025-26",
                season_type=main.SEASON_TYPE_PLAYOFFS,
                min_games=1,
                limit=5,
            )

        self.assertEqual(len(result["results"]), 1)
        row = result["results"][0]
        self.assertEqual(row["ranking_source"], "playoff_blend")
        self.assertEqual(row["h2h_games_count"], 0)
        self.assertEqual(row["h2h_weight_pct"], 0.0)
        self.assertEqual(row["side"], "OVER")
        self.assertAlmostEqual(row["ranking_sort_score"], 72.4, places=1)

    def test_todays_games_uses_live_scoreboard_status_and_series_context(self) -> None:
        scoreboard_row = {
            "GAME_ID": "0042500115",
            "GAME_DATE_EST": "2026-04-28",
            "HOME_TEAM_ID": 1610612738,
            "VISITOR_TEAM_ID": 1610612755,
            "GAME_STATUS_TEXT": "",
            "PTS_HOME": 0,
            "PTS_AWAY": 0,
        }
        live_payload = {
            "scoreboard": {
                "gameDate": "2026-04-28",
                "games": [
                    {
                        "gameId": "0042500115",
                        "gameStatus": 1,
                        "gameStatusText": "7:00 pm ET",
                        "gameTimeUTC": "2026-04-28T23:00:00Z",
                        "seriesGameNumber": "Game 5",
                        "gameLabel": "East First Round",
                        "gameSubLabel": "Game 5",
                        "seriesText": "BOS leads 3-2",
                        "seriesConference": "East",
                        "poRoundDesc": "1st Round",
                        "homeTeam": {"teamId": 1610612738, "wins": 3, "losses": 2, "score": 0, "seed": 2},
                        "awayTeam": {"teamId": 1610612755, "wins": 2, "losses": 3, "score": 0, "seed": 7},
                    }
                ],
            }
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "fetch_scoreboard_games", return_value=[scoreboard_row]))
            stack.enter_context(patch.object(main, "_fetch_nba_live_scoreboard", return_value=live_payload))
            stack.enter_context(patch.object(main, "_fetch_nba_schedule_league", return_value=None))
            stack.enter_context(patch.object(main, "get_cached_injury_report_payload_fast", return_value={"ok": False}))
            result = main.todays_games(game_date="2026-04-28")

        game = result["games"][0]
        self.assertEqual(game["status_text"], "7:00 am PHT")
        self.assertEqual(game["status_category"], "scheduled")
        self.assertEqual(game["season_type"], main.SEASON_TYPE_PLAYOFFS)
        self.assertEqual(game["series_summary"], "BOS leads 3-2")
        self.assertEqual(game["playoff_game_number"], 5)
        self.assertEqual(game["competition_label"], "East First Round")

    def test_todays_games_uses_schedule_context_when_live_scoreboard_misses_slate(self) -> None:
        scoreboard_row = {
            "GAME_ID": "0042500215",
            "GAME_DATE_EST": "2026-04-28",
            "HOME_TEAM_ID": 1610612765,
            "VISITOR_TEAM_ID": 1610612753,
            "GAME_STATUS_TEXT": "",
            "PTS_HOME": 0,
            "PTS_AWAY": 0,
        }
        schedule_payload = {
            "leagueSchedule": {
                "gameDates": [
                    {
                        "gameDate": "2026-04-28",
                        "games": [
                            {
                                "gameId": "0042500215",
                                "gameStatus": 1,
                                "gameStatusText": "",
                                "gameDateTimeUTC": "2026-04-29T00:30:00Z",
                                "gameLabel": "East First Round",
                                "gameSubLabel": "Game 5",
                                "seriesText": "",
                                "homeTeam": {"teamId": 1610612765, "wins": 2, "losses": 2, "score": 0, "seed": 5},
                                "awayTeam": {"teamId": 1610612753, "wins": 2, "losses": 2, "score": 0, "seed": 4},
                            }
                        ],
                    }
                ],
            }
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "fetch_scoreboard_games", return_value=[scoreboard_row]))
            stack.enter_context(patch.object(main, "_fetch_nba_live_scoreboard", return_value={"scoreboard": {"gameDate": "2026-04-27", "games": []}}))
            stack.enter_context(patch.object(main, "_fetch_nba_schedule_league", return_value=schedule_payload))
            stack.enter_context(patch.object(main, "get_cached_injury_report_payload_fast", return_value={"ok": False}))
            result = main.todays_games(game_date="2026-04-28")

        game = result["games"][0]
        self.assertEqual(game["status_text"], "8:30 am PHT")
        self.assertEqual(game["season_type"], main.SEASON_TYPE_PLAYOFFS)
        self.assertEqual(game["series_summary"], "Series tied 2-2")
        self.assertEqual(game["standings_summary"], "Series tied 2-2")
        self.assertEqual(game["playoff_game_number"], 5)
        self.assertEqual(game["competition_label"], "East First Round")

    def test_todays_games_has_playoff_safe_fallback_when_series_feed_unavailable(self) -> None:
        scoreboard_row = {
            "GAME_ID": "0042500215",
            "GAME_DATE_EST": "2026-04-28",
            "HOME_TEAM_ID": 1610612765,
            "VISITOR_TEAM_ID": 1610612753,
            "GAME_STATUS_TEXT": "",
            "PTS_HOME": 0,
            "PTS_AWAY": 0,
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "fetch_scoreboard_games", return_value=[scoreboard_row]))
            stack.enter_context(patch.object(main, "_fetch_nba_live_scoreboard", return_value=None))
            stack.enter_context(patch.object(main, "_fetch_nba_schedule_league", return_value=None))
            stack.enter_context(patch.object(main, "get_cached_injury_report_payload_fast", return_value={"ok": False}))
            result = main.todays_games(game_date="2026-04-28")

        game = result["games"][0]
        self.assertEqual(game["status_text"], "Scheduled")
        self.assertEqual(game["season_type"], main.SEASON_TYPE_PLAYOFFS)
        self.assertEqual(game["series_summary"], "")
        self.assertEqual(game["standings_summary"], "Series standing unavailable")
        self.assertEqual(game["playoff_game_number"], 5)

    def test_todays_games_marks_if_necessary_placeholder_when_series_is_over(self) -> None:
        scoreboard_row = {
            "GAME_ID": "0042500145",
            "GAME_DATE_EST": "2026-04-29",
            "HOME_TEAM_ID": 1610612760,
            "VISITOR_TEAM_ID": 1610612756,
            "GAME_STATUS_TEXT": "TBD",
            "PTS_HOME": 0,
            "PTS_AWAY": 0,
        }
        schedule_payload = {
            "leagueSchedule": {
                "gameDates": [
                    {
                        "gameDate": "04/27/2026 00:00:00",
                        "games": [
                            {
                                "gameId": "0042500144",
                                "gameStatus": 3,
                                "gameStatusText": "Final",
                                "gameLabel": "West First Round",
                                "gameSubLabel": "Game 4",
                                "seriesText": "OKC wins 4-0",
                                "homeTeam": {"teamId": 1610612756, "wins": 0, "losses": 4, "score": 122, "seed": 8},
                                "awayTeam": {"teamId": 1610612760, "wins": 4, "losses": 0, "score": 131, "seed": 1},
                            }
                        ],
                    }
                ],
            }
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "fetch_scoreboard_games", return_value=[scoreboard_row]))
            stack.enter_context(patch.object(main, "_fetch_nba_live_scoreboard", return_value=None))
            stack.enter_context(patch.object(main, "_fetch_nba_schedule_league", return_value=schedule_payload))
            stack.enter_context(patch.object(main, "get_cached_injury_report_payload_fast", return_value={"ok": False}))
            result = main.todays_games(game_date="2026-04-29")

        game = result["games"][0]
        self.assertEqual(game["status_text"], "Series over")
        self.assertEqual(game["season_type"], main.SEASON_TYPE_PLAYOFFS)
        self.assertEqual(game["competition_label"], "West First Round")
        self.assertEqual(game["game_sub_label"], "Game 5")
        self.assertEqual(game["series_summary"], "OKC wins 4-0")
        self.assertEqual(game["standings_summary"], "OKC wins 4-0")
        self.assertEqual(game["playoff_game_number"], 5)
        self.assertEqual(game["home"]["record"], "4-0")
        self.assertEqual(game["away"]["record"], "0-4")
        self.assertEqual(game["home"]["seed"], 1)
        self.assertEqual(game["away"]["seed"], 8)

    def test_todays_games_falls_back_to_live_scoreboard_when_stats_scoreboard_empty(self) -> None:
        live_payload = {
            "scoreboard": {
                "gameDate": "2026-04-28",
                "games": [
                    {
                        "gameId": "0042500125",
                        "gameStatus": 2,
                        "gameStatusText": "Q3 7:30",
                        "gameTimeUTC": "2026-04-29T00:00:00Z",
                        "seriesGameNumber": "Game 5",
                        "gameLabel": "East First Round",
                        "gameSubLabel": "Game 5",
                        "seriesText": "Series tied 2-2",
                        "homeTeam": {"teamId": 1610612752, "wins": 2, "losses": 2, "score": 73, "seed": 3},
                        "awayTeam": {"teamId": 1610612737, "wins": 2, "losses": 2, "score": 57, "seed": 6},
                    }
                ],
            }
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(main, "current_nba_game_date", return_value="2026-04-29"))
            stack.enter_context(patch.object(main, "fetch_scoreboard_games", return_value=[]))
            stack.enter_context(patch.object(main, "_fetch_nba_live_scoreboard", return_value=live_payload))
            stack.enter_context(patch.object(main, "_fetch_nba_schedule_league", return_value=None))
            stack.enter_context(patch.object(main, "get_cached_injury_report_payload_fast", return_value={"ok": False}))
            result = main.todays_games()

        self.assertEqual(result["resolved_date"], "2026-04-28")
        self.assertTrue(result["fallback_used"])
        game = result["games"][0]
        self.assertEqual(game["game_label"], "ATL @ NYK")
        self.assertEqual(game["status_text"], "Q3 7:30")
        self.assertEqual(game["status_category"], "live")
        self.assertEqual(game["season_type"], main.SEASON_TYPE_PLAYOFFS)
        self.assertEqual(game["series_summary"], "Series tied 2-2")
        self.assertEqual(game["playoff_game_number"], 5)

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
