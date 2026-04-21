import unittest
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


if __name__ == "__main__":
    unittest.main()
