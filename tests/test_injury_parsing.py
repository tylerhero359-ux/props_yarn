"""
Snapshot tests for InjuryReportService.parse_report_rows().

These tests verify that parsing works correctly against known text.
If the NBA changes their PDF format, these tests will catch it immediately.

To update after a verified format change:
  1. Extract text from a new real PDF.
  2. Update SAMPLE_REPORT_TEXT below.
  3. Update EXPECTED_* constants to match.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from injury_service import InjuryReportService


def _make_service() -> InjuryReportService:
    """Minimal service instance — no HTTP client needed for parse_report_rows."""
    return InjuryReportService(
        team_pool=[
            {
                "id": 1610612747,
                "full_name": "Los Angeles Lakers",
                "abbreviation": "LAL",
                "nickname": "Lakers",
                "city": "Los Angeles",
            },
            {
                "id": 1610612744,
                "full_name": "Golden State Warriors",
                "abbreviation": "GSW",
                "nickname": "Warriors",
                "city": "Golden State",
            },
        ],
        http_get=None,  # not needed for parse_report_rows
        timed_call=lambda name: (lambda f: f),
    )


# Sample text matching the real NBA injury report PDF layout.
# Each team header is followed by players with their status.
SAMPLE_REPORT_TEXT = """\
NBA Injury Report
Los Angeles Lakers
LeBron James Out Left ankle soreness
Anthony Davis Questionable Right knee bruise
Austin Reaves Available
Golden State Warriors
Stephen Curry Available
Draymond Green Doubtful Personal reasons
Klay Thompson Out Right shoulder
"""

EXPECTED_PLAYERS = [
    {"player_display": "LeBron James",   "status": "Out",          "team_name": "Los Angeles Lakers"},
    {"player_display": "Anthony Davis",  "status": "Questionable", "team_name": "Los Angeles Lakers"},
    {"player_display": "Austin Reaves",  "status": "Available",    "team_name": "Los Angeles Lakers"},
    {"player_display": "Stephen Curry",  "status": "Available",    "team_name": "Golden State Warriors"},
    {"player_display": "Draymond Green", "status": "Doubtful",     "team_name": "Golden State Warriors"},
    {"player_display": "Klay Thompson",  "status": "Out",          "team_name": "Golden State Warriors"},
]


def test_all_players_found():
    svc = _make_service()
    result = svc.parse_report_rows(SAMPLE_REPORT_TEXT)
    rows = result.get("rows") or []
    found = {r["player_display"] for r in rows}
    for exp in EXPECTED_PLAYERS:
        assert exp["player_display"] in found, (
            f"Expected '{exp['player_display']}' not found. Got: {sorted(found)}"
        )


def test_statuses_correct():
    svc = _make_service()
    result = svc.parse_report_rows(SAMPLE_REPORT_TEXT)
    by_name = {r["player_display"]: r for r in result.get("rows") or []}
    for exp in EXPECTED_PLAYERS:
        row = by_name.get(exp["player_display"])
        assert row is not None, f"Player '{exp['player_display']}' missing from rows"
        assert row["status"] == exp["status"], (
            f"{exp['player_display']}: expected status '{exp['status']}', got '{row['status']}'"
        )


def test_team_assignment_correct():
    svc = _make_service()
    result = svc.parse_report_rows(SAMPLE_REPORT_TEXT)
    by_name = {r["player_display"]: r for r in result.get("rows") or []}
    for exp in EXPECTED_PLAYERS:
        row = by_name.get(exp["player_display"])
        assert row is not None
        assert row["team_name"] == exp["team_name"], (
            f"{exp['player_display']}: expected team '{exp['team_name']}', got '{row['team_name']}'"
        )


def test_row_has_required_keys():
    svc = _make_service()
    result = svc.parse_report_rows(SAMPLE_REPORT_TEXT)
    for row in result.get("rows") or []:
        for key in ("team_name", "player_display", "player_key", "status"):
            assert key in row, f"Row missing key '{key}': {row}"


def test_header_line_not_parsed_as_player():
    """The 'NBA Injury Report' header line must not appear as a player."""
    svc = _make_service()
    result = svc.parse_report_rows(SAMPLE_REPORT_TEXT)
    names = [r["player_display"] for r in result.get("rows") or []]
    assert not any("NBA Injury Report" in n for n in names), (
        f"Header line incorrectly parsed as player: {names}"
    )


def test_pending_report_team():
    """A team listed as 'Pending report' should appear in pending_teams, not rows."""
    svc = _make_service()
    text = """\
Los Angeles Lakers
Pending report
Golden State Warriors
Stephen Curry Available
"""
    result = svc.parse_report_rows(text)
    pending = result.get("pending_teams") or []
    rows = result.get("rows") or []
    assert "Los Angeles Lakers" in pending, f"Expected Lakers in pending_teams. Got: {pending}"
    # No Lakers players should appear in rows since the whole team is pending.
    laker_rows = [r for r in rows if r.get("team_name") == "Los Angeles Lakers"]
    assert len(laker_rows) == 0, f"Lakers rows should be empty when pending: {laker_rows}"


def test_empty_text_returns_empty():
    svc = _make_service()
    result = svc.parse_report_rows("")
    assert result.get("rows") == []
    assert result.get("pending_teams") == []
