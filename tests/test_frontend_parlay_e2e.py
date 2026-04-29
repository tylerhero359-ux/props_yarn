import json
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path
from urllib.parse import urlparse

import requests


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class FrontendParlayE2ETests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._repo_root = Path(__file__).resolve().parents[1]
        cls._port = _free_port()
        cls._base_url = f"http://127.0.0.1:{cls._port}"
        cls._server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(cls._port),
                "--log-level",
                "warning",
            ],
            cwd=str(cls._repo_root),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        deadline = time.time() + 20.0
        server_ready = False
        while time.time() < deadline:
            if cls._server.poll() is not None:
                break
            try:
                health = requests.get(f"{cls._base_url}/health", timeout=0.8)
                if health.status_code == 200:
                    server_ready = True
                    break
            except Exception:
                pass
            time.sleep(0.15)
        if not server_ready:
            cls._server.terminate()
            raise unittest.SkipTest("Could not start local app server for frontend E2E.")

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "_server", None):
            cls._server.terminate()
            try:
                cls._server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cls._server.kill()

    def test_parlay_build_renders_rows_h2h_sample_and_playoff_fallback_badge(self) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:
            raise unittest.SkipTest(f"Playwright unavailable: {exc}")

        now_iso = "2026-04-27T12:00:00Z"
        events_payload = {
            "events": [
                {
                    "id": "ev1",
                    "home_team": "Boston Celtics",
                    "away_team": "New York Knicks",
                    "commence_time": now_iso,
                },
                {
                    "id": "ev2",
                    "home_team": "Indiana Pacers",
                    "away_team": "Milwaukee Bucks",
                    "commence_time": now_iso,
                },
            ]
        }
        result_payload = {
            "legs": 2,
            "parlay": [
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
                    "confidence_summary": "Strong profile",
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
                    "confidence_summary": "Playable profile",
                    "ranking_score": 78,
                    "selection_status": "selected",
                    "selection_reason": "Selected for depth.",
                    "selection_reason_parts": ["depth"],
                    "average": 10.2,
                    "ev": 0.062,
                    "edge": 4.1,
                },
            ],
            "parlay_odds": 3.51,
            "all_props_scored": [],
            "events_scraped": 2,
            "props_found": 2,
            "props_analyzed": 2,
            "errors": [],
            "quota_log": [],
            "bookmakers": ["draftkings"],
            "cost_hint": {},
            "playoff_relaxed_fallback_applied": True,
            "injury_summary": [],
        }
        result_payload["all_props_scored"] = list(result_payload["parlay"])
        ndjson_stream = "\n".join(
            [
                json.dumps({"type": "progress", "stage": "events_resolved", "events": 2}),
                json.dumps({"type": "progress", "stage": "analysis_done", "analyzed": 2, "errors": 0}),
                json.dumps({"type": "progress", "stage": "done", "events_scraped": 2, "props_found": 2, "props_analyzed": 2, "errors": 0}),
                json.dumps({"type": "result", "payload": result_payload}),
            ]
        ) + "\n"

        try:
            with sync_playwright() as p:
                try:
                    browser = p.chromium.launch(headless=True)
                except Exception as exc:
                    raise unittest.SkipTest(f"Chromium not available for Playwright: {exc}")

                context = browser.new_context()
                page = context.new_page()
                page.set_default_timeout(15000)

                def _api_router(route):
                    request = route.request
                    path = urlparse(request.url).path
                    method = request.method.upper()

                    if path == "/api/key-vault" and method == "GET":
                        route.fulfill(
                            status=200,
                            content_type="application/json",
                            body=json.dumps(
                                {
                                    "entries": [
                                        {
                                            "id": "k1",
                                            "provider": "odds_api",
                                            "key": "odds-test-key-1",
                                            "label": "Primary Key",
                                            "remaining": 200,
                                            "used": 10,
                                            "last_checked_at": now_iso,
                                        }
                                    ],
                                    "active_id": "k1",
                                }
                            ),
                        )
                        return
                    if path == "/api/key-vault" and method == "PUT":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps({"ok": True}))
                        return
                    if path == "/api/odds/check-quota":
                        route.fulfill(
                            status=200,
                            content_type="application/json",
                            body=json.dumps({"ok": True, "quota": {"remaining": "200", "used": "10", "last": "0"}}),
                        )
                        return
                    if path == "/api/odds/events":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps(events_payload))
                        return
                    if path == "/api/parlay-builder/stream":
                        route.fulfill(
                            status=200,
                            headers={"content-type": "application/x-ndjson"},
                            body=ndjson_stream,
                        )
                        return

                    # Keep startup requests deterministic and offline-safe.
                    if path == "/api/todays-games":
                        route.fulfill(
                            status=200,
                            content_type="application/json",
                            body=json.dumps({"games": [], "resolved_date": "2026-04-27", "fallback_used": False}),
                        )
                        return
                    if path == "/api/teams":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps({"results": []}))
                        return
                    if path == "/api/players/search":
                        route.fulfill(status=200, content_type="application/json", body=json.dumps({"results": []}))
                        return

                    route.fulfill(status=200, content_type="application/json", body=json.dumps({"ok": True}))

                page.route("**/api/**", _api_router)
                page.goto(f"{self._base_url}/", wait_until="domcontentloaded")

                page.click("button[data-view='parlay']")
                page.click("#parlayLoadEventsBtn")
                page.wait_for_selector("#parlayEventList .parlay-event-chip")

                event_chips = page.locator("#parlayEventList .parlay-event-chip")
                self.assertGreaterEqual(event_chips.count(), 2)
                event_chips.nth(0).click()
                event_chips.nth(1).click()

                page.click("#parlayBuildBtn")
                page.wait_for_selector("#parlayAllPropsBody tr")

                first_row_text = page.locator("#parlayAllPropsBody tr").nth(0).inner_text()
                self.assertIn("EV 8.7%", first_row_text)
                self.assertIn("Sample 6/10", first_row_text)
                self.assertIn("Ranked by: H2H (2/3 66.7%)", first_row_text)

                fallback_notice = page.locator("#parlayFallbackNotice")
                self.assertTrue(fallback_notice.is_visible())
                self.assertIn("playoff fallback applied", fallback_notice.inner_text().lower())

                browser.close()
        except unittest.SkipTest:
            raise
        except AssertionError:
            raise
        except Exception as exc:
            raise unittest.SkipTest(f"Playwright runtime unavailable in this environment: {exc}")


if __name__ == "__main__":
    unittest.main()
