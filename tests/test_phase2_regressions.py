import unittest
from unittest.mock import patch

from fastapi import HTTPException
from starlette.requests import Request
from starlette.types import Scope

import main


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, body=None, text: str = "") -> None:
        self.status_code = status_code
        self.ok = status_code < 400
        self.headers = {
            "x-requests-remaining": "100",
            "x-requests-used": "1",
            "x-requests-last": "1",
        }
        self._body = {"ok": True} if body is None else body
        self.text = text

    def json(self):
        return self._body


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()
        self.put_count = 0

    def getconn(self):
        return self.conn

    def putconn(self, conn):
        self.put_count += 1


class _FakeConn:
    def __init__(self) -> None:
        self.commit_count = 0
        self.rollback_count = 0

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


class Phase2RegressionTests(unittest.TestCase):
    def test_odds_api_fetch_uses_header_auth_and_no_query_apikey(self) -> None:
        fake_response = _FakeResponse(status_code=200)
        captured: dict[str, object] = {}

        def _fake_get(url, timeout=None, headers=None):
            captured["url"] = url
            captured["headers"] = headers
            return fake_response

        with patch.object(main.requests, "get", side_effect=_fake_get), patch.object(
            main, "_submit_pg_write", return_value=None
        ):
            result = main.odds_api_fetch("/sports", "secret-key", {"all": "false"})

        self.assertIn("quota", result)
        self.assertNotIn("apiKey=", str(captured["url"]))
        headers = captured["headers"] or {}
        self.assertEqual(headers.get("X-API-Key"), "secret-key")

    def test_odds_api_fetch_falls_back_to_query_api_key_on_auth_rejection(self) -> None:
        responses = [
            _FakeResponse(status_code=401, text="unauthorized"),
            _FakeResponse(status_code=200),
        ]
        calls: list[dict[str, object]] = []

        def _fake_get(url, timeout=None, headers=None):
            calls.append({"url": url, "headers": headers})
            return responses.pop(0)

        with patch.object(main.requests, "get", side_effect=_fake_get), patch.object(
            main, "_submit_pg_write", return_value=None
        ), patch.object(main, "ODDS_API_QUERY_AUTH_FALLBACK_ENABLED", True):
            result = main.odds_api_fetch("/sports", "fallback-key", {"all": "false"})

        self.assertIn("quota", result)
        self.assertEqual(len(calls), 2)
        self.assertNotIn("apiKey=", str(calls[0]["url"]))
        self.assertIn("apiKey=fallback-key", str(calls[1]["url"]))

    def test_odds_api_fetch_falls_back_when_missing_key_error_body_present(self) -> None:
        responses = [
            _FakeResponse(status_code=400, text='{"message":"API key is missing","error_code":"MISSING_KEY"}'),
            _FakeResponse(status_code=200),
        ]
        calls: list[dict[str, object]] = []

        def _fake_get(url, timeout=None, headers=None):
            calls.append({"url": url, "headers": headers})
            return responses.pop(0)

        with patch.object(main.requests, "get", side_effect=_fake_get), patch.object(
            main, "_submit_pg_write", return_value=None
        ):
            result = main.odds_api_fetch(
                "/sports",
                "fallback-key",
                {"all": "false"},
                allow_query_auth_fallback=True,
            )

        self.assertIn("quota", result)
        self.assertEqual(len(calls), 2)
        self.assertIn("apiKey=fallback-key", str(calls[1]["url"]))

    def test_cors_middleware_is_registered(self) -> None:
        middleware_names = [mw.cls.__name__ for mw in main.app.user_middleware]
        self.assertIn("CORSMiddleware", middleware_names)

    def test_rate_limit_enforced(self) -> None:
        scope: Scope = {"type": "http", "method": "GET", "path": "/", "headers": [], "client": ("127.0.0.1", 12345)}
        request = Request(scope)
        with patch.object(main, "RATE_LIMIT_ENABLED", True), patch.object(main, "RATE_LIMIT_WINDOW_SECONDS", 60), patch.object(
            main, "RATE_LIMIT_HEAVY_MAX_REQUESTS", 1
        ), patch.object(main, "_RATE_LIMIT_BUCKETS", {}):
            main.enforce_heavy_rate_limit(request, "test_scope")
            with self.assertRaises(HTTPException) as ctx:
                main.enforce_heavy_rate_limit(request, "test_scope")
            self.assertEqual(ctx.exception.status_code, 429)

    def test_bulk_player_props_callable_without_request_for_async_jobs(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            main._bulk_player_props_core({"rows": []})
        self.assertEqual(ctx.exception.status_code, 400)

    def test_postgres_connect_uses_pool_context_when_available(self) -> None:
        fake_pool = _FakePool()
        with patch.object(main, "POSTGRES_CONNECTION_POOL", fake_pool):
            with main.postgres_connect() as conn:
                self.assertIs(conn, fake_pool.conn)
            self.assertEqual(fake_pool.conn.commit_count, 1)
            self.assertEqual(fake_pool.put_count, 1)


if __name__ == "__main__":
    unittest.main()
