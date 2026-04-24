from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

@dataclass
class MarketScanService:
    submit_async_job: Callable[[str, Callable[..., dict[str, Any]], dict[str, Any]], dict[str, Any]]

    @staticmethod
    def run_sync(run_func, payload: dict[str, Any]) -> dict[str, Any]:
        return run_func(payload)

    def run_async(self, run_func, payload: dict[str, Any]) -> dict[str, Any]:
        return self.submit_async_job("market_scan", run_func, payload)
