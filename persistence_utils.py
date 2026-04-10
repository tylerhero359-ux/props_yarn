from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable


def save_json_snapshot(
    *,
    enabled: bool,
    path: Path,
    payload_factory: Callable[[], dict[str, Any]],
    on_error: Callable[[Exception], None],
) -> None:
    if not enabled:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = payload_factory()
        if "version" not in payload:
            payload["version"] = 1
        if "saved_at" not in payload:
            payload["saved_at"] = time.time()
        temp_path = path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        temp_path.replace(path)
    except Exception as exc:
        on_error(exc)


def load_json_snapshot(
    *,
    enabled: bool,
    path: Path,
    extract_entries: Callable[[dict[str, Any]], Any],
    apply_entries: Callable[[Any], None],
    on_error: Callable[[Exception], None],
) -> None:
    if not enabled or not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        entries = extract_entries(payload)
        apply_entries(entries)
    except Exception as exc:
        on_error(exc)
