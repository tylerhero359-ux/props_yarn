from __future__ import annotations

import json
import queue
import threading
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException
from fastapi.responses import StreamingResponse


@dataclass
class ProgressStreamService:
    @staticmethod
    def emit_progress(progress_cb, stage: str, **extra: Any) -> None:
        if not progress_cb:
            return
        payload = {"stage": stage}
        payload.update(extra)
        progress_cb(payload)

    @staticmethod
    def stream_with_progress(run_func, payload: dict[str, Any]) -> StreamingResponse:
        def generator():
            q: queue.Queue[dict[str, Any]] = queue.Queue()
            done = threading.Event()

            def progress_cb(update: dict[str, Any]) -> None:
                q.put({"type": "progress", **update})

            def runner() -> None:
                try:
                    result = run_func(payload, progress_cb)
                    q.put({"type": "result", "payload": result})
                except HTTPException as exc:
                    q.put({"type": "error", "status": exc.status_code, "message": exc.detail})
                except Exception as exc:
                    q.put({"type": "error", "status": 500, "message": str(exc)})
                finally:
                    done.set()

            thread = threading.Thread(target=runner, name="progress-stream", daemon=True)
            thread.start()

            while not done.is_set() or not q.empty():
                try:
                    item = q.get(timeout=0.25)
                except queue.Empty:
                    continue
                yield json.dumps(item) + "\n"

        return StreamingResponse(generator(), media_type="application/x-ndjson")
