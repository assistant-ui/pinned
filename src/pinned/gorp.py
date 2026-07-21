"""Provisional gorp stream frame producer, v1.

This module owns the wire format for the Thread state stream so it stays
swappable; nothing outside it should build or parse frames.

Frames are JSON objects delivered as SSE ``data:`` events, one frame per event:

    {"v": 1, "op": "set", "path": [...], "value": ..., "seq": n}
    {"v": 1, "op": "append", "path": [...], "value": ..., "seq": n}
    {"v": 1, "op": "terminal", "reason": "completed" | "evicted" | "error", "seq": n}

The first frame on every attach is a full snapshot: a "set" with path []
whose value is the entire current state, carrying the current seq watermark.
Subsequent frames are incremental updates as the run mutates state.

A terminal frame names why the stream ended: "completed" (run finished),
"evicted" (instance aborted or wound down; the client may reconnect and gets
a fresh instance), "error" (run failed). A bare close without a terminal
frame is a transport fault and the client should reconnect.
"""

from __future__ import annotations

import json
from typing import Any

VERSION = 1


def set_op(path: list[Any], value: Any, seq: int) -> dict[str, Any]:
    return {"v": VERSION, "op": "set", "path": path, "value": value, "seq": seq}


def append_op(path: list[Any], value: Any, seq: int) -> dict[str, Any]:
    return {"v": VERSION, "op": "append", "path": path, "value": value, "seq": seq}


def terminal(reason: str, seq: int) -> dict[str, Any]:
    return {"v": VERSION, "op": "terminal", "reason": reason, "seq": seq}


def snapshot(state: dict[str, Any], seq: int) -> dict[str, Any]:
    return set_op([], state, seq)


def is_terminal(frame: dict[str, Any]) -> bool:
    return frame.get("op") == "terminal"


def encode(frame: dict[str, Any]) -> bytes:
    return b"data: " + json.dumps(frame, separators=(",", ":")).encode() + b"\n\n"
