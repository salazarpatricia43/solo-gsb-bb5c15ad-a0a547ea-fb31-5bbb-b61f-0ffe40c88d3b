"""Opaque keyset pagination cursors (base64-encoded JSON tuples)."""
from __future__ import annotations

import base64
import json

from .errors import BadRequestError


def encode_cursor(parts: tuple) -> str:
    raw = json.dumps(parts, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str, expected: int) -> tuple:
    try:
        padding = "=" * (-len(cursor) % 4)
        parts = tuple(json.loads(base64.urlsafe_b64decode(cursor + padding)))
    except Exception as exc:  # malformed base64 / json
        raise BadRequestError("malformed cursor") from exc
    if len(parts) != expected:
        raise BadRequestError("malformed cursor")
    return parts
