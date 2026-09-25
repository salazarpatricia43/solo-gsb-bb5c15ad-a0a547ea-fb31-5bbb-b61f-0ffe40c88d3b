"""Timestamp helpers.

All event times are normalised to UTC internally. The API accepts the most
common ISO 8601 forms (``2026-09-21T10:00:00Z``, with offsets, or a naive
timestamp which is assumed to be UTC) and always returns UTC with a ``Z``
suffix.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from fractions import Fraction


class TimestampError(ValueError):
    """Raised when an observed_at value cannot be parsed."""


def parse_event_time(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise TimestampError("observed_at must be a non-empty ISO 8601 string")
    text = value.strip()
    # datetime.fromisoformat in Python 3.11 understands the trailing Z.
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError as exc:
        raise TimestampError(f"invalid ISO 8601 timestamp: {value!r}") from exc
    if dt.tzinfo is None:
        # Naive timestamps are treated as UTC so ordering stays unambiguous.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _utc_iso(dt: datetime, *, timespec: str) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    if timespec == "seconds":
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Microsecond precision (6 digits) so every stored timestamp has the same
    # width: this makes lexicographic string comparison in SQLite identical to
    # chronological comparison.
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def store_event_time(dt: datetime) -> str:
    """Fixed-width, sortable UTC form used for all stored/compared times."""
    return _utc_iso(dt, timespec="microseconds")


def floor_seconds_to_micros(seconds: str) -> int:
    """Greatest whole-microsecond count not exceeding ``seconds`` exactly.

    ``seconds`` is the request's seconds value as text (already validated as
    a finite number); its exact decimal value -- not the rounded binary
    float -- is scaled to microseconds and floored, so ``"0.0000015"``
    yields 1: a sub-microsecond fraction is never rounded up to a whole
    microsecond.  Stored event times have microsecond resolution, so for an
    integer microsecond distance ``m`` this floor reproduces the exact
    comparison against the requested seconds: ``m > floor`` iff ``m``
    micros is strictly more than ``seconds``, and ``m <= floor`` iff it is
    at most ``seconds``.  Arbitrarily large finite inputs floor to
    arbitrarily large ints without overflow.
    """
    return math.floor(Fraction(seconds.strip()) * 1_000_000)


def format_event_time(dt: datetime) -> str:
    """Human-facing canonical ISO 8601 UTC form.

    Fractional seconds are preserved with trailing zeros removed (e.g.
    ``...:05.000001Z`` keeps its microseconds while ``...:05.000000Z`` is
    rendered as ``...:05Z``).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    base = dt.strftime("%Y-%m-%dT%H:%M:%S")
    frac = f"{dt.microsecond:06d}".rstrip("0")
    return base + (f".{frac}" if frac else "") + "Z"
