"""Read-only threshold exposure over one source's published readings.

``GET /sessions/{session_id}/exposure`` answers, for one declared source, how
many seconds of the **half-open** event-time interval ``[from, to)`` were
covered by a published value **strictly above** ``threshold``.  It also
reports how many seconds had any valid finite sample at all
(``covered_seconds``) and how many were unknown (``unknown_seconds``); when
nothing was covered the over-threshold ratio is ``null``, otherwise it is
``above_threshold_seconds / covered_seconds``.

Holding semantics
-----------------

Every published reading's value holds until that source's next published
reading or the query endpoint; the interval start inherits the most recent
published reading at or before ``from``.  Several readings sharing one event
time take effect in ``seq`` order, i.e. the last (highest-``seq``) one wins.

Missing values, non-finite numbers (NaN/Infinity) and booleans form
**unknown** stretches: an earlier valid value is *not* carried across them,
and with no reading at or before ``from`` the start of the interval is
unknown as well.  Only ``published`` readings participate -- buffered and
quarantined rows are never visible -- so the whole computation runs over one
consistent SQLite read snapshot and never writes anything (watermarks,
alerts and checkpoints are untouched).
"""
from __future__ import annotations

import json
import math
import sqlite3
from typing import Any, Optional

from .alerts import extract_numeric
from .database import read_transaction
from .errors import BadRequestError
from .service import _load_session
from .time_utils import format_event_time, parse_event_time, store_event_time

__all__ = ["source_exposure"]

# Segment classifications for the piecewise-constant value timeline.
_ABOVE = "above"        # finite non-boolean sample strictly over the threshold
_BELOW_OR_EQUAL = "ok"  # finite non-boolean sample not strictly over it
_UNKNOWN = "unknown"   # no comparable value (missing / NaN / Infinity / bool)


# ---------------------------------------------------------------------------
# request validation
# ---------------------------------------------------------------------------

def _required_time(value: Any, field: str) -> str:
    """Validate/normalise a required event-time bound (half-open interval)."""
    if value is None:
        raise BadRequestError(f"{field} is required")
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError(f"{field} must be a non-empty ISO 8601 string")
    # TimestampError is mapped to HTTP 400 by the application handler.
    return store_event_time(parse_event_time(value))


def _validate_threshold(value: Any) -> float:
    """Parse the required finite ``threshold`` query parameter (400 on error).

    Query parameters arrive as strings; a missing value, junk, NaN or
    Infinity is a bad request rather than a Pydantic 422.
    """
    if value is None:
        raise BadRequestError("threshold is required")
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError("threshold must be a finite number")
    try:
        threshold = float(value.strip())
    except ValueError as exc:
        raise BadRequestError(
            f"threshold must be a finite number, got {value!r}"
        ) from exc
    if not math.isfinite(threshold):
        raise BadRequestError("threshold must be a finite number")
    return threshold


# ---------------------------------------------------------------------------
# timeline classification
# ---------------------------------------------------------------------------

def _classify(payload: Optional[str], value_key: Optional[str], threshold: float) -> str:
    """Classify one stored reading payload for the piecewise timeline.

    Mirrors the alert state machines' value extraction: with ``value_key``
    the top-level object key is read, otherwise the reading value itself is
    used.  Missing keys/payloads, non-numbers, booleans and NaN/Infinity are
    all unknown -- they never inherit an older valid value.
    """
    number = extract_numeric(payload, value_key)
    if number is None:
        return _UNKNOWN
    # The comparison is strict: a value exactly on the threshold is valid
    # coverage but not over-threshold time.
    return _ABOVE if number > threshold else _BELOW_OR_EQUAL


def _micros_between(start: Any, end: Any) -> int:
    """Exact whole-microsecond distance between two UTC datetimes."""
    delta = end - start
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


# ---------------------------------------------------------------------------
# exposure computation
# ---------------------------------------------------------------------------

def source_exposure(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    source_id: Any,
    from_: Any,
    to: Any,
    threshold: Any,
    value_key: Any = None,
) -> dict[str, Any]:
    """Return the over-threshold/covered/unknown seconds for one source.

    Read-only: every value comes from one deferred read snapshot and no
    table is ever written.  Malformed parameters raise
    :class:`~app.errors.BadRequestError` / :class:`TimestampError` (400);
    an unknown session raises 404; an undeclared ``source_id`` raises 400.
    """
    if source_id is None:
        raise BadRequestError("source_id is required")
    if not isinstance(source_id, str) or not source_id.strip():
        raise BadRequestError("source_id must be a non-empty string")
    if value_key is not None:
        if not isinstance(value_key, str) or not value_key.strip():
            raise BadRequestError("value_key must be a non-empty string")
        value_key = value_key.strip()

    from_store = _required_time(from_, "from")
    to_store = _required_time(to, "to")
    # The interval is half-open [from, to); an empty or backwards interval is
    # invalid rather than a request that happens to total zero seconds.
    if from_store >= to_store:
        raise BadRequestError(
            f"'from' ({format_event_time(parse_event_time(from_store))}) must be"
            f" strictly earlier than 'to' ({format_event_time(parse_event_time(to_store))})"
        )
    threshold_value = _validate_threshold(threshold)

    from_dt = parse_event_time(from_store)
    to_dt = parse_event_time(to_store)

    with read_transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = set(json.loads(session["source_ids"]))
        if source_id not in allowed_sources:
            raise BadRequestError(
                f"unknown source_id {source_id!r} for this session;"
                f" declared sources: {sorted(allowed_sources)}"
            )

        # Every published reading that can influence [from, to):
        #   * observed_at < to      -- a reading exactly at `to` takes effect
        #                              at the excluded endpoint, so never;
        #   * observed_at <= from   -- only the latest one carries its value
        #                              into the interval's starting state;
        #   * from < observed_at    -- creates a segment boundary inside.
        # Buffered/quarantined rows are excluded by status; everything here
        # is read from one consistent snapshot.
        rows = conn.execute(
            "SELECT observed_at, seq, payload FROM readings"
            " WHERE session_id = ? AND source_id = ? AND status = 'published'"
            " AND observed_at < ?"
            " ORDER BY observed_at, seq",
            (session_id, source_id, to_store),
        ).fetchall()

    # Fold readings sharing one event time: in (observed_at, seq) order the
    # last one is the value in force at that instant.
    events: list[tuple[str, Optional[str]]] = []
    for row in rows:
        if events and events[-1][0] == row["observed_at"]:
            events[-1] = (row["observed_at"], row["payload"])
        else:
            events.append((row["observed_at"], row["payload"]))

    # Walk the folded events: those at or before `from` only set the carried-in
    # starting state; later ones close one segment and start the next.
    state = _UNKNOWN
    window_events: list[tuple[Any, str]] = []
    for observed_at, payload in events:
        kind = _classify(payload, value_key, threshold_value)
        if observed_at <= from_store:
            state = kind
        else:
            window_events.append((parse_event_time(observed_at), kind))

    totals = {_ABOVE: 0, _BELOW_OR_EQUAL: 0, _UNKNOWN: 0}
    cursor_dt = from_dt
    for event_dt, kind in window_events:
        totals[state] += _micros_between(cursor_dt, event_dt)
        state = kind
        cursor_dt = event_dt
    totals[state] += _micros_between(cursor_dt, to_dt)

    covered_micros = totals[_ABOVE] + totals[_BELOW_OR_EQUAL]
    above_seconds = totals[_ABOVE] / 1_000_000
    covered_seconds = covered_micros / 1_000_000
    unknown_seconds = totals[_UNKNOWN] / 1_000_000
    ratio = (
        totals[_ABOVE] / covered_micros
        if covered_micros > 0
        else None
    )

    return {
        "session_id": session_id,
        "source_id": source_id,
        "from": format_event_time(from_dt),
        "to": format_event_time(to_dt),
        "threshold": threshold_value,
        "value_key": value_key,
        "above_threshold_seconds": above_seconds,
        "covered_seconds": covered_seconds,
        "unknown_seconds": unknown_seconds,
        "ratio_above": ratio,
    }
