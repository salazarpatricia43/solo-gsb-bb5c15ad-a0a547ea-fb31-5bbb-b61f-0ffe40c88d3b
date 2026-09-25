"""Read-only local-spike detection (neighbour median + MAD) over published readings.

``GET /sessions/{session_id}/local-spikes`` scans the **published** readings of
one declared source whose ``observed_at`` lies inside the **inclusive** event
time interval ``[from, to]`` and flags readings that stand out relative to the
other valid readings nearby in event time.

Only finite, non-boolean numeric values are *valid* readings; with
``value_key`` the number is read from that top-level key of an object-valued
reading.  Missing keys/payloads, booleans, strings and NaN/Infinity values are
skipped (they are neither candidates nor neighbours).

For every valid reading the neighbours are the **other** valid readings whose
event-time distance is at most ``window_seconds`` (the boundary is inclusive);
readings outside ``[from, to]`` never participate.  The window is compared at
its exact requested value against the integer-microsecond stored timeline, so
a fractional microsecond is never rounded up (a 0.0000015-second window does
not reach a reading 2 microseconds away).  With fewer than three
neighbours the reading is left undecided.  Otherwise its local median (the
median of the neighbour values) and MAD (the median of the neighbours'
absolute deviations from that median) are computed, and the reading is a spike
exactly when::

    |value - local_median| > sensitivity * MAD

The comparison is strict, so when every neighbour is identical (``MAD = 0``)
only a reading whose value differs from the median (strict deviation above
zero) is flagged.

Even-count medians average the two middle values *without* an intermediate
overflow: the midpoint of two finite doubles is itself finite, so four
neighbours of ``1e308`` (whose naive ``(1e308 + 1e308) / 2`` overflows to
Infinity) still yield the finite median ``1e308`` and, when the neighbours
agree, ``MAD = 0``.  Every statistic whose mathematical value is a finite
double is therefore returned as a finite JSON number -- never NaN/Infinity.

To keep the O(n^2) neighbour scan bounded, an interval containing more than
200 published readings is rejected with 400 instead of being analysed.  The
whole read -- the capped count, the readings and every statistic -- happens
inside one deferred SQLite read transaction, i.e. one consistent snapshot, and
nothing is ever written.
"""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime
from typing import Any

from .alerts import extract_numeric
from .database import read_transaction
from .errors import BadRequestError
from .service import _load_session
from .time_utils import (
    floor_seconds_to_micros,
    format_event_time,
    parse_event_time,
    store_event_time,
)

__all__ = ["local_spikes", "MAX_READINGS"]

# An interval with more published readings than this is refused: the
# neighbourhood scan is quadratic and the endpoint must stay cheap.
MAX_READINGS = 200


# ---------------------------------------------------------------------------
# request validation
# ---------------------------------------------------------------------------

def _required_time(value: Any, field: str) -> str:
    """Validate/normalise a required inclusive event-time bound."""
    if value is None:
        raise BadRequestError(f"{field} is required")
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError(f"{field} must be a non-empty ISO 8601 string")
    # TimestampError is mapped to HTTP 400 by the application handler.
    return store_event_time(parse_event_time(value))


def _validate_positive(value: Any, field: str) -> float:
    """Parse a required strictly-positive finite query parameter (400 on error).

    Query parameters arrive as strings; a missing value, junk, NaN, Infinity,
    zero or a negative number is a bad request rather than a Pydantic 422.
    """
    if value is None:
        raise BadRequestError(f"{field} is required")
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError(f"{field} must be a positive finite number")
    try:
        number = float(value.strip())
    except ValueError as exc:
        raise BadRequestError(
            f"{field} must be a positive finite number, got {value!r}"
        ) from exc
    if not math.isfinite(number) or number <= 0:
        raise BadRequestError(
            f"{field} must be a strictly positive finite number, got {value!r}"
        )
    return number


def _abs_micros(a: datetime, b: datetime) -> int:
    """Absolute event-time distance between two UTC datetimes, in microseconds."""
    delta = a - b if a >= b else b - a
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _midpoint(low: float, high: float) -> float:
    """Finite arithmetic mean ``(low + high) / 2`` of two ordered doubles.

    A direct ``(low + high) / 2`` overflows to Infinity for two large values
    of the same sign even though their midpoint is finite and representable
    (e.g. two ``1e308`` neighbours must average back to ``1e308``).  When the
    naive sum stays finite it is bit-for-bit identical to this path, so the
    fallback only changes inputs that previously produced Infinity; mixed-sign
    values sum without overflow, and summing the halves cannot either.
    """
    total = low + high
    if math.isfinite(total):
        return total / 2.0
    return low / 2.0 + high / 2.0


def _finite_median(values: list[float]) -> float:
    """Median that stays finite whenever the result is a representable double.

    Unlike ``statistics.median``, averaging the two middle values of an
    even-length list never passes through an intermediate overflow, so a
    median whose true value is finite is never returned as Infinity.
    """
    ordered = sorted(values)
    mid, remainder = divmod(len(ordered), 2)
    if remainder:
        return ordered[mid]
    return _midpoint(ordered[mid - 1], ordered[mid])


# ---------------------------------------------------------------------------
# spike computation
# ---------------------------------------------------------------------------

def local_spikes(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    source_id: Any,
    from_: Any,
    to: Any,
    window_seconds: Any,
    sensitivity: Any,
    value_key: Any = None,
) -> dict[str, Any]:
    """Return the local spikes among one source's published interval readings.

    Read-only: every value comes from one deferred read snapshot and no table
    is ever written.  Malformed parameters raise
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
    # The interval is inclusive [from, to]; a backwards interval is invalid but
    # a point interval [t, t] is legal (its single reading can never spike).
    if from_store > to_store:
        raise BadRequestError(
            f"'from' ({format_event_time(parse_event_time(from_store))}) must not"
            f" be later than 'to' ({format_event_time(parse_event_time(to_store))})"
        )
    window = _validate_positive(window_seconds, "window_seconds")
    coefficient = _validate_positive(sensitivity, "sensitivity")

    from_dt = parse_event_time(from_store)
    to_dt = parse_event_time(to_store)
    # Stored timestamps have microsecond resolution; the inclusive distance
    # test ``abs(dt_i - dt_j) <= window`` is evaluated against the exact
    # whole-microsecond floor of the requested window, so a sub-microsecond
    # fraction is never rounded up (a 0.0000015-second window reaches 1
    # microsecond, not 2).  ``window_seconds`` is a validated finite string
    # here, and an enormous window floors to an enormous int -- the pure
    # Python comparison below cannot overflow.
    window_micros = floor_seconds_to_micros(window_seconds)

    filter_sql = (
        "session_id = ? AND source_id = ? AND status = 'published'"
        " AND observed_at >= ? AND observed_at <= ?"
    )
    params = (session_id, source_id, from_store, to_store)

    with read_transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = set(json.loads(session["source_ids"]))
        if source_id not in allowed_sources:
            raise BadRequestError(
                f"unknown source_id {source_id!r} for this session;"
                f" declared sources: {sorted(allowed_sources)}"
            )

        readings_count = conn.execute(
            f"SELECT COUNT(*) AS c FROM readings WHERE {filter_sql}", params
        ).fetchone()["c"]
        if readings_count > MAX_READINGS:
            raise BadRequestError(
                f"interval contains {readings_count} published readings for"
                f" source {source_id!r}; at most {MAX_READINGS} are analysed per"
                " request -- narrow the 'from'/'to' interval"
            )

        rows = conn.execute(
            "SELECT observed_at, seq, payload FROM readings"
            f" WHERE {filter_sql} ORDER BY observed_at, seq",
            params,
        ).fetchall()

    # Extract valid finite numbers in deterministic (observed_at, seq) order.
    samples: list[dict[str, Any]] = []
    for row in rows:
        number = extract_numeric(row["payload"], value_key)
        if number is None:
            # Missing, non-finite, boolean or non-numeric: skip entirely.
            continue
        samples.append(
            {
                "observed_dt": parse_event_time(row["observed_at"]),
                "seq": row["seq"],
                "value": number,
            }
        )

    insufficient = 0
    spikes: list[dict[str, Any]] = []
    for i, sample in enumerate(samples):
        neighbours = [
            other["value"]
            for j, other in enumerate(samples)
            if j != i and _abs_micros(sample["observed_dt"], other["observed_dt"]) <= window_micros
        ]
        if len(neighbours) < 3:
            # Too little local context to judge this reading.
            insufficient += 1
            continue
        local_median = _finite_median(neighbours)
        mad = _finite_median([abs(v - local_median) for v in neighbours])
        deviation = abs(sample["value"] - local_median)
        # Strict inequality; with MAD == 0 this still requires a non-zero
        # deviation (coefficient * 0 == 0).
        if deviation > coefficient * mad:
            spikes.append(
                {
                    "key": {
                        "observed_at": format_event_time(sample["observed_dt"]),
                        "source_id": source_id,
                        "seq": sample["seq"],
                    },
                    "value": sample["value"],
                    "local_median": local_median,
                    "mad": mad,
                    "deviation": deviation,
                }
            )

    return {
        "session_id": session_id,
        "source_id": source_id,
        "from": format_event_time(from_dt),
        "to": format_event_time(to_dt),
        "window_seconds": window,
        "sensitivity": coefficient,
        "value_key": value_key,
        "readings_count": readings_count,
        "valid_count": len(samples),
        "insufficient_neighbors_count": insufficient,
        "spikes": spikes,
    }
