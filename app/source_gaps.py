"""Read-only per-source gap detection over published readings.

``GET /sessions/{session_id}/source-gaps?max_gap_seconds=N`` scans every
*published* reading of the session -- per source, in deterministic event
order ``(observed_at, seq)`` -- and reports each pair of *adjacent*
readings whose event-time distance is **strictly greater** than
``max_gap_seconds``.  The threshold is compared at its exact requested
value against the integer-microsecond stored timeline, so a fractional
microsecond is never rounded away (a 2-microsecond gap exceeds a
0.0000015-second threshold).  Buffered and quarantined readings never
participate (quarantined readings are not part of the published timeline;
buffered readings have not been published yet).

Filtering and ordering

* ``source_id`` restricts the scan to one declared source; an unknown
  source returns 400.
* ``from`` / ``to`` are **inclusive** bounds on ``observed_at``.  Adjacent
  pairs are formed among the readings inside the window, so a gap that
  straddles a boundary (one endpoint outside) is not reported, while a gap
  wholly inside is.  ``from`` later than ``to`` returns 400.
* Gaps are ordered globally by the keyset of their starting reading
  ``(start.observed_at, source_id, start.seq)``; ties inside one source
  break on ``seq``, ties across sources on ``source_id``.  Keyset
  pagination (opaque ``cursor`` / ``next_cursor``) keeps that order stable
  across pages and independent of the filters in force.

The whole scan -- filtered readings, paired gaps, total count and the
current page -- runs inside one deferred SQLite read transaction, i.e. a
single consistent snapshot.  Nothing is ever written: watermarks, alerts,
checkpoints, seals and the reading rows themselves are untouched.
"""
from __future__ import annotations

import json
import math
import sqlite3
from typing import Any, Optional

from .cursor import decode_cursor, encode_cursor
from .database import read_transaction
from .errors import BadRequestError
from .service import _load_session
from .time_utils import (
    floor_seconds_to_micros,
    format_event_time,
    parse_event_time,
    store_event_time,
)

__all__ = ["list_source_gaps", "DEFAULT_LIMIT", "MAX_LIMIT"]

DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

# SQLite integers are signed 64-bit; a threshold at or above this many
# microseconds can never be exceeded by a stored timestamp gap, so it is
# clamped here instead of overflowing the parameter binding.
_MAX_THRESHOLD_MICROS = 2**63 - 1

# Stored timestamps are fixed-width UTC strings "...HH:MM:SS.ffffffZ":
# the six microsecond digits start at position 20 (1-based 21).
_MICROS_EXPR = (
    "(CAST(strftime('%s', {col}) AS INTEGER) * 1000000"
    " + CAST(substr({col}, 21, 6) AS INTEGER))"
)


# ---------------------------------------------------------------------------
# request validation
# ---------------------------------------------------------------------------

def _optional_time(value: Any, field: str) -> Optional[str]:
    """Validate/normalise an optional inclusive event-time bound."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError(f"{field} must be a non-empty ISO 8601 string")
    # TimestampError is mapped to HTTP 400 by the application handler.
    return store_event_time(parse_event_time(value))


def _validate_max_gap(value: Any) -> tuple[float, int]:
    """Parse the required ``max_gap_seconds`` query parameter (400 on error).

    Query parameters arrive as strings; anything that is not a finite,
    non-negative number -- missing value, junk, NaN, Infinity, negative --
    is a bad request rather than a Pydantic 422.  Returns the seconds float
    (echoed in the response) together with the exact whole-microsecond
    floor of the requested threshold, clamped to SQLite's maximum integer.
    """
    if value is None:
        raise BadRequestError("max_gap_seconds is required")
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError("max_gap_seconds must be a non-negative number")
    try:
        seconds = float(value)
    except ValueError as exc:
        raise BadRequestError(
            f"max_gap_seconds must be a non-negative number, got {value!r}"
        ) from exc
    if not math.isfinite(seconds):
        raise BadRequestError("max_gap_seconds must be a finite number")
    if seconds < 0:
        raise BadRequestError(
            f"max_gap_seconds must be >= 0, got {seconds}"
        )
    return seconds, _threshold_micros(value)


def _validate_limit(value: Any) -> int:
    if value is None:
        return DEFAULT_LIMIT
    # Query params are strings; reject bools/non-integers/floats explicitly.
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError(f"limit must be an integer between 1 and {MAX_LIMIT}")
    text = value.strip()
    if isinstance(value, bool) or not text.lstrip("-").isdigit():
        raise BadRequestError(f"limit must be an integer between 1 and {MAX_LIMIT}")
    limit_value = int(text)
    if not 1 <= limit_value <= MAX_LIMIT:
        raise BadRequestError(f"limit must be between 1 and {MAX_LIMIT}")
    return limit_value


def _threshold_micros(seconds_text: str) -> int:
    """Convert the validated threshold text to integer storage microseconds.

    Stored timestamps have microsecond resolution, so the strict comparison
    ``gap_micros > floor(seconds)`` is exactly "adjacent event times more
    than ``max_gap_seconds`` apart": the requested seconds are floored to
    whole microseconds at their exact decimal value, never rounded (a
    threshold of ``0.0000015`` floors to 1 microsecond, so a 2-microsecond
    gap still exceeds it).  Enormous (but finite) inputs clamp to SQLite's
    maximum integer: such a threshold can never be exceeded by any stored
    gap, so the scan simply finds nothing instead of overflowing the
    parameter binding.
    """
    return min(floor_seconds_to_micros(seconds_text), _MAX_THRESHOLD_MICROS)


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

def _gap_query(filter_sql: str) -> str:
    """Build the adjacency/gap query around the caller's filter clause.

    ``filtered`` restricts published readings by session / source / time
    window; ``paired`` attaches each reading's per-source successor via a
    LEAD window partitioned on source and ordered by ``(observed_at, seq)``;
    ``gaps`` keeps only real pairs and computes their exact microsecond
    distance.  The outer statement appends the threshold/keyset predicates.
    """
    start_micros = _MICROS_EXPR.format(col="start_observed_at")
    end_micros = _MICROS_EXPR.format(col="end_observed_at")
    return f"""
WITH filtered AS (
    SELECT source_id, observed_at, seq
    FROM readings
    WHERE {filter_sql}
),
paired AS (
    SELECT source_id,
           observed_at AS start_observed_at,
           seq         AS start_seq,
           LEAD(observed_at) OVER w AS end_observed_at,
           LEAD(seq) OVER w        AS end_seq
    FROM filtered
    WINDOW w AS (PARTITION BY source_id ORDER BY observed_at, seq)
),
gaps AS (
    SELECT source_id,
           start_observed_at,
           start_seq,
           end_observed_at,
           end_seq,
           ({end_micros}) - ({start_micros}) AS gap_micros
    FROM paired
    WHERE end_observed_at IS NOT NULL
)
{{outer}}
"""


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------

def list_source_gaps(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    max_gap_seconds: Any,
    source_id: Any = None,
    from_: Any = None,
    to: Any = None,
    limit: Any = None,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    """Return adjacent published-reading pairs with a gap over the threshold.

    Read-only: every value comes from one deferred read snapshot and no
    table is ever written.  Malformed parameters raise
    :class:`~app.errors.BadRequestError` / :class:`TimestampError` (400);
    an unknown session raises 404; an undeclared ``source_id`` raises 400.
    """
    max_gap, threshold_micros = _validate_max_gap(max_gap_seconds)

    if source_id is not None and (
        not isinstance(source_id, str) or not source_id.strip()
    ):
        raise BadRequestError("source_id must be a non-empty string")

    from_store = _optional_time(from_, "from")
    to_store = _optional_time(to, "to")
    if from_store is not None and to_store is not None and from_store > to_store:
        raise BadRequestError(
            f"'from' ({format_event_time(parse_event_time(from_store))}) must not"
            f" be later than 'to' ({format_event_time(parse_event_time(to_store))})"
        )

    limit_value = _validate_limit(limit)

    cursor_keyset: Optional[tuple] = None
    if cursor is not None:
        cursor_keyset = decode_cursor(cursor, 3)
        ts, src, seq = cursor_keyset
        # The opaque cursor is only ever an encoded (timestamp, source, seq)
        # keyset; a structurally decodable payload with wrong value types is
        # still a malformed cursor (400), never something to compare against.
        if (
            not isinstance(ts, str)
            or not isinstance(src, str)
            or not isinstance(seq, int)
            or isinstance(seq, bool)
        ):
            raise BadRequestError("malformed cursor")

    filter_sql = "session_id = ? AND status = 'published'"
    filter_params: list[Any] = [session_id]
    if source_id is not None:
        filter_sql += " AND source_id = ?"
        filter_params.append(source_id)
    if from_store is not None:
        filter_sql += " AND observed_at >= ?"
        filter_params.append(from_store)
    if to_store is not None:
        filter_sql += " AND observed_at <= ?"
        filter_params.append(to_store)

    query_tmpl = _gap_query(filter_sql)

    page_where = "WHERE gap_micros > ?"
    page_params: list[Any] = [threshold_micros]
    if cursor_keyset is not None:
        ts, src, seq = cursor_keyset
        # Keyset on the gaps' global order: start (observed_at, source, seq).
        page_where += (
            " AND (start_observed_at > ?"
            " OR (start_observed_at = ? AND source_id > ?)"
            " OR (start_observed_at = ? AND source_id = ? AND start_seq > ?))"
        )
        page_params += [ts, ts, src, ts, src, seq]

    count_sql = query_tmpl.format(
        outer="SELECT COUNT(*) AS c FROM gaps WHERE gap_micros > ?"
    )
    page_sql = query_tmpl.format(
        outer=(
            "SELECT source_id, start_observed_at, start_seq,"
            " end_observed_at, end_seq, gap_micros FROM gaps"
            f" {page_where}"
            " ORDER BY start_observed_at, source_id, start_seq"
            " LIMIT ?"
        )
    )

    with read_transaction(conn):
        session = _load_session(conn, session_id)
        if source_id is not None:
            allowed_sources = set(json.loads(session["source_ids"]))
            if source_id not in allowed_sources:
                raise BadRequestError(
                    f"unknown source_id {source_id!r} for this session;"
                    f" declared sources: {sorted(allowed_sources)}"
                )

        matched = conn.execute(
            count_sql, (*filter_params, threshold_micros)
        ).fetchone()["c"]
        rows = conn.execute(
            page_sql, (*filter_params, *page_params, limit_value + 1)
        ).fetchall()

    has_more = len(rows) > limit_value
    page = rows[:limit_value]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = encode_cursor(
            (last["start_observed_at"], last["source_id"], last["start_seq"])
        )

    gaps = [
        {
            "source_id": r["source_id"],
            "start_key": {
                "observed_at": format_event_time(
                    parse_event_time(r["start_observed_at"])
                ),
                "source_id": r["source_id"],
                "seq": r["start_seq"],
            },
            "end_key": {
                "observed_at": format_event_time(
                    parse_event_time(r["end_observed_at"])
                ),
                "source_id": r["source_id"],
                "seq": r["end_seq"],
            },
            "gap_seconds": r["gap_micros"] / 1_000_000,
        }
        for r in page
    ]

    return {
        "session_id": session_id,
        "source_id": source_id,
        "from": (
            format_event_time(parse_event_time(from_store))
            if from_store is not None
            else None
        ),
        "to": (
            format_event_time(parse_event_time(to_store))
            if to_store is not None
            else None
        ),
        "max_gap_seconds": max_gap,
        "limit": limit_value,
        "matched": matched,
        "next_cursor": next_cursor,
        "gaps": gaps,
    }
