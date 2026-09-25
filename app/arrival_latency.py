"""Read-only per-source arrival-latency profiling over readings.

``GET /sessions/{session_id}/arrival-latency`` profiles, for every reading of
the session that matches the optional filters, the delay between its **event
time** (``observed_at``) and its **first arrival time** at the service
(``first_received_at``)::

    latency_seconds = first_received_at - observed_at

Every accepted reading participates regardless of its lifecycle status --
``buffered``, ``published`` and ``quarantined`` rows all have a real arrival
time -- and a reading whose clock was ahead of the server (arriving before its
own event time) keeps its **negative** latency exactly as computed.

Filtering

* ``source_id`` restricts the profile to one declared source; an unknown
  source returns 400.
* ``from`` / ``to`` are **inclusive** bounds on the reading's **event time**
  ``observed_at``.  ``from`` later than ``to`` returns 400.

Results are aggregated per source: sample count, minimum, maximum and the
arithmetic mean of the per-reading latency (``SUM / COUNT``, computed over
exact integer microseconds).  Sources without any matching reading do not
appear in the response, and the ``sources`` list is stably ordered by
``source_id``.

The whole computation -- session/source validation, filtering and the single
grouped aggregate query -- runs inside one deferred SQLite read transaction,
i.e. one consistent snapshot.  Nothing is ever written.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from .database import read_transaction
from .errors import BadRequestError
from .service import _load_session
from .time_utils import format_event_time, parse_event_time, store_event_time

__all__ = ["arrival_latency_profile"]

# Stored timestamps are fixed-width UTC strings "...HH:MM:SS.ffffffZ":
# the six microsecond digits start at position 20 (1-based 21).  The exact
# integer microsecond value keeps aggregation (min/max/sum/count) exact; only
# the final seconds values returned to the caller are divided.
_MICROS_EXPR = (
    "(CAST(strftime('%s', {col}) AS INTEGER) * 1000000"
    " + CAST(substr({col}, 21, 6) AS INTEGER))"
)


def _optional_time(value: Any, field: str) -> Optional[str]:
    """Validate/normalise an optional inclusive event-time bound."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError(f"{field} must be a non-empty ISO 8601 string")
    # TimestampError is mapped to HTTP 400 by the application handler.
    return store_event_time(parse_event_time(value))


def arrival_latency_profile(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    source_id: Any = None,
    from_: Any = None,
    to: Any = None,
) -> dict[str, Any]:
    """Return per-source arrival-latency statistics for matching readings.

    Read-only: every value comes from one deferred read snapshot and no table
    is ever written.  Malformed parameters raise
    :class:`~app.errors.BadRequestError` / :class:`TimestampError` (400);
    an unknown session raises 404; an undeclared ``source_id`` raises 400.
    """
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

    # The filter is on event time (observed_at); the delay is arrival time
    # minus event time.  The GROUP BY only yields rows for sources that have
    # at least one matching reading, so empty sources are absent by
    # construction; the explicit ORDER BY makes the output order stable.
    filter_sql = "session_id = ?"
    params: list[Any] = [session_id]
    if source_id is not None:
        filter_sql += " AND source_id = ?"
        params.append(source_id)
    if from_store is not None:
        filter_sql += " AND observed_at >= ?"
        params.append(from_store)
    if to_store is not None:
        filter_sql += " AND observed_at <= ?"
        params.append(to_store)

    observed_micros = _MICROS_EXPR.format(col="observed_at")
    received_micros = _MICROS_EXPR.format(col="first_received_at")

    sql = f"""
SELECT source_id,
       COUNT(*)                          AS sample_count,
       MIN(({received_micros}) - ({observed_micros})) AS min_latency_micros,
       MAX(({received_micros}) - ({observed_micros})) AS max_latency_micros,
       AVG(({received_micros}) - ({observed_micros})) AS avg_latency_micros
FROM readings
WHERE {filter_sql}
GROUP BY source_id
ORDER BY source_id
"""

    with read_transaction(conn):
        session = _load_session(conn, session_id)
        if source_id is not None:
            allowed_sources = set(json.loads(session["source_ids"]))
            if source_id not in allowed_sources:
                raise BadRequestError(
                    f"unknown source_id {source_id!r} for this session;"
                    f" declared sources: {sorted(allowed_sources)}"
                )
        rows = conn.execute(sql, params).fetchall()

    sources: list[dict[str, Any]] = []
    total_samples = 0
    for row in rows:
        sample_count = row["sample_count"]
        total_samples += sample_count
        sources.append(
            {
                "source_id": row["source_id"],
                "sample_count": sample_count,
                # Negative delays (a clock ahead of the arrival wall clock)
                # are returned unchanged: no abs(), no clamping.
                "min_latency_seconds": row["min_latency_micros"] / 1_000_000,
                "max_latency_seconds": row["max_latency_micros"] / 1_000_000,
                # SQLite AVG is sum/count over integer microseconds, i.e. the
                # arithmetic mean of the per-reading delays.
                "avg_latency_seconds": row["avg_latency_micros"] / 1_000_000,
            }
        )

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
        "sample_count": total_samples,
        "sources": sources,
    }
