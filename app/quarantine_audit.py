"""Read-only impact rehearsal for quarantined readings.

``POST /sessions/{session_id}/quarantine-audit`` answers, for the session's
quarantined (late) readings: "if this reading were inserted into the
deterministically ordered published timeline by its event time, where would
it land, between which two published neighbours, and which is the earliest
consumer checkpoint whose resume window would gain it?"  Nothing is mutated.

The published timeline is ordered by the keyset
``(observed_at, source_id, seq)``; quarantined readings never share a key
with a published one (the primary key is ``(session_id, source_id, seq)``),
so every audit row has exactly one well-defined insertion slot:

* ``insert_position`` is the number of published readings with a strictly
  smaller keyset -- the zero-based offset the reading would occupy;
* ``prev_key`` / ``next_key`` are the keysets of the published readings
  immediately before / after that slot (``null`` at the head / tail, and
  both ``null`` on an empty published stream);
* ``gap_prev_seconds`` / ``gap_next_seconds`` are the event-time distances
  to those neighbours (``null`` when the neighbour is missing);
* ``earliest_affected_checkpoint`` names the earliest checkpoint (by its
  immutable captured keyset) whose resume would return the inserted reading,
  i.e. whose captured keyset is strictly smaller than the insertion keyset --
  an empty-stream checkpoint (null keyset) is always affected.

The whole audit -- quarantined rows, published neighbours, counts and
checkpoints -- is read inside one deferred SQLite read transaction, so the
entire answer derives from a single consistent snapshot and repeated calls
are idempotent.  Filtering on event time uses inclusive bounds
(``from <= observed_at <= to``) and the result order
``(observed_at, source_id, seq)`` is fully stable.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from .database import read_transaction
from .errors import BadRequestError
from .service import _load_session
from .time_utils import format_event_time, parse_event_time, store_event_time

__all__ = ["quarantine_audit", "DEFAULT_LIMIT", "MAX_LIMIT"]

DEFAULT_LIMIT = 100
MAX_LIMIT = 1000


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


def _validate_params(
    source_id: Any, from_: Any, to: Any, limit: Any
) -> tuple[Optional[str], Optional[str], Optional[str], int]:
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

    if limit is None:
        limit_value = DEFAULT_LIMIT
    else:
        # ``bool`` is a subclass of ``int``; never silently accept it as 0/1.
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise BadRequestError(
                f"limit must be an integer between 1 and {MAX_LIMIT}"
            )
        limit_value = limit
    if not 1 <= limit_value <= MAX_LIMIT:
        raise BadRequestError(f"limit must be between 1 and {MAX_LIMIT}")

    return source_id, from_store, to_store, limit_value


# ---------------------------------------------------------------------------
# serialisation helpers
# ---------------------------------------------------------------------------

def _key_out(observed_at: str, source_id: str, seq: int) -> dict[str, Any]:
    return {
        "observed_at": format_event_time(parse_event_time(observed_at)),
        "source_id": source_id,
        "seq": seq,
    }


def _keyset_less(observed_at: str, source_id: str, seq: int) -> tuple[str, list[Any]]:
    """SQL fragment: published keyset strictly smaller than the given keyset."""
    return (
        "(observed_at < ?"
        " OR (observed_at = ? AND source_id < ?)"
        " OR (observed_at = ? AND source_id = ? AND seq < ?))",
        [observed_at, observed_at, source_id, observed_at, source_id, seq],
    )


def _keyset_greater(observed_at: str, source_id: str, seq: int) -> tuple[str, list[Any]]:
    """SQL fragment: published keyset strictly greater than the given keyset."""
    return (
        "(observed_at > ?"
        " OR (observed_at = ? AND source_id > ?)"
        " OR (observed_at = ? AND source_id = ? AND seq > ?))",
        [observed_at, observed_at, source_id, observed_at, source_id, seq],
    )


def _published_neighbours(
    conn: sqlite3.Connection,
    session_id: str,
    observed_at: str,
    source_id: str,
    seq: int,
) -> tuple[Optional[sqlite3.Row], Optional[sqlite3.Row], int]:
    """The predecessor/successor published rows and the insertion position.

    All three are derived against the same snapshot: the insertion position
    is exactly the count of strictly-smaller published keysets, which is also
    the number of readings preceding the slot.
    """
    less_sql, less_params = _keyset_less(observed_at, source_id, seq)
    greater_sql, greater_params = _keyset_greater(observed_at, source_id, seq)

    position = conn.execute(
        f"SELECT COUNT(*) AS c FROM readings"
        f" WHERE session_id = ? AND status = 'published' AND {less_sql}",
        [session_id, *less_params],
    ).fetchone()["c"]
    prev = conn.execute(
        f"SELECT observed_at, source_id, seq FROM readings"
        f" WHERE session_id = ? AND status = 'published' AND {less_sql}"
        " ORDER BY observed_at DESC, source_id DESC, seq DESC LIMIT 1",
        [session_id, *less_params],
    ).fetchone()
    nxt = conn.execute(
        f"SELECT observed_at, source_id, seq FROM readings"
        f" WHERE session_id = ? AND status = 'published' AND {greater_sql}"
        " ORDER BY observed_at, source_id, seq LIMIT 1",
        [session_id, *greater_params],
    ).fetchone()
    return prev, nxt, position


def _earliest_affected_checkpoint(
    conn: sqlite3.Connection,
    session_id: str,
    observed_at: str,
    source_id: str,
    seq: int,
) -> Optional[str]:
    """Name of the earliest checkpoint whose resume would include the slot.

    A checkpoint resume returns every published reading whose keyset is
    strictly greater than the captured keyset, so a checkpoint is affected
    exactly when its captured keyset is strictly smaller than the insertion
    keyset -- including the null-keyset (empty-stream) checkpoint, whose
    resume starts at the very first reading.  Checkpoints at or after the
    slot never see it.  Among the affected ones the earliest by the
    immutable captured keyset is reported; the captured stream offset is
    not consulted because it shrinks with retention deletes and no longer
    orders checkpoints after a cleanup.
    """
    params_keyset = [observed_at, observed_at, source_id, observed_at, source_id, seq]
    # The checkpoints table stores its captured keyset in the last_* columns.
    cp_less_sql = (
        "(last_observed_at < ?"
        " OR (last_observed_at = ? AND last_source_id < ?)"
        " OR (last_observed_at = ? AND last_source_id = ? AND last_seq < ?))"
    )
    row = conn.execute(
        "SELECT name FROM checkpoints WHERE session_id = ?"
        f" AND (last_observed_at IS NULL OR {cp_less_sql})"
        # Null keyset (empty stream at capture) first, then the keyset
        # lexicographically; name only ties apart identical positions.
        " ORDER BY (last_observed_at IS NOT NULL) ASC,"
        " last_observed_at ASC,"
        " last_source_id ASC,"
        " last_seq ASC,"
        " name ASC"
        " LIMIT 1",
        [session_id, *params_keyset],
    ).fetchone()
    return row["name"] if row is not None else None


def _audit_item(
    conn: sqlite3.Connection,
    session_id: str,
    row: sqlite3.Row,
    stream_size: int,
) -> dict[str, Any]:
    observed_dt = parse_event_time(row["observed_at"])
    prev, nxt, position = _published_neighbours(
        conn, session_id, row["observed_at"], row["source_id"], row["seq"]
    )
    earliest_cp = _earliest_affected_checkpoint(
        conn, session_id, row["observed_at"], row["source_id"], row["seq"]
    )

    prev_dt = parse_event_time(prev["observed_at"]) if prev is not None else None
    next_dt = parse_event_time(nxt["observed_at"]) if nxt is not None else None
    return {
        "session_id": session_id,
        "source_id": row["source_id"],
        "seq": row["seq"],
        "observed_at": format_event_time(observed_dt),
        "value": json.loads(row["payload"]) if row["payload"] is not None else None,
        "quarantined_watermark": format_event_time(
            parse_event_time(row["quarantined_watermark"])
        ),
        "reason": "behind_published_watermark",
        "insert_position": position,
        "stream_size": stream_size,
        "prev_key": _key_out(prev["observed_at"], prev["source_id"], prev["seq"])
        if prev is not None
        else None,
        "next_key": _key_out(nxt["observed_at"], nxt["source_id"], nxt["seq"])
        if nxt is not None
        else None,
        "gap_prev_seconds": (
            (observed_dt - prev_dt).total_seconds() if prev_dt is not None else None
        ),
        "gap_next_seconds": (
            (next_dt - observed_dt).total_seconds() if next_dt is not None else None
        ),
        "earliest_affected_checkpoint": earliest_cp,
    }


# ---------------------------------------------------------------------------
# audit
# ---------------------------------------------------------------------------

def quarantine_audit(
    conn: sqlite3.Connection,
    session_id: str,
    *,
    source_id: Any = None,
    from_: Any = None,
    to: Any = None,
    limit: Any = None,
) -> dict[str, Any]:
    """Rehearse inserting the (optionally filtered) quarantined readings.

    Read-only: every value comes from one deferred read snapshot and no table
    is ever written.  Malformed parameters raise
    :class:`~app.errors.BadRequestError` / :class:`TimestampError` (400); an
    unknown session raises 404; an undeclared ``source_id`` raises 400.
    """
    source_id, from_store, to_store, limit_value = _validate_params(
        source_id, from_, to, limit
    )

    where = "session_id = ? AND status = 'quarantined'"
    params: list[Any] = [session_id]
    if source_id is not None:
        where += " AND source_id = ?"
        params.append(source_id)
    if from_store is not None:
        where += " AND observed_at >= ?"
        params.append(from_store)
    if to_store is not None:
        where += " AND observed_at <= ?"
        params.append(to_store)

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
            f"SELECT COUNT(*) AS c FROM readings WHERE {where}", params
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM readings WHERE {where}"
            " ORDER BY observed_at, source_id, seq LIMIT ?",
            (*params, limit_value + 1),
        ).fetchall()
        stream_size = conn.execute(
            "SELECT COUNT(*) AS c FROM readings"
            " WHERE session_id = ? AND status = 'published'",
            (session_id,),
        ).fetchone()["c"]

        has_more = len(rows) > limit_value
        page = rows[:limit_value]
        items = [_audit_item(conn, session_id, r, stream_size) for r in page]

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
        "limit": limit_value,
        "matched": matched,
        "returned": len(items),
        "has_more": has_more,
        "readings": items,
    }
