"""Watermark timeline merge business logic.

Guarantees implemented here:

* A session fixes its source list and ``allowed_lateness_seconds`` at
  creation; reports from unknown sources are rejected.
* The watermark is defined only once *every* source has reported, as
  ``min(source max observed_at) - allowed_lateness``. It is monotonic.
* Buffered readings with ``observed_at`` strictly before the watermark are
  published exactly once, in the deterministic order
  ``(observed_at, source_id, seq)``.
* A reading arriving with ``observed_at`` strictly before the already
  published watermark can never be (re)published: it is diverted to the
  quarantine table and never mutates history.
* Re-posting the same ``(source_id, seq)`` with the same payload is an
  idempotent no-op; a different payload is a 409 conflict.
* A reading deleted by retention leaves a tombstone (key + canonical-payload
  digest) written in the delete's own transaction, so the dedup rule above
  keeps holding for cleaned-up keys: the same payload stays an idempotent
  duplicate that restores nothing, moves no watermark and re-triggers no
  alert; a different payload is a 409 conflict.
* A source may be sealed (its finite collection ended) with a
  ``final_observed_at`` no earlier than the largest event time already seen
  for it. Sealing the same value again is an idempotent no-op; sealing a
  different value is a 409 conflict. A sealed source accepts no new readings
  (an exact repeat of an existing reading is still counted as a duplicate),
  while its declared final event time advances the watermark exactly as a
  regular maximum-time report would.
* When the last source of a session is sealed, that same write transaction
  publishes every remaining buffered reading under a terminal watermark,
  advances both kinds of alert rules in deterministic merge order, and flips
  the session to ``closed``; afterwards every previously-unseen reading key
  is rejected.
* Every actual publish (watermark advance or terminal tail flush) increments
  the session's persistent, monotonically increasing ``published_seq``
  marker inside the same transaction.  Checkpoints snapshot the marker at
  creation, so checkpoint idempotency survives retention cleanup of the
  published rows; retention never writes the marker.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any, Optional

from .cursor import decode_cursor as _decode_cursor
from .cursor import encode_cursor as _encode_cursor
from .database import read_transaction, transaction, utc_now
from .errors import BadRequestError, ConflictError, NotFoundError, ServiceError
from .time_utils import format_event_time, parse_event_time, store_event_time

# One microsecond beyond the latest buffered event time is enough for the
# terminal watermark: stored timestamps have microsecond resolution, so every
# remaining buffered reading is then strictly behind it.
_TERMINAL_EPSILON = timedelta(microseconds=1)

# Re-exported so existing imports (``app.service.ServiceError`` etc.) keep
# working; the canonical definitions live in :mod:`app.errors`.
__all__ = [
    "ServiceError",
    "NotFoundError",
    "BadRequestError",
    "ConflictError",
    "canonical_payload",
    "payload_digest",
    "create_session",
    "get_session",
    "seal_source",
    "ingest_batch",
    "list_published",
    "list_quarantined",
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def canonical_payload(value: Any) -> str:
    """Deterministic JSON serialisation so idempotency is payload-based.

    Raises TypeError for values that cannot be represented as JSON; API
    input already went through Pydantic so this stays strict (no silent
    ``str()`` coercion).
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_digest(payload: str) -> str:
    """Stable digest of a canonical payload, stored in retention tombstones.

    Retention deletes the readings row but must remember which value each
    deleted key carried, so a later retry can be told apart from a
    conflicting re-report.  The digest of the canonical JSON form suffices:
    identical payloads digest identically, different ones (practically)
    never collide.
    """
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise NotFoundError(f"session {session_id!r} not found")
    return row


def _source_rows(conn: sqlite3.Connection, session_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM session_sources WHERE session_id = ? ORDER BY rowid",
        (session_id,),
    ).fetchall()


def _source_maxes_from(rows: list[sqlite3.Row]) -> dict[str, Optional[str]]:
    """Effective per-source event-time clocks.

    A sealed source's clock is its declared ``final_observed_at`` (which is
    always >= the largest observed event time), so sealing preserves the
    watermark semantics: a sealed source is simply a source whose maximum is
    known for certain.
    """
    return {r["source_id"]: r["final_observed_at"] or r["max_observed_at"] for r in rows}


def _source_maxes(conn: sqlite3.Connection, session_id: str) -> dict[str, Optional[str]]:
    return _source_maxes_from(_source_rows(conn, session_id))


def _compute_watermark(
    source_max: dict[str, Optional[str]], allowed_lateness_seconds: float
) -> Optional[str]:
    """Slowest-source max event time minus allowed lateness.

    Returns None while at least one source has neither reported nor been
    sealed: before that the slowest source's event-time clock is unknown, so
    no reading may be released.
    """
    if not source_max or any(v is None for v in source_max.values()):
        return None
    slowest = parse_event_time(min(source_max.values()))  # type: ignore[arg-type]
    return store_event_time(slowest - timedelta(seconds=allowed_lateness_seconds))


def _publish_under_watermark(
    conn: sqlite3.Connection,
    session_id: str,
    new_wm: str,
    triggered_at: str,
) -> int:
    """Publish every buffered reading strictly before ``new_wm``.

    Readings are first flipped to ``published`` in the deterministic merge
    order; the alert state machines (both kinds) are then advanced over them
    in that same order inside the caller's serialised write transaction.
    Returns the number of newly published readings.
    """
    cur = conn.execute(
        "UPDATE readings SET status = 'published', published_watermark = ?"
        " WHERE session_id = ? AND status = 'buffered' AND observed_at < ?",
        (new_wm, session_id, new_wm),
    )
    published_now = cur.rowcount
    conn.execute(
        "UPDATE sessions SET published_watermark = ? WHERE session_id = ?",
        (new_wm, session_id),
    )
    if published_now:
        # Advance the session's monotonic publish marker in the same
        # transaction as the publish itself.  Checkpoints snapshot this
        # marker at creation and compare it on same-name retries; it only
        # ever grows, so retention deleting the published rows later can
        # never make an unchanged stream look advanced (or vice versa).
        conn.execute(
            "UPDATE sessions SET published_seq = published_seq + ?"
            " WHERE session_id = ?",
            (published_now, session_id),
        )
        # Deterministically-published readings are the *only* input to the
        # per-source alert state machines; buffered readings that merely
        # became published now drive it, in timeline order, inside the same
        # serialised write transaction.
        from .alerts import advance_alerts
        from .correlated import advance_correlated

        advance_alerts(conn, session_id, new_wm, triggered_at)
        # Cross-source rules consume the global merge order (every session
        # source advances event time and can expire a window).
        advance_correlated(conn, session_id, new_wm, triggered_at)
    return published_now


def _reading_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "seq": row["seq"],
        "observed_at": format_event_time(parse_event_time(row["observed_at"])),
        "value": json.loads(row["payload"]) if row["payload"] is not None else None,
        "published_watermark": format_event_time(parse_event_time(row["published_watermark"])),
    }


def _quarantine_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "seq": row["seq"],
        "observed_at": format_event_time(parse_event_time(row["observed_at"])),
        "value": json.loads(row["payload"]) if row["payload"] is not None else None,
        "quarantined_watermark": format_event_time(
            parse_event_time(row["quarantined_watermark"])
        ),
        "reason": "behind_published_watermark",
    }


# ---------------------------------------------------------------------------
# sessions
# ---------------------------------------------------------------------------

def create_session(
    conn: sqlite3.Connection,
    source_ids: list[str],
    allowed_lateness_seconds: float,
    session_id: Optional[str] = None,
) -> dict[str, Any]:
    sid = session_id or uuid.uuid4().hex
    created_at = store_event_time(utc_now())
    sources = json.dumps(source_ids)
    try:
        with transaction(conn):
            conn.execute(
                "INSERT INTO sessions (session_id, source_ids, allowed_lateness_seconds,"
                " published_watermark, created_at) VALUES (?, ?, ?, NULL, ?)",
                (sid, sources, allowed_lateness_seconds, created_at),
            )
            conn.executemany(
                "INSERT INTO session_sources (session_id, source_id, max_observed_at,"
                " first_seen_at) VALUES (?, ?, NULL, ?)",
                [(sid, s, created_at) for s in source_ids],
            )
    except sqlite3.IntegrityError as exc:
        raise ConflictError(f"session {sid!r} already exists") from exc
    return {
        "session_id": sid,
        "source_ids": source_ids,
        "allowed_lateness_seconds": allowed_lateness_seconds,
        "created_at": format_event_time(parse_event_time(created_at)),
    }


def _session_state(
    session: sqlite3.Row,
    rows: list[sqlite3.Row],
) -> dict[str, Any]:
    """Serialise a session row plus its per-source seal state."""
    source_max = _source_maxes_from(rows)
    all_reported = bool(source_max) and all(v is not None for v in source_max.values())
    effective = (
        _compute_watermark(source_max, session["allowed_lateness_seconds"])
        if all_reported
        else None
    )
    sources = [
        {
            "source_id": r["source_id"],
            "max_observed_at": (
                format_event_time(parse_event_time(r["max_observed_at"]))
                if r["max_observed_at"]
                else None
            ),
            "final_observed_at": (
                format_event_time(parse_event_time(r["final_observed_at"]))
                if r["final_observed_at"]
                else None
            ),
            "sealed_at": (
                format_event_time(parse_event_time(r["sealed_at"]))
                if r["sealed_at"]
                else None
            ),
            "sealed": r["final_observed_at"] is not None,
        }
        for r in rows
    ]
    status = session["status"]
    closed_at = session["closed_at"]
    return {
        "session_id": session["session_id"],
        "source_ids": json.loads(session["source_ids"]),
        "allowed_lateness_seconds": session["allowed_lateness_seconds"],
        "published_watermark": (
            format_event_time(parse_event_time(session["published_watermark"]))
            if session["published_watermark"]
            else None
        ),
        "all_sources_reported": all_reported,
        "effective_watermark": format_event_time(parse_event_time(effective)) if effective else None,
        "created_at": format_event_time(parse_event_time(session["created_at"])),
        "status": status,
        "closed": status == "closed",
        "closed_at": (
            format_event_time(parse_event_time(closed_at)) if closed_at else None
        ),
        "sources": sources,
    }


def get_session(conn: sqlite3.Connection, session_id: str) -> dict[str, Any]:
    with read_transaction(conn):
        session = _load_session(conn, session_id)
        rows = _source_rows(conn, session_id)
        return _session_state(session, rows)


# ---------------------------------------------------------------------------
# source sealing (finite collection end)
# ---------------------------------------------------------------------------

def seal_source(
    conn: sqlite3.Connection,
    session_id: str,
    source_id: str,
    final_observed_at: str,
) -> dict[str, Any]:
    """Seal one source of a session at its final observed event time.

    ``final_observed_at`` must not be earlier than the largest event time
    already seen for the source.  Repeating the seal with the same value is
    idempotent; a different value conflicts with 409.  Sealing the last source
    performs the tail flush (all remaining buffered readings published under a
    terminal watermark, both alert kinds advanced) and closes the session, all
    inside this single serialised write transaction.
    """
    final_store = store_event_time(parse_event_time(final_observed_at))
    sealed_at = store_event_time(utc_now())

    with transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = json.loads(session["source_ids"])
        if source_id not in allowed_sources:
            raise BadRequestError(
                f"unknown source_id {source_id!r} for this session;"
                f" declared sources: {sorted(allowed_sources)}"
            )

        source = conn.execute(
            "SELECT * FROM session_sources WHERE session_id = ? AND source_id = ?",
            (session_id, source_id),
        ).fetchone()

        existing_final = source["final_observed_at"]
        if existing_final is not None:
            if existing_final != final_store:
                # Retrying with the same final time is idempotent; a different
                # one would rewrite the sealed stream's end, so it conflicts.
                raise ConflictError(
                    f"source {source_id!r} is already sealed with"
                    f" final_observed_at={existing_final!r};"
                    f" got {final_store!r}"
                )
            # Same value: idempotent retry, no side effects whatsoever.
            rows = _source_rows(conn, session_id)
            return _session_state(session, rows)

        if session["status"] == "closed":
            # Defensive: a closed session has every source sealed already.
            raise ConflictError(f"session {session_id!r} is closed")

        max_seen = source["max_observed_at"]
        if max_seen is not None and final_store < max_seen:
            raise BadRequestError(
                f"final_observed_at {final_observed_at!r} is earlier than the"
                f" largest event time already seen for source {source_id!r}"
                f" ({format_event_time(parse_event_time(max_seen))})"
            )

        # The declared final event time becomes the source's effective clock
        # for watermark purposes (identical to a maximum-time report).
        conn.execute(
            "UPDATE session_sources SET final_observed_at = ?, sealed_at = ?"
            " WHERE session_id = ? AND source_id = ?",
            (final_store, sealed_at, session_id, source_id),
        )

        rows = _source_rows(conn, session_id)
        source_max = _source_maxes_from(rows)
        # The collection ends only when every source has actually been sealed;
        # a source that merely reported once is still open even though its
        # event-time clock is already known.
        all_sealed = all(r["final_observed_at"] is not None for r in rows)
        lateness = session["allowed_lateness_seconds"]

        if not all_sealed:
            # Other sources are still open: keep the ordinary watermark
            # semantics, simply driven by the sealed source's final time.
            all_known = all(v is not None for v in source_max.values())
            new_wm = _compute_watermark(source_max, lateness) if all_known else None
            published_wm = session["published_watermark"]
            if new_wm is not None and (published_wm is None or new_wm > published_wm):
                _publish_under_watermark(conn, session_id, new_wm, sealed_at)
        else:
            # The last source is sealed: the finite collection is complete.
            # Drain every remaining buffered reading in the established global
            # order under one terminal watermark, advance both alert kinds, and
            # close the session -- atomically, in this same transaction.
            terminal_wm = _compute_watermark(source_max, lateness)
            latest_buffered = conn.execute(
                "SELECT MAX(observed_at) AS m FROM readings"
                " WHERE session_id = ? AND status = 'buffered'",
                (session_id,),
            ).fetchone()["m"]
            if latest_buffered is not None:
                drain_floor = store_event_time(
                    parse_event_time(latest_buffered) + _TERMINAL_EPSILON
                )
                if terminal_wm is None or drain_floor > terminal_wm:
                    terminal_wm = drain_floor
            if terminal_wm is not None:
                published_wm = session["published_watermark"]
                if published_wm is None or terminal_wm > published_wm:
                    _publish_under_watermark(conn, session_id, terminal_wm, sealed_at)
            conn.execute(
                "UPDATE sessions SET status = 'closed', closed_at = ?"
                " WHERE session_id = ?",
                (sealed_at, session_id),
            )

        session = _load_session(conn, session_id)
        rows = _source_rows(conn, session_id)
        return _session_state(session, rows)


# ---------------------------------------------------------------------------
# ingestion
# ---------------------------------------------------------------------------

def ingest_batch(
    conn: sqlite3.Connection, session_id: str, readings: list[dict[str, Any]]
) -> dict[str, Any]:
    # Validate/normalise the whole batch up front: bad timestamps or
    # undumpable values reject the batch atomically (unknown sources are
    # checked against the session inside the transaction).
    parsed_times: dict[int, str] = {}
    payloads: dict[int, str] = {}
    for i, r in enumerate(readings):
        observed = parse_event_time(r["observed_at"])
        parsed_times[i] = store_event_time(observed)
        payloads[i] = canonical_payload(r["value"])

    with transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = set(json.loads(session["source_ids"]))
        unknown = {r["source_id"] for r in readings} - allowed_sources
        if unknown:
            raise BadRequestError(
                f"unknown source_id(s) for this session: {sorted(unknown)}"
            )

        # A sealed source accepts only exact retries of readings already
        # stored; a closed session accepts no new reading keys at all.  These
        # checks happen in the same serialised write transaction as the
        # inserts below (a failure rolls the whole batch back), so a
        # concurrent seal can never sneak a new reading through.
        source_rows = {
            r["source_id"]: r for r in _source_rows(conn, session_id)
        }
        session_closed = session["status"] == "closed"

        received_at = store_event_time(utc_now())
        lateness = session["allowed_lateness_seconds"]
        published_wm: Optional[str] = session["published_watermark"]

        duplicates = 0
        quarantined = 0

        for i, r in enumerate(readings):
            key = (session_id, r["source_id"], r["seq"])
            observed_store = parsed_times[i]
            payload = payloads[i]

            existing = conn.execute(
                "SELECT status, payload FROM readings"
                " WHERE session_id = ? AND source_id = ? AND seq = ?",
                key,
            ).fetchone()
            if existing is not None:
                if existing["payload"] != payload:
                    # Same key, different measurement: never silently overwrite.
                    raise ConflictError(
                        f"reading ({r['source_id']}, seq={r['seq']}) already exists"
                        " with a different payload"
                    )
                # An exact retry stays an idempotent duplicate even after the
                # source has been sealed (or the whole session closed).
                duplicates += 1
                continue

            # A key whose reading was deleted by retention keeps its dedup
            # identity via the tombstone written in the delete's transaction.
            tombstone = conn.execute(
                "SELECT payload_digest FROM retention_tombstones"
                " WHERE session_id = ? AND source_id = ? AND seq = ?",
                key,
            ).fetchone()
            if tombstone is not None:
                if tombstone["payload_digest"] != payload_digest(payload):
                    # Same key, different measurement than the deleted row.
                    raise ConflictError(
                        f"reading ({r['source_id']}, seq={r['seq']}) was deleted"
                        " by retention with a different payload"
                    )
                # Exact retry of a cleaned-up reading: idempotent duplicate.
                # The reading is not restored, the watermark does not move
                # and no alert is re-triggered -- nothing is written, even
                # when the source is sealed or the session closed.
                duplicates += 1
                continue

            # Previously unseen key: it is admitted only while both the
            # session and this particular source are still open.
            if session_closed:
                raise ConflictError(
                    f"session {session_id!r} is closed: new readings are no longer"
                    f" accepted (source {r['source_id']!r}, seq={r['seq']})"
                )
            if source_rows[r["source_id"]]["final_observed_at"] is not None:
                raise ConflictError(
                    f"source {r['source_id']!r} is sealed: new readings are no"
                    f" longer accepted (seq={r['seq']})"
                )
            if published_wm is not None and observed_store < published_wm:
                # Too late: the timeline up to the watermark is immutable.
                conn.execute(
                    "INSERT INTO readings (session_id, source_id, seq, observed_at,"
                    " payload, status, first_received_at, published_watermark,"
                    " quarantined_watermark)"
                    " VALUES (?, ?, ?, ?, ?, 'quarantined', ?, NULL, ?)",
                    (*key, observed_store, payload, received_at, published_wm),
                )
                quarantined += 1
                continue

            conn.execute(
                "INSERT INTO readings (session_id, source_id, seq, observed_at,"
                " payload, status, first_received_at, published_watermark,"
                " quarantined_watermark)"
                " VALUES (?, ?, ?, ?, ?, 'buffered', ?, NULL, NULL)",
                (*key, observed_store, payload, received_at),
            )
            # Advance this source's event-time clock.
            conn.execute(
                "UPDATE session_sources"
                " SET max_observed_at ="
                " CASE WHEN max_observed_at IS NULL OR ? > max_observed_at THEN ?"
                " ELSE max_observed_at END"
                " WHERE session_id = ? AND source_id = ?",
                (observed_store, observed_store, session_id, r["source_id"]),
            )

        # Recompute the watermark from the slowest source (a sealed source
        # contributes its declared final event time).
        source_max = _source_maxes(conn, session_id)
        all_reported = bool(source_max) and all(v is not None for v in source_max.values())
        new_wm = _compute_watermark(source_max, lateness) if all_reported else None
        published_now = 0
        if new_wm is not None and (published_wm is None or new_wm > published_wm):
            published_now = _publish_under_watermark(conn, session_id, new_wm, received_at)
            published_wm = new_wm

        published_total = conn.execute(
            "SELECT COUNT(*) AS c FROM readings WHERE session_id = ? AND status = 'published'",
            (session_id,),
        ).fetchone()["c"]
        buffered = conn.execute(
            "SELECT COUNT(*) AS c FROM readings WHERE session_id = ? AND status = 'buffered'",
            (session_id,),
        ).fetchone()["c"]

    return {
        "session_id": session_id,
        "received": len(readings),
        "duplicates": duplicates,
        "quarantined": quarantined,
        "published_now": published_now,
        "published_total": published_total,
        "buffered": buffered,
        "all_sources_reported": all_reported,
        "published_watermark": (
            format_event_time(parse_event_time(published_wm)) if published_wm else None
        ),
    }


# ---------------------------------------------------------------------------
# reads
# ---------------------------------------------------------------------------

def list_published(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ? AND status = 'published'"
    params: list[Any] = [session_id]
    if cursor:
        ts, src, seq = _decode_cursor(cursor, 3)
        # Keyset on the deterministic timeline order
        # (observed_at, source_id, seq).
        where += (
            " AND (observed_at > ?"
            " OR (observed_at = ? AND source_id > ?)"
            " OR (observed_at = ? AND source_id = ? AND seq > ?))"
        )
        params += [ts, ts, src, ts, src, seq]

    with read_transaction(conn):
        session = _load_session(conn, session_id)
        rows = conn.execute(
            f"SELECT * FROM readings WHERE {where}"
            " ORDER BY observed_at, source_id, seq LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = _encode_cursor((last["observed_at"], last["source_id"], last["seq"]))

    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "published_watermark": (
            format_event_time(parse_event_time(session["published_watermark"]))
            if session["published_watermark"]
            else None
        ),
        "readings": [_reading_out(r) for r in page],
    }


def list_quarantined(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ? AND status = 'quarantined'"
    params: list[Any] = [session_id]
    if cursor:
        received, src, seq = _decode_cursor(cursor, 3)
        where += (
            " AND (first_received_at > ?"
            " OR (first_received_at = ? AND source_id > ?)"
            " OR (first_received_at = ? AND source_id = ? AND seq > ?))"
        )
        params += [received, received, src, received, src, seq]

    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            f"SELECT * FROM readings WHERE {where}"
            " ORDER BY first_received_at, source_id, seq LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = None
    if has_more and page:
        last = page[-1]
        next_cursor = _encode_cursor(
            (last["first_received_at"], last["source_id"], last["seq"])
        )

    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "readings": [_quarantine_out(r) for r in page],
    }
