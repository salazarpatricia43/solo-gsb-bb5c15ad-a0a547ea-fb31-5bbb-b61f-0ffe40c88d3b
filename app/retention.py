"""Checkpoint-safe storage retention for old readings.

``POST /sessions/{session_id}/retention`` reports (``dry_run=true``) or
performs (``dry_run=false``) the deletion of readings whose **event time** is
strictly earlier than the caller-supplied ``cutoff``.  Only rows that are
already terminal participate -- ``published`` and ``quarantined``; buffered
readings are never touched (they may still be published and drive the
watermark / alert state machines).

Quarantined rows carry no stream semantics beyond their own listing, so every
quarantined candidate is deletable.  Published rows, however, may still be
(re)read by consumers resuming from a checkpoint: a checkpoint resume returns
every published reading whose deterministic-order keyset
``(observed_at, source_id, seq)`` is *strictly greater* than the keyset the
checkpoint captured.  It is therefore enough to keep everything that the
session's **earliest** checkpoint may still return: a published reading whose
keyset is greater than that checkpoint's keyset is retained, while the
checkpoint boundary itself (the last reading it already covered) and every
reading before it can never be returned by any resume and is deletable.

The earliest protector is chosen by the **immutable captured keyset only**
``(last_observed_at, last_source_id, last_seq)``; a null keyset (checkpoint
captured on an empty stream) sorts before every other one.  ``stream_offset``
must never participate in the choice: it is merely the number of published
rows present when the checkpoint was created, and once retention removes the
rows an older checkpoint already covered, a checkpoint created afterwards on
the re-grown stream can carry a *smaller* offset despite its keyset being
later -- choosing it would delete readings the older checkpoint must still
resume into.  The keyset, frozen in the checkpoint row at creation, identifies
the true resume position regardless of intervening deletes.

When the session has no checkpoint at all no resume can be served, so no
published reading is protected.  A checkpoint captured on an empty stream
(``stream_offset = 0``, null keyset) protects *every* published reading:
resuming from it starts at the beginning.

Dry runs derive all counts in one deferred read snapshot and write nothing.
Confirmation executes the candidate deletes in a single serialised
``BEGIN IMMEDIATE`` write transaction, so the report and the delete are one
atomic unit: any failure rolls the transaction back unchanged.  Every row
actually deleted leaves a **tombstone** in ``retention_tombstones`` (key plus
canonical-payload digest) inside that same transaction, so a later retry of
the same key keeps its dedup identity instead of looking like a brand-new
reading.  Retention never writes to ``sessions``, ``session_sources``, the
alert tables or ``checkpoints`` -- watermarks, alerts and checkpoint
semantics are unchanged.  Repeating the same request is idempotent: once the
candidates are gone the second call simply reports zero deletable rows.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from .database import read_transaction, transaction, utc_now
from .service import _load_session, payload_digest
from .time_utils import format_event_time, parse_event_time, store_event_time

__all__ = ["apply_retention"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _earliest_checkpoint(
    conn: sqlite3.Connection, session_id: str
) -> Optional[sqlite3.Row]:
    """The checkpoint whose resume window reaches furthest back in the stream.

    Ordering is by the immutable keyset captured at creation
    ``(last_observed_at, last_source_id, last_seq)``; ``name`` only breaks
    the tie between distinct checkpoints taken at the exact same position.
    The null keyset (empty stream at creation) sorts first and therefore
    always wins.  ``stream_offset`` is deliberately absent: it counts rows
    extant at creation and shrinks as retention removes covered readings, so
    it no longer orders checkpoints chronologically after the first cleanup.
    Retaining everything this checkpoint can still read covers every later
    one.
    """
    return conn.execute(
        "SELECT * FROM checkpoints WHERE session_id = ?"
        " ORDER BY (last_observed_at IS NOT NULL) ASC,"
        " last_observed_at ASC,"
        " last_source_id ASC,"
        " last_seq ASC,"
        " name ASC"
        " LIMIT 1",
        (session_id,),
    ).fetchone()


def _checkpoint_protection(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "checkpoint_name": row["name"],
        "stream_offset": row["stream_offset"],
        "last_observed_at": (
            format_event_time(parse_event_time(row["last_observed_at"]))
            if row["last_observed_at"]
            else None
        ),
        "last_source_id": row["last_source_id"],
        "last_seq": row["last_seq"],
    }


def _count(conn: sqlite3.Connection, where: str, *params: Any) -> int:
    return conn.execute(
        f"SELECT COUNT(*) AS c FROM readings WHERE {where}", params
    ).fetchone()["c"]


def _record_tombstones(
    conn: sqlite3.Connection, session_id: str, rows: list[sqlite3.Row]
) -> None:
    """Persist the dedup identity of every row this transaction deletes.

    Runs inside the delete's own serialised transaction, so a tombstone
    exists exactly when its reading is gone: a crash rolls both back
    together.  A later retry of the same key is matched against the stored
    payload digest (same payload -> idempotent duplicate, different -> 409)
    instead of being admitted as a brand-new reading.
    """
    if not rows:
        return
    deleted_at = store_event_time(utc_now())
    conn.executemany(
        "INSERT OR REPLACE INTO retention_tombstones"
        " (session_id, source_id, seq, payload_digest, deleted_at)"
        " VALUES (?, ?, ?, ?, ?)",
        [
            (
                session_id,
                row["source_id"],
                row["seq"],
                payload_digest(row["payload"]),
                deleted_at,
            )
            for row in rows
        ],
    )


# ---------------------------------------------------------------------------
# retention report / delete
# ---------------------------------------------------------------------------

def apply_retention(
    conn: sqlite3.Connection,
    session_id: str,
    cutoff: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Report or atomically delete pre-cutoff terminal readings.

    Returns the retention report (same shape for dry runs and executions;
    ``deleted`` is 0 for dry runs and the number of rows removed otherwise).
    Invalid ``cutoff`` raises :class:`~app.time_utils.TimestampError` (400);
    an unknown session raises 404.
    """
    cutoff_store = store_event_time(parse_event_time(cutoff))

    if dry_run:
        with read_transaction(conn):
            _load_session(conn, session_id)
            earliest = _earliest_checkpoint(conn, session_id)
            counts = _compute_counts(conn, session_id, cutoff_store, earliest)
            protection = _checkpoint_protection(earliest) if earliest else None
        return _result(session_id, cutoff_store, dry_run, protection, counts,
                       deleted=0)

    deleted_quarantined = 0
    deleted_published = 0
    with transaction(conn):
        _load_session(conn, session_id)
        earliest = _earliest_checkpoint(conn, session_id)
        counts = _compute_counts(conn, session_id, cutoff_store, earliest)

        # The doomed rows are selected with the exact delete predicates first,
        # so each one can leave its tombstone in this same transaction.
        doomed: list[sqlite3.Row] = []

        # Every quarantined candidate is unreachable from the published stream
        # and can be removed outright.
        quarantined_where = (
            "session_id = ? AND status = 'quarantined' AND observed_at < ?"
        )
        doomed += conn.execute(
            f"SELECT source_id, seq, payload FROM readings WHERE {quarantined_where}",
            (session_id, cutoff_store),
        ).fetchall()
        cur = conn.execute(
            f"DELETE FROM readings WHERE {quarantined_where}",
            (session_id, cutoff_store),
        )
        deleted_quarantined = cur.rowcount

        # Published candidates: keep precisely the rows the earliest
        # checkpoint can still return on resume (keyset strictly greater than
        # the captured keyset).  The boundary keyset itself is already covered
        # by the checkpoint, so it is deleted like everything before it.
        if earliest is None:
            published_where = (
                "session_id = ? AND status = 'published' AND observed_at < ?"
            )
            published_params: tuple[Any, ...] = (session_id, cutoff_store)
        elif earliest["last_observed_at"] is None:
            # Empty-stream checkpoint: its resume starts at the very first
            # reading, so every published row is protected.
            published_where = ""
            published_params = ()
        else:
            b_ts = earliest["last_observed_at"]
            b_src = earliest["last_source_id"]
            b_seq = earliest["last_seq"]
            published_where = (
                "session_id = ? AND status = 'published' AND observed_at < ?"
                " AND NOT (observed_at > ?"
                " OR (observed_at = ? AND source_id > ?)"
                " OR (observed_at = ? AND source_id = ? AND seq > ?))"
            )
            published_params = (
                session_id, cutoff_store,
                b_ts, b_ts, b_src, b_ts, b_src, b_seq,
            )
        if published_where:
            doomed += conn.execute(
                f"SELECT source_id, seq, payload FROM readings"
                f" WHERE {published_where}",
                published_params,
            ).fetchall()
            cur = conn.execute(
                f"DELETE FROM readings WHERE {published_where}",
                published_params,
            )
            deleted_published = cur.rowcount

        # Tombstones for the rows actually removed, in the same transaction:
        # a later retry of a deleted key is matched against these digests.
        _record_tombstones(conn, session_id, doomed)

        protection = _checkpoint_protection(earliest) if earliest else None

    return _result(
        session_id,
        cutoff_store,
        dry_run,
        protection,
        counts,
        deleted=deleted_published + deleted_quarantined,
    )


def _compute_counts(
    conn: sqlite3.Connection,
    session_id: str,
    cutoff_store: str,
    earliest: Optional[sqlite3.Row],
) -> dict[str, int]:
    """Count candidate / retained / deletable rows in the current snapshot.

    ``retained`` counts only *protected* candidates (checkpoint-retained
    published readings); buffered readings are not candidates and therefore
    not counted as retained.
    """
    quarantined = _count(
        conn,
        "session_id = ? AND status = 'quarantined' AND observed_at < ?",
        session_id, cutoff_store,
    )

    if earliest is None:
        published_deletable = _count(
            conn,
            "session_id = ? AND status = 'published' AND observed_at < ?",
            session_id, cutoff_store,
        )
        published_retained = 0
    elif earliest["last_observed_at"] is None:
        # Empty-stream checkpoint: all published candidates stay.
        published_deletable = 0
        published_retained = _count(
            conn,
            "session_id = ? AND status = 'published' AND observed_at < ?",
            session_id, cutoff_store,
        )
    else:
        b_ts = earliest["last_observed_at"]
        b_src = earliest["last_source_id"]
        b_seq = earliest["last_seq"]
        # Keyset strictly greater than the checkpoint boundary == still
        # readable on resume == retained.
        published_retained = _count(
            conn,
            "session_id = ? AND status = 'published' AND observed_at < ?"
            " AND (observed_at > ?"
            " OR (observed_at = ? AND source_id > ?)"
            " OR (observed_at = ? AND source_id = ? AND seq > ?))",
            session_id, cutoff_store,
            b_ts, b_ts, b_src, b_ts, b_src, b_seq,
        )
        published_total = _count(
            conn,
            "session_id = ? AND status = 'published' AND observed_at < ?",
            session_id, cutoff_store,
        )
        published_deletable = published_total - published_retained

    candidates = quarantined + published_retained + published_deletable
    return {
        "candidates": candidates,
        "retained": published_retained,
        "deletable": quarantined + published_deletable,
        "published_candidates": published_retained + published_deletable,
        "published_retained": published_retained,
        "published_deletable": published_deletable,
        "quarantined_candidates": quarantined,
        "quarantined_deletable": quarantined,
    }


def _result(
    session_id: str,
    cutoff_store: str,
    dry_run: bool,
    protection: Optional[dict[str, Any]],
    counts: dict[str, int],
    *,
    deleted: int,
) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "cutoff": format_event_time(parse_event_time(cutoff_store)),
        "dry_run": dry_run,
        "deleted": deleted,
        "protected_by_checkpoint": protection,
        "candidates": counts["candidates"],
        "retained": counts["retained"],
        "deletable": counts["deletable"],
        "by_status": {
            "published": {
                "candidates": counts["published_candidates"],
                "retained": counts["published_retained"],
                "deletable": counts["published_deletable"],
            },
            "quarantined": {
                "candidates": counts["quarantined_candidates"],
                "retained": 0,
                "deletable": counts["quarantined_deletable"],
            },
        },
    }
