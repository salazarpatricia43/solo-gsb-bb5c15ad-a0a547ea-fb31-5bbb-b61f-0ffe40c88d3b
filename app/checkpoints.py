"""Consumer checkpoints over a session's published reading stream.

A checkpoint captures, inside one serialised write transaction, the current
*position* of the published stream and the session's *published watermark* at
that same instant.  The position is the count of published readings
(``stream_offset``) plus the deterministic-order keyset
``(last_observed_at, last_source_id, last_seq)`` of the last published
reading -- both are empty/zero before the first publish.

Checkpoint names are unique within a session.  Whether creating the same
name again is idempotent is decided by the session's **publish marker**
(``sessions.published_seq``): a persistent counter that only ever
increments, by exactly the number of readings published, inside the publish
transaction itself.  Each checkpoint records the marker value at creation,
so the decision does not depend on which published rows are still stored:

* **idempotent** (200) when the marker has not advanced since the checkpoint
  was created -- no reading was published in between, so the retry returns
  the original checkpoint with its original ``created_at`` and watermark
  snapshot, *even if retention has since deleted the rows it covered*;
* a **409 conflict** when readings were published in between -- the marker
  only ever grows, so this still holds even if those rows were later
  cleaned up as well.

Checkpoint rows are immutable once written: later ingestion, source seals,
session closure and retention cleanup append to (or shrink) the stream but
never rewrite a checkpoint.  Resuming from a checkpoint therefore always
resumes at exactly the stream position captured at creation; only the
optional ``cursor`` page position moves forward.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Optional

from .cursor import decode_cursor as _decode_cursor
from .cursor import encode_cursor as _encode_cursor
from .database import read_transaction, transaction, utc_now
from .errors import BadRequestError, ConflictError, NotFoundError
from .service import _load_session, _reading_out
from .time_utils import format_event_time, parse_event_time, store_event_time

__all__ = [
    "create_checkpoint",
    "list_checkpoints",
    "delete_checkpoint",
    "continue_from_checkpoint",
]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _stream_position(
    conn: sqlite3.Connection, session_id: str
) -> tuple[int, Optional[str], Optional[str], Optional[int]]:
    """Read the published-stream position in one consistent snapshot.

    Returns ``(offset, last_observed_at, last_source_id, last_seq)`` where the
    keyset components are ``None`` while nothing has been published yet.  The
    scan follows the same deterministic order as the stream endpoint
    ``(observed_at, source_id, seq)``.
    """
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM readings"
        " WHERE session_id = ? AND status = 'published'",
        (session_id,),
    ).fetchone()
    offset = row["c"]
    if offset == 0:
        return 0, None, None, None
    last = conn.execute(
        "SELECT observed_at, source_id, seq FROM readings"
        " WHERE session_id = ? AND status = 'published'"
        " ORDER BY observed_at DESC, source_id DESC, seq DESC LIMIT 1",
        (session_id,),
    ).fetchone()
    return offset, last["observed_at"], last["source_id"], last["seq"]


def _checkpoint_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "name": row["name"],
        "created_at": format_event_time(parse_event_time(row["created_at"])),
        "stream_offset": row["stream_offset"],
        "last_observed_at": (
            format_event_time(parse_event_time(row["last_observed_at"]))
            if row["last_observed_at"]
            else None
        ),
        "last_source_id": row["last_source_id"],
        "last_seq": row["last_seq"],
        "published_watermark": (
            format_event_time(parse_event_time(row["published_watermark"]))
            if row["published_watermark"]
            else None
        ),
    }


# ---------------------------------------------------------------------------
# create / list / delete
# ---------------------------------------------------------------------------

def create_checkpoint(
    conn: sqlite3.Connection,
    session_id: str,
    name: str,
) -> tuple[dict[str, Any], bool]:
    """Create (or idempotently re-create) a stream checkpoint.

    The current stream position, the session's published watermark and its
    publish marker are read in the same serialised write transaction as the
    insert, so they are one consistent snapshot.  Returns ``(checkpoint,
    created)`` -- ``created`` is False for an idempotent retry of a name
    whose publish marker is unchanged (no reading was published since, even
    if retention deleted the covered rows in the meantime).  Readings having
    been published since raises 409; existing checkpoints are never
    rewritten.
    """
    created_at = store_event_time(utc_now())
    with transaction(conn):
        _load_session(conn, session_id)
        offset, last_ts, last_src, last_seq = _stream_position(conn, session_id)
        session_row = conn.execute(
            "SELECT published_watermark, published_seq FROM sessions"
            " WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        watermark = session_row["published_watermark"]
        publish_seq = session_row["published_seq"]

        existing = conn.execute(
            "SELECT * FROM checkpoints WHERE session_id = ? AND name = ?",
            (session_id, name),
        ).fetchone()
        if existing is not None:
            if existing["published_seq"] != publish_seq:
                raise ConflictError(
                    f"checkpoint {name!r} already exists in session {session_id!r}"
                    f" at stream_offset={existing['stream_offset']}"
                    f" (last_observed_at={existing['last_observed_at']!r},"
                    f" last_source_id={existing['last_source_id']!r},"
                    f" last_seq={existing['last_seq']}); readings have been"
                    f" published since (current stream_offset={offset},"
                    f" last_observed_at={last_ts!r}, last_source_id={last_src!r},"
                    f" last_seq={last_seq})"
                )
            # The publish marker has not moved: no reading was published
            # since the checkpoint was created, so this is an idempotent
            # retry even if retention has deleted the rows it covered.  The
            # original snapshot (created_at, watermark) is returned
            # unchanged.
            return _checkpoint_out(existing), False

        conn.execute(
            "INSERT INTO checkpoints (session_id, name, created_at, stream_offset,"
            " last_observed_at, last_source_id, last_seq, published_watermark,"
            " published_seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (session_id, name, created_at, offset, last_ts, last_src, last_seq,
             watermark, publish_seq),
        )
        row = conn.execute(
            "SELECT * FROM checkpoints WHERE session_id = ? AND name = ?",
            (session_id, name),
        ).fetchone()
        return _checkpoint_out(row), True


def list_checkpoints(
    conn: sqlite3.Connection,
    session_id: str,
) -> dict[str, Any]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            "SELECT * FROM checkpoints WHERE session_id = ?"
            " ORDER BY created_at, name",
            (session_id,),
        ).fetchall()
    return {
        "session_id": session_id,
        "checkpoints": [_checkpoint_out(r) for r in rows],
    }


def delete_checkpoint(conn: sqlite3.Connection, session_id: str, name: str) -> None:
    with transaction(conn):
        _load_session(conn, session_id)
        cur = conn.execute(
            "DELETE FROM checkpoints WHERE session_id = ? AND name = ?",
            (session_id, name),
        )
        if cur.rowcount == 0:
            raise NotFoundError(
                f"checkpoint {name!r} not found in session {session_id!r}"
            )


# ---------------------------------------------------------------------------
# resuming a read at a checkpoint
# ---------------------------------------------------------------------------

def continue_from_checkpoint(
    conn: sqlite3.Connection,
    session_id: str,
    name: str,
    limit: int,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    """Read the published stream starting after a checkpoint's position.

    Ordering and keyset pagination are exactly those of the stream endpoint.
    Without a ``cursor`` the first page begins strictly *after* the position
    captured at checkpoint creation; an explicit ``cursor`` resumes a previous
    page instead (it must never point at or before the checkpoint position).
    Later publishes and session closure do not change the checkpoint, so the
    resume point stays identical for the checkpoint's lifetime.
    """
    where = "session_id = ? AND status = 'published'"
    params: list[Any] = [session_id]

    with read_transaction(conn):
        checkpoint = conn.execute(
            "SELECT * FROM checkpoints WHERE session_id = ? AND name = ?",
            (session_id, name),
        ).fetchone()
        if checkpoint is None:
            # Distinguish an unknown session from a missing checkpoint.
            _load_session(conn, session_id)
            raise NotFoundError(
                f"checkpoint {name!r} not found in session {session_id!r}"
            )

        cp_ts = checkpoint["last_observed_at"]
        cp_src = checkpoint["last_source_id"]
        cp_seq = checkpoint["last_seq"]

        if cursor:
            ts, src, seq = _decode_cursor(cursor, 3)
            # The cursor belongs to this keyset and must not move backwards
            # past the immutable checkpoint position.
            cursor_pos = (ts, src, seq)
            if cp_ts is not None and cursor_pos <= (cp_ts, cp_src, cp_seq):
                raise BadRequestError(
                    "cursor is at or before its checkpoint position"
                )
        elif cp_ts is not None:
            # First page: start strictly after the captured position.
            ts, src, seq = cp_ts, cp_src, cp_seq
        else:
            # Empty-stream checkpoint: start at the beginning.
            ts = src = seq = None

        if ts is not None:
            where += (
                " AND (observed_at > ?"
                " OR (observed_at = ? AND source_id > ?)"
                " OR (observed_at = ? AND source_id = ? AND seq > ?))"
            )
            params += [ts, ts, src, ts, src, seq]

        session = conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
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
        "checkpoint": _checkpoint_out(checkpoint),
        "next_cursor": next_cursor,
        "published_watermark": (
            format_event_time(parse_event_time(session["published_watermark"]))
            if session["published_watermark"]
            else None
        ),
        "readings": [_reading_out(r) for r in page],
    }
