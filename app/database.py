"""SQLite persistence: schema, migrations, connection lifecycle and transactions."""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import Request

# The v0 schema creates every table that existed before versioned migrations
# were introduced.  It stays idempotent (``CREATE TABLE IF NOT EXISTS``) so
# brand-new databases are bootstrapped straight from it; subsequent schema
# changes live in :data:`MIGRATIONS` and are applied one version at a time.
SCHEMA_V0 = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id                TEXT PRIMARY KEY,
    source_ids                TEXT NOT NULL,          -- JSON array, canonical order
    allowed_lateness_seconds  REAL NOT NULL,
    published_watermark       TEXT,                   -- NULL until first publish
    created_at                TEXT NOT NULL,
    status                    TEXT NOT NULL DEFAULT 'open'
                                   CHECK (status IN ('open','closed')),
    closed_at                 TEXT                    -- set by the last source seal
);

CREATE TABLE IF NOT EXISTS session_sources (
    session_id        TEXT NOT NULL REFERENCES sessions(session_id),
    source_id         TEXT NOT NULL,
    max_observed_at   TEXT,                           -- NULL = no report yet
    first_seen_at     TEXT NOT NULL,
    final_observed_at TEXT,                           -- NULL = still open
    sealed_at         TEXT,
    PRIMARY KEY (session_id, source_id)
);

CREATE TABLE IF NOT EXISTS readings (
    session_id             TEXT NOT NULL REFERENCES sessions(session_id),
    source_id              TEXT NOT NULL,
    seq                    INTEGER NOT NULL,
    observed_at            TEXT NOT NULL,             -- normalised UTC, fixed format
    payload                TEXT,                      -- canonical JSON of value
    status                 TEXT NOT NULL CHECK (status IN ('buffered','published','quarantined')),
    first_received_at      TEXT NOT NULL,
    published_watermark    TEXT,                      -- set when published
    quarantined_watermark TEXT,                      -- set when quarantined
    PRIMARY KEY (session_id, source_id, seq)
);

-- Publish scan: buffered readings of a session ordered on the event timeline.
CREATE INDEX IF NOT EXISTS idx_readings_publish
    ON readings (session_id, status, observed_at, source_id, seq);

-- Isolation listing: quarantine queue in arrival order.
CREATE INDEX IF NOT EXISTS idx_readings_quarantine
    ON readings (session_id, status, first_received_at, source_id, seq);

-- ---------------------------------------------------------------------------
-- Post-publish threshold alerting
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alert_rules (
    rule_id                    TEXT PRIMARY KEY,
    session_id                 TEXT NOT NULL REFERENCES sessions(session_id),
    source_id                  TEXT NOT NULL,
    direction                  TEXT NOT NULL CHECK (direction IN ('above','below')),
    enter_threshold            REAL NOT NULL,
    recover_threshold          REAL NOT NULL,
    enter_consecutive          INTEGER NOT NULL,
    recover_consecutive        INTEGER NOT NULL,
    value_key                  TEXT,                 -- NULL => the reading value itself
    created_at                 TEXT NOT NULL,
    -- Per-source state machine state ('normal' | 'active') plus the current
    -- streak length for the state's relevant predicate.
    state                      TEXT NOT NULL DEFAULT 'normal'
                                   CHECK (state IN ('normal','active')),
    streak                     INTEGER NOT NULL DEFAULT 0,
    active_since_observed_at   TEXT,                 -- set on enter, cleared on recovery
    active_since_seq           INTEGER,
    last_observed_at           TEXT,                 -- last published reading evaluated
    last_seq                   INTEGER
);

-- Look up the rules of a session grouped by source during a publish scan.
CREATE INDEX IF NOT EXISTS idx_alert_rules_lookup
    ON alert_rules (session_id, source_id);

CREATE TABLE IF NOT EXISTS alert_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,  -- stable chronological id
    rule_id         TEXT NOT NULL REFERENCES alert_rules(rule_id),
    session_id      TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN ('entered','recovered')),
    seq             INTEGER NOT NULL,
    observed_at     TEXT NOT NULL,            -- event time of the triggering reading
    value           REAL,                     -- numeric value that crossed the threshold
    threshold       REAL NOT NULL,            -- enter / recover threshold at trigger time
    triggered_at    TEXT NOT NULL,            -- wall-clock time the event was produced
    -- A rule cannot emit the same transition for the same reading twice.
    UNIQUE (rule_id, seq, event_type)
);

-- History listing per session, newest first, keyed on the monotonic event id.
CREATE INDEX IF NOT EXISTS idx_alert_events_session
    ON alert_events (session_id, id);
"""


# ---------------------------------------------------------------------------
# versioned migrations (applied on startup, each exactly once per database)
# ---------------------------------------------------------------------------

def _migration_1(conn: sqlite3.Connection, applied_at: str) -> None:
    """Cross-source correlated alert rules (2-4 sources, per-condition window).

    Runs inside the ``BEGIN IMMEDIATE`` transaction opened by :func:`init_db`
    (individual ``execute`` calls, not ``executescript`` which would commit it).
    """
    conn.execute(
        """
CREATE TABLE correlated_rules (
    rule_id                     TEXT PRIMARY KEY,
    session_id                  TEXT NOT NULL REFERENCES sessions(session_id),
    created_at                  TEXT NOT NULL,
    state                       TEXT NOT NULL DEFAULT 'waiting'
                                    CHECK (state IN ('waiting','correlated')),
    -- Event-time position of the last published reading evaluated, in global
    -- (cross-source) merge order (observed_at, source_id, seq).
    last_observed_at            TEXT,
    last_source_id              TEXT,
    last_seq                    INTEGER,
    -- Set while correlated: the reading that completed the last correlation.
    correlated_since_observed_at TEXT,
    correlated_since_source_id   TEXT,
    correlated_since_seq         INTEGER
)
"""
    )
    conn.execute(
        "CREATE INDEX idx_correlated_rules_session"
        " ON correlated_rules (session_id)"
    )
    conn.execute(
        """
CREATE TABLE correlated_conditions (
    rule_id      TEXT NOT NULL REFERENCES correlated_rules(rule_id),
    position     INTEGER NOT NULL,              -- merge-declaration order, 0-based
    source_id    TEXT NOT NULL,
    direction    TEXT NOT NULL CHECK (direction IN ('above','below')),
    threshold    REAL NOT NULL,
    value_key    TEXT,                           -- NULL => the reading value itself
    window_seconds REAL NOT NULL,
    -- Most recent in-window breach ("hit") seen for this condition.
    last_hit_observed_at TEXT,
    last_hit_seq         INTEGER,
    last_hit_value       REAL,
    PRIMARY KEY (rule_id, position)
)
"""
    )
    # Every condition is fetched for a rule on each published reading.
    conn.execute(
        "CREATE INDEX idx_correlated_conditions_rule"
        " ON correlated_conditions (rule_id)"
    )
    conn.execute(
        """
CREATE TABLE correlated_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,  -- stable chronological id
    rule_id      TEXT NOT NULL REFERENCES correlated_rules(rule_id),
    session_id   TEXT NOT NULL,
    event_type   TEXT NOT NULL CHECK (event_type IN ('correlated_entered')),
    -- The reading whose evaluation completed the correlation.
    completed_by_source_id  TEXT NOT NULL,
    completed_by_seq        INTEGER NOT NULL,
    completed_by_observed_at TEXT NOT NULL,
    -- Per-condition hit snapshot at completion (JSON: position-indexed list).
    hits         TEXT NOT NULL,
    triggered_at TEXT NOT NULL,                    -- wall-clock production time
    -- The same completing reading can only complete the rule once.
    UNIQUE (rule_id, completed_by_source_id, completed_by_seq)
)
"""
    )
    conn.execute(
        "CREATE INDEX idx_correlated_events_session"
        " ON correlated_events (session_id, id)"
    )
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (1, ?)",
        (applied_at,),
    )


def _add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, ddl: str
) -> None:
    """Idempotent ``ALTER TABLE ADD COLUMN`` (migrations must be re-runnable)."""
    existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _migration_2(conn: sqlite3.Connection, applied_at: str) -> None:
    """Finite-collection source sealing and the closed-session tail flush.

    A source may be sealed with a ``final_observed_at`` that is not earlier
    than the largest event time ever seen for it.  Once every source of a
    session is sealed the session closes: the write transaction that seals
    the last source also publishes every remaining buffered reading under a
    terminal watermark and drives both alert kinds before setting
    ``sessions.status = 'closed'``.
    """
    _add_column_if_missing(
        conn,
        "sessions",
        "status",
        "status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed'))",
    )
    _add_column_if_missing(
        conn, "sessions", "closed_at", "closed_at TEXT"
    )
    _add_column_if_missing(
        conn,
        "session_sources",
        "final_observed_at",
        "final_observed_at TEXT",
    )
    _add_column_if_missing(
        conn,
        "session_sources",
        "sealed_at",
        "sealed_at TEXT",
    )
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (2, ?)",
        (applied_at,),
    )


def _migration_3(conn: sqlite3.Connection, applied_at: str) -> None:
    """Consumer checkpoints over a session's published reading stream.

    A checkpoint snapshots, inside one serialised transaction, the current
    position of the published stream (``stream_offset`` published readings plus
    the keyset ``(last_observed_at, last_source_id, last_seq)`` of its last
    reading -- all NULL/0 before the first publish) and the session's
    ``published_watermark`` at that same instant.  Names are unique per
    session.  Checkpoint rows are immutable: later publishes, source seals and
    session closure never rewrite them, so resuming from a checkpoint always
    means the same stream position.
    """
    conn.execute(
        """
CREATE TABLE checkpoints (
    session_id           TEXT NOT NULL REFERENCES sessions(session_id),
    name                 TEXT NOT NULL,
    created_at           TEXT NOT NULL,
    -- Stream position at creation: the number of readings published so far
    -- and the deterministic-order keyset of the last one (NULL while offset=0).
    stream_offset        INTEGER NOT NULL,
    last_observed_at     TEXT,
    last_source_id       TEXT,
    last_seq             INTEGER,
    -- Snapshot of sessions.published_watermark taken in the same transaction.
    published_watermark  TEXT,
    PRIMARY KEY (session_id, name)
)
"""
    )
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (3, ?)",
        (applied_at,),
    )


def _migration_4(conn: sqlite3.Connection, applied_at: str) -> None:
    """Retention tombstones: a deleted reading keeps its dedup identity.

    Retention removes published/quarantined rows outright.  Without a record
    of what was removed, a later retry of the same ``(source_id, seq)`` looks
    like a brand-new key to ingestion: it re-enters quarantine behind the
    watermark, or is rejected outright once the source is sealed / the
    session closed.  Every reading actually deleted by retention therefore
    leaves a tombstone here, written in the same serialised transaction as
    the delete: the reading key plus a digest of its canonical payload.
    Ingestion consults the tombstones before admitting an unseen key -- same
    key with the same payload is an idempotent duplicate (nothing is
    restored, republished or re-alerted), same key with a different payload
    is a 409 conflict.
    """
    conn.execute(
        """
CREATE TABLE retention_tombstones (
    session_id     TEXT NOT NULL REFERENCES sessions(session_id),
    source_id      TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    -- SHA-256 (hex) of the deleted reading's canonical JSON payload.
    payload_digest TEXT NOT NULL,
    -- Wall-clock time of the retention delete that created the tombstone.
    deleted_at     TEXT NOT NULL,
    PRIMARY KEY (session_id, source_id, seq)
)
"""
    )
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (4, ?)",
        (applied_at,),
    )


def _migration_5(conn: sqlite3.Connection, applied_at: str) -> None:
    """Monotonic publish marker for retention-proof checkpoint idempotency.

    Checkpoint idempotency used to compare the checkpoint's captured stream
    position (published-row count + last keyset) against the *current*
    published rows.  Once retention deleted the rows a checkpoint had already
    covered, the recomputed position no longer matched and a same-name retry
    was wrongly rejected with 409 even though nothing new had been published.

    This migration adds a per-session marker ``sessions.published_seq`` that
    counts every reading ever published: it is only ever incremented, inside
    the same serialised transaction that flips the readings to published, and
    retention never writes it.  Each checkpoint row records the marker value
    at creation (``checkpoints.published_seq``), so a same-name retry is
    idempotent exactly when the marker has not advanced -- independent of
    which rows retention has since removed, and a 409 exactly when readings
    were published in between, even if those rows were cleaned up later.

    Backfill for existing databases (checkpoints and their resume boundaries
    are preserved, never rewritten): a session's marker starts at its current
    published-row count N.  Readings published after a checkpoint's creation
    carry keysets strictly greater than every checkpoint's captured keyset,
    so retention can never have deleted them while any checkpoint exists --
    "nothing was published after this checkpoint" is exactly "its captured
    keyset is at or beyond the current stream frontier" (or the stream is
    empty).  Such a checkpoint shares the session marker N and its retry
    stays idempotent -- even when retention already removed the rows it
    covered, which is precisely the false 409 this marker fixes.  Every other
    checkpoint (behind the frontier, or an empty-stream checkpoint on a now
    non-empty stream) gets a strictly smaller marker, so its retry keeps
    correctly conflicting with 409.
    """
    _add_column_if_missing(
        conn,
        "sessions",
        "published_seq",
        "published_seq INTEGER NOT NULL DEFAULT 0",
    )
    _add_column_if_missing(
        conn,
        "checkpoints",
        "published_seq",
        "published_seq INTEGER NOT NULL DEFAULT 0",
    )

    for session in conn.execute("SELECT session_id FROM sessions").fetchall():
        sid = session["session_id"]
        stream_count = conn.execute(
            "SELECT COUNT(*) AS c FROM readings"
            " WHERE session_id = ? AND status = 'published'",
            (sid,),
        ).fetchone()["c"]
        frontier = conn.execute(
            "SELECT observed_at, source_id, seq FROM readings"
            " WHERE session_id = ? AND status = 'published'"
            " ORDER BY observed_at DESC, source_id DESC, seq DESC LIMIT 1",
            (sid,),
        ).fetchone()
        conn.execute(
            "UPDATE sessions SET published_seq = ? WHERE session_id = ?",
            (stream_count, sid),
        )
        checkpoints = conn.execute(
            "SELECT name, last_observed_at, last_source_id, last_seq"
            " FROM checkpoints WHERE session_id = ?",
            (sid,),
        ).fetchall()
        for checkpoint in checkpoints:
            marker = _backfill_checkpoint_marker(
                conn, sid, checkpoint, frontier, stream_count
            )
            conn.execute(
                "UPDATE checkpoints SET published_seq = ?"
                " WHERE session_id = ? AND name = ?",
                (marker, sid, checkpoint["name"]),
            )
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (5, ?)",
        (applied_at,),
    )


def _backfill_checkpoint_marker(
    conn: sqlite3.Connection,
    session_id: str,
    checkpoint: sqlite3.Row,
    frontier: Optional[sqlite3.Row],
    stream_count: int,
) -> int:
    """Publish-marker value assigned to a pre-v5 checkpoint by migration 5.

    ``frontier`` is the last published row currently in the stream (``None``
    when nothing is published); ``stream_count`` is the session's backfilled
    marker.  A checkpoint at or beyond the frontier (or on an empty stream)
    had nothing published after it and shares the session marker; any other
    checkpoint gets a strictly smaller value so its same-name retry keeps
    returning 409 exactly as before the upgrade.
    """
    key = (
        checkpoint["last_observed_at"],
        checkpoint["last_source_id"],
        checkpoint["last_seq"],
    )
    if stream_count == 0 or (
        key[0] is not None
        and key >= (frontier["observed_at"], frontier["source_id"], frontier["seq"])
    ):
        # Empty stream, or nothing published after this checkpoint (its
        # covered rows may already be cleaned up): the marker is the
        # session's, so a same-name retry stays idempotent.
        return stream_count
    if key[0] is None:
        # Empty-stream checkpoint on a now non-empty stream: every extant
        # row was necessarily published after it.
        return 0
    # The frontier advanced beyond the captured keyset: readings were
    # published after this checkpoint.  The count of extant rows at or
    # before the keyset is strictly below the session marker (the frontier
    # row itself is never counted).
    return conn.execute(
        "SELECT COUNT(*) AS c FROM readings"
        " WHERE session_id = ? AND status = 'published'"
        " AND (observed_at < ?"
        " OR (observed_at = ? AND source_id < ?)"
        " OR (observed_at = ? AND source_id = ? AND seq <= ?))",
        (
            session_id,
            key[0], key[0], key[1], key[0], key[1], key[2],
        ),
    ).fetchone()["c"]


MIGRATIONS: dict[int, Callable[[sqlite3.Connection, str], None]] = {
    1: _migration_1,
    2: _migration_2,
    3: _migration_3,
    4: _migration_4,
    5: _migration_5,
}
LATEST_VERSION = max(MIGRATIONS)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(db_path: str) -> None:
    """Create the database directory, base schema and pending migrations.

    Safe to call repeatedly: the v0 schema is created idempotently and each
    numbered migration runs at most once (recorded in ``schema_migrations``).
    An existing database from an earlier release therefore upgrades in place
    without losing any data.
    """
    from .time_utils import store_event_time

    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = connect(db_path)
    try:
        # Bootstrap the complete base schema (no-ops on existing databases).
        conn.executescript(SCHEMA_V0)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        applied = {
            r["version"]
            for r in conn.execute("SELECT version FROM schema_migrations")
        }
        for version in sorted(MIGRATIONS):
            if version in applied:
                continue
            # Each migration is one serialised unit: its DDL and the ledger
            # insert commit together, so a crash before completion leaves the
            # version unrecorded and the (idempotent) migration re-runnable.
            conn.execute("BEGIN IMMEDIATE")
            try:
                MIGRATIONS[version](conn, store_event_time(utc_now()))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Serialised write transaction (required for correct watermark updates).

    BEGIN IMMEDIATE takes a reserved lock up front so concurrent report
    batches are linearised instead of failing with SQLITE_BUSY mid-flight.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


@contextmanager
def read_transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Deferred (read) transaction: consistent snapshot without taking a lock."""
    conn.execute("BEGIN")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_db(request: Request) -> Iterator[sqlite3.Connection]:
    """FastAPI dependency: one connection per request."""
    conn = connect(request.app.state.db_path)
    try:
        yield conn
    finally:
        conn.close()
