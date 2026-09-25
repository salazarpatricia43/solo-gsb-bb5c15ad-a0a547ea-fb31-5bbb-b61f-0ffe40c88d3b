"""Cross-source correlated alert rules.

A correlated rule declares **2-4 conditions** (each bound to a distinct
session source with its own ``direction``, ``threshold``, optional
``value_key`` and ``window_seconds``) and watches the session's globally
merged published stream.  Whenever a **deterministically published** reading
is processed, in merge order ``(observed_at, source_id, seq)``:

* if the reading belongs to one of the rule's conditions and strictly
  breaches that condition's threshold (``value > threshold`` for ``above``,
  ``value < threshold`` for ``below``), it becomes the condition's most
  recent *hit*;
* after updating the hit (and also for any other published reading, which
  simply advances event time), every condition must currently have a hit no
  older than its own ``window_seconds`` measured against the reading's event
  time.  If all conditions are satisfied inside their windows while the rule
  is ``waiting``, it transitions to ``correlated`` and emits exactly one
  ``correlated_entered`` event;
* if any condition's most recent hit is older than its window (or missing),
  the rule drops back to ``waiting`` (silently -- no event).

Because readings are evaluated strictly by event time, repeated satisfaction
while already ``correlated`` never produces a second event: the rule must
fall back to ``waiting`` (a window lapse seen on a later reading) before a
fresh correlation can fire.  The window boundary is inclusive: a hit whose
age equals ``window_seconds`` still counts.

Only **published** readings are ever consumed.  Buffered readings do not
advance the rule and late readings diverted to quarantine never become
published, so they can neither create nor destroy a correlation.  Re-posting
a duplicate reading is a no-op and likewise cannot retrigger anything.

Like the per-source rules, the correlation is a pure function of the merged
published timeline: a rule created after the fact is initialised by replaying
every published reading of the session in global merge order.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any, Optional

from .alerts import _is_breach, extract_numeric
from .cursor import decode_cursor, encode_cursor
from .database import read_transaction, transaction, utc_now
from .errors import BadRequestError, ConflictError, NotFoundError
from .time_utils import format_event_time, parse_event_time, store_event_time

STATE_WAITING = "waiting"
STATE_CORRELATED = "correlated"

EVENT_TYPE = "correlated_entered"
DIRECTIONS = ("above", "below")

MIN_CONDITIONS = 2
MAX_CONDITIONS = 4


# ---------------------------------------------------------------------------
# serialisation helpers
# ---------------------------------------------------------------------------

def _load_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()

    if row is None:
        raise NotFoundError(f"session {session_id!r} not found")
    return row


def _format_observed(value: Optional[str]) -> Optional[str]:
    return format_event_time(parse_event_time(value)) if value is not None else None


def _condition_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "position": row["position"],
        "source_id": row["source_id"],
        "direction": row["direction"],
        "threshold": row["threshold"],
        "value_key": row["value_key"],
        "window_seconds": row["window_seconds"],
        "last_hit": (
            None
            if row["last_hit_observed_at"] is None
            else {
                "source_id": row["source_id"],
                "seq": row["last_hit_seq"],
                "observed_at": format_event_time(
                    parse_event_time(row["last_hit_observed_at"])
                ),
                "value": row["last_hit_value"],
            }
        ),
    }


def _rule_out(
    rule: sqlite3.Row, conditions: list[sqlite3.Row]
) -> dict[str, Any]:
    return {
        "rule_id": rule["rule_id"],
        "session_id": rule["session_id"],
        "created_at": format_event_time(parse_event_time(rule["created_at"])),
        "state": rule["state"],
        "conditions": [_condition_out(c) for c in conditions],
        "correlated_since": (
            None
            if rule["correlated_since_observed_at"] is None
            else {
                "source_id": rule["correlated_since_source_id"],
                "seq": rule["correlated_since_seq"],
                "observed_at": format_event_time(
                    parse_event_time(rule["correlated_since_observed_at"])
                ),
            }
        ),
        "last_observed_at": _format_observed(rule["last_observed_at"]),
        "last_source_id": rule["last_source_id"],
        "last_seq": rule["last_seq"],
    }


def _fetch_rule(
    conn: sqlite3.Connection, session_id: str, rule_id: str, *, for_update: bool = False
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM correlated_rules WHERE rule_id = ? AND session_id = ?",
        (rule_id, session_id),
    ).fetchone()
    if row is None:
        raise NotFoundError(
            f"correlated rule {rule_id!r} not found in session {session_id!r}"
        )
    return row


def _fetch_conditions(
    conn: sqlite3.Connection, rule_id: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM correlated_conditions WHERE rule_id = ? ORDER BY position",
        (rule_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# rule management
# ---------------------------------------------------------------------------

def create_rule(
    conn: sqlite3.Connection,
    session_id: str,
    conditions: list[dict[str, Any]],
    rule_id: Optional[str] = None,
) -> dict[str, Any]:
    rid = rule_id or uuid.uuid4().hex
    created_at = store_event_time(utc_now())
    with transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = set(json.loads(session["source_ids"]))
        unknown = [
            c["source_id"] for c in conditions if c["source_id"] not in allowed_sources
        ]
        if unknown:
            raise BadRequestError(
                f"unknown source_id(s) for this session: {sorted(set(unknown))};"
                f" declared sources: {sorted(allowed_sources)}"
            )
        try:
            conn.execute(
                "INSERT INTO correlated_rules (rule_id, session_id, created_at, state,"
                " last_observed_at, last_source_id, last_seq,"
                " correlated_since_observed_at, correlated_since_source_id,"
                " correlated_since_seq)"
                " VALUES (?, ?, ?, 'waiting', NULL, NULL, NULL, NULL, NULL, NULL)",
                (rid, session_id, created_at),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError(f"correlated rule {rid!r} already exists") from exc

        conn.executemany(
            "INSERT INTO correlated_conditions (rule_id, position, source_id, direction,"
            " threshold, value_key, window_seconds, last_hit_observed_at, last_hit_seq,"
            " last_hit_value) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)",
            [
                (
                    rid,
                    i,
                    c["source_id"],
                    c["direction"],
                    c["threshold"],
                    c.get("value_key"),
                    c["window_seconds"],
                )
                for i, c in enumerate(conditions)
            ],
        )

        # Replay the whole merged published stream so the rule state matches
        # whatever the timeline already contains, independently of when the
        # rule was created.
        rule = _fetch_rule(conn, session_id, rid)
        _backfill_rule(conn, rule, created_at)
        rule = _fetch_rule(conn, session_id, rid)
        cond_rows = _fetch_conditions(conn, rid)
    return _rule_out(rule, cond_rows)


def list_rules(conn: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        rules = conn.execute(
            "SELECT * FROM correlated_rules WHERE session_id = ?"
            " ORDER BY created_at, rule_id",
            (session_id,),
        ).fetchall()
        result = [
            _rule_out(r, _fetch_conditions(conn, r["rule_id"])) for r in rules
        ]
    return result


def get_rule(
    conn: sqlite3.Connection, session_id: str, rule_id: str
) -> dict[str, Any]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        rule = _fetch_rule(conn, session_id, rule_id)
        conditions = _fetch_conditions(conn, rule_id)
    return _rule_out(rule, conditions)


# ---------------------------------------------------------------------------
# advancement (called from the ingest write transaction, in merge order)
# ---------------------------------------------------------------------------

def _new_published_readings(
    conn: sqlite3.Connection,
    session_id: str,
    rule: sqlite3.Row,
    watermark: Optional[str],
) -> list[sqlite3.Row]:
    """Published readings of the whole session past the rule's checkpoint.

    The rule consumes the *global* cross-source merge order because readings
    from non-participating sources still advance event time and can therefore
    expire a condition's window.  ``watermark`` bounds the scan during live
    ingestion (only readings published up to that watermark); for backfill it
    is ``None`` and every already-published reading is replayed.
    """
    where = "session_id = ? AND status = 'published'"
    params: list[Any] = [session_id]
    if rule["last_observed_at"] is not None:
        where += (
            " AND (observed_at > ?"
            " OR (observed_at = ? AND source_id > ?)"
            " OR (observed_at = ? AND source_id = ? AND seq > ?))"
        )
        params += [
            rule["last_observed_at"],
            rule["last_observed_at"],
            rule["last_source_id"],
            rule["last_observed_at"],
            rule["last_source_id"],
            rule["last_seq"],
        ]
    if watermark is not None:
        where += " AND observed_at <= ?"
        params.append(watermark)
    return conn.execute(
        f"SELECT source_id, seq, observed_at, payload FROM readings"
        f" WHERE {where} ORDER BY observed_at, source_id, seq",
        params,
    ).fetchall()


def _within_window(
    hit_observed_at: str, now: str, window_seconds: float
) -> bool:
    age = parse_event_time(now) - parse_event_time(hit_observed_at)
    # Inclusive boundary: an age of exactly window_seconds still satisfies.
    return timedelta(0) <= age <= timedelta(seconds=window_seconds)


def _persist_hit(conn: sqlite3.Connection, rule_id: str, condition: sqlite3.Row,
                 reading: sqlite3.Row, value: float) -> None:
    conn.execute(
        "UPDATE correlated_conditions SET last_hit_observed_at = ?, last_hit_seq = ?,"
        " last_hit_value = ? WHERE rule_id = ? AND position = ?",
        (reading["observed_at"], reading["seq"], value, rule_id, condition["position"]),
    )


def _hit_snapshot(
    conn: sqlite3.Connection, rule_id: str
) -> list[dict[str, Any]]:
    """Serialise every condition's current hit (called right at completion)."""
    rows = _fetch_conditions(conn, rule_id)
    snapshot: list[dict[str, Any]] = []
    for c in rows:
        snapshot.append({
            "position": c["position"],
            "source_id": c["source_id"],
            "direction": c["direction"],
            "threshold": c["threshold"],
            "value_key": c["value_key"],
            "window_seconds": c["window_seconds"],
            "seq": c["last_hit_seq"],
            "observed_at": format_event_time(
                parse_event_time(c["last_hit_observed_at"])
            ),
            "value": c["last_hit_value"],
        })
    return snapshot


def _apply_reading(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    conditions: list[sqlite3.Row],
    reading: sqlite3.Row,
    triggered_at: str,
) -> None:
    """Evaluate one globally-published reading against one correlated rule."""
    by_source = {c["source_id"]: c for c in conditions}
    condition = by_source.get(reading["source_id"])
    if condition is not None:
        value = extract_numeric(reading["payload"], condition["value_key"])
        if value is not None and _is_breach(
            condition["direction"], value, condition["threshold"]
        ):
            _persist_hit(conn, rule["rule_id"], condition, reading, value)
            condition = conn.execute(
                "SELECT * FROM correlated_conditions WHERE rule_id = ? AND position = ?",
                (rule["rule_id"], condition["position"]),
            ).fetchone()
            by_source[reading["source_id"]] = condition

    now = reading["observed_at"]
    satisfied = all(
        c["last_hit_observed_at"] is not None
        and _within_window(c["last_hit_observed_at"], now, c["window_seconds"])
        for c in by_source.values()
    )

    new_state = STATE_CORRELATED if satisfied else STATE_WAITING
    correlated_since_observed = rule["correlated_since_observed_at"]
    correlated_since_source = rule["correlated_since_source_id"]
    correlated_since_seq = rule["correlated_since_seq"]

    if new_state == STATE_CORRELATED:
        if rule["state"] == STATE_WAITING:
            # Completion: one event exactly, anchored to this reading. The
            # UNIQUE (rule_id, completing reading) constraint also guards
            # against a duplicated event at the storage level.
            hits = json.dumps(_hit_snapshot(conn, rule["rule_id"]),
                              separators=(",", ":"), ensure_ascii=False)
            conn.execute(
                "INSERT INTO correlated_events (rule_id, session_id, event_type,"
                " completed_by_source_id, completed_by_seq, completed_by_observed_at,"
                " hits, triggered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    rule["rule_id"], rule["session_id"], EVENT_TYPE,
                    reading["source_id"], reading["seq"], reading["observed_at"],
                    hits, triggered_at,
                ),
            )
            correlated_since_observed = reading["observed_at"]
            correlated_since_source = reading["source_id"]
            correlated_since_seq = reading["seq"]
        # already correlated: repeated satisfaction must not duplicate the event
    else:
        # Any condition outside its window returns the rule to waiting; the
        # stale hits themselves are retained (they may be replaced at any time).
        correlated_since_observed = None
        correlated_since_source = None
        correlated_since_seq = None

    conn.execute(
        "UPDATE correlated_rules SET state = ?, last_observed_at = ?,"
        " last_source_id = ?, last_seq = ?,"
        " correlated_since_observed_at = ?, correlated_since_source_id = ?,"
        " correlated_since_seq = ? WHERE rule_id = ?",
        (
            new_state, reading["observed_at"], reading["source_id"], reading["seq"],
            correlated_since_observed, correlated_since_source,
            correlated_since_seq, rule["rule_id"],
        ),
    )


def _backfill_rule(
    conn: sqlite3.Connection, rule: sqlite3.Row, triggered_at: str
) -> None:
    """Replay every existing published reading of the session, merge ordered."""
    readings = _new_published_readings(conn, rule["session_id"], rule, None)
    for reading in readings:
        conditions = _fetch_conditions(conn, rule["rule_id"])
        _apply_reading(conn, rule, conditions, reading, triggered_at)
        rule = _fetch_rule(conn, rule["session_id"], rule["rule_id"])


def advance_correlated(
    conn: sqlite3.Connection,
    session_id: str,
    watermark: str,
    triggered_at: str,
) -> None:
    """Feed every correlated rule the session's newly published readings.

    Only readings published up to and including ``watermark`` and beyond the
    rule's merge-order checkpoint are evaluated, in global
    ``(observed_at, source_id, seq)`` order.  Quarantined readings are
    ``status='quarantined'`` and therefore never appear here.
    """
    rules = conn.execute(
        "SELECT * FROM correlated_rules WHERE session_id = ?", (session_id,)
    ).fetchall()
    for rule in rules:
        readings = _new_published_readings(conn, session_id, rule, watermark)
        for reading in readings:
            conditions = _fetch_conditions(conn, rule["rule_id"])
            _apply_reading(conn, rule, conditions, reading, triggered_at)
            rule = _fetch_rule(conn, session_id, rule["rule_id"])


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------

def _parse_hits(raw: str) -> list[dict[str, Any]]:
    return json.loads(raw)


def list_current(
    conn: sqlite3.Connection, session_id: str
) -> list[dict[str, Any]]:
    """Rules currently in the ``correlated`` state with their enter detail."""
    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            "SELECT r.*, e.id AS event_id, e.hits AS event_hits,"
            " e.triggered_at AS event_triggered_at"
            " FROM correlated_rules r"
            " JOIN correlated_events e ON e.rule_id = r.rule_id"
            "  AND e.completed_by_observed_at = r.correlated_since_observed_at"
            "  AND e.completed_by_source_id = r.correlated_since_source_id"
            "  AND e.completed_by_seq = r.correlated_since_seq"
            " WHERE r.session_id = ? AND r.state = 'correlated'"
            " ORDER BY r.correlated_since_observed_at,"
            " r.correlated_since_source_id, r.correlated_since_seq, r.rule_id",
            (session_id,),
        ).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            out.append({
                "rule_id": row["rule_id"],
                "session_id": session_id,
                "state": STATE_CORRELATED,
                "correlated_entered": {
                    "id": row["event_id"],
                    "event_type": EVENT_TYPE,
                    "completed_by": {
                        "source_id": row["correlated_since_source_id"],
                        "seq": row["correlated_since_seq"],
                        "observed_at": format_event_time(
                            parse_event_time(row["correlated_since_observed_at"])
                        ),
                    },
                    "conditions": _parse_hits(row["event_hits"]),
                    "triggered_at": format_event_time(
                        parse_event_time(row["event_triggered_at"])
                    ),
                },
                "last_observed_at": _format_observed(row["last_observed_at"]),
                "last_source_id": row["last_source_id"],
                "last_seq": row["last_seq"],
            })
    return out


def _event_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "rule_id": row["rule_id"],
        "session_id": row["session_id"],
        "event_type": row["event_type"],
        "completed_by": {
            "source_id": row["completed_by_source_id"],
            "seq": row["completed_by_seq"],
            "observed_at": format_event_time(
                parse_event_time(row["completed_by_observed_at"])
            ),
        },
        "conditions": _parse_hits(row["hits"]),
        "triggered_at": format_event_time(parse_event_time(row["triggered_at"])),
    }


def list_event_history(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
    rule_id: Optional[str] = None,
) -> dict[str, Any]:
    """``correlated_entered`` events, newest first, with keyset pagination."""
    where = "session_id = ?"
    params: list[Any] = [session_id]
    if rule_id is not None:
        where += " AND rule_id = ?"
        params.append(rule_id)
    if cursor:
        (after_id,) = decode_cursor(cursor, 1)
        if not isinstance(after_id, int):
            raise BadRequestError("malformed cursor")
        where += " AND id < ?"
        params.append(after_id)

    with read_transaction(conn):
        _load_session(conn, session_id)
        if rule_id is not None and conn.execute(
            "SELECT 1 FROM correlated_rules WHERE rule_id = ? AND session_id = ?",
            (rule_id, session_id),
        ).fetchone() is None:
            raise NotFoundError(
                f"correlated rule {rule_id!r} not found in session {session_id!r}"
            )
        rows = conn.execute(
            f"SELECT * FROM correlated_events WHERE {where}"
            " ORDER BY id DESC LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = encode_cursor((page[-1]["id"],)) if has_more and page else None
    return {
        "session_id": session_id,
        "next_cursor": next_cursor,
        "events": [_event_out(r) for r in page],
    }
