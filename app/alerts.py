"""Post-publish threshold alert state machines.

Each alert rule binds to one source inside a session and runs an independent
two-state machine over that source's **published** readings only:

* ``normal``  -- consecutive breach readings (above/below the enter threshold)
  are counted; when the count reaches ``enter_consecutive`` the machine
  transitions to ``active`` and an ``entered`` event is emitted.
* ``active``  -- consecutive recovery readings (beyond the *recover* threshold,
  back on the safe side of the hysteresis band) are counted; when the count
  reaches ``recover_consecutive`` the machine returns to ``normal`` and a
  ``recovered`` event is emitted.

The enter/recover thresholds form a hysteresis band so that a value jittering
around a single boundary cannot flap the state. Readings inside the band,
non-numeric values, or missing values reset the current streak but never
change the state.

The machine is a pure function of the published timeline in deterministic
order ``(observed_at, source_id, seq)``: buffered readings do not advance it
and late readings diverted to quarantine can never touch it (they never
become published).  Rules created after readings already exist are therefore
initialised by replaying the existing published stream, which yields exactly
the state the rule would have had if created up front.
"""
from __future__ import annotations

import json
import math
import sqlite3
import uuid
from typing import Any, Optional

from .cursor import decode_cursor, encode_cursor
from .database import read_transaction, transaction, utc_now
from .errors import BadRequestError, ConflictError, NotFoundError
from .time_utils import format_event_time, parse_event_time, store_event_time

STATE_NORMAL = "normal"
STATE_ACTIVE = "active"

DIRECTIONS = ("above", "below")
EVENT_TYPES = ("entered", "recovered")


# ---------------------------------------------------------------------------
# pure state machine
# ---------------------------------------------------------------------------

def _is_breach(direction: str, value: float, threshold: float) -> bool:
    """Strictly beyond the enter threshold (boundary equality does not count)."""
    return value > threshold if direction == "above" else value < threshold


def _is_recovered(direction: str, value: float, threshold: float) -> bool:
    """Safely back across the recover threshold, out of the hysteresis band."""
    return value < threshold if direction == "above" else value > threshold


def transition(
    state: str,
    streak: int,
    value: Optional[float],
    *,
    direction: str,
    enter_threshold: float,
    recover_threshold: float,
    enter_consecutive: int,
    recover_consecutive: int,
) -> tuple[str, int, Optional[str]]:
    """Advance the machine by one published reading.

    Returns ``(new_state, new_streak, event_type)`` where ``event_type`` is
    ``'entered'`` / ``'recovered'`` on a transition and ``None`` otherwise.
    ``value=None`` means the reading could not supply a comparable number:
    the streak resets and the state is left untouched.
    """
    if value is None:
        return state, 0, None

    if state == STATE_ACTIVE:
        if _is_recovered(direction, value, recover_threshold):
            nxt = streak + 1
            if nxt >= recover_consecutive:
                return STATE_NORMAL, 0, "recovered"
            return STATE_ACTIVE, nxt, None
        # Still inside the band or still breaching: recovery streak restarts.
        return STATE_ACTIVE, 0, None

    # state == normal
    if _is_breach(direction, value, enter_threshold):
        nxt = streak + 1
        if nxt >= enter_consecutive:
            return STATE_ACTIVE, 0, "entered"
        return STATE_NORMAL, nxt, None
    return STATE_NORMAL, 0, None


def extract_numeric(payload: Optional[str], value_key: Optional[str]) -> Optional[float]:
    """Pull a finite, non-boolean number out of the stored JSON payload.

    With ``value_key=None`` the reading value itself must be numeric;
    otherwise the top-level object key is read.  Missing keys, non-numeric
    payloads, booleans and NaN/Infinity all yield ``None`` (streak reset).
    """
    if payload is None:
        return None
    try:
        data = json.loads(payload)
    except (ValueError, TypeError):
        return None
    if value_key is not None:
        if not isinstance(data, dict):
            return None
        data = data.get(value_key)
    # bool is a subclass of int -- exclude it explicitly (true > threshold is
    # not a meaningful breach).
    if isinstance(data, bool) or not isinstance(data, (int, float)):
        return None
    number = float(data)
    return number if math.isfinite(number) else None


# ---------------------------------------------------------------------------
# persistence helpers
# ---------------------------------------------------------------------------

def _load_session(conn: sqlite3.Connection, session_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    if row is None:
        raise NotFoundError(f"session {session_id!r} not found")
    return row


def _rule_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "rule_id": row["rule_id"],
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "direction": row["direction"],
        "enter_threshold": row["enter_threshold"],
        "recover_threshold": row["recover_threshold"],
        "enter_consecutive": row["enter_consecutive"],
        "recover_consecutive": row["recover_consecutive"],
        "value_key": row["value_key"],
        "state": row["state"],
        "streak": row["streak"],
        "created_at": format_event_time(parse_event_time(row["created_at"])),
    }


def _event_out(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "rule_id": row["rule_id"],
        "session_id": row["session_id"],
        "source_id": row["source_id"],
        "event_type": row["event_type"],
        "seq": row["seq"],
        "observed_at": format_event_time(parse_event_time(row["observed_at"])),
        "value": row["value"],
        "threshold": row["threshold"],
        "triggered_at": format_event_time(parse_event_time(row["triggered_at"])),
    }


# ---------------------------------------------------------------------------
# rule management
# ---------------------------------------------------------------------------

def create_rule(
    conn: sqlite3.Connection,
    session_id: str,
    source_id: str,
    direction: str,
    enter_threshold: float,
    recover_threshold: float,
    enter_consecutive: int,
    recover_consecutive: int,
    value_key: Optional[str] = None,
    rule_id: Optional[str] = None,
) -> dict[str, Any]:
    # Semantic validation that pydantic cannot express cross-field: the
    # hysteresis band has to open in the correct direction.
    if direction == "above" and not recover_threshold < enter_threshold:
        raise BadRequestError(
            "for direction 'above', recover_threshold must be strictly below"
            " enter_threshold (hysteresis band)"
        )
    if direction == "below" and not recover_threshold > enter_threshold:
        raise BadRequestError(
            "for direction 'below', recover_threshold must be strictly above"
            " enter_threshold (hysteresis band)"
        )

    rid = rule_id or uuid.uuid4().hex
    created_at = store_event_time(utc_now())
    with transaction(conn):
        session = _load_session(conn, session_id)
        allowed_sources = set(json.loads(session["source_ids"]))
        if source_id not in allowed_sources:
            raise BadRequestError(
                f"unknown source_id {source_id!r} for this session;"
                f" declared sources: {sorted(allowed_sources)}"
            )
        try:
            conn.execute(
                "INSERT INTO alert_rules (rule_id, session_id, source_id, direction,"
                " enter_threshold, recover_threshold, enter_consecutive,"
                " recover_consecutive, value_key, created_at, state, streak,"
                " active_since_observed_at, active_since_seq, last_observed_at, last_seq)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'normal', 0,"
                " NULL, NULL, NULL, NULL)",
                (
                    rid, session_id, source_id, direction, enter_threshold,
                    recover_threshold, enter_consecutive, recover_consecutive,
                    value_key, created_at,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise ConflictError(f"alert rule {rid!r} already exists") from exc

        # Replay everything already published for this source so the machine
        # state (and any events) match the published timeline regardless of
        # when the rule was created.
        rule = conn.execute(
            "SELECT * FROM alert_rules WHERE rule_id = ?", (rid,)
        ).fetchone()
        _backfill_rule(conn, rule, created_at)
        row = conn.execute(
            "SELECT * FROM alert_rules WHERE rule_id = ?", (rid,)
        ).fetchone()
    return _rule_out(row)


def _backfill_rule(conn: sqlite3.Connection, rule: sqlite3.Row, triggered_at: str) -> None:
    """Replay existing published readings for a freshly inserted rule."""
    rows = conn.execute(
        "SELECT observed_at, seq, payload FROM readings"
        " WHERE session_id = ? AND source_id = ? AND status = 'published'"
        " ORDER BY observed_at, source_id, seq",
        (rule["session_id"], rule["source_id"]),
    ).fetchall()
    for reading in rows:
        _apply_reading(conn, rule, reading, triggered_at)
        rule = conn.execute(
            "SELECT * FROM alert_rules WHERE rule_id = ?", (rule["rule_id"],)
        ).fetchone()


def list_rules(conn: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        rows = conn.execute(
            "SELECT * FROM alert_rules WHERE session_id = ?"
            " ORDER BY created_at, rule_id",
            (session_id,),
        ).fetchall()
    return [_rule_out(r) for r in rows]


def get_rule(conn: sqlite3.Connection, session_id: str, rule_id: str) -> dict[str, Any]:
    with read_transaction(conn):
        _load_session(conn, session_id)
        row = conn.execute(
            "SELECT * FROM alert_rules WHERE rule_id = ? AND session_id = ?",
            (rule_id, session_id),
        ).fetchone()
    if row is None:
        raise NotFoundError(f"alert rule {rule_id!r} not found in session {session_id!r}")
    return _rule_out(row)


# ---------------------------------------------------------------------------
# state machine advancement (called from the ingest write transaction)
# ---------------------------------------------------------------------------

def advance_alerts(
    conn: sqlite3.Connection,
    session_id: str,
    watermark: str,
    triggered_at: str,
) -> None:
    """Feed every rule its source's newly published readings.

    Only readings published *up to and including this watermark* and not yet
    seen by a rule (``observed_at > rule.last_observed_at`` or later seq on a
    tie) are evaluated.  Quarantined readings are status='quarantined' and
    therefore can never appear here.
    """
    rules = conn.execute(
        "SELECT * FROM alert_rules WHERE session_id = ?", (session_id,)
    ).fetchall()
    for rule in rules:
        if rule["last_observed_at"] is None:
            where = "r.observed_at <= ?"
            params: tuple[Any, ...] = (watermark,)
        else:
            where = (
                "(r.observed_at > ?"
                " OR (r.observed_at = ? AND r.seq > ?))"
                " AND r.observed_at <= ?"
            )
            params = (
                rule["last_observed_at"],
                rule["last_observed_at"],
                rule["last_seq"],
                watermark,
            )
        readings = conn.execute(
            f"SELECT r.observed_at, r.seq, r.payload FROM readings r"
            f" WHERE r.session_id = ? AND r.source_id = ? AND r.status = 'published'"
            f" AND {where}"
            " ORDER BY r.observed_at, r.source_id, r.seq",
            (session_id, rule["source_id"], *params),
        ).fetchall()
        for reading in readings:
            _apply_reading(conn, rule, reading, triggered_at)
            rule = conn.execute(
                "SELECT * FROM alert_rules WHERE rule_id = ?", (rule["rule_id"],)
            ).fetchone()


def _apply_reading(
    conn: sqlite3.Connection,
    rule: sqlite3.Row,
    reading: sqlite3.Row,
    triggered_at: str,
) -> None:
    """Evaluate one published reading against one rule and persist the result."""
    value = extract_numeric(reading["payload"], rule["value_key"])
    new_state, new_streak, event_type = transition(
        rule["state"],
        rule["streak"],
        value,
        direction=rule["direction"],
        enter_threshold=rule["enter_threshold"],
        recover_threshold=rule["recover_threshold"],
        enter_consecutive=rule["enter_consecutive"],
        recover_consecutive=rule["recover_consecutive"],
    )

    active_since = rule["active_since_observed_at"]
    active_seq = rule["active_since_seq"]
    if event_type == "entered":
        active_since = reading["observed_at"]
        active_seq = reading["seq"]
    elif event_type == "recovered":
        active_since = None
        active_seq = None

    conn.execute(
        "UPDATE alert_rules SET state = ?, streak = ?,"
        " active_since_observed_at = ?, active_since_seq = ?,"
        " last_observed_at = ?, last_seq = ? WHERE rule_id = ?",
        (
            new_state, new_streak, active_since, active_seq,
            reading["observed_at"], reading["seq"], rule["rule_id"],
        ),
    )

    if event_type is not None:
        threshold = (
            rule["enter_threshold"] if event_type == "entered"
            else rule["recover_threshold"]
        )
        conn.execute(
            "INSERT INTO alert_events (rule_id, session_id, source_id, event_type,"
            " seq, observed_at, value, threshold, triggered_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                rule["rule_id"], rule["session_id"], rule["source_id"], event_type,
                reading["seq"], reading["observed_at"], value, threshold, triggered_at,
            ),
        )


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------

def list_current_alerts(
    conn: sqlite3.Connection,
    session_id: str,
    source_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Rules currently in the ``active`` state, with their enter event detail."""
    with read_transaction(conn):
        session = _load_session(conn, session_id)
        allowed = set(json.loads(session["source_ids"]))
        if source_id is not None and source_id not in allowed:
            raise BadRequestError(
                f"unknown source_id {source_id!r} for this session;"
                f" declared sources: {sorted(allowed)}"
            )

        where = "r.state = 'active' AND r.session_id = ?"
        params: list[Any] = [session_id]
        if source_id is not None:
            where += " AND r.source_id = ?"
            params.append(source_id)
        rows = conn.execute(
            f"SELECT r.*, e.observed_at AS entered_observed_at, e.seq AS entered_seq,"
            f" e.value AS entered_value, e.threshold AS entered_threshold,"
            f" e.triggered_at AS entered_triggered_at"
            f" FROM alert_rules r"
            f" JOIN alert_events e ON e.rule_id = r.rule_id"
            f"  AND e.event_type = 'entered'"
            f"  AND e.observed_at = r.active_since_observed_at"
            f"  AND e.seq = r.active_since_seq"
            f" WHERE {where}"
            " ORDER BY e.observed_at, e.rule_id",
            params,
        ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append({
            "rule_id": row["rule_id"],
            "session_id": session_id,
            "source_id": row["source_id"],
            "direction": row["direction"],
            "enter_threshold": row["enter_threshold"],
            "recover_threshold": row["recover_threshold"],
            "enter_consecutive": row["enter_consecutive"],
            "recover_consecutive": row["recover_consecutive"],
            "value_key": row["value_key"],
            "state": row["state"],
            "streak": row["streak"],
            "entered": {
                "seq": row["entered_seq"],
                "observed_at": format_event_time(
                    parse_event_time(row["entered_observed_at"])
                ),
                "value": row["entered_value"],
                "threshold": row["entered_threshold"],
                "triggered_at": format_event_time(
                    parse_event_time(row["entered_triggered_at"])
                ),
            },
            "last_observed_at": (
                format_event_time(parse_event_time(row["last_observed_at"]))
                if row["last_observed_at"] else None
            ),
            "last_seq": row["last_seq"],
        })
    return out


def list_event_history(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int,
    cursor: Optional[str] = None,
    source_id: Optional[str] = None,
    rule_id: Optional[str] = None,
    event_type: Optional[str] = None,
) -> dict[str, Any]:
    where = "session_id = ?"
    params: list[Any] = [session_id]
    if source_id is not None:
        where += " AND source_id = ?"
        params.append(source_id)
    if rule_id is not None:
        where += " AND rule_id = ?"
        params.append(rule_id)
    if event_type is not None:
        where += " AND event_type = ?"
        params.append(event_type)
    if cursor:
        (after_id,) = decode_cursor(cursor, 1)
        if not isinstance(after_id, int):
            raise BadRequestError("malformed cursor")
        where += " AND id < ?"
        params.append(after_id)

    with read_transaction(conn):
        session = _load_session(conn, session_id)
        allowed = set(json.loads(session["source_ids"]))
        if source_id is not None and source_id not in allowed:
            raise BadRequestError(
                f"unknown source_id {source_id!r} for this session;"
                f" declared sources: {sorted(allowed)}"
            )
        if rule_id is not None and conn.execute(
            "SELECT 1 FROM alert_rules WHERE rule_id = ? AND session_id = ?",
            (rule_id, session_id),
        ).fetchone() is None:
            raise NotFoundError(f"alert rule {rule_id!r} not found in session {session_id!r}")

        rows = conn.execute(
            f"SELECT * FROM alert_events WHERE {where}"
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
