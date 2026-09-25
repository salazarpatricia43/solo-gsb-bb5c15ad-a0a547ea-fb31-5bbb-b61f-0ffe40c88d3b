"""Read-only release planning for buffered readings.

``GET /sessions/{session_id}/release-plan?count=N`` answers, for an *open*
session: "what is the smallest per-source clock advancement that would
release the earliest N buffered readings?"  Nothing is mutated.

The whole computation runs inside one deferred SQLite read transaction, so
the buffered prefix, the per-source clocks and the session's allowed
lateness all come from a single consistent snapshot:

* The earliest N buffered readings are taken in the deterministic global
  order ``(observed_at, source_id, seq)``; published and quarantined rows
  never participate.
* The target watermark is one microsecond beyond the Nth reading's
  ``observed_at`` -- publication is strict (``observed_at < watermark``)
  and stored timestamps have microsecond resolution, so this is the
  smallest watermark that releases the Nth reading.
* Since ``watermark = min(source clocks) - allowed_lateness``, every
  source's effective clock must reach ``target watermark +
  allowed_lateness``.  Per source the plan reports the current clock, that
  required clock, the remaining ``advance_seconds`` and a ``blocker``.
* A source that never reported has ``current = null`` and no computable
  advance.  A *sealed* source whose final clock falls short can never
  advance again, so the plan must not suggest advancing it: the source
  reports ``blocker = "sealed_below_required"`` with no advance, and the
  plan as a whole comes back ``attainable = false`` with a reason.
"""
from __future__ import annotations

import sqlite3
from datetime import timedelta
from typing import Any, Optional

from .database import read_transaction
from .errors import ConflictError, UnprocessableEntityError
from .service import _load_session, _source_maxes_from, _source_rows
from .time_utils import format_event_time, parse_event_time, store_event_time

__all__ = ["compute_release_plan"]

# Publication is strict (observed_at < watermark) and stored timestamps have
# microsecond resolution, so one microsecond beyond the Nth buffered reading
# is the smallest target watermark that releases it.
_EPSILON = timedelta(microseconds=1)

# Per-source blocker reasons reported by the plan.
BLOCKER_BELOW_REQUIRED = "below_required"
BLOCKER_NOT_REPORTED = "not_reported"
BLOCKER_SEALED_BELOW_REQUIRED = "sealed_below_required"


def compute_release_plan(
    conn: sqlite3.Connection, session_id: str, count: int
) -> dict[str, Any]:
    """Minimal per-source clock advancement releasing ``count`` readings.

    Read-only: everything is derived inside one read snapshot and nothing is
    written.  ``count`` is range-validated by the route (1..100); here it is
    only checked against the number of buffered readings (422 when larger).
    A closed session has no buffered readings left to plan for (409).
    """
    with read_transaction(conn):
        session = _load_session(conn, session_id)
        if session["status"] == "closed":
            # The final seal already tail-published every buffered reading.
            raise ConflictError(
                f"session {session_id!r} is closed: all buffered readings were"
                " published by the final seal"
            )
        lateness = session["allowed_lateness_seconds"]

        buffered = conn.execute(
            "SELECT COUNT(*) AS c FROM readings"
            " WHERE session_id = ? AND status = 'buffered'",
            (session_id,),
        ).fetchone()["c"]
        if count > buffered:
            raise UnprocessableEntityError(
                f"count={count} exceeds the number of buffered readings"
                f" ({buffered}) in session {session_id!r}"
            )

        # The Nth earliest buffered reading in the deterministic global order
        # sets the target: the watermark must move strictly past its event
        # time.  (Published and quarantined rows are excluded by the status
        # filter.)
        nth = conn.execute(
            "SELECT observed_at FROM readings"
            " WHERE session_id = ? AND status = 'buffered'"
            " ORDER BY observed_at, source_id, seq LIMIT 1 OFFSET ?",
            (session_id, count - 1),
        ).fetchone()
        target_wm = parse_event_time(nth["observed_at"]) + _EPSILON
        required_dt = target_wm + timedelta(seconds=lateness)
        # Fixed-width stored form: lexicographic comparison is chronological.
        required_store = store_event_time(required_dt)

        rows = _source_rows(conn, session_id)
        source_max = _source_maxes_from(rows)

        sources: list[dict[str, Any]] = []
        sealed_short: list[str] = []
        for r in rows:
            source_id = r["source_id"]
            current = source_max[source_id]
            sealed = r["final_observed_at"] is not None
            advance: Optional[float]
            blocker: Optional[str]
            if current is None:
                # No clock yet: the source must start reporting and reach the
                # required time; a delta cannot be computed from nothing.
                advance = None
                blocker = BLOCKER_NOT_REPORTED
            elif current >= required_store:
                advance = 0.0
                blocker = None
            elif sealed:
                # A sealed source's clock is final: never suggest advancing
                # it.  This makes the whole plan unattainable.
                advance = None
                blocker = BLOCKER_SEALED_BELOW_REQUIRED
                sealed_short.append(source_id)
            else:
                advance = (required_dt - parse_event_time(current)).total_seconds()
                blocker = BLOCKER_BELOW_REQUIRED
            sources.append(
                {
                    "source_id": source_id,
                    "current": (
                        format_event_time(parse_event_time(current))
                        if current
                        else None
                    ),
                    "required": format_event_time(required_dt),
                    "advance_seconds": advance,
                    "blocker": blocker,
                }
            )

        attainable = not sealed_short
        reason = None
        if not attainable:
            reason = (
                f"source(s) {sealed_short} are sealed with a final_observed_at"
                " below the required effective max; sealed clocks cannot"
                " advance, so no clock-advancement plan can release the"
                f" earliest {count} buffered reading(s)"
            )

        return {
            "session_id": session_id,
            "count": count,
            "buffered": buffered,
            "allowed_lateness_seconds": lateness,
            "target_watermark": format_event_time(target_wm),
            "attainable": attainable,
            "reason": reason,
            "sources": sources,
        }
