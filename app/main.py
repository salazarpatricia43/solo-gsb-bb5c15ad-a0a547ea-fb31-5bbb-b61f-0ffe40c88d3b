"""FastAPI application: lab sensor timeline merge API."""
from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse

from . import (
    alerts,
    arrival_latency,
    checkpoints,
    correlated,
    exposure,
    local_spikes,
    quarantine_audit,
    release_plan,
    retention,
    service,
    source_gaps,
)
from .config import settings
from .database import get_db, init_db
from .models import (
    AlertHistoryResponse,
    AlertRuleOut,
    AlertRulesResponse,
    ArrivalLatencyResponse,
    CheckpointOut,
    CheckpointStreamResponse,
    CheckpointsResponse,
    CorrelatedHistoryResponse,
    CorrelatedRuleOut,
    CorrelatedRulesResponse,
    CreateAlertRuleIn,
    CreateCheckpointIn,
    CreateCorrelatedRuleIn,
    CreateSessionIn,
    CreatedSessionResponse,
    CurrentAlertsResponse,
    CurrentCorrelatedAlertsResponse,
    ExposureResponse,
    LocalSpikesResponse,
    PullResponse,
    QuarantineAuditResponse,
    QuarantineResponse,
    ReadingIn,
    ReleasePlanResponse,
    ReportResponse,
    RetentionIn,
    RetentionResponse,
    SealSourceIn,
    SessionOut,
    SourceGapsResponse,
)
from .time_utils import TimestampError


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Database directory + schema are created as part of startup.
    init_db(app.state.db_path)
    yield


app = FastAPI(
    title="Lab Sensor Timeline Merge API",
    version="1.11.0",
    description=(
        "Out-of-order sensor reporting with per-session fixed source lists, "
        "a slowest-source watermark (minus allowed lateness), deterministic "
        "publish order and late-reading quarantine. Sources of a finite "
        "collection can be sealed with their final observed event time; "
        "sealing the last source tail-publishes every remaining buffered "
        "reading and closes the session atomically. Published readings also "
        "drive per-source threshold alert state machines (enter / active / "
        "recovered) with hysteresis and consecutive-count debouncing, plus "
        "cross-source correlated rules that fire once when 2-4 sources breach "
        "their own thresholds together inside a shared event-time window. A "
        "read-only release-plan endpoint computes, in one snapshot, the "
        "minimal per-source clock advancement needed to release the earliest "
        "N buffered readings. A read-only quarantine-audit endpoint previews, "
        "in one snapshot, where each quarantined reading would land if it "
        "were inserted into the published timeline by event time, which "
        "published neighbours and checkpoint it would affect. A read-only "
        "source-gaps endpoint scans per-source adjacent published readings in "
        "one snapshot and reports pairs whose event-time gap strictly exceeds "
        "a threshold, with inclusive time filters and stable cursor paging. A "
        "read-only arrival-latency endpoint profiles, in one snapshot, each "
        "reading's delay (first received at minus event time) with inclusive "
        "event-time and source filters, returning per-source sample count, "
        "minimum, maximum and arithmetic mean; negative latencies are kept. A "
        "read-only exposure endpoint measures, in one snapshot, the seconds of "
        "a source's piecewise-constant published timeline inside a half-open "
        "[from, to) interval that are strictly above a threshold, alongside "
        "the valid-sample coverage and unknown seconds (missing, non-finite "
        "and boolean values do not carry an older value over). A read-only "
        "local-spikes endpoint flags, in one snapshot, published readings in "
        "an inclusive event-time interval whose finite value deviates from "
        "the median of their in-window neighbours by strictly more than a "
        "sensitivity coefficient times the neighbours' median absolute "
        "deviation (MAD), skipping missing/non-finite/boolean values and "
        "leaving readings with fewer than three neighbours undecided."
    ),
    lifespan=lifespan,
)
app.state.db_path = settings.db_path


@app.exception_handler(service.ServiceError)
async def service_error_handler(request: Request, exc: service.ServiceError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})


@app.exception_handler(TimestampError)
async def timestamp_error_handler(request: Request, exc: TimestampError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(sqlite3.Error)
async def sqlite_error_handler(request: Request, exc: sqlite3.Error) -> JSONResponse:
    return JSONResponse(status_code=500, content={"detail": f"database error: {exc}"})


def _error(status: int, detail: Any) -> JSONResponse:
    return JSONResponse(status_code=status, content={"detail": detail})


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/sessions", response_model=CreatedSessionResponse, status_code=201, tags=["sessions"])
def create_session(
    body: CreateSessionIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> CreatedSessionResponse:
    result = service.create_session(
        conn,
        source_ids=body.source_ids,
        allowed_lateness_seconds=body.allowed_lateness_seconds,
        session_id=body.session_id,
    )
    return CreatedSessionResponse(**result)


@app.get("/sessions/{session_id}", response_model=SessionOut, tags=["sessions"])
def get_session(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> SessionOut:
    return SessionOut(**service.get_session(conn, session_id))


@app.post("/sessions/{session_id}/sources/{source_id}/seal",
           response_model=SessionOut, status_code=200, tags=["sessions"])
def seal_source(
    session_id: str,
    source_id: str,
    body: SealSourceIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> SessionOut:
    """Seal one finite-collection source at its final observed event time.

    ``final_observed_at`` must not be earlier than the largest event time
    already seen for the source.  Retrying with the same value is idempotent;
    a different value returns 409.  Once the last source is sealed the
    remaining buffered readings are tail-published in deterministic order,
    both alert kinds are advanced and the session becomes ``closed`` -- all in
    the same serialised write transaction.
    """
    result = service.seal_source(
        conn,
        session_id=session_id,
        source_id=source_id,
        final_observed_at=body.final_observed_at,
    )
    return SessionOut(**result)


@app.post(
    "/sessions/{session_id}/readings",
    response_model=ReportResponse,
    status_code=200,
    tags=["readings"],
)
async def report_readings(
    session_id: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
) -> ReportResponse:
    """Batch-report readings.

    Accepts either ``{"readings": [ ... ]}`` or a bare JSON array.
    Semantic failures (bad timestamps, unknown sources) return 400;
    structural field failures return 422.
    """
    try:
        payload = await request.json()
    except Exception:
        return _error(400, "request body must be valid JSON")

    if isinstance(payload, dict) and isinstance(payload.get("readings"), list):
        raw = payload["readings"]
    elif isinstance(payload, list):
        raw = payload
    else:
        return _error(400, "body must be {'readings': [...]} or a JSON array")

    if not raw:
        return _error(400, "readings batch must not be empty")

    items: list[dict] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            return _error(400, f"reading at index {idx} must be an object")
        try:
            parsed = ReadingIn(**item)
        except Exception as exc:
            detail = exc.errors() if hasattr(exc, "errors") else str(exc)
            return _error(422, f"invalid reading at index {idx}: {detail}")
        items.append(parsed.model_dump())

    # TimestampError and ServiceError are handled by the registered handlers.
    result = service.ingest_batch(conn, session_id, items)
    return ReportResponse(**result)


@app.get(
    "/sessions/{session_id}/stream",
    response_model=PullResponse,
    tags=["readings"],
)
def pull_stream(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    conn: sqlite3.Connection = Depends(get_db),
) -> PullResponse:
    """Published readings only, in deterministic timeline order."""
    return PullResponse(**service.list_published(conn, session_id, limit, cursor))


@app.get(
    "/sessions/{session_id}/quarantine",
    response_model=QuarantineResponse,
    tags=["readings"],
)
def pull_quarantine(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    conn: sqlite3.Connection = Depends(get_db),
) -> QuarantineResponse:
    """Late readings that arrived behind the already-published watermark."""
    return QuarantineResponse(**service.list_quarantined(conn, session_id, limit, cursor))


@app.post(
    "/sessions/{session_id}/quarantine-audit",
    response_model=QuarantineAuditResponse,
    tags=["readings"],
)
async def quarantine_impact_audit(
    session_id: str,
    request: Request,
    conn: sqlite3.Connection = Depends(get_db),
) -> QuarantineAuditResponse:
    """Read-only impact rehearsal for the session's quarantined readings.

    The optional JSON body may carry ``source_id`` (one declared source),
    inclusive event-time bounds ``from`` / ``to`` and a ``limit``
    (1-1000, default 100).  Inside one SQLite read snapshot, every matching
    quarantined reading is placed against the current published timeline in
    deterministic ``(observed_at, source_id, seq)`` order: the response
    reports its zero-based insertion position, the predecessor/successor
    published keysets, the event-time gaps to them and the earliest
    checkpoint whose resume would gain the reading.  Nothing is published,
    quarantined, alerted or checkpointed -- the call never writes.  Malformed
    JSON, a non-object body, an invalid interval/limit or an undeclared
    source return 400; an unknown session returns 404.
    """
    # ``from``/``to``/``limit``/``source_id`` are all optional; every semantic
    # error must be 400 (not Pydantic's 422), so the body is parsed here and
    # validated inside the audit module, like the readings endpoint.
    try:
        body = await request.json()
    except Exception:
        return _error(400, "request body must be valid JSON")
    if not isinstance(body, dict):
        return _error(400, "body must be a JSON object")

    # TimestampError and ServiceError are handled by the registered handlers.
    result = quarantine_audit.quarantine_audit(
        conn,
        session_id,
        source_id=body.get("source_id"),
        from_=body.get("from"),
        to=body.get("to"),
        limit=body.get("limit"),
    )
    return QuarantineAuditResponse(**result)


@app.get(
    "/sessions/{session_id}/arrival-latency",
    response_model=ArrivalLatencyResponse,
    tags=["readings"],
)
def get_arrival_latency(
    session_id: str,
    source_id: Optional[str] = Query(
        None, description="Restrict the profile to one declared source of the session"
    ),
    from_: Optional[str] = Query(
        None,
        alias="from",
        description="Inclusive event-time lower bound (ISO 8601)",
    ),
    to: Optional[str] = Query(
        None, description="Inclusive event-time upper bound (ISO 8601)"
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> ArrivalLatencyResponse:
    """Read-only per-source arrival-latency profile.

    Inside one SQLite read snapshot, every matching reading of the session
    (buffered, published or quarantined) contributes the latency
    ``first_received_at - observed_at``.  Optional ``source_id`` and
    inclusive ``from``/``to`` event-time bounds filter the readings.  For
    each source that has at least one matching reading the response reports
    the sample count, minimum, maximum and arithmetic mean latency in
    seconds; negative latencies are kept as-is and sources with no matching
    reading are omitted.  Results are ordered by ``source_id``.  The call
    never writes.  An unknown source or invalid bounds return 400; an
    unknown session returns 404.
    """
    # Every parameter error must be 400 (not FastAPI's default 422), so the
    # query params are parsed/validated inside the module, like source-gaps.
    return ArrivalLatencyResponse(
        **arrival_latency.arrival_latency_profile(
            conn,
            session_id,
            source_id=source_id,
            from_=from_,
            to=to,
        )
    )


@app.get(
    "/sessions/{session_id}/exposure",
    response_model=ExposureResponse,
    tags=["readings"],
)
def get_source_exposure(
    session_id: str,
    source_id: Optional[str] = Query(
        None, description="One declared source of the session to measure"
    ),
    from_: Optional[str] = Query(
        None,
        alias="from",
        description="Inclusive event-time interval start (ISO 8601)",
    ),
    to: Optional[str] = Query(
        None, description="Exclusive event-time interval end (ISO 8601)"
    ),
    threshold: Optional[str] = Query(
        None, description="Finite numeric threshold; values strictly above it count"
    ),
    value_key: Optional[str] = Query(
        None,
        description="Top-level key read from object-valued readings (default: value itself)",
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> ExposureResponse:
    """Read-only over-threshold exposure for one source's published timeline.

    Inside one SQLite read snapshot, the source's published readings define a
    piecewise-constant value timeline over the half-open interval
    ``[from, to)``: each reading's value holds until the source's next
    published reading or ``to``, and ``from`` inherits the latest reading at
    or before it (same-time readings resolve in ``seq`` order, last one
    wins).  The response reports the seconds strictly above ``threshold``,
    the seconds covered by any valid finite sample and the unknown seconds;
    the over-threshold ratio is ``null`` with no coverage, otherwise above /
    covered.  Missing, non-finite and boolean values create unknown
    stretches instead of carrying an older value over.  Buffered and
    quarantined readings never participate and nothing is written.  An
    invalid interval, a non-finite threshold or an unknown source returns
    400; an unknown session returns 404.
    """
    # Every parameter error must be 400 (not FastAPI's default 422), so the
    # query params are parsed/validated inside the module, like source-gaps.
    return ExposureResponse(
        **exposure.source_exposure(
            conn,
            session_id,
            source_id=source_id,
            from_=from_,
            to=to,
            threshold=threshold,
            value_key=value_key,
        )
    )


@app.get(
    "/sessions/{session_id}/local-spikes",
    response_model=LocalSpikesResponse,
    tags=["readings"],
)
def get_local_spikes(
    session_id: str,
    source_id: Optional[str] = Query(
        None, description="One declared source of the session to analyse"
    ),
    from_: Optional[str] = Query(
        None,
        alias="from",
        description="Inclusive event-time interval start (ISO 8601)",
    ),
    to: Optional[str] = Query(
        None,
        description="Inclusive event-time interval end (ISO 8601)",
    ),
    window_seconds: Optional[str] = Query(
        None,
        description=(
            "Positive finite neighbour window in seconds: other valid readings"
            " whose event-time distance is at most this are neighbours"
        ),
    ),
    sensitivity: Optional[str] = Query(
        None,
        description=(
            "Positive finite coefficient; flag when deviation is strictly"
            " greater than sensitivity * MAD"
        ),
    ),
    value_key: Optional[str] = Query(
        None,
        description="Top-level key read from object-valued readings (default: value itself)",
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> LocalSpikesResponse:
    """Read-only local-spike analysis of one source's published readings.

    Inside one SQLite read snapshot, the source's published readings in the
    inclusive ``[from, to]`` interval are stably ordered by
    ``(observed_at, seq)`` and only finite non-boolean numeric values
    participate (an optional ``value_key`` names the top-level key read from
    object values; missing and boolean values are skipped).  For each valid
    reading the neighbours are the other valid readings whose event-time
    distance is at most ``window_seconds``.  With fewer than three neighbours
    the reading is not judged; otherwise its local median and the neighbours'
    median absolute deviation (MAD) are computed and the reading is flagged
    exactly when ``|value - median| > sensitivity * MAD`` -- strict even when
    the MAD is zero.  The response lists each spike's key, value, local
    median, MAD and deviation, plus the valid and undecided counts.  The
    even-count medians of the median and MAD steps are averaged without an
    intermediate overflow, so every statistic whose true value is a finite
    double (e.g. a median of four 1e308 neighbours) is returned as a finite
    number rather than Infinity.  More than 200 published readings in the
    interval returns 400.  Buffered and quarantined readings never participate and nothing is written.  Invalid
    interval/parameters or an unknown source return 400; an unknown session
    returns 404.
    """
    # Every parameter error must be 400 (not FastAPI's default 422), so the
    # query params are parsed/validated inside the module, like exposure.
    return LocalSpikesResponse(
        **local_spikes.local_spikes(
            conn,
            session_id,
            source_id=source_id,
            from_=from_,
            to=to,
            window_seconds=window_seconds,
            sensitivity=sensitivity,
            value_key=value_key,
        )
    )


@app.get(
    "/sessions/{session_id}/source-gaps",
    response_model=SourceGapsResponse,
    tags=["readings"],
)
def get_source_gaps(
    session_id: str,
    max_gap_seconds: Optional[str] = Query(
        None,
        description=(
            "Report adjacent published readings of one source whose event-time"
            " distance is strictly greater than this (seconds)"
        ),
    ),
    source_id: Optional[str] = Query(
        None, description="Restrict the scan to one declared source of the session"
    ),
    from_: Optional[str] = Query(
        None,
        alias="from",
        description="Inclusive event-time lower bound (ISO 8601)",
    ),
    to: Optional[str] = Query(
        None, description="Inclusive event-time upper bound (ISO 8601)"
    ),
    limit: Optional[str] = Query(
        None, description="Page size (1-1000, default 100)"
    ),
    cursor: Optional[str] = Query(
        None, description="Opaque next_cursor from a previous page"
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> SourceGapsResponse:
    """Read-only per-source gaps in the session's published timeline.

    Inside one SQLite read snapshot, each source's published readings are
    scanned in ``(observed_at, seq)`` order and every adjacent pair whose
    event-time distance is **strictly greater** than ``max_gap_seconds`` is
    returned -- with the source, both endpoint reading keys and the gap in
    seconds.  Optional ``source_id`` and inclusive ``from``/``to`` bounds
    filter the readings; gaps are ordered by the start keyset
    ``(observed_at, source_id, seq)`` and paginated with an opaque stable
    cursor.  The call never writes: watermarks, alerts, checkpoints and
    seals are untouched.  Unknown source or invalid parameters return 400;
    an unknown session returns 404.
    """
    # Every parameter error must be 400 (not FastAPI's default 422), so the
    # query params are parsed/validated inside the module, like the
    # quarantine-audit body.
    return SourceGapsResponse(
        **source_gaps.list_source_gaps(
            conn,
            session_id,
            max_gap_seconds=max_gap_seconds,
            source_id=source_id,
            from_=from_,
            to=to,
            limit=limit,
            cursor=cursor,
        )
    )


@app.get(
    "/sessions/{session_id}/release-plan",
    response_model=ReleasePlanResponse,
    tags=["readings"],
)
def get_release_plan(
    session_id: str,
    count: int = Query(
        ...,
        ge=1,
        le=100,
        description="Release the earliest `count` buffered readings (1-100)",
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> ReleasePlanResponse:
    """Minimal per-source clock advancement that would release the earliest
    ``count`` buffered readings of an open session.

    Read-only: the buffered prefix, the per-source clocks and the allowed
    lateness are all read inside one SQLite snapshot.  The target watermark
    is one microsecond beyond the Nth buffered reading's ``observed_at``;
    every source's required effective max is that target plus the allowed
    lateness.  Sealed sources that fall short make the plan unattainable
    instead of being suggested to advance.  ``count`` larger than the
    buffered backlog returns 422; a closed session returns 409.
    """
    return ReleasePlanResponse(
        **release_plan.compute_release_plan(conn, session_id, count)
    )


@app.post(
    "/sessions/{session_id}/retention",
    response_model=RetentionResponse,
    tags=["readings"],
)
def apply_retention(
    session_id: str,
    body: RetentionIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> RetentionResponse:
    """Checkpoint-safe cleanup of old published/quarantined readings.

    Only readings whose **event time** is strictly earlier than ``cutoff`` and
    whose status is ``published`` or ``quarantined`` are candidates; buffered
    readings never participate.  Quarantined candidates are always removable;
    a published candidate is retained while the session's *earliest* checkpoint
    can still return it on resume (its deterministic-order keyset is strictly
    greater than the checkpoint's captured keyset), and the response reports
    that protection boundary.  With ``dry_run=true`` nothing is written and
    the call only reports ``candidates`` / ``retained`` / ``deletable`` (plus
    the per-status breakdown); with ``dry_run=false`` the deletable candidates
    are removed atomically in one serialised SQLite write transaction.  The
    same request repeated is idempotent (the second call deletes nothing), any
    failure rolls the transaction back, and watermarks, alerts and checkpoints
    are never modified.
    """
    return RetentionResponse(
        **retention.apply_retention(conn, session_id, body.cutoff, body.dry_run)
    )


# ---------------------------------------------------------------------------
# consumer checkpoints over the published stream
# ---------------------------------------------------------------------------

@app.post(
    "/sessions/{session_id}/checkpoints",
    response_model=CheckpointOut,
    status_code=201,
    tags=["checkpoints"],
)
def create_checkpoint(
    session_id: str,
    body: CreateCheckpointIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> Response:
    """Capture a consumer checkpoint at the current published-stream position.

    The position (number of published readings and the last reading's
    deterministic-order keyset), the session's published watermark and its
    monotonic publish marker are recorded together in one serialised write
    transaction, i.e. one read snapshot.  Checkpoint names are unique within
    a session: re-creating the same name while no reading has been published
    since is idempotent (200, the original snapshot is returned unchanged) --
    even if retention has deleted the covered rows in the meantime; creating
    it once readings have been published since returns 409, even if those
    rows were cleaned up later.  Later publishes and session closure never
    mutate a checkpoint.
    """
    result, created = checkpoints.create_checkpoint(conn, session_id, body.name)
    return JSONResponse(
        status_code=201 if created else 200,
        content=CheckpointOut(**result).model_dump(),
    )


@app.get(
    "/sessions/{session_id}/checkpoints",
    response_model=CheckpointsResponse,
    tags=["checkpoints"],
)
def list_checkpoints(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> CheckpointsResponse:
    """List the session's checkpoints (oldest first)."""
    return CheckpointsResponse(**checkpoints.list_checkpoints(conn, session_id))


@app.delete(
    "/sessions/{session_id}/checkpoints/{name}",
    status_code=204,
    tags=["checkpoints"],
)
def delete_checkpoint(
    session_id: str,
    name: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> Response:
    """Delete one checkpoint.  Unknown session -> 404; unknown name -> 404."""
    checkpoints.delete_checkpoint(conn, session_id, name)
    return Response(status_code=204)


@app.get(
    "/sessions/{session_id}/checkpoints/{name}/stream",
    response_model=CheckpointStreamResponse,
    tags=["checkpoints"],
)
def continue_at_checkpoint(
    session_id: str,
    name: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(
        None,
        description=(
            "Opaque next_cursor from a previous page; omit to start right after"
            " the checkpoint position"
        ),
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> CheckpointStreamResponse:
    """Resume the published stream at a checkpoint, in the same deterministic
    order and with the same opaque keyset pagination as ``/stream``.

    With no ``cursor`` the first page returns readings strictly after the
    checkpoint's captured position; subsequent pages pass ``next_cursor`` back
    verbatim.  The checkpoint itself is immutable, so reads published (or a
    session closed) afterwards do not shift the resume point.
    """
    return CheckpointStreamResponse(
        **checkpoints.continue_from_checkpoint(conn, session_id, name, limit, cursor)
    )


# ---------------------------------------------------------------------------
# post-publish threshold alerts
# ---------------------------------------------------------------------------

@app.post(
    "/sessions/{session_id}/alert-rules",
    response_model=AlertRuleOut,
    status_code=201,
    tags=["alerts"],
)
def create_alert_rule(
    session_id: str,
    body: CreateAlertRuleIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertRuleOut:
    """Create a per-source threshold alert rule.

    The rule's state machine immediately replays the session's already
    published readings for that source, so its initial state is independent
    of when the rule was created.  Unknown sources and rules whose
    enter/recover thresholds do not form a valid hysteresis band return 400.
    """
    result = alerts.create_rule(
        conn,
        session_id=session_id,
        source_id=body.source_id,
        direction=body.direction,
        enter_threshold=body.enter_threshold,
        recover_threshold=body.recover_threshold,
        enter_consecutive=body.enter_consecutive,
        recover_consecutive=body.recover_consecutive,
        value_key=body.value_key,
        rule_id=body.rule_id,
    )
    return AlertRuleOut(**result)


@app.get(
    "/sessions/{session_id}/alert-rules",
    response_model=AlertRulesResponse,
    tags=["alerts"],
)
def list_alert_rules(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertRulesResponse:
    return AlertRulesResponse(session_id=session_id, rules=alerts.list_rules(conn, session_id))


@app.get(
    "/sessions/{session_id}/alert-rules/{rule_id}",
    response_model=AlertRuleOut,
    tags=["alerts"],
)
def get_alert_rule(
    session_id: str,
    rule_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertRuleOut:
    return AlertRuleOut(**alerts.get_rule(conn, session_id, rule_id))


@app.get(
    "/sessions/{session_id}/alerts/current",
    response_model=CurrentAlertsResponse,
    tags=["alerts"],
)
def current_alerts(
    session_id: str,
    source_id: Optional[str] = Query(
        None, description="Restrict to one declared source of the session"
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> CurrentAlertsResponse:
    """Rules currently in the ``active`` state (entered, not yet recovered)."""
    return CurrentAlertsResponse(
        session_id=session_id,
        alerts=alerts.list_current_alerts(conn, session_id, source_id),
    )


@app.get(
    "/sessions/{session_id}/alerts/history",
    response_model=AlertHistoryResponse,
    tags=["alerts"],
)
def alert_history(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    source_id: Optional[str] = Query(None, description="Filter by source"),
    rule_id: Optional[str] = Query(None, description="Filter by rule"),
    event_type: Optional[str] = Query(
        None, description="Filter: 'entered' or 'recovered'"
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> AlertHistoryResponse:
    """Entered/recovered events, newest first, with keyset pagination."""
    if event_type is not None and event_type not in alerts.EVENT_TYPES:
        return _error(
            400,
            f"event_type must be one of {list(alerts.EVENT_TYPES)},"
            f" got {event_type!r}",
        )
    return AlertHistoryResponse(
        **alerts.list_event_history(
            conn, session_id, limit, cursor, source_id, rule_id, event_type
        )
    )


# ---------------------------------------------------------------------------
# cross-source correlated alerts
# ---------------------------------------------------------------------------

@app.post(
    "/sessions/{session_id}/correlated-rules",
    response_model=CorrelatedRuleOut,
    status_code=201,
    tags=["correlated-alerts"],
)
def create_correlated_rule(
    session_id: str,
    body: CreateCorrelatedRuleIn,
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelatedRuleOut:
    """Create a cross-source correlated alert rule (2-4 conditions).

    Every condition binds one distinct declared source to a direction, a
    strict threshold, an optional ``value_key`` and its own
    ``window_seconds``.  The merged published stream is replayed on creation,
    so the rule's initial state is independent of when it was created.
    Unknown sources return 400; duplicate sources or a condition count
    outside 2-4 return 422.
    """
    result = correlated.create_rule(
        conn,
        session_id=session_id,
        conditions=[c.model_dump() for c in body.conditions],
        rule_id=body.rule_id,
    )
    return CorrelatedRuleOut(**result)


@app.get(
    "/sessions/{session_id}/correlated-rules",
    response_model=CorrelatedRulesResponse,
    tags=["correlated-alerts"],
)
def list_correlated_rules(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelatedRulesResponse:
    return CorrelatedRulesResponse(
        session_id=session_id, rules=correlated.list_rules(conn, session_id)
    )


@app.get(
    "/sessions/{session_id}/correlated-rules/{rule_id}",
    response_model=CorrelatedRuleOut,
    tags=["correlated-alerts"],
)
def get_correlated_rule(
    session_id: str,
    rule_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelatedRuleOut:
    return CorrelatedRuleOut(**correlated.get_rule(conn, session_id, rule_id))


@app.get(
    "/sessions/{session_id}/correlated-alerts/current",
    response_model=CurrentCorrelatedAlertsResponse,
    tags=["correlated-alerts"],
)
def current_correlated_alerts(
    session_id: str,
    conn: sqlite3.Connection = Depends(get_db),
) -> CurrentCorrelatedAlertsResponse:
    """Correlated rules currently satisfied (``correlated`` state)."""
    return CurrentCorrelatedAlertsResponse(
        session_id=session_id,
        alerts=correlated.list_current(conn, session_id),
    )


@app.get(
    "/sessions/{session_id}/correlated-alerts/history",
    response_model=CorrelatedHistoryResponse,
    tags=["correlated-alerts"],
)
def correlated_alert_history(
    session_id: str,
    limit: int = Query(100, ge=1, le=1000),
    cursor: Optional[str] = Query(None, description="Opaque next_cursor from a previous page"),
    rule_id: Optional[str] = Query(None, description="Filter by correlated rule"),
    conn: sqlite3.Connection = Depends(get_db),
) -> CorrelatedHistoryResponse:
    """``correlated_entered`` events, newest first, with keyset pagination."""
    return CorrelatedHistoryResponse(
        **correlated.list_event_history(conn, session_id, limit, cursor, rule_id)
    )
