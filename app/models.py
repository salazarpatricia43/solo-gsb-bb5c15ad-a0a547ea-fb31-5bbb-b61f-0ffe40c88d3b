"""Pydantic request/response models."""
from __future__ import annotations

import math
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class ReadingIn(BaseModel):
    source_id: str = Field(..., min_length=1, description="Fixed sensor source declared on the session")
    seq: int = Field(..., ge=0, description="Per-source strictly increasing sequence number")
    observed_at: str = Field(..., description="Event time, ISO 8601 (UTC or with offset)")
    value: Optional[Any] = Field(None, description="Arbitrary JSON-serializable measurement payload")

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id must not be blank")
        return v

    @field_validator("observed_at")
    @classmethod
    def _observed_not_blank(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("observed_at must be a non-empty ISO 8601 string")
        return v


class CreateSessionIn(BaseModel):
    source_ids: list[str] = Field(..., min_length=1, description="Fixed source list for this session")
    allowed_lateness_seconds: float = Field(
        ..., ge=0, description="How far behind the slowest source the watermark lags"
    )
    session_id: Optional[str] = Field(None, min_length=1, description="Optional caller-chosen id")

    @field_validator("source_ids")
    @classmethod
    def _sources_unique_nonblank(cls, v: list[str]) -> list[str]:
        if any(not isinstance(s, str) or not s.strip() for s in v):
            raise ValueError("source_ids must be non-empty strings")
        if len(set(v)) != len(v):
            raise ValueError("source_ids must be unique")
        return v


class ReadingOut(BaseModel):
    session_id: str
    source_id: str
    seq: int
    observed_at: str
    value: Optional[Any] = None
    published_watermark: str


class QuarantineOut(BaseModel):
    session_id: str
    source_id: str
    seq: int
    observed_at: str
    value: Optional[Any] = None
    quarantined_watermark: str
    reason: str = "behind_published_watermark"


class ReportResponse(BaseModel):
    session_id: str
    received: int
    duplicates: int
    quarantined: int
    published_now: int
    published_total: int
    buffered: int
    all_sources_reported: bool
    published_watermark: Optional[str] = None


class PullResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    published_watermark: Optional[str] = None
    readings: list[ReadingOut]


class QuarantineResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    readings: list[QuarantineOut]


# ---------------------------------------------------------------------------
# read-only quarantine impact rehearsal
# ---------------------------------------------------------------------------

class TimelineKeyOut(BaseModel):
    observed_at: str
    source_id: str
    seq: int


class QuarantineAuditOut(BaseModel):
    session_id: str
    source_id: str
    seq: int
    observed_at: str
    value: Optional[Any] = None
    quarantined_watermark: str
    reason: str = "behind_published_watermark"
    insert_position: int = Field(
        ...,
        description=(
            "Zero-based offset the reading would occupy in the published"
            " timeline: the count of published readings whose keyset is"
            " strictly smaller"
        ),
    )
    stream_size: int = Field(
        ..., description="Total published readings visible in the same snapshot"
    )
    prev_key: Optional[TimelineKeyOut] = Field(
        None, description="Published neighbour immediately before the slot (null at the head)"
    )
    next_key: Optional[TimelineKeyOut] = Field(
        None, description="Published neighbour immediately after the slot (null at the tail)"
    )
    gap_prev_seconds: Optional[float] = Field(
        None, description="Event-time distance from prev_key (null when there is no predecessor)"
    )
    gap_next_seconds: Optional[float] = Field(
        None, description="Event-time distance to next_key (null when there is no successor)"
    )
    earliest_affected_checkpoint: Optional[str] = Field(
        None,
        description=(
            "Earliest checkpoint (by captured stream position) whose resume"
            " would gain this reading; null when no checkpoint would"
        ),
    )


class QuarantineAuditResponse(BaseModel):
    session_id: str
    source_id: Optional[str] = None
    from_: Optional[str] = Field(
        None,
        validation_alias="from",
        serialization_alias="from",
    )
    to: Optional[str] = None
    limit: int
    matched: int = Field(
        ..., description="Quarantined rows matching the filters (pre-limit)"
    )
    returned: int = Field(..., description="Rows in this page")
    has_more: bool = Field(..., description="True when more matched rows remain beyond limit")
    readings: list[QuarantineAuditOut]


class SealSourceIn(BaseModel):
    final_observed_at: str = Field(
        ...,
        description=(
            "The source's last possible event time; must not be earlier than"
            " the largest observed_at already seen for it"
        ),
    )

    @field_validator("final_observed_at")
    @classmethod
    def _final_not_blank(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("final_observed_at must be a non-empty ISO 8601 string")
        return v


class SourceSealOut(BaseModel):
    source_id: str
    max_observed_at: Optional[str] = None
    final_observed_at: Optional[str] = None
    sealed_at: Optional[str] = None
    sealed: bool = False


class SessionOut(BaseModel):
    session_id: str
    source_ids: list[str]
    allowed_lateness_seconds: float
    published_watermark: Optional[str] = None
    all_sources_reported: bool
    effective_watermark: Optional[str] = None
    created_at: str
    status: Literal["open", "closed"] = "open"
    closed: bool = False
    closed_at: Optional[str] = None
    sources: list[SourceSealOut] = Field(default_factory=list)


class CreatedSessionResponse(BaseModel):
    session_id: str
    source_ids: list[str]
    allowed_lateness_seconds: float
    created_at: str


# ---------------------------------------------------------------------------
# alert rules
# ---------------------------------------------------------------------------

class CreateAlertRuleIn(BaseModel):
    source_id: str = Field(
        ..., min_length=1, description="Fixed sensor source declared on the session"
    )
    direction: Literal["above", "below"] = Field(
        ..., description="'above' alerts on high values, 'below' on low values"
    )
    enter_threshold: float = Field(
        ..., description="Breach threshold; crossing it (strictly) counts toward enter"
    )
    recover_threshold: float = Field(
        ...,
        description=(
            "Safe-side threshold forming a hysteresis band with enter_threshold"
        ),
    )
    enter_consecutive: int = Field(
        ..., ge=1, le=10_000,
        description="Consecutive breaching published readings required to enter",
    )
    recover_consecutive: int = Field(
        ..., ge=1, le=10_000,
        description="Consecutive recovered published readings required to clear",
    )
    value_key: Optional[str] = Field(
        default=None,
        min_length=1,
        description=(
            "Top-level key read from an object-valued reading; omit to compare the"
            " reading value itself. Missing/non-numeric values reset the streak."
        ),
    )
    rule_id: Optional[str] = Field(None, min_length=1, description="Optional caller-chosen id")

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id must not be blank")
        return v

    @field_validator("enter_threshold", "recover_threshold")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("threshold must be a finite number")
        return v

    @field_validator("value_key")
    @classmethod
    def _value_key_not_blank(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("value_key must not be blank")
        return v.strip() if v is not None else None


class AlertRuleOut(BaseModel):
    rule_id: str
    session_id: str
    source_id: str
    direction: Literal["above", "below"]
    enter_threshold: float
    recover_threshold: float
    enter_consecutive: int
    recover_consecutive: int
    value_key: Optional[str] = None
    state: Literal["normal", "active"]
    streak: int
    created_at: str


class AlertRulesResponse(BaseModel):
    session_id: str
    rules: list[AlertRuleOut]


class AlertTriggerOut(BaseModel):
    seq: int
    observed_at: str
    value: Optional[float] = None
    threshold: float
    triggered_at: str


class CurrentAlertOut(BaseModel):
    rule_id: str
    session_id: str
    source_id: str
    direction: Literal["above", "below"]
    enter_threshold: float
    recover_threshold: float
    enter_consecutive: int
    recover_consecutive: int
    value_key: Optional[str] = None
    state: Literal["active"]
    streak: int
    entered: AlertTriggerOut
    last_observed_at: Optional[str] = None
    last_seq: Optional[int] = None


class CurrentAlertsResponse(BaseModel):
    session_id: str
    alerts: list[CurrentAlertOut]


class AlertEventOut(BaseModel):
    id: int
    rule_id: str
    session_id: str
    source_id: str
    event_type: Literal["entered", "recovered"]
    seq: int
    observed_at: str
    value: Optional[float] = None
    threshold: float
    triggered_at: str


class AlertHistoryResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    events: list[AlertEventOut]


# ---------------------------------------------------------------------------
# cross-source correlated alert rules
# ---------------------------------------------------------------------------

class CorrelatedConditionIn(BaseModel):
    source_id: str = Field(
        ..., min_length=1, description="Fixed sensor source declared on the session"
    )
    direction: Literal["above", "below"] = Field(
        ..., description="'above' breaches on high values, 'below' on low values"
    )
    threshold: float = Field(
        ..., description="Breach threshold; the comparison is strict"
    )
    window_seconds: float = Field(
        ..., gt=0, le=365 * 24 * 3600,
        description="How long a hit on this condition stays valid (seconds)",
    )
    value_key: Optional[str] = Field(
        default=None,
        min_length=1,
        description=(
            "Top-level key read from an object-valued reading; omit to compare the"
            " reading value itself. Missing/non-numeric values never hit."
        ),
    )

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id must not be blank")
        return v

    @field_validator("threshold")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("threshold must be a finite number")
        return v

    @field_validator("value_key")
    @classmethod
    def _value_key_not_blank(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("value_key must not be blank")
        return v.strip() if v is not None else None


class CreateCorrelatedRuleIn(BaseModel):
    conditions: list[CorrelatedConditionIn] = Field(
        ..., min_length=2, max_length=4,
        description="2-4 conditions; declaration order is the merge order",
    )
    rule_id: Optional[str] = Field(None, min_length=1, description="Optional caller-chosen id")

    @field_validator("conditions")
    @classmethod
    def _sources_unique(cls, v: list[CorrelatedConditionIn]) -> list[CorrelatedConditionIn]:
        sources = [c.source_id for c in v]
        if len(set(sources)) != len(sources):
            raise ValueError("each condition must reference a distinct source_id")
        return v


class CorrelatedHitOut(BaseModel):
    source_id: str
    seq: int
    observed_at: str
    value: Optional[float] = None


class CorrelatedConditionOut(BaseModel):
    position: int
    source_id: str
    direction: Literal["above", "below"]
    threshold: float
    value_key: Optional[str] = None
    window_seconds: float
    last_hit: Optional[CorrelatedHitOut] = None


class CorrelatedAnchorOut(BaseModel):
    source_id: str
    seq: int
    observed_at: str


class CorrelatedRuleOut(BaseModel):
    rule_id: str
    session_id: str
    created_at: str
    state: Literal["waiting", "correlated"]
    conditions: list[CorrelatedConditionOut]
    correlated_since: Optional[CorrelatedAnchorOut] = None
    last_observed_at: Optional[str] = None
    last_source_id: Optional[str] = None
    last_seq: Optional[int] = None


class CorrelatedRulesResponse(BaseModel):
    session_id: str
    rules: list[CorrelatedRuleOut]


class CorrelatedHitSnapshotOut(CorrelatedHitOut):
    position: int
    direction: Literal["above", "below"]
    threshold: float
    value_key: Optional[str] = None
    window_seconds: float


class CorrelatedEnteredOut(BaseModel):
    id: int
    event_type: Literal["correlated_entered"]
    completed_by: CorrelatedAnchorOut
    conditions: list[CorrelatedHitSnapshotOut]
    triggered_at: str


class CurrentCorrelatedAlertOut(BaseModel):
    rule_id: str
    session_id: str
    state: Literal["correlated"]
    correlated_entered: CorrelatedEnteredOut
    last_observed_at: Optional[str] = None
    last_source_id: Optional[str] = None
    last_seq: Optional[int] = None


class CurrentCorrelatedAlertsResponse(BaseModel):
    session_id: str
    alerts: list[CurrentCorrelatedAlertOut]


class CorrelatedEventOut(BaseModel):
    id: int
    rule_id: str
    session_id: str
    event_type: Literal["correlated_entered"]
    completed_by: CorrelatedAnchorOut
    conditions: list[CorrelatedHitSnapshotOut]
    triggered_at: str


class CorrelatedHistoryResponse(BaseModel):
    session_id: str
    next_cursor: Optional[str] = None
    events: list[CorrelatedEventOut]


# ---------------------------------------------------------------------------
# consumer checkpoints on the published stream
# ---------------------------------------------------------------------------

# Checkpoint names identify a consumer position inside one session; restrict
# them to URL/path-safe, shell-safe characters so they can be used unescaped.
_CHECKPOINT_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"


class CreateCheckpointIn(BaseModel):
    name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        pattern=_CHECKPOINT_NAME_PATTERN,
        description=(
            "Consumer-chosen checkpoint name, unique within the session"
            " (letters, digits, '.', '_', '-'; must start alphanumerically)"
        ),
    )


class StreamPositionOut(BaseModel):
    stream_offset: int = Field(
        ..., description="Number of published readings captured at creation"
    )
    last_observed_at: Optional[str] = Field(
        None, description="Event time of the last published reading at capture"
    )
    last_source_id: Optional[str] = Field(
        None, description="Source of the last published reading at capture"
    )
    last_seq: Optional[int] = Field(
        None, description="Sequence number of the last published reading at capture"
    )


class CheckpointOut(StreamPositionOut):
    session_id: str
    name: str
    created_at: str
    published_watermark: Optional[str] = Field(
        None,
        description="Session published watermark captured in the same snapshot",
    )


class CheckpointsResponse(BaseModel):
    session_id: str
    checkpoints: list[CheckpointOut]


class CheckpointStreamResponse(BaseModel):
    session_id: str
    checkpoint: CheckpointOut = Field(
        ..., description="The immutable checkpoint the read resumed from"
    )
    next_cursor: Optional[str] = None
    published_watermark: Optional[str] = Field(
        None, description="The session's current published watermark (may advance later)"
    )
    readings: list[ReadingOut]


# ---------------------------------------------------------------------------
# release planning over buffered readings
# ---------------------------------------------------------------------------

class ReleasePlanSourceOut(BaseModel):
    source_id: str
    current: Optional[str] = Field(
        None,
        description=(
            "The source's current effective clock (its final_observed_at when"
            " sealed); null while the source has never reported"
        ),
    )
    required: str = Field(
        ...,
        description=(
            "Effective max this source's clock must reach: target watermark"
            " plus the session's allowed lateness"
        ),
    )
    advance_seconds: Optional[float] = Field(
        None,
        description=(
            "How many seconds the source clock must still advance (0 when"
            " already sufficient); null when not computable (never reported)"
            " or not allowed (sealed below required)"
        ),
    )
    blocker: Optional[
        Literal["below_required", "not_reported", "sealed_below_required"]
    ] = Field(
        None,
        description=(
            "Why this source is holding the release back; null when its clock"
            " already satisfies the requirement"
        ),
    )


class ReleasePlanResponse(BaseModel):
    session_id: str
    count: int = Field(..., description="Number of earliest buffered readings planned for")
    buffered: int = Field(..., description="Total buffered readings in the same snapshot")
    allowed_lateness_seconds: float
    target_watermark: str = Field(
        ...,
        description=(
            "One microsecond beyond the Nth buffered reading's observed_at:"
            " the smallest watermark that releases it"
        ),
    )
    attainable: bool = Field(
        ...,
        description=(
            "False when a sealed source's final clock falls short of required,"
            " so no clock-advancement plan can release the readings"
        ),
    )
    reason: Optional[str] = Field(
        None, description="Why the plan is unattainable; null when attainable"
    )
    sources: list[ReleasePlanSourceOut]


# ---------------------------------------------------------------------------
# checkpoint-safe storage retention
# ---------------------------------------------------------------------------

class RetentionIn(BaseModel):
    cutoff: str = Field(
        ...,
        description=(
            "Event-time boundary: only published/quarantined readings whose"
            " observed_at is strictly earlier than it are candidates"
        ),
    )
    dry_run: bool = Field(
        default=True,
        description=(
            "True (default): report counts without changing anything."
            " False: atomically delete the deletable candidates"
        ),
    )

    @field_validator("cutoff")
    @classmethod
    def _cutoff_not_blank(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("cutoff must be a non-empty ISO 8601 string")
        return v


class CheckpointProtectionOut(BaseModel):
    checkpoint_name: str = Field(
        ...,
        description=(
            "The session's earliest checkpoint: its resume window is the"
            " furthest back, so protecting it covers every other checkpoint"
        ),
    )
    stream_offset: int = Field(
        ..., description="Published readings the boundary checkpoint had covered"
    )
    last_observed_at: Optional[str] = Field(
        None, description="Boundary keyset event time (null for an empty-stream checkpoint)"
    )
    last_source_id: Optional[str] = Field(
        None, description="Boundary keyset source (null for an empty-stream checkpoint)"
    )
    last_seq: Optional[int] = Field(
        None, description="Boundary keyset seq (null for an empty-stream checkpoint)"
    )


class RetentionStatusCountsOut(BaseModel):
    candidates: int = Field(
        ..., description="Rows of this status with observed_at strictly before cutoff"
    )
    retained: int = Field(
        ...,
        description=(
            "Candidates kept because the earliest checkpoint can still read"
            " them (always 0 for quarantined rows)"
        ),
    )
    deletable: int = Field(
        ..., description="Candidates safe to delete (or deleted when dry_run=false)"
    )


class RetentionStatusBreakdownOut(BaseModel):
    published: RetentionStatusCountsOut
    quarantined: RetentionStatusCountsOut


class RetentionResponse(BaseModel):
    session_id: str
    cutoff: str = Field(..., description="Normalised UTC form of the requested cutoff")
    dry_run: bool
    deleted: int = Field(
        ...,
        description=(
            "Rows actually removed in this call (0 for dry runs; 0 on an"
            " idempotent repeat after the candidates were already removed)"
        ),
    )
    protected_by_checkpoint: Optional[CheckpointProtectionOut] = Field(
        None,
        description=(
            "The checkpoint protection boundary: every published reading with"
            " a keyset strictly greater than it is retained. Null when the"
            " session has no checkpoint, in which case no published reading is"
            " protected."
        ),
    )
    candidates: int = Field(
        ...,
        description=(
            "Total published/quarantined readings with observed_at strictly"
            " before cutoff (buffered readings are never candidates)"
        ),
    )
    retained: int = Field(
        ...,
        description=(
            "Candidates retained because the earliest checkpoint can still"
            " read them"
        ),
    )
    deletable: int = Field(
        ...,
        description=(
            "Candidates not needed by any checkpoint: candidates - retained"
        ),
    )
    by_status: RetentionStatusBreakdownOut


# ---------------------------------------------------------------------------
# read-only per-source arrival-latency profiling
# ---------------------------------------------------------------------------

class ArrivalLatencySourceOut(BaseModel):
    source_id: str
    sample_count: int = Field(
        ..., description="Readings of this source matching the filters"
    )
    min_latency_seconds: float = Field(
        ...,
        description=(
            "Smallest first_received_at - observed_at of the samples (seconds);"
            " negative when a reading arrived before its own event time"
        ),
    )
    max_latency_seconds: float = Field(
        ..., description="Largest first_received_at - observed_at of the samples (seconds)"
    )
    avg_latency_seconds: float = Field(
        ..., description="Arithmetic mean of the per-reading latencies (seconds)"
    )


class ArrivalLatencyResponse(BaseModel):
    session_id: str
    source_id: Optional[str] = None
    from_: Optional[str] = Field(
        None,
        validation_alias="from",
        serialization_alias="from",
    )
    to: Optional[str] = None
    sample_count: int = Field(
        ...,
        description=(
            "Total readings matching the filters across every returned source"
        ),
    )
    sources: list[ArrivalLatencySourceOut] = Field(
        ...,
        description=(
            "Per-source statistics, sorted by source_id; sources without a"
            " matching reading are absent"
        ),
    )


# ---------------------------------------------------------------------------
# read-only per-source gap detection
# ---------------------------------------------------------------------------

class SourceGapOut(BaseModel):
    source_id: str
    start_key: TimelineKeyOut = Field(
        ...,
        description="Earlier published reading of the adjacent pair (inclusive window endpoint)",
    )
    end_key: TimelineKeyOut = Field(
        ...,
        description="Later published reading of the adjacent pair (inclusive window endpoint)",
    )
    gap_seconds: float = Field(
        ...,
        description=(
            "Event-time distance end_key.observed_at - start_key.observed_at"
            " in seconds; always strictly greater than max_gap_seconds"
        ),
    )


class SourceGapsResponse(BaseModel):
    session_id: str
    source_id: Optional[str] = None
    from_: Optional[str] = Field(
        None,
        validation_alias="from",
        serialization_alias="from",
    )
    to: Optional[str] = None
    max_gap_seconds: float
    limit: int
    matched: int = Field(
        ..., description="Gaps matching the filters and threshold (pre-cursor/limit)"
    )
    next_cursor: Optional[str] = Field(
        None, description="Opaque keyset cursor; pass it back verbatim to fetch the next page"
    )
    gaps: list[SourceGapOut]


# ---------------------------------------------------------------------------
# read-only threshold exposure over one source's published readings
# ---------------------------------------------------------------------------

class ExposureResponse(BaseModel):
    session_id: str
    source_id: str
    from_: str = Field(
        ...,
        validation_alias="from",
        serialization_alias="from",
        description="Normalised UTC form of the half-open interval start (inclusive)",
    )
    to: str = Field(
        ...,
        description="Normalised UTC form of the half-open interval end (exclusive)",
    )
    threshold: float = Field(..., description="Finite threshold; the comparison is strict")
    value_key: Optional[str] = Field(
        None,
        description="Top-level key read from object-valued readings; null compares value itself",
    )
    above_threshold_seconds: float = Field(
        ...,
        description=(
            "Seconds of [from, to) covered by a published finite value strictly"
            " above the threshold"
        ),
    )
    covered_seconds: float = Field(
        ...,
        description=(
            "Seconds of [from, to) covered by any valid finite sample (above or"
            " not); the complementary time is unknown"
        ),
    )
    unknown_seconds: float = Field(
        ...,
        description=(
            "Seconds without a usable value: before the first reading, or under"
            " a missing/non-finite/boolean reading (no carry-over)"
        ),
    )
    ratio_above: Optional[float] = Field(
        ...,
        description=(
            "above_threshold_seconds / covered_seconds; null when covered_seconds"
            " is zero (no valid sample anywhere in the interval)"
        ),
    )


# ---------------------------------------------------------------------------
# read-only local-spike detection (neighbour median + MAD)
# ---------------------------------------------------------------------------

class LocalSpikeKeyOut(TimelineKeyOut):
    """Key of a published reading flagged as a local spike."""


class LocalSpikeOut(BaseModel):
    key: LocalSpikeKeyOut = Field(
        ..., description="Published reading key (observed_at, source_id, seq)"
    )
    value: float = Field(..., description="The reading's finite numeric value")
    local_median: float = Field(
        ...,
        description="Median of the in-window neighbour values (excluding this reading)",
    )
    mad: float = Field(
        ...,
        description=(
            "Median absolute deviation of the neighbours from local_median;"
            " zero when every neighbour equals the median"
        ),
    )
    deviation: float = Field(
        ..., description="Absolute difference |value - local_median|"
    )


class LocalSpikesResponse(BaseModel):
    session_id: str
    source_id: str
    from_: str = Field(
        ...,
        validation_alias="from",
        serialization_alias="from",
        description="Normalised UTC form of the inclusive interval start",
    )
    to: str = Field(
        ..., description="Normalised UTC form of the inclusive interval end"
    )
    window_seconds: float = Field(
        ..., description="Inclusive event-time neighbour window (strictly positive)"
    )
    sensitivity: float = Field(
        ...,
        description="Strictly positive coefficient; a reading spikes when deviation > sensitivity * MAD",
    )
    value_key: Optional[str] = Field(
        None,
        description="Top-level key read from object-valued readings; null compares value itself",
    )
    readings_count: int = Field(
        ...,
        description=(
            "Published readings of the source inside [from, to] in the snapshot,"
            " before valid-value filtering; requests with more than 200 fail"
        ),
    )
    valid_count: int = Field(
        ...,
        description="Readings with a finite non-boolean numeric value, ordered (observed_at, seq)",
    )
    insufficient_neighbors_count: int = Field(
        ...,
        description=(
            "Valid readings left undecided because they have fewer than three"
            " other valid readings within the event-time window"
        ),
    )
    spikes: list[LocalSpikeOut] = Field(
        ...,
        description=(
            "Readings whose absolute deviation from the local neighbour median"
            " is strictly greater than sensitivity times the neighbour MAD,"
            " ordered (observed_at, seq)"
        ),
    )
