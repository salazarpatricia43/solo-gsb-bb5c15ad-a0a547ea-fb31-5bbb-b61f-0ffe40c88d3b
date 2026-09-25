"""End-to-end tests for the read-only threshold exposure endpoint."""
from __future__ import annotations

import json
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("SENSOR_DB_PATH", db_path)
    from app import database, main

    main.app.state.db_path = db_path
    database.init_db(db_path)
    with TestClient(main.app) as c:
        yield c, db_path


def create_session(client, sources=("a", "b"), lateness=10.0, session_id="s"):
    r = client.post(
        "/sessions",
        json={
            "session_id": session_id,
            "source_ids": list(sources),
            "allowed_lateness_seconds": lateness,
        },
    )
    assert r.status_code == 201, r.text
    return session_id


def report(client, sid, readings):
    return client.post(
        f"/sessions/{sid}/readings", json={"readings": readings}
    )


def t(value: str) -> str:
    from app.time_utils import parse_event_time, store_event_time

    return store_event_time(parse_event_time(value))


def insert_row(
    db_path,
    sid,
    source,
    seq,
    observed_at,
    payload,
    *,
    status="published",
    published_watermark=None,
):
    """Insert one reading row with an explicit stored JSON payload."""
    if not isinstance(payload, str):
        payload = json.dumps(payload)
    with sqlite3.connect(db_path) as raw:
        raw.execute(
            "INSERT INTO readings (session_id, source_id, seq, observed_at,"
            " payload, status, first_received_at, published_watermark,"
            " quarantined_watermark)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                sid,
                source,
                seq,
                t(observed_at),
                payload,
                status,
                t(observed_at),
                t(published_watermark or observed_at),
                None,
            ),
        )
        raw.commit()


def publish_all(client, sid, target_observed_at="2026-01-01T11:00:00Z"):
    """Seal both sources at a far-future time so every reading is published."""
    for source in ("a", "b"):
        r = client.post(
            f"/sessions/{sid}/sources/{source}/seal",
            json={"final_observed_at": target_observed_at},
        )
        assert r.status_code == 200, r.text


def exposure(client, sid, **params):
    return client.get(f"/sessions/{sid}/exposure", params=params)


def ex(client, sid, source="a", lo="2026-01-01T10:00:00Z",
       hi="2026-01-01T10:01:00Z", threshold="10", **extra):
    params = {"source_id": source, "from": lo, "to": hi, "threshold": threshold}
    params.update(extra)
    return exposure(client, sid, **params).json()


class TestExposureBasic:
    def test_value_holds_until_next_reading_and_endpoint(self, client):
        # a: @:00 = 20 (above 10), @:30 = 5 (not above).  Value holds forward.
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:30Z", 5)
        body = ex(c, sid)
        # [00,30): above; [30,60): covered but not above.
        assert body["above_threshold_seconds"] == 30.0
        assert body["covered_seconds"] == 60.0
        assert body["unknown_seconds"] == 0.0
        assert body["ratio_above"] == 0.5

    def test_interval_start_inherits_latest_prior_reading(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Reading before the window: its value is carried into `from`.
        insert_row(db_path, sid, "a", 1, "2026-01-01T09:59:30Z", 20)
        # First in-window reading drops below at :20.
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z", 5)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 20.0
        assert body["covered_seconds"] == 60.0
        assert body["unknown_seconds"] == 0.0
        assert body["ratio_above"] == pytest.approx(1 / 3)

    def test_no_reading_before_start_is_unknown(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:10Z", 20)
        body = ex(c, sid)
        # [00,10) unknown; [10,60) above.
        assert body["unknown_seconds"] == 10.0
        assert body["above_threshold_seconds"] == 50.0
        assert body["covered_seconds"] == 50.0
        assert body["ratio_above"] == 1.0

    def test_no_readings_at_all_ratio_null(self, client):
        c, _ = client
        sid = create_session(c, sources=("a",))
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 0.0
        assert body["covered_seconds"] == 0.0
        assert body["unknown_seconds"] == 60.0
        assert body["ratio_above"] is None

    def test_exact_threshold_value_is_not_above(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 10)
        body = ex(c, sid, threshold="10")
        assert body["above_threshold_seconds"] == 0.0
        assert body["covered_seconds"] == 60.0
        assert body["ratio_above"] == 0.0
        # A tiny threshold nudge makes it strictly above.
        body = ex(c, sid, threshold="9.999999")
        assert body["above_threshold_seconds"] == 60.0
        assert body["ratio_above"] == 1.0

    def test_endpoint_is_exclusive_reading_at_to_excluded(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 5)
        # A low reading exactly at `to` must not influence the interval.
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:01:00Z", 20)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 0.0
        assert body["covered_seconds"] == 60.0

    def test_reading_exactly_at_from_sets_start_state(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:40Z", 5)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 40.0
        assert body["covered_seconds"] == 60.0

    def test_last_value_holds_to_query_end(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T09:00:00Z", 5)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:10Z", 20)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 50.0
        assert body["covered_seconds"] == 60.0


class TestSameTimestampSeq:
    def test_same_event_time_last_seq_wins(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Same event time, three readings; the last (highest seq) is in force.
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:00Z", 5)
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:00Z", 20)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 60.0
        assert body["covered_seconds"] == 60.0
        assert body["unknown_seconds"] == 0.0

    def test_same_event_time_seed_last_seq_wins(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Two readings exactly at `from`: seq order, the low one wins.
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:00Z", 5)
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:30Z", 20)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 30.0
        assert body["covered_seconds"] == 60.0

    def test_same_event_time_group_creates_zero_length_segment(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:20Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z", 5)
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:20Z", 20)
        insert_row(db_path, sid, "a", 4, "2026-01-01T10:00:40Z", 5)
        body = ex(c, sid)
        # [00,20) unknown, [20,40) above (last seq), [40,60) covered low.
        assert body["unknown_seconds"] == 20.0
        assert body["above_threshold_seconds"] == 20.0
        assert body["covered_seconds"] == 40.0


class TestUnknownIntervals:
    def test_missing_value_makes_unknown_without_carryover(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        # JSON null payload => missing value.
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z", None)
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:40Z", 20)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 40.0  # [00,20) + [40,60)
        assert body["unknown_seconds"] == 20.0
        assert body["covered_seconds"] == 40.0

    def test_non_finite_values_make_unknown(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z", "NaN")
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:40Z", "Infinity")
        insert_row(db_path, sid, "a", 4, "2026-01-01T10:00:50Z", 20)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 30.0  # [00,20) + [50,60)
        assert body["unknown_seconds"] == 30.0
        assert body["covered_seconds"] == 30.0

    def test_boolean_values_make_unknown(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z", True)
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:40Z", False)
        insert_row(db_path, sid, "a", 4, "2026-01-01T10:00:50Z", 20)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 30.0
        assert body["unknown_seconds"] == 30.0

    def test_non_numeric_string_makes_unknown(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", '"hot"')
        body = ex(c, sid)
        assert body["unknown_seconds"] == 60.0
        assert body["covered_seconds"] == 0.0
        assert body["ratio_above"] is None

    def test_unknown_seed_before_window(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # The latest reading before `from` is non-numeric -> unknown start.
        insert_row(db_path, sid, "a", 1, "2026-01-01T09:59:00Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T09:59:30Z", None)
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:20Z", 20)
        body = ex(c, sid)
        assert body["unknown_seconds"] == 20.0
        assert body["above_threshold_seconds"] == 40.0
        assert body["covered_seconds"] == 40.0

    def test_valid_value_after_unknown_restores_coverage(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:10Z", None)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z", 20)
        body = ex(c, sid)
        # [00,10) no reading (unknown), [10,20) null (unknown), [20,60) above.
        assert body["unknown_seconds"] == 20.0
        assert body["above_threshold_seconds"] == 40.0
        assert body["covered_seconds"] == 40.0


class TestValueKey:
    def test_reads_top_level_object_key(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z",
                   {"temp": 20, "other": 1})
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:30Z",
                   {"temp": 5, "other": 99})
        body = ex(c, sid, threshold="10", value_key="temp")
        assert body["above_threshold_seconds"] == 30.0
        assert body["covered_seconds"] == 60.0
        assert body["unknown_seconds"] == 0.0
        assert body["value_key"] == "temp"

    def test_missing_key_is_unknown(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z",
                   {"temp": 20})
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z",
                   {"other": 99})
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:40Z",
                   {"temp": 20})
        body = ex(c, sid, threshold="10", value_key="temp")
        assert body["above_threshold_seconds"] == 40.0
        assert body["unknown_seconds"] == 20.0
        assert body["covered_seconds"] == 40.0

    def test_boolean_key_value_is_unknown(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z",
                   {"temp": True})
        body = ex(c, sid, threshold="0", value_key="temp")
        assert body["unknown_seconds"] == 60.0
        assert body["ratio_above"] is None

    def test_non_object_reading_with_value_key_is_unknown(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        body = ex(c, sid, threshold="10", value_key="temp")
        assert body["unknown_seconds"] == 60.0
        assert body["covered_seconds"] == 0.0


class TestStatuses:
    def test_buffered_and_quarantined_never_participate(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20,
                   status="published")
        # A buffered row would (incorrectly) create unknown [20,40), above [40,60).
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z", None,
                   status="buffered")
        # A quarantined row would (incorrectly) lower the value after :40.
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:40Z", 5,
                   status="quarantined")
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 60.0
        assert body["covered_seconds"] == 60.0
        assert body["unknown_seconds"] == 0.0

    def test_uses_real_published_stream_through_ingest(self, client):
        c, _ = client
        sid = create_session(c, sources=("a", "b"), lateness=10.0)
        r = report(c, sid, [
            {"source_id": "a", "seq": 1,
             "observed_at": "2026-01-01T10:00:00Z", "value": 20},
            {"source_id": "a", "seq": 2,
             "observed_at": "2026-01-01T10:00:30Z", "value": 5},
            # a/b clocks at :50 -> WM :40, so a@:00 and a@:30 publish; the
            # :50 readings stay buffered and must not touch the exposure.
            {"source_id": "a", "seq": 3,
             "observed_at": "2026-01-01T10:00:50Z", "value": 20},
            {"source_id": "b", "seq": 1,
             "observed_at": "2026-01-01T10:00:50Z", "value": 1},
        ])
        assert r.json()["published_total"] == 2, r.text
        body = ex(c, sid, source="a")
        assert body["above_threshold_seconds"] == 30.0
        assert body["covered_seconds"] == 60.0
        assert body["unknown_seconds"] == 0.0

    def test_closed_session_exposure_uses_frozen_stream(self, client):
        c, _ = client
        sid = create_session(c, sources=("a", "b"), lateness=10.0)
        report(c, sid, [
            {"source_id": "a", "seq": 1,
             "observed_at": "2026-01-01T10:00:00Z", "value": 20},
            {"source_id": "b", "seq": 1,
             "observed_at": "2026-01-01T10:00:00Z", "value": 1},
        ])
        publish_all(c, sid, target_observed_at="2026-01-01T10:01:00Z")
        assert c.get(f"/sessions/{sid}").json()["closed"] is True
        body = ex(c, sid, source="a")
        assert body["above_threshold_seconds"] == 60.0


class TestPrecisionAndWindow:
    def test_microsecond_precision_is_exact(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1,
                   "2026-01-01T10:00:00.000000Z", 20)
        insert_row(db_path, sid, "a", 2,
                   "2026-01-01T10:00:00.500000Z", 5)
        body = ex(
            c, sid,
            lo="2026-01-01T10:00:00Z",
            hi="2026-01-01T10:00:01Z",
        )
        assert body["above_threshold_seconds"] == 0.5
        assert body["covered_seconds"] == 1.0
        assert body["unknown_seconds"] == 0.0
        assert body["ratio_above"] == 0.5

    def test_window_filter_excludes_readings_after_to(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T09:00:00Z", 5)
        # Far in the future relative to the window: the carried value stays 5.
        insert_row(db_path, sid, "a", 9, "2026-01-01T12:00:00Z", 20)
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 0.0
        assert body["covered_seconds"] == 60.0

    def test_offset_and_naive_timestamps_normalised_to_utc(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T12:00:00+02:00", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:30Z", 5)
        body = ex(
            c, sid,
            lo="2026-01-01T12:00:00+02:00",  # == 10:00:00Z
            hi="2026-01-01T10:01:00",        # naive -> UTC
        )
        assert body["from"] == "2026-01-01T10:00:00Z"
        assert body["to"] == "2026-01-01T10:01:00Z"
        assert body["above_threshold_seconds"] == 30.0


class TestErrors:
    def test_unknown_session_404(self, client):
        c, _ = client
        r = exposure(c, "nope", source_id="a",
                     **{"from": "2026-01-01T10:00:00Z",
                        "to": "2026-01-01T10:01:00Z"}, threshold="10")
        assert r.status_code == 404

    def test_unknown_source_400(self, client):
        c, _ = client
        sid = create_session(c, sources=("a", "b"))
        r = exposure(c, sid, source_id="zzz",
                     **{"from": "2026-01-01T10:00:00Z",
                        "to": "2026-01-01T10:01:00Z"}, threshold="10")
        assert r.status_code == 400

    def test_missing_source_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = exposure(c, sid,
                     **{"from": "2026-01-01T10:00:00Z",
                        "to": "2026-01-01T10:01:00Z"}, threshold="10")
        assert r.status_code == 400

    def test_blank_source_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = exposure(c, sid, source_id="  ",
                     **{"from": "2026-01-01T10:00:00Z",
                        "to": "2026-01-01T10:01:00Z"}, threshold="10")
        assert r.status_code == 400

    @pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity", "nan", "inf", "junk", ""])
    def test_non_finite_or_non_numeric_threshold_400(self, client, bad):
        c, _ = client
        sid = create_session(c)
        r = exposure(c, sid, source_id="a",
                     **{"from": "2026-01-01T10:00:00Z",
                        "to": "2026-01-01T10:01:00Z"}, threshold=bad)
        assert r.status_code == 400

    def test_missing_threshold_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = exposure(c, sid, source_id="a",
                     **{"from": "2026-01-01T10:00:00Z",
                        "to": "2026-01-01T10:01:00Z"})
        assert r.status_code == 400

    @pytest.mark.parametrize("lo,hi", [
        ("2026-01-01T10:01:00Z", "2026-01-01T10:00:00Z"),  # backwards
        ("2026-01-01T10:00:00Z", "2026-01-01T10:00:00Z"),  # empty [t,t)
    ])
    def test_invalid_interval_400(self, client, lo, hi):
        c, _ = client
        sid = create_session(c)
        r = exposure(c, sid, source_id="a", **{"from": lo, "to": hi},
                     threshold="10")
        assert r.status_code == 400

    @pytest.mark.parametrize("field", ["from", "to"])
    def test_missing_or_unparsable_bounds_400(self, client, field):
        c, _ = client
        sid = create_session(c)
        params = {
            "source_id": "a",
            "from": "2026-01-01T10:00:00Z",
            "to": "2026-01-01T10:01:00Z",
            "threshold": "10",
        }
        del params[field]
        assert exposure(c, sid, **params).status_code == 400
        params[field] = "not-a-timestamp"
        assert exposure(c, sid, **params).status_code == 400

    def test_blank_value_key_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = exposure(c, sid, source_id="a",
                     **{"from": "2026-01-01T10:00:00Z",
                        "to": "2026-01-01T10:01:00Z"},
                     threshold="10", value_key="   ")
        assert r.status_code == 400


class TestReadOnlySnapshot:
    def test_call_changes_nothing(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a", "b"))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20,
                   status="published")
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:20Z", None,
                   status="buffered")
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:40Z", 5,
                   status="quarantined")

        def snapshot():
            with sqlite3.connect(db_path) as raw:
                raw.row_factory = sqlite3.Row
                return {
                    name: [dict(r) for r in raw.execute(f"SELECT * FROM {name}")]
                    for name in (
                        "sessions", "session_sources", "readings",
                        "alert_rules", "alert_events", "checkpoints",
                    )
                }

        before = snapshot()
        for _ in range(3):
            r = exposure(c, sid, source_id="a",
                         **{"from": "2026-01-01T09:00:00Z",
                            "to": "2026-01-01T11:00:00Z"},
                         threshold="10", value_key="temp")
            assert r.status_code == 200, r.text
        assert snapshot() == before

    def test_alerts_and_watermark_untouched(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a", "b"))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        before = c.get(f"/sessions/{sid}").json()
        ex(c, sid)
        after = c.get(f"/sessions/{sid}").json()
        assert after == before
        # No alert rule rows appeared.
        assert c.get(f"/sessions/{sid}/alert-rules").json()["rules"] == []

    def test_reads_one_snapshot_during_uncommitted_write(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 5)

        opened = threading.Event()
        release = threading.Event()

        def hold_uncommitted_insert():
            writer = sqlite3.connect(db_path, timeout=30)
            try:
                writer.execute("BEGIN IMMEDIATE")
                writer.execute(
                    "INSERT INTO readings (session_id, source_id, seq,"
                    " observed_at, payload, status, first_received_at,"
                    " published_watermark, quarantined_watermark)"
                    " VALUES (?, 'a', 2, ?, '20', 'published', ?, ?, NULL)",
                    (sid, t("2026-01-01T10:00:30Z"),
                     t("2026-01-01T10:00:30Z"),
                     t("2026-01-01T10:00:30Z")),
                )
                opened.set()
                release.wait(2.0)
                writer.commit()
            finally:
                writer.close()

        thread = threading.Thread(target=hold_uncommitted_insert)
        thread.start()
        try:
            assert opened.wait(2.0)
            # The uncommitted high reading is invisible: the whole minute is
            # covered by value 5, so nothing is above the threshold.
            body = ex(c, sid)
            assert body["above_threshold_seconds"] == 0.0
            assert body["covered_seconds"] == 60.0
        finally:
            release.set()
            thread.join()

        # After commit the second half of the interval is above.
        body = ex(c, sid)
        assert body["above_threshold_seconds"] == 30.0
        assert body["covered_seconds"] == 60.0


class TestHugeIntegerValues:
    """A JSON integer too large for a finite double (e.g. 10**400) is not a
    valid sample: it forms an unknown stretch instead of crashing the read."""

    BIG = 10**400

    def test_huge_int_value_is_unknown_not_above(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 20)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:30Z", self.BIG)
        body = ex(c, sid, source="a", threshold="10")
        # [00,30) is above; [30,60) is unknown -- the huge integer neither
        # counts as covered nor carries the earlier valid value forward.
        assert body["above_threshold_seconds"] == 30.0
        assert body["covered_seconds"] == 30.0
        assert body["unknown_seconds"] == 30.0
        assert body["ratio_above"] == 1.0

    def test_huge_int_under_value_key_is_unknown(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z",
                   {"temp": self.BIG})
        body = ex(c, sid, source="a", threshold="10", value_key="temp")
        assert body["covered_seconds"] == 0.0
        assert body["unknown_seconds"] == 60.0
        assert body["ratio_above"] is None
