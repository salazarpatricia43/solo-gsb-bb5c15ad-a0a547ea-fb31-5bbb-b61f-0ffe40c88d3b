"""End-to-end tests for the read-only local-spikes (median + MAD) endpoint."""
from __future__ import annotations

import json
import math
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


def spikes(client, sid, **params):
    return client.get(f"/sessions/{sid}/local-spikes", params=params)


def call(
    client,
    sid,
    source="a",
    lo="2026-01-01T10:00:00Z",
    hi="2026-01-01T11:00:00Z",
    window="60",
    sensitivity="3",
    **extra,
):
    params = {
        "source_id": source,
        "from": lo,
        "to": hi,
        "window_seconds": window,
        "sensitivity": sensitivity,
    }
    params.update(extra)
    return spikes(client, sid, **params)


def spike_keys(body):
    return [(s["key"]["observed_at"], s["key"]["seq"]) for s in body["spikes"]]


class TestBasicDetection:
    def test_single_outlier_against_identical_neighbours_mad_zero(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Eight calm readings of 10, one 15 in the middle; window covers all.
        times = [
            "2026-01-01T10:00:00Z",
            "2026-01-01T10:00:05Z",
            "2026-01-01T10:00:10Z",
            "2026-01-01T10:00:20Z",
            "2026-01-01T10:00:25Z",
            "2026-01-01T10:00:30Z",
            "2026-01-01T10:00:35Z",
        ]
        for seq, ts in enumerate(times, start=1):
            insert_row(db_path, sid, "a", seq, ts, 10)
        insert_row(db_path, sid, "a", 100, "2026-01-01T10:00:15Z", 15)

        body = call(c, sid, window="3600", sensitivity="3").json()
        assert body["readings_count"] == 8
        assert body["valid_count"] == 8
        assert body["insufficient_neighbors_count"] == 0
        assert len(body["spikes"]) == 1
        sp = body["spikes"][0]
        assert sp["key"] == {
            "observed_at": "2026-01-01T10:00:15Z",
            "source_id": "a",
            "seq": 100,
        }
        assert sp["value"] == 15
        assert sp["local_median"] == 10
        assert sp["mad"] == 0
        assert sp["deviation"] == 5

    def test_mad_nonzero_and_full_spike_fields(self, client):
        # Candidate 50 at :30; neighbours alternate 8/10/12 within 35s.
        c, db_path = client
        sid = create_session(c, sources=("a",))
        rows = [
            (1, "2026-01-01T10:00:00Z", 10),
            (2, "2026-01-01T10:00:10Z", 12),
            (3, "2026-01-01T10:00:20Z", 8),
            (4, "2026-01-01T10:00:30Z", 50),
            (5, "2026-01-01T10:00:40Z", 10),
            (6, "2026-01-01T10:00:50Z", 12),
            (7, "2026-01-01T10:01:00Z", 8),
        ]
        for seq, ts, value in rows:
            insert_row(db_path, sid, "a", seq, ts, value)

        body = call(c, sid, window="35", sensitivity="3").json()
        assert body["valid_count"] == 7
        assert body["insufficient_neighbors_count"] == 0
        assert len(body["spikes"]) == 1
        sp = body["spikes"][0]
        assert sp["key"]["seq"] == 4
        assert sp["value"] == 50
        assert sp["local_median"] == 10
        assert sp["mad"] == 2
        assert sp["deviation"] == 40

    def test_sensitivity_coefficient_suppresses_spike(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        for seq, (ts, value) in enumerate(
            [
                ("2026-01-01T10:00:00Z", 10),
                ("2026-01-01T10:00:10Z", 12),
                ("2026-01-01T10:00:20Z", 8),
                ("2026-01-01T10:00:30Z", 50),
                ("2026-01-01T10:00:40Z", 10),
                ("2026-01-01T10:00:50Z", 12),
                ("2026-01-01T10:01:00Z", 8),
            ],
            start=1,
        ):
            insert_row(db_path, sid, "a", seq, ts, value)
        # deviation 40, MAD 2: k=3 flags, k=100 does not.
        body = call(c, sid, window="35", sensitivity="3").json()
        assert len(body["spikes"]) == 1
        body = call(c, sid, window="35", sensitivity="100").json()
        assert body["spikes"] == []
        assert body["insufficient_neighbors_count"] == 0

    def test_strict_boundary_deviation_equals_k_times_mad_not_spike(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Candidate at :30; with window 20s its neighbours are exactly
        # :10=10, :20=10, :40=12, :50=12 (boundary inclusive) -> median 11,
        # MAD 1.  A candidate value of 12 sits exactly on 1 * MAD.
        layout = [
            (1, "2026-01-01T10:00:10Z", 10),
            (2, "2026-01-01T10:00:20Z", 10),
            (3, "2026-01-01T10:00:30Z", 12),
            (4, "2026-01-01T10:00:40Z", 12),
            (5, "2026-01-01T10:00:50Z", 12),
        ]
        for seq, ts, value in layout:
            insert_row(db_path, sid, "a", seq, ts, value)
        body = call(c, sid, window="20", sensitivity="1").json()
        assert all(s["key"]["seq"] != 3 for s in body["spikes"])
        # And the statistics on the boundary-distance neighbours are exact.
        cand = next(
            (
                s
                for s in call(c, sid, window="20", sensitivity="0.0001").json()["spikes"]
                if s["key"]["seq"] == 3
            ),
            None,
        )
        # With a tiny coefficient the same reading now flags, exposing the
        # computed stats: median 11, MAD 1, deviation 1.
        assert cand is not None
        assert cand["local_median"] == 11
        assert cand["mad"] == 1
        assert cand["deviation"] == 1


class TestNeighbourWindow:
    def test_fewer_than_three_neighbours_never_judged(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Huge outlier in the middle but every reading has < 3 neighbours.
        for seq, (ts, value) in enumerate(
            [
                ("2026-01-01T10:00:00Z", 10),
                ("2026-01-01T10:00:30Z", 1000),
                ("2026-01-01T10:01:00Z", 10),
            ],
            start=1,
        ):
            insert_row(db_path, sid, "a", seq, ts, value)
        body = call(c, sid, window="30", sensitivity="3").json()
        assert body["valid_count"] == 3
        assert body["insufficient_neighbors_count"] == 3
        assert body["spikes"] == []

    def test_window_boundary_is_inclusive(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Distances from :30: 30, 15, 15, 30 seconds; window 30 must include
        # all four (>= 3 neighbours), window 29 only the two at 15 (< 3).
        layout = [
            (1, "2026-01-01T10:00:00Z", 10),
            (2, "2026-01-01T10:00:15Z", 10),
            (3, "2026-01-01T10:00:30Z", 50),
            (4, "2026-01-01T10:00:45Z", 10),
            (5, "2026-01-01T10:01:00Z", 10),
        ]
        for seq, ts, value in layout:
            insert_row(db_path, sid, "a", seq, ts, value)
        body = call(c, sid, window="29", sensitivity="3").json()
        # Candidate has two neighbours -> undecided, no spike.
        assert body["insufficient_neighbors_count"] >= 1
        assert all(s["key"]["seq"] != 3 for s in body["spikes"])
        body = call(c, sid, window="30", sensitivity="3").json()
        cand = next(s for s in body["spikes"] if s["key"]["seq"] == 3)
        assert cand["mad"] == 0
        assert cand["deviation"] == 40

    def test_sub_microsecond_window_is_not_rounded_up(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # One microsecond between adjacent readings; the outlier seq 3 sits
        # 2us away from seqs 1 and 5 and 1us away from seqs 2 and 4.
        layout = [
            (1, "2026-01-01T10:00:00.000000Z", 10),
            (2, "2026-01-01T10:00:00.000001Z", 10),
            (3, "2026-01-01T10:00:00.000002Z", 50),
            (4, "2026-01-01T10:00:00.000003Z", 10),
            (5, "2026-01-01T10:00:00.000004Z", 10),
        ]
        for seq, ts, value in layout:
            insert_row(db_path, sid, "a", seq, ts, value)
        # Window 1.5us reaches only the 1us-distant pair (2 neighbours), so
        # seq 3 stays undecided; rounding the window up to 2us would wrongly
        # pull in seqs 1 and 5 and flag a spike.
        body = call(c, sid, window="0.0000015", sensitivity="3").json()
        assert body["spikes"] == []
        assert body["insufficient_neighbors_count"] == 5
        # At exactly 2us the boundary is inclusive: seq 3 gains four
        # identical neighbours (MAD 0) and is flagged.
        body = call(c, sid, window="0.000002", sensitivity="3").json()
        assert spike_keys(body) == [("2026-01-01T10:00:00.000002Z", 3)]
        assert body["spikes"][0]["local_median"] == 10
        assert body["spikes"][0]["mad"] == 0
        assert body["spikes"][0]["deviation"] == 40

    def test_neighbours_are_other_valid_readings_only(self, client):
        # The reading itself is never its own neighbour; readings outside the
        # window are ignored even if they would change the median.
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T09:00:00Z", 1000)
        insert_row(db_path, sid, "a", 2, "2026-01-01T09:30:00Z", 1000)
        insert_row(db_path, sid, "a", 3, "2026-01-01T09:59:59Z", 1000)
        for seq, (secs, value) in enumerate(
            [("00", 10), ("10", 10), ("20", 10), ("30", 10)], start=4
        ):
            insert_row(db_path, sid, "a", seq, f"2026-01-01T10:00:{secs}Z", value)
        insert_row(db_path, sid, "a", 99, "2026-01-01T11:30:00Z", 1000)
        # All in-window values are 10 (the :59 distant reading is > 0 away but
        # inside a huge window too); use a 60s window so far readings drop out.
        body = call(
            c, sid,
            lo="2026-01-01T10:00:00Z",
            hi="2026-01-01T10:00:30Z",
            window="60", sensitivity="3",
        ).json()
        assert body["readings_count"] == 4
        assert body["insufficient_neighbors_count"] == 0
        assert body["spikes"] == []

    def test_interval_bounds_are_inclusive(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        for seq, secs in enumerate(["00", "10", "20"], start=1):
            insert_row(db_path, sid, "a", seq, f"2026-01-01T10:00:{secs}Z", 10)
        insert_row(db_path, sid, "a", 9, "2026-01-01T09:59:59Z", 1000)
        insert_row(db_path, sid, "a", 10, "2026-01-01T10:00:21Z", 1000)
        body = call(
            c, sid,
            lo="2026-01-01T10:00:00Z",
            hi="2026-01-01T10:00:20Z",
            window="60", sensitivity="3",
        ).json()
        assert body["readings_count"] == 3
        assert spike_keys(body) == []

    def test_point_interval_is_allowed(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 1000)
        r = call(
            c, sid,
            lo="2026-01-01T10:00:00Z",
            hi="2026-01-01T10:00:00Z",
            window="60",
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["readings_count"] == 1
        assert body["valid_count"] == 1
        assert body["insufficient_neighbors_count"] == 1
        assert body["spikes"] == []


class TestValueExtraction:
    def test_value_key_extraction_and_invalid_skips(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        rows = [
            (1, "2026-01-01T10:00:00Z", {"temp": 10}),
            (2, "2026-01-01T10:00:05Z", {"temp": 10}),
            (3, "2026-01-01T10:00:10Z", {"other": 10}),          # missing key
            (4, "2026-01-01T10:00:15Z", {"temp": True}),         # boolean
            (5, "2026-01-01T10:00:20Z", {"temp": None}),         # null
            (6, "2026-01-01T10:00:25Z", {"temp": "10"}),         # string
            (7, "2026-01-01T10:00:30Z", {"temp": float("nan")}),  # non-finite
            (8, "2026-01-01T10:00:35Z", 10),                     # non-object
            (9, "2026-01-01T10:00:40Z", True),                   # bare boolean
            (10, "2026-01-01T10:00:45Z", {"temp": 10}),
            (11, "2026-01-01T10:00:50Z", {"temp": 10}),
            (12, "2026-01-01T10:00:55Z", {"temp": 99}),
        ]
        for seq, ts, value in rows:
            insert_row(db_path, sid, "a", seq, ts, value)
        body = call(c, sid, window="3600", sensitivity="3", value_key="temp").json()
        # Valid readings: seqs 1, 2, 10, 11, 12 (five finite numbers).
        assert body["readings_count"] == 12
        assert body["valid_count"] == 5
        # The 99 spike has four identical-10 neighbours -> MAD 0 -> flagged.
        assert len(body["spikes"]) == 1
        assert body["spikes"][0]["key"]["seq"] == 12
        assert body["spikes"][0]["value"] == 99

    def test_bare_numeric_values_without_value_key(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        for seq, value in enumerate([10, 10, 10, 50], start=1):
            insert_row(
                db_path, sid, "a", seq,
                f"2026-01-01T10:00:0{seq}Z", value,
            )
        body = call(c, sid, window="60", sensitivity="3").json()
        assert body["valid_count"] == 4
        assert body["spikes"][0]["key"]["seq"] == 4

    def test_published_only(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 10)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:05Z", 10)
        # Non-published outliers would otherwise be spike neighbours/candidates.
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:10Z", 5000,
                   status="buffered")
        insert_row(db_path, sid, "a", 4, "2026-01-01T10:00:15Z", 5000,
                   status="quarantined")
        insert_row(db_path, sid, "a", 5, "2026-01-01T10:00:20Z", 10)
        insert_row(db_path, sid, "a", 6, "2026-01-01T10:00:25Z", 10)
        body = call(c, sid, window="60", sensitivity="3").json()
        assert body["readings_count"] == 4
        assert body["valid_count"] == 4
        assert body["spikes"] == []


class TestOrdering:
    def test_same_observed_at_orders_by_seq(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        ts = "2026-01-01T10:00:00Z"
        # Six calm readings then two outliers, all at the same event time:
        # stable (observed_at, seq) order must put seq 7 before seq 8.
        for seq, value in [(i, 10) for i in range(1, 7)] + [(7, 50), (8, 60)]:
            insert_row(db_path, sid, "a", seq, ts, value)
        body = call(c, sid, window="60", sensitivity="3").json()
        assert spike_keys(body) == [(ts, 7), (ts, 8)]
        assert [s["value"] for s in body["spikes"]] == [50, 60]

    def test_spikes_sorted_by_observed_at(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Interleave calm and outlying readings with small windows so each
        # spike is judged against its own local cluster.
        layout = [
            (1, "2026-01-01T10:00:00Z", 10),
            (2, "2026-01-01T10:00:01Z", 10),
            (3, "2026-01-01T10:00:02Z", 10),
            (4, "2026-01-01T10:00:03Z", 99),
            (5, "2026-01-01T10:00:30Z", 10),
            (6, "2026-01-01T10:00:31Z", 10),
            (7, "2026-01-01T10:00:32Z", 10),
            (8, "2026-01-01T10:00:33Z", -77),
        ]
        for seq, ts, value in layout:
            insert_row(db_path, sid, "a", seq, ts, value)
        body = call(c, sid, window="5", sensitivity="3").json()
        assert [s["key"]["seq"] for s in body["spikes"]] == [4, 8]
        assert body["spikes"][1]["value"] == -77


class TestIntervalCap:
    def _insert_many(self, db_path, sid, n, value_fn=None):
        for i in range(1, n + 1):
            value = value_fn(i) if value_fn else 10
            insert_row(
                db_path, sid, "a", i,
                f"2026-01-01T10:{i // 60:02d}:{i % 60:02d}Z", value,
            )

    def test_exactly_200_is_allowed(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        self._insert_many(db_path, sid, 200)
        r = call(c, sid, window="5", sensitivity="3")
        assert r.status_code == 200, r.text
        assert r.json()["readings_count"] == 200

    def test_201_published_readings_rejected(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        self._insert_many(db_path, sid, 201)
        r = call(c, sid, window="5", sensitivity="3")
        assert r.status_code == 400
        assert "200" in r.json()["detail"]

    def test_cap_counts_rows_regardless_of_validity(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # 200 valid numbers plus one non-numeric payload = 201 rows -> rejected.
        self._insert_many(db_path, sid, 200)
        insert_row(db_path, sid, "a", 201, "2026-01-01T10:04:00Z", "hot")
        r = call(c, sid, window="5", sensitivity="3")
        assert r.status_code == 400

    def test_cap_scoped_to_interval_and_source(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a", "b"))
        self._insert_many(db_path, sid, 201)
        insert_row(db_path, sid, "b", 1, "2026-01-01T10:00:00Z", 10)
        # A narrow interval contains few rows even though the source has 201;
        # source b is unaffected by source a's overflow.
        r = call(
            c, sid,
            lo="2026-01-01T10:00:00Z",
            hi="2026-01-01T10:00:30Z",
            window="5",
        )
        assert r.status_code == 200, r.text
        assert r.json()["readings_count"] == 30
        r = call(c, sid, source="b", window="5")
        assert r.status_code == 200
        assert r.json()["readings_count"] == 1


class TestErrors:
    def test_unknown_session_404(self, client):
        c, _ = client
        r = call(c, "nope")
        assert r.status_code == 404

    def test_unknown_source_400(self, client):
        c, _ = client
        sid = create_session(c, sources=("a", "b"))
        assert call(c, sid, source="zzz").status_code == 400

    @pytest.mark.parametrize("field", ["source_id", "from", "to", "window_seconds", "sensitivity"])
    def test_missing_required_params_400(self, client, field):
        c, _ = client
        sid = create_session(c)
        params = {
            "source_id": "a",
            "from": "2026-01-01T10:00:00Z",
            "to": "2026-01-01T11:00:00Z",
            "window_seconds": "60",
            "sensitivity": "3",
        }
        del params[field]
        assert spikes(c, sid, **params).status_code == 400

    @pytest.mark.parametrize("bad", ["0", "-1", "-0.001", "NaN", "Infinity", "-Infinity", "nan", "inf", "junk", "", "   "])
    def test_non_positive_window_400(self, client, bad):
        c, _ = client
        sid = create_session(c)
        assert call(c, sid, window=bad).status_code == 400

    @pytest.mark.parametrize("bad", ["0", "-5", "NaN", "Infinity", "-inf", "x", "", "  "])
    def test_non_positive_sensitivity_400(self, client, bad):
        c, _ = client
        sid = create_session(c)
        assert call(c, sid, sensitivity=bad).status_code == 400

    @pytest.mark.parametrize("field", ["from", "to"])
    def test_unparsable_bounds_400(self, client, field):
        c, _ = client
        sid = create_session(c)
        kwargs = {"lo": "2026-01-01T10:00:00Z", "hi": "2026-01-01T11:00:00Z"}
        if field == "from":
            kwargs["lo"] = "not-a-time"
        else:
            kwargs["hi"] = "nonsense"
        assert call(c, sid, **kwargs).status_code == 400

    def test_backwards_interval_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = call(c, sid,
                 lo="2026-01-01T11:00:00Z",
                 hi="2026-01-01T10:00:00Z")
        assert r.status_code == 400

    def test_blank_source_and_value_key_400(self, client):
        c, _ = client
        sid = create_session(c)
        assert call(c, sid, source="  ").status_code == 400
        assert call(c, sid, **{"value_key": "  "}).status_code == 400


class TestNormalisationAndEcho:
    def test_offsets_normalised_to_utc_and_params_echoed(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T12:00:00+02:00", 10)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:10Z", 10)
        body = call(
            c, sid,
            lo="2026-01-01T12:00:00+02:00",
            hi="2026-01-01T10:01:00",
            window="30.5", sensitivity="2.5",
        ).json()
        assert body["from"] == "2026-01-01T10:00:00Z"
        assert body["to"] == "2026-01-01T10:01:00Z"
        assert body["window_seconds"] == 30.5
        assert body["sensitivity"] == 2.5
        assert body["value_key"] is None


class TestEndToEnd:
    def test_published_through_watermark_then_analysed(self, client):
        c, _ = client
        sid = create_session(c, sources=("a",), lateness=0.0)
        # Push a small published timeline for the single source.
        readings = [
            {"source_id": "a", "seq": i,
             "observed_at": f"2026-01-01T10:00:{i:02d}Z",
             "value": (99 if i == 3 else 10)}
            for i in range(0, 8)
        ]
        r = c.post(
            f"/sessions/{sid}/readings",
            json={"readings": readings + [
                {"source_id": "a", "seq": 8,
                 "observed_at": "2026-01-01T10:01:30Z", "value": 10},
            ]},
        )
        assert r.status_code == 200, r.text
        r = call(c, sid,
                 lo="2026-01-01T10:00:00Z",
                 hi="2026-01-01T10:00:07Z",
                 window="10", sensitivity="3")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["readings_count"] == 8
        assert len(body["spikes"]) == 1
        assert body["spikes"][0]["key"]["seq"] == 3


class TestReadOnlySnapshot:
    def test_call_changes_nothing(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a", "b"))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 10)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:05Z", 50)
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:10Z", 10,
                   status="buffered")

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
            r = call(c, sid, window="60", sensitivity="3")
            assert r.status_code == 200, r.text
        assert snapshot() == before

    def test_reads_one_snapshot_during_uncommitted_write(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        for seq, secs in enumerate(["00", "01", "02"], start=1):
            insert_row(db_path, sid, "a", seq,
                       f"2026-01-01T10:00:{secs}Z", 10)

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
                    " VALUES (?, 'a', 4, ?, '999', 'published', ?, ?, NULL)",
                    (sid, t("2026-01-01T10:00:03Z"),
                     t("2026-01-01T10:00:03Z"),
                     t("2026-01-01T10:00:03Z")),
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
            # Uncommitted reading invisible: three readings, each has two
            # neighbours -> all undecided, no spike.
            body = call(c, sid, window="60", sensitivity="3").json()
            assert body["readings_count"] == 3
            assert body["insufficient_neighbors_count"] == 3
            assert body["spikes"] == []
        finally:
            release.set()
            thread.join()

        # After commit the 999 has three calm neighbours -> flagged.
        body = call(c, sid, window="60", sensitivity="3").json()
        assert body["readings_count"] == 4
        assert len(body["spikes"]) == 1
        assert body["spikes"][0]["value"] == 999


class TestOverflowSafeMedian:
    """Even-count medians must never poison finite stats with Infinity.

    ``statistics.median`` averages the two middle values as ``(a + b) / 2``,
    whose intermediate sum overflows for very large readings; the endpoint
    must instead return the finite midpoint and keep strict MAD detection.
    """

    def test_four_1e308_neighbours_candidate_zero_is_flagged(self, client):
        # The reported regression: candidate 0, four neighbours of 1e308, all
        # inside one window.  The naive (1e308 + 1e308) / 2 overflows to
        # Infinity, which made median/MAD/deviation Infinity and the strict
        # comparison false -- the spike was missed.
        c, db_path = client
        sid = create_session(c, sources=("a",))
        layout = [
            (1, "2026-01-01T10:00:00Z", 1e308),
            (2, "2026-01-01T10:00:05Z", 1e308),
            (3, "2026-01-01T10:00:10Z", 0.0),
            (4, "2026-01-01T10:00:15Z", 1e308),
            (5, "2026-01-01T10:00:20Z", 1e308),
        ]
        for seq, ts, value in layout:
            insert_row(db_path, sid, "a", seq, ts, value)

        # Strict JSON: NaN/Infinity are invalid tokens, so a poisoned result
        # fails at decoding rather than silently round-tripping.
        r = call(c, sid, window="60", sensitivity="3")
        assert r.status_code == 200, r.text
        body = json.loads(r.content.decode("utf-8"), parse_constant=_reject_nonfinite)
        assert body["valid_count"] == 5
        assert body["insufficient_neighbors_count"] == 0
        assert spike_keys(body) == [("2026-01-01T10:00:10Z", 3)]
        sp = body["spikes"][0]
        assert sp["value"] == 0
        assert sp["local_median"] == 1e308
        assert sp["mad"] == 0
        assert sp["deviation"] == 1e308
        assert all(math.isfinite(x) for x in (
            sp["local_median"], sp["mad"], sp["deviation"]
        ))

    def test_four_negative_1e308_neighbours_candidate_zero_is_flagged(self, client):
        # Same overflow on the negative half-axis: (-1e308 + -1e308) / 2.
        c, db_path = client
        sid = create_session(c, sources=("a",))
        layout = [
            (1, "2026-01-01T10:00:00Z", -1e308),
            (2, "2026-01-01T10:00:05Z", -1e308),
            (3, "2026-01-01T10:00:10Z", 0.0),
            (4, "2026-01-01T10:00:15Z", -1e308),
            (5, "2026-01-01T10:00:20Z", -1e308),
        ]
        for seq, ts, value in layout:
            insert_row(db_path, sid, "a", seq, ts, value)

        r = call(c, sid, window="60", sensitivity="3")
        assert r.status_code == 200, r.text
        body = json.loads(r.content.decode("utf-8"), parse_constant=_reject_nonfinite)
        assert spike_keys(body) == [("2026-01-01T10:00:10Z", 3)]
        sp = body["spikes"][0]
        assert sp["local_median"] == -1e308
        assert sp["mad"] == 0
        assert sp["deviation"] == 1e308

    def test_mad_even_average_near_max_does_not_overflow(self, client):
        # Candidate 1e308 with two 1e308 and two -1e308 neighbours: the
        # neighbour median is 0 and every deviation is 1e308, so the MAD's
        # even-count average is (1e308 + 1e308) / 2 -- another intermediate
        # overflow.  With a finite MAD of 1e308 and deviation also 1e308, the
        # strict ``deviation > sensitivity * MAD`` must stay false at k = 1;
        # under the bug the MAD became Infinity.
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Two extremes (one of each sign) exactly 60s on either side of the
        # centre candidate.  The inclusive 60s window gives the candidate four
        # neighbours, while each extreme sees only its same-moment companion
        # and the centre (two neighbours) and stays undecided.
        layout = [
            (1, "2026-01-01T09:59:00Z", 1e308),
            (2, "2026-01-01T09:59:00Z", -1e308),
            (3, "2026-01-01T10:00:00Z", 1e308),
            (4, "2026-01-01T10:01:00Z", 1e308),
            (5, "2026-01-01T10:01:00Z", -1e308),
        ]
        for seq, ts, value in layout:
            insert_row(db_path, sid, "a", seq, ts, value)

        def analyse(sensitivity):
            r = call(
                c, sid,
                lo="2026-01-01T09:59:00Z",
                hi="2026-01-01T10:01:00Z",
                window="60", sensitivity=sensitivity,
            )
            assert r.status_code == 200, r.text
            return json.loads(r.content.decode("utf-8"), parse_constant=_reject_nonfinite)

        body = analyse("1")
        assert body["insufficient_neighbors_count"] == 4
        assert body["spikes"] == []

        # With sensitivity below 1 the same finite MAD lets the candidate flag,
        # exposing median 0 / MAD 1e308 / deviation 1e308 as finite numbers.
        body = analyse("0.5")
        assert spike_keys(body) == [("2026-01-01T10:00:00Z", 3)]
        cand = body["spikes"][0]
        assert cand["value"] == 1e308
        assert cand["local_median"] == 0
        assert cand["mad"] == 1e308
        assert cand["deviation"] == 1e308
        assert all(math.isfinite(x) for x in (
            cand["local_median"], cand["mad"], cand["deviation"]
        ))


class TestFiniteMedianHelper:
    """Direct checks of the overflow-safe median building blocks."""

    def test_midpoint_of_two_max_values(self):
        from app.local_spikes import _midpoint

        assert _midpoint(1e308, 1e308) == 1e308
        assert _midpoint(-1e308, -1e308) == -1e308
        assert math.isfinite(_midpoint(1e308, 1e308))
        assert math.isfinite(_midpoint(-1e308, -1e308))

    def test_midpoint_opposite_signs(self):
        from app.local_spikes import _midpoint

        assert _midpoint(-1e308, 1e308) == 0.0
        # Ordinary mixed-sign midpoint is the exact arithmetic mean.
        assert _midpoint(-2.0, 4.0) == 1.0
        # A tiny positive value next to -1e308 still yields a finite midpoint
        # (the +0.5 contribution is lost at this scale, but never Infinity).
        result = _midpoint(-1e308, 1.0)
        assert math.isfinite(result)
        assert result == pytest.approx(-5e307, rel=1e-15)

    def test_median_odd_and_even_counts(self):
        from app.local_spikes import _finite_median

        assert _finite_median([1e308, 1e308, 1e308]) == 1e308
        assert _finite_median([1e308] * 4) == 1e308
        assert _finite_median([-1e308] * 4) == -1e308
        # Mixed extreme neighbours: the even-count mean is 0 (no overflow).
        assert _finite_median([-1e308, -1e308, 1e308, 1e308]) == 0.0
        # Ordinary values keep the usual arithmetic mean.
        assert _finite_median([8.0, 10.0, 10.0, 12.0]) == 10.0
        assert _finite_median([10.0, 10.0, 12.0, 12.0]) == 11.0

    def test_mad_median_of_extreme_deviations_is_finite(self):
        from app.local_spikes import _finite_median

        deviations = [1e308, 1e308, 1e308, 1e308]
        mad = _finite_median(deviations)
        assert mad == 1e308
        assert math.isfinite(mad)


def _reject_nonfinite(token: str):
    """``parse_constant`` hook: strict JSON must never carry NaN/Infinity."""
    raise ValueError(f"non-finite JSON token: {token}")


class TestHugeIntegerValues:
    """A JSON integer too large for a finite double (e.g. 10**400) is skipped
    like any other invalid value: neither a candidate nor a neighbour."""

    BIG = 10**400

    def test_huge_int_value_is_skipped(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_row(db_path, sid, "a", 1, "2026-01-01T10:00:00Z", 10)
        insert_row(db_path, sid, "a", 2, "2026-01-01T10:00:10Z", 10)
        insert_row(db_path, sid, "a", 3, "2026-01-01T10:00:20Z", self.BIG)
        insert_row(db_path, sid, "a", 4, "2026-01-01T10:00:30Z", 10)
        insert_row(db_path, sid, "a", 5, "2026-01-01T10:00:40Z", 10)
        r = call(c, sid, source="a", window="60", sensitivity="3")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["readings_count"] == 5
        assert body["valid_count"] == 4
        assert body["insufficient_neighbors_count"] == 0
        assert body["spikes"] == []
