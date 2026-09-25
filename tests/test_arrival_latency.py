"""End-to-end tests for the read-only arrival-latency profile endpoint."""
from __future__ import annotations

import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app.time_utils import parse_event_time, store_event_time


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("SENSOR_DB_PATH", db_path)
    from app import database, main

    main.app.state.db_path = db_path
    database.init_db(db_path)
    with TestClient(main.app) as c:
        yield c, db_path


def create_session(client, sources=("a", "b", "c"), lateness=10.0, session_id="s"):
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


def latency(client, sid, **params):
    return client.get(f"/sessions/{sid}/arrival-latency", params=params)


def t(value: str) -> str:
    return store_event_time(parse_event_time(value))


def insert_reading(
    db_path,
    sid,
    source,
    seq,
    observed_at,
    first_received_at,
    *,
    status="buffered",
    value=1,
    published_watermark=None,
    quarantined_watermark=None,
):
    """Insert a reading row with an explicit, deterministic arrival time."""
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
                "1" if value == 1 else str(value),
                status,
                t(first_received_at),
                t(published_watermark) if published_watermark else None,
                t(quarantined_watermark) if quarantined_watermark else None,
            ),
        )
        raw.commit()


class TestArrivalLatencyProfile:
    def test_per_source_count_min_max_mean(self, client):
        c, db_path = client
        sid = create_session(c)
        # a: +5s and -3s (clock ahead of the server) -> min -3, max 5, mean 1
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:05Z")
        insert_reading(db_path, sid, "a", 2,
                       "2026-01-01T10:00:10Z", "2026-01-01T10:00:07Z")
        # b: exactly +20s
        insert_reading(db_path, sid, "b", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:20Z")

        r = latency(c, sid)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["sample_count"] == 3
        assert body["source_id"] is None
        assert body["from"] is None
        assert body["to"] is None
        # stable ordering by source_id
        assert [s["source_id"] for s in body["sources"]] == ["a", "b"]

        a = body["sources"][0]
        assert a["sample_count"] == 2
        assert a["min_latency_seconds"] == -3.0
        assert a["max_latency_seconds"] == 5.0
        assert a["avg_latency_seconds"] == 1.0

        b = body["sources"][1]
        assert b["sample_count"] == 1
        assert b["min_latency_seconds"] == 20.0
        assert b["max_latency_seconds"] == 20.0
        assert b["avg_latency_seconds"] == 20.0

    def test_negative_latency_kept_for_single_sample(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:10Z", "2026-01-01T10:00:00Z")
        body = latency(c, sid).json()
        stats = body["sources"][0]
        assert stats["sample_count"] == 1
        assert stats["min_latency_seconds"] == -10.0
        assert stats["max_latency_seconds"] == -10.0
        assert stats["avg_latency_seconds"] == -10.0

    def test_microsecond_exact_mean(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00.000010Z",
                       "2026-01-01T10:00:00.000011Z")  # +1 us
        insert_reading(db_path, sid, "a", 2,
                       "2026-01-01T10:00:00.000020Z",
                       "2026-01-01T10:00:00.000023Z")  # +3 us
        stats = latency(c, sid).json()["sources"][0]
        assert stats["min_latency_seconds"] == pytest.approx(0.000001)
        assert stats["max_latency_seconds"] == pytest.approx(0.000003)
        assert stats["avg_latency_seconds"] == pytest.approx(0.000002)

    def test_all_lifecycle_statuses_participate(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:02Z",
                       status="buffered")
        insert_reading(db_path, sid, "a", 2,
                       "2026-01-01T10:00:10Z", "2026-01-01T10:00:14Z",
                       status="published", published_watermark="2026-01-01T10:00:30Z")
        insert_reading(db_path, sid, "a", 3,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:01:00Z",
                       status="quarantined",
                       quarantined_watermark="2026-01-01T10:00:30Z")
        stats = latency(c, sid).json()["sources"][0]
        # delays 2s, 4s, 60s over all three statuses
        assert stats["sample_count"] == 3
        assert stats["min_latency_seconds"] == 2.0
        assert stats["max_latency_seconds"] == 60.0
        assert stats["avg_latency_seconds"] == pytest.approx(22.0)

    def test_source_filter(self, client):
        c, db_path = client
        sid = create_session(c)
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:05Z")
        insert_reading(db_path, sid, "b", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:20Z")

        r = latency(c, sid, source_id="b")
        assert r.status_code == 200
        body = r.json()
        assert body["source_id"] == "b"
        assert [s["source_id"] for s in body["sources"]] == ["b"]
        assert body["sources"][0]["sample_count"] == 1

    def test_unknown_source_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = latency(c, sid, source_id="nope")
        assert r.status_code == 400

    def test_blank_source_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = latency(c, sid, source_id="   ")
        assert r.status_code == 400

    def test_inclusive_event_time_window(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        # Filter is on the EVENT time observed_at; delays differ per reading.
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:01Z")  # +1
        insert_reading(db_path, sid, "a", 2,
                       "2026-01-01T10:00:10Z", "2026-01-01T10:00:12Z")  # +2
        insert_reading(db_path, sid, "a", 3,
                       "2026-01-01T10:00:20Z", "2026-01-01T10:00:23Z")  # +3

        body = latency(
            c, sid, **{"from": "2026-01-01T10:00:10Z", "to": "2026-01-01T10:00:20Z"}
        ).json()
        assert body["from"] == "2026-01-01T10:00:10Z"
        assert body["to"] == "2026-01-01T10:00:20Z"
        stats = body["sources"][0]
        # both bounds inclusive -> seq 2 and seq 3 only
        assert stats["sample_count"] == 2
        assert stats["min_latency_seconds"] == 2.0
        assert stats["max_latency_seconds"] == 3.0

        # lower bound alone, inclusive
        body = latency(c, sid, **{"from": "2026-01-01T10:00:10Z"}).json()
        assert body["sources"][0]["sample_count"] == 2

        # upper bound alone, inclusive
        body = latency(c, sid, **{"to": "2026-01-01T10:00:10Z"}).json()
        assert body["sources"][0]["sample_count"] == 2

    def test_bounds_echo_normalised_to_utc(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:05Z")
        body = latency(
            c, sid, **{"from": "2026-01-01T12:00:00+02:00", "to": "2026-01-01T10:00:00Z"}
        ).json()
        # from == to after normalisation, and the single reading sits on it
        assert body["from"] == "2026-01-01T10:00:00Z"
        assert body["sources"][0]["sample_count"] == 1

    def test_empty_sources_omitted_and_sorted(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a", "b", "c"))
        # b never reports; c reports first; ordering must still be a, c.
        insert_reading(db_path, sid, "c", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:09Z")
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:01Z")
        body = latency(c, sid).json()
        assert [s["source_id"] for s in body["sources"]] == ["a", "c"]
        assert body["sample_count"] == 2

    def test_no_readings_at_all(self, client):
        c, _ = client
        sid = create_session(c)
        body = latency(c, sid).json()
        assert body["sources"] == []
        assert body["sample_count"] == 0

    def test_window_matches_nothing(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:05Z")
        body = latency(c, sid, **{"from": "2027-01-01T00:00:00Z"}).json()
        assert body["sources"] == []
        assert body["sample_count"] == 0

    def test_works_on_closed_session(self, client):
        c, _ = client
        sid = create_session(c, sources=("a",))
        r = c.post(
            f"/sessions/{sid}/readings",
            json={"readings": [
                {"source_id": "a", "seq": 1,
                 "observed_at": "2000-01-01T00:00:00Z", "value": 1},
            ]},
        )
        assert r.status_code == 200
        r = c.post(
            f"/sessions/{sid}/sources/a/seal",
            json={"final_observed_at": "2000-01-01T00:00:00Z"},
        )
        assert r.status_code == 200 and r.json()["closed"] is True
        r = latency(c, sid)
        assert r.status_code == 200
        stats = r.json()["sources"][0]
        assert stats["sample_count"] == 1
        assert stats["min_latency_seconds"] > 365 * 24 * 3600


class TestArrivalLatencyErrors:
    def test_unknown_session_404(self, client):
        c, _ = client
        assert latency(c, "missing").status_code == 404

    def test_bad_time_bounds_400(self, client):
        c, _ = client
        sid = create_session(c)
        assert latency(c, sid, **{"from": "not-a-time"}).status_code == 400
        assert latency(c, sid, **{"to": "2026-13-99T00:00:00Z"}).status_code == 400

    def test_from_after_to_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = latency(
            c, sid,
            **{"from": "2026-01-01T10:00:20Z", "to": "2026-01-01T10:00:10Z"},
        )
        assert r.status_code == 400


class TestArrivalLatencyReadOnly:
    def test_call_changes_nothing(self, client):
        c, db_path = client
        sid = create_session(c)
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:05Z",
                       status="published",
                       published_watermark="2026-01-01T10:00:30Z")
        insert_reading(db_path, sid, "b", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:01:00Z",
                       status="quarantined",
                       quarantined_watermark="2026-01-01T10:00:30Z")

        def snapshot():
            with sqlite3.connect(db_path) as raw:
                raw.row_factory = sqlite3.Row
                return {
                    name: [dict(r) for r in raw.execute(f"SELECT * FROM {name}")]
                    for name in ("sessions", "session_sources", "readings")
                }

        before = snapshot()
        for _ in range(3):
            assert latency(c, sid).status_code == 200
            assert latency(
                c, sid, source_id="a",
                **{"from": "2026-01-01T10:00:00Z", "to": "2026-01-01T11:00:00Z"},
            ).status_code == 200
        assert snapshot() == before

    def test_session_state_unchanged(self, client):
        c, db_path = client
        sid = create_session(c)
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:05Z")
        before = c.get(f"/sessions/{sid}").json()
        latency(c, sid)
        after = c.get(f"/sessions/{sid}").json()
        assert after == before


class TestArrivalLatencySnapshot:
    def test_reads_one_snapshot_during_uncommitted_write(self, client):
        c, db_path = client
        sid = create_session(c, sources=("a",))
        insert_reading(db_path, sid, "a", 1,
                       "2026-01-01T10:00:00Z", "2026-01-01T10:00:05Z")

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
                    " VALUES (?, 'a', 2, ?, '1', 'buffered', ?, NULL, NULL)",
                    (sid, t("2026-01-01T10:00:10Z"), t("2026-01-01T10:00:40Z")),
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
            # The uncommitted row must not be visible (one consistent snapshot);
            # in WAL mode the read is not blocked by the held write lock.
            body = latency(c, sid).json()
            assert body["sample_count"] == 1
            assert body["sources"][0]["sample_count"] == 1
        finally:
            release.set()
            thread.join()

        # After commit the second sample is visible.
        body = latency(c, sid).json()
        assert body["sample_count"] == 2
