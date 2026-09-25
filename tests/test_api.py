"""End-to-end behaviour tests for the timeline merge API."""
from __future__ import annotations

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
        yield c


def create_session(client, sources=("a", "b"), lateness=10.0, session_id=None):
    body = {
        "source_ids": list(sources),
        "allowed_lateness_seconds": lateness,
    }
    if session_id:
        body["session_id"] = session_id
    r = client.post("/sessions", json=body)
    assert r.status_code == 201, r.text
    return r.json()["session_id"]


def report(client, sid, readings):
    return client.post(f"/sessions/{sid}/readings", json={"readings": readings})


def published_times(client, sid):
    out = []
    cursor = None
    while True:
        params = {"limit": 1000}
        if cursor:
            params["cursor"] = cursor
        page = client.get(f"/sessions/{sid}/stream", params=params).json()
        out.extend(page["readings"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


class TestSessionCreation:
    def test_create_and_get(self, client):
        sid = create_session(client, sources=["x", "y"], lateness=5.5, session_id="s1")
        assert sid == "s1"
        info = client.get("/sessions/s1").json()
        assert info["source_ids"] == ["x", "y"]
        assert info["allowed_lateness_seconds"] == 5.5
        assert info["all_sources_reported"] is False
        assert info["published_watermark"] is None
        assert info["effective_watermark"] is None

    def test_duplicate_session_id_conflicts(self, client):
        create_session(client, session_id="dup")
        r = client.post(
            "/sessions",
            json={"session_id": "dup", "source_ids": ["a"], "allowed_lateness_seconds": 1},
        )
        assert r.status_code == 409

    def test_invalid_source_list(self, client):
        r = client.post(
            "/sessions", json={"source_ids": ["a", "a"], "allowed_lateness_seconds": 1}
        )
        assert r.status_code == 422
        r = client.post("/sessions", json={"source_ids": [], "allowed_lateness_seconds": 1})
        assert r.status_code == 422
        r = client.post(
            "/sessions", json={"source_ids": ["a"], "allowed_lateness_seconds": -1}
        )
        assert r.status_code == 422

    def test_unknown_session_404(self, client):
        assert client.get("/sessions/nope").status_code == 404
        assert report(client, "nope", [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"}
        ]).status_code == 404


class TestWatermark:
    def test_nothing_published_until_all_sources_report(self, client):
        sid = create_session(client)
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:05Z", "value": 1},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:09Z", "value": 2},
        ]).json()
        assert r["all_sources_reported"] is False
        assert r["published_total"] == 0
        assert client.get(f"/sessions/{sid}/stream").json()["readings"] == []

    def test_watermark_slowest_minus_lateness_publishes_strictly_before(self, client):
        sid = create_session(client, lateness=10.0)
        # a up to :10, b up to :20 -> slowest :10, watermark 00:00:00.
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 1},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:10Z", "value": 2},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:20Z", "value": 3},
        ]).json()
        assert r["all_sources_reported"] is True
        assert r["published_watermark"] == "2026-01-01T00:00:00Z"
        assert r["published_total"] == 0  # strictly before :00 => nothing
        assert r["buffered"] == 3

        # advance slowest (a :10 -> :20): wm :00 -> :10; a@:00 published,
        # a@:10 sits exactly on the boundary and stays buffered.
        r = report(client, sid, [
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T00:00:20Z", "value": 4},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:30Z", "value": 5},
        ]).json()
        assert r["published_watermark"] == "2026-01-01T00:00:10Z"
        assert r["published_now"] == 1
        assert r["buffered"] == 4  # a@10, b@20, a@20, b@30

        rows = published_times(client, sid)
        assert [(x["source_id"], x["seq"]) for x in rows] == [("a", 1)]
        assert rows[0]["published_watermark"] == "2026-01-01T00:00:10Z"

    def test_deterministic_order_observed_source_seq(self, client):
        sid = create_session(client, sources=["a", "b", "c"], lateness=0.0)
        t0 = "2026-01-01T00:00:00Z"
        t1 = "2026-01-01T00:00:01Z"
        readings = [
            {"source_id": "c", "seq": 5, "observed_at": t0, "value": "c5"},
            {"source_id": "a", "seq": 2, "observed_at": t0, "value": "a2"},
            {"source_id": "a", "seq": 1, "observed_at": t0, "value": "a1"},
            {"source_id": "b", "seq": 9, "observed_at": t1, "value": "b9"},
            {"source_id": "c", "seq": 1, "observed_at": t1, "value": "c1"},
            {"source_id": "a", "seq": 3, "observed_at": t1, "value": "a3"},
        ]
        r = report(client, sid, readings).json()
        # slowest is min max = :01; wm :01 -> exactly the three t0 readings
        assert r["published_total"] == 3
        rows = published_times(client, sid)
        assert [(x["source_id"], x["seq"]) for x in rows] == [
            ("a", 1), ("a", 2), ("c", 5)
        ]

        # push all sources forward, t1 readings publish in tie-break order
        report(client, sid, [
            {"source_id": "a", "seq": 4, "observed_at": "2026-01-01T00:00:02Z"},
            {"source_id": "b", "seq": 10, "observed_at": "2026-01-01T00:00:02Z"},
            {"source_id": "c", "seq": 2, "observed_at": "2026-01-01T00:00:02Z"},
        ])
        rows = published_times(client, sid)
        assert [(x["source_id"], x["seq"]) for x in rows[3:]] == [
            ("a", 3), ("b", 9), ("c", 1)
        ]

    def test_watermark_monotonic_when_clocks_jump_back(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        assert client.get(f"/sessions/{sid}").json()["published_watermark"] == \
            "2026-01-01T00:00:10Z"
        # b reports an older event: slowest min(max) would drop, wm must not
        report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:05Z"},
        ])
        info = client.get(f"/sessions/{sid}").json()
        assert info["published_watermark"] == "2026-01-01T00:00:10Z"

    def test_out_of_order_within_watermark_window_publishes(self, client):
        sid = create_session(client, lateness=60.0)
        # Both sources start at :00:00 -> wm stays at -:01:00, nothing released.
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 10},
        ])
        # a jumps ahead alone. Slowest source is still b@:00:00, so the
        # published watermark does NOT move: a gap reading (seq 2 @ :00:30)
        # arriving now is late in wall-clock terms but still admissible.
        report(client, sid, [
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T00:02:00Z", "value": 3},
        ])
        r = report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:30Z", "value": 2},
        ]).json()
        assert r["quarantined"] == 0
        # published watermark is still the initial :00:00 - 60s, held back
        # by the slow source b.
        assert r["published_watermark"] == "2025-12-31T23:59:00Z"

        # b advances to :02:00 -> wm = :01:00. Both out-of-order a readings
        # are before the watermark and merge into the stream in event order.
        r = report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:02:00Z", "value": 11},
        ]).json()
        assert r["published_now"] == 3
        rows = published_times(client, sid)
        assert [(x["source_id"], x["seq"], x["value"]) for x in rows] == [
            ("a", 1, 1), ("b", 1, 10), ("a", 2, 2)
        ]


class TestQuarantine:
    def test_late_behind_watermark_is_quarantined_and_history_unchanged(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": "old-a"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": "old-b"},
        ])
        before = published_times(client, sid)

        r = report(client, sid, [
            {"source_id": "a", "seq": 99, "observed_at": "2026-01-01T00:00:01Z", "value": "late"},
        ]).json()
        assert r["quarantined"] == 1
        assert r["published_now"] == 0
        after = published_times(client, sid)
        assert before == after

        q = client.get(f"/sessions/{sid}/quarantine").json()["readings"]
        assert len(q) == 1
        assert q[0]["source_id"] == "a"
        assert q[0]["seq"] == 99
        assert q[0]["observed_at"] == "2026-01-01T00:00:01Z"
        assert q[0]["quarantined_watermark"] == "2026-01-01T00:00:10Z"
        assert q[0]["reason"] == "behind_published_watermark"

    def test_never_quarantine_before_first_watermark(self, client):
        sid = create_session(client, lateness=0.0)
        # Only one source has reported -> no watermark yet, ancient event buffers
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2000-01-01T00:00:00Z"},
        ]).json()
        assert r["quarantined"] == 0
        assert r["buffered"] == 1
        # b first report makes the watermark; ancient reading is >= wm? b older
        r = report(client, sid, [
            {"source_id": "b", "seq": 1, "observed_at": "1999-01-01T00:00:00Z"},
        ]).json()
        # wm 1999; a@2000 buffered; nothing strictly before wm, nothing quarantined
        assert r["quarantined"] == 0
        assert r["published_total"] == 0


class TestIdempotency:
    def test_duplicate_report_is_noop(self, client):
        sid = create_session(client)
        payload = {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:05Z",
                   "value": {"k": 1, "x": [1, 2]}}
        first = report(client, sid, [payload]).json()
        assert first["received"] == 1 and first["duplicates"] == 0
        second = report(client, sid, [dict(payload)]).json()
        assert second["received"] == 1
        assert second["duplicates"] == 1
        assert second["quarantined"] == 0

        # key order in value JSON must not matter
        reordered = dict(payload, value={"x": [1, 2], "k": 1})
        third = report(client, sid, [reordered]).json()
        assert third["duplicates"] == 1

        # only one buffered row exists for (a, 1)
        status = client.get(f"/sessions/{sid}").json()
        assert status["all_sources_reported"] is False

    def test_same_key_different_payload_conflicts_atomically(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": "v1"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": "b"},
        ])
        # batch: one clean insert + one conflicting key -> whole batch rejected
        r = report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:11Z", "value": "new"},
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": "DIFFERENT"},
        ])
        assert r.status_code == 409
        # seq 2 must not exist either (atomic rollback)
        follow = report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:11Z", "value": "new"},
        ]).json()
        assert follow["duplicates"] == 0

    def test_duplicate_quarantined_reading_is_idempotent(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        late = {"source_id": "a", "seq": 7, "observed_at": "2026-01-01T00:00:01Z", "value": 1}
        r1 = report(client, sid, [late]).json()
        r2 = report(client, sid, [late]).json()
        assert r1["quarantined"] == 1
        assert r2["quarantined"] == 0 and r2["duplicates"] == 1
        q = client.get(f"/sessions/{sid}/quarantine").json()["readings"]
        assert len(q) == 1


class TestValidation:
    def test_unknown_source_rejected(self, client):
        sid = create_session(client, sources=["a", "b"])
        r = report(client, sid, [
            {"source_id": "zzz", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
        ])
        assert r.status_code == 400
        assert "unknown source" in r.json()["detail"]

    def test_bad_timestamp_rejected(self, client):
        sid = create_session(client)
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "not-a-time"},
        ])
        assert r.status_code == 400

    def test_missing_fields_422(self, client):
        sid = create_session(client)
        r = report(client, sid, [{"source_id": "a"}])
        assert r.status_code == 422
        r = report(client, sid, [{"source_id": "a", "seq": -1, "observed_at": "2026-01-01T00:00:00Z"}])
        assert r.status_code == 422

    def test_empty_and_garbage_body(self, client):
        sid = create_session(client)
        assert report(client, sid, []).status_code == 400
        r = client.post(f"/sessions/{sid}/readings", json={"nope": 1})
        assert r.status_code == 400

    def test_bare_json_array_accepted(self, client):
        sid = create_session(client)
        r = client.post(
            f"/sessions/{sid}/readings",
            json=[{"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"}],
        )
        assert r.status_code == 200
        assert r.json()["received"] == 1

    def test_offset_timestamp_normalized_to_utc(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T02:00:00+02:00"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
        ])
        rows = published_times(client, sid)
        assert rows == []  # equal to wm :00:00 -> not strictly before


class TestPagination:
    def test_keyset_pagination_and_order(self, client):
        sid = create_session(client, lateness=0.0)
        batch_a = [
            {"source_id": "a", "seq": i, "observed_at": f"2026-01-01T00:00:{i:02d}Z"}
            for i in range(5)
        ]
        batch_b = [
            {"source_id": "b", "seq": i,
             "observed_at": f"2026-01-01T00:01:{i:02d}Z"}
            for i in range(5)
        ]
        report(client, sid, batch_a + batch_b)
        # publish everything: move both sources well ahead
        report(client, sid, [
            {"source_id": "a", "seq": 100, "observed_at": "2026-01-01T00:02:00Z"},
            {"source_id": "b", "seq": 100, "observed_at": "2026-01-01T00:02:00Z"},
        ])

        seen = []
        cursor = None
        for _ in range(20):
            params = {"limit": 3}
            if cursor:
                params["cursor"] = cursor
            page = client.get(f"/sessions/{sid}/stream", params=params).json()
            seen.extend(page["readings"])
            cursor = page["next_cursor"]
            if not cursor:
                break
        assert len(seen) == 10
        keys = [(r["observed_at"], r["source_id"], r["seq"]) for r in seen]
        assert keys == sorted(keys)

    def test_bad_cursor_400(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/stream", params={"cursor": "%%%garbage"})
        assert r.status_code == 400
