"""End-to-end tests for finite-collection source sealing and tail publish."""
from __future__ import annotations

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
        yield c


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
    return client.post(f"/sessions/{sid}/readings", json={"readings": readings})


def seal(client, sid, source, final_observed_at):
    return client.post(
        f"/sessions/{sid}/sources/{source}/seal",
        json={"final_observed_at": final_observed_at},
    )


def all_published(client, sid):
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


T0 = "2026-01-01T00:00:00Z"


class TestSealValidation:
    def test_session_state_reports_seals_and_open_flag(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:20Z"},
        ])
        state = client.get(f"/sessions/{sid}").json()
        assert state["status"] == "open"
        assert state["closed"] is False
        assert state["closed_at"] is None
        assert {s["source_id"]: s["sealed"] for s in state["sources"]} == {
            "a": False,
            "b": False,
        }
        assert all(s["final_observed_at"] is None for s in state["sources"])
        assert {s["max_observed_at"] for s in state["sources"]} == {
            "2026-01-01T00:00:10Z",
            "2026-01-01T00:00:20Z",
        }
        r = seal(client, sid, "a", "2026-01-01T00:00:10Z")
        assert r.status_code == 200
        body = r.json()
        assert body["closed"] is False
        sealed_a = next(s for s in body["sources"] if s["source_id"] == "a")
        assert sealed_a["sealed"] is True
        assert sealed_a["final_observed_at"] == "2026-01-01T00:00:10Z"
        assert sealed_a["sealed_at"] is not None
        sealed_b = next(s for s in body["sources"] if s["source_id"] == "b")
        assert sealed_b["sealed"] is False

    def test_final_earlier_than_seen_max_rejected(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        r = seal(client, sid, "a", "2026-01-01T00:00:09Z")
        assert r.status_code == 400
        assert "earlier" in r.json()["detail"]

    def test_final_equal_to_or_later_than_max_accepted(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        assert seal(client, sid, "a", "2026-01-01T00:00:10Z").status_code == 200
        assert seal(client, sid, "b", "2026-01-01T00:00:30Z").status_code == 200

    def test_seal_unknown_source_400(self, client):
        sid = create_session(client)
        r = seal(client, sid, "zzz", T0)
        assert r.status_code == 400
        assert "unknown source" in r.json()["detail"]

    def test_seal_unknown_session_404(self, client):
        assert seal(client, "nope", "a", T0).status_code == 404

    def test_seal_bad_timestamp_400(self, client):
        sid = create_session(client)
        r = seal(client, sid, "a", "not-a-time")
        assert r.status_code == 400

    def test_seal_missing_body_422(self, client):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/sources/a/seal", json={})
        assert r.status_code == 422

    def test_same_value_retry_idempotent_even_normalised(self, client):
        sid = create_session(client)
        r1 = seal(client, sid, "a", "2026-01-01T02:00:00+02:00")
        assert r1.status_code == 200
        sealed_at = r1.json()["sources"][0]["sealed_at"]
        # Offset form normalises to the same UTC instant -> idempotent retry.
        r2 = seal(client, sid, "a", "2026-01-01T00:00:00Z")
        assert r2.status_code == 200
        assert r2.json()["sources"][0]["sealed_at"] == sealed_at

    def test_different_value_conflicts(self, client):
        sid = create_session(client)
        assert seal(client, sid, "a", "2026-01-01T00:00:10Z").status_code == 200
        r = seal(client, sid, "a", "2026-01-01T00:00:11Z")
        assert r.status_code == 409
        # the original value is preserved
        state = client.get(f"/sessions/{sid}").json()
        assert state["sources"][0]["final_observed_at"] == "2026-01-01T00:00:10Z"


class TestSealedSourceIngestion:
    def test_new_reading_for_sealed_source_rejected(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        r = report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:05Z", "value": 9},
        ])
        assert r.status_code == 409
        assert "sealed" in r.json()["detail"]

    def test_other_open_source_still_accepts_readings(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        r = report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"},
        ])
        assert r.status_code == 200

    def test_exact_duplicate_after_seal_still_counts_as_duplicate(self, client):
        sid = create_session(client)
        original = {
            "source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z",
            "value": {"v": 1},
        }
        report(client, sid, [original, {
            "source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"}])
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        r = report(client, sid, [dict(original)]).json()
        assert r["duplicates"] == 1
        assert r["received"] == 1

    def test_duplicate_key_with_different_payload_still_conflicts(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": 2},
        ])
        assert r.status_code == 409

    def test_sealed_source_in_batch_rejects_whole_batch(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        # one clean insert for the open source + one new key for the sealed one
        r = report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:05Z"},
        ])
        assert r.status_code == 409
        # atomic: the b reading must not have landed either
        follow = report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"},
        ]).json()
        assert follow["duplicates"] == 0

    def test_sealing_source_without_reports_does_not_define_watermark(self, client):
        # A sealed source that never reported has a known final time, so its
        # clock *is* defined; but a still-open source that never reported keeps
        # the watermark undefined.
        sid = create_session(client, sources=("a", "b", "c"))
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        state = client.get(f"/sessions/{sid}").json()
        assert state["all_sources_reported"] is False
        assert state["effective_watermark"] is None
        report(client, sid, [
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:20Z"},
        ])
        state = client.get(f"/sessions/{sid}").json()
        assert state["all_sources_reported"] is False


class TestWatermarkWhileSealing:
    def test_seal_advances_watermark_with_ordinary_semantics(self, client):
        sid = create_session(client, lateness=10.0)
        # a up to :30, b up to :20 -> slowest b@:20, wm :10
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 1},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:30Z", "value": 2},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:20Z", "value": 3},
        ])
        # Seal b with a later final time (:40): slowest becomes a@:30, wm :20;
        # a@:00 publishes, a@:30 sits on the far side, session still open.
        r = seal(client, sid, "b", "2026-01-01T00:00:40Z")
        body = r.json()
        assert body["closed"] is False
        assert body["published_watermark"] == "2026-01-01T00:00:20Z"
        rows = all_published(client, sid)
        assert [(x["source_id"], x["seq"]) for x in rows] == [("a", 1)]

    def test_watermark_never_regresses_via_sealing(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        assert client.get(f"/sessions/{sid}").json()["published_watermark"] == \
            "2026-01-01T00:00:10Z"
        # final == max cannot lower the watermark
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        assert client.get(f"/sessions/{sid}").json()["published_watermark"] == \
            "2026-01-01T00:00:10Z"


class TestLastSealTailPublish:
    def test_last_seal_publishes_everything_and_closes(self, client):
        sid = create_session(client, lateness=10.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 1},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:10Z", "value": 2},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:20Z", "value": 3},
        ])
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        r = seal(client, sid, "b", "2026-01-01T00:00:20Z")
        body = r.json()
        assert body["status"] == "closed"
        assert body["closed"] is True
        assert body["closed_at"] is not None
        assert all(s["sealed"] for s in body["sources"])

        rows = all_published(client, sid)
        # deterministic global order (observed_at, source_id, seq)
        assert [(x["source_id"], x["seq"]) for x in rows] == [
            ("a", 1), ("a", 2), ("b", 1)
        ]
        # nothing left buffered
        state = client.get(f"/sessions/{sid}").json()
        assert state["closed"] is True

    def test_last_seal_watermark_is_past_every_buffered_reading(self, client):
        sid = create_session(client, lateness=0.0)
        # The slowest source seals with an event time earlier than buffered
        # readings of other sources: the ordinary watermark could not release
        # them, but the tail flush must publish all of them anyway.
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:01:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:05Z"},
        ])
        seal(client, sid, "a", "2026-01-01T00:01:00Z")
        r = seal(client, sid, "b", "2026-01-01T00:00:05Z")
        assert r.json()["closed"] is True
        rows = all_published(client, sid)
        assert len(rows) == 2
        wm = client.get(f"/sessions/{sid}").json()["published_watermark"]
        from app.time_utils import parse_event_time

        assert parse_event_time(wm) > parse_event_time("2026-01-01T00:01:00Z")
        assert all(x["published_watermark"] == wm for x in rows)

    def test_close_with_no_readings_at_all(self, client):
        sid = create_session(client)
        r1 = seal(client, sid, "a", "2026-01-01T00:00:00Z")
        assert r1.json()["closed"] is False
        r2 = seal(client, sid, "b", "2026-01-01T00:00:00Z")
        assert r2.status_code == 200
        assert r2.json()["closed"] is True
        assert all_published(client, sid) == []

    def test_new_key_rejected_after_close_but_duplicates_still_work(self, client):
        sid = create_session(client, lateness=0.0)
        reading = {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": 7}
        report(client, sid, [reading])
        report(client, sid, [
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": 7}])
        seal(client, sid, "a", "2026-01-01T00:00:10Z")
        seal(client, sid, "b", "2026-01-01T00:00:10Z")

        r = report(client, sid, [
            {"source_id": "a", "seq": 99, "observed_at": "2026-01-01T00:00:11Z", "value": 1}])
        assert r.status_code == 409
        assert "closed" in r.json()["detail"]

        dup = report(client, sid, [dict(reading)]).json()
        assert dup["duplicates"] == 1

    def test_tail_flush_advances_single_source_alerts(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 31},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:01Z", "value": 1},
        ])
        # rule created while the readings are still buffered -> no replay yet
        rule = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "above",
            "enter_threshold": 30, "recover_threshold": 25,
            "enter_consecutive": 1, "recover_consecutive": 1,
        }).json()
        assert rule["state"] == "normal"

        seal(client, sid, "a", "2026-01-01T00:00:00Z")
        seal(client, sid, "b", "2026-01-01T00:00:01Z")

        active = client.get(f"/sessions/{sid}/alerts/current").json()["alerts"]
        assert len(active) == 1
        assert active[0]["entered"]["observed_at"] == "2026-01-01T00:00:00Z"
        history = client.get(f"/sessions/{sid}/alerts/history").json()["events"]
        assert [e["event_type"] for e in history] == ["entered"]

    def test_tail_flush_advances_correlated_rules(self, client):
        sid = create_session(client, sources=("a", "b"), lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 31},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:05Z", "value": 29},
        ])
        client.post(f"/sessions/{sid}/correlated-rules", json={"conditions": [
            {"source_id": "a", "direction": "above", "threshold": 30, "window_seconds": 10},
            {"source_id": "b", "direction": "above", "threshold": 28, "window_seconds": 10},
        ]})
        seal(client, sid, "a", "2026-01-01T00:00:00Z")
        seal(client, sid, "b", "2026-01-01T00:00:05Z")

        current = client.get(f"/sessions/{sid}/correlated-alerts/current").json()["alerts"]
        assert len(current) == 1
        assert current[0]["correlated_entered"]["completed_by"]["source_id"] == "b"
        history = client.get(f"/sessions/{sid}/correlated-alerts/history").json()["events"]
        assert len(history) == 1

    def test_tail_flush_publishes_in_deterministic_merge_order(self, client):
        sid = create_session(client, sources=("a", "b", "c"), lateness=0.0)
        report(client, sid, [
            {"source_id": "c", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:01Z"},
        ])
        seal(client, sid, "a", "2026-01-01T00:00:02Z")
        seal(client, sid, "b", "2026-01-01T00:00:02Z")
        seal(client, sid, "c", "2026-01-01T00:00:02Z")
        rows = all_published(client, sid)
        assert [(x["source_id"], x["seq"]) for x in rows] == [
            ("a", 1), ("a", 2), ("c", 1), ("b", 1)
        ]


class TestSealReportConcurrency:
    def test_concurrent_seal_and_report_are_serialised(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
        ])
        outcomes = {}
        barrier = threading.Barrier(2)

        def do_seal():
            barrier.wait()
            outcomes["seal"] = seal(
                client, sid, "a", "2026-01-01T00:00:00Z").status_code

        def do_report():
            barrier.wait()
            outcomes["report"] = report(client, sid, [
                {"source_id": "a", "seq": 2,
                 "observed_at": "2026-01-01T00:00:00Z", "value": 2}]).status_code

        t1 = threading.Thread(target=do_seal)
        t2 = threading.Thread(target=do_report)
        t1.start(); t2.start(); t1.join(); t2.join()

        assert outcomes["seal"] == 200
        # The new key must either land before the seal (200) or be rejected by
        # it (409) -- never a 500, and never both "sealed" and "new reading".
        assert outcomes["report"] in (200, 409)
        if outcomes["report"] == 409:
            # seq 2 never landed: retrying it is still rejected, not a duplicate
            again = report(client, sid, [
                {"source_id": "a", "seq": 2,
                 "observed_at": "2026-01-01T00:00:00Z", "value": 2}])
            assert again.status_code == 409
