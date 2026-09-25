"""End-to-end tests for the read-only buffered-readings release plan."""
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


def plan(client, sid, count):
    return client.get(f"/sessions/{sid}/release-plan", params={"count": count})


def by_source(body):
    return {s["source_id"]: s for s in body["sources"]}


def ladder(client, sid):
    """Deterministic backlog with lateness 10 and published watermark 10:00:30.

    Published: a@10:00:00, b@10:00:00.
    Buffered in global order: a@10:00:30, b@10:00:30, a@10:00:40, b@10:01:00
    (the two 10:00:30 readings sit exactly on the published watermark and are
    therefore still buffered; source clocks end at a=10:00:40, b=10:01:00).
    """
    report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
        {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 2},
    ])
    report(client, sid, [
        {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 3},
        {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 4},
    ])
    report(client, sid, [
        {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T10:00:40Z", "value": 5}])
    report(client, sid, [
        {"source_id": "b", "seq": 3, "observed_at": "2026-01-01T10:01:00Z", "value": 6}])


class TestReleasePlan:
    def test_plan_for_first_buffered_reading(self, client):
        sid = create_session(client)
        ladder(client, sid)
        r = plan(client, sid, 1)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["session_id"] == sid
        assert body["count"] == 1
        assert body["buffered"] == 4
        assert body["allowed_lateness_seconds"] == 10.0
        # Target: one microsecond past the 1st buffered reading (a@10:00:30).
        assert body["target_watermark"] == "2026-01-01T10:00:30.000001Z"
        assert body["attainable"] is True
        assert body["reason"] is None
        # Sources are reported in the session's declared order.
        assert [s["source_id"] for s in body["sources"]] == ["a", "b"]
        a, b = by_source(body)["a"], by_source(body)["b"]
        # required = target watermark + allowed lateness, same for all sources.
        assert a["required"] == "2026-01-01T10:00:40.000001Z"
        assert b["required"] == "2026-01-01T10:00:40.000001Z"
        # a's clock (10:00:40) is one microsecond short of required.
        assert a["current"] == "2026-01-01T10:00:40Z"
        assert a["advance_seconds"] == pytest.approx(0.000001)
        assert a["blocker"] == "below_required"
        # b's clock (10:01:00) already satisfies the requirement.
        assert b["current"] == "2026-01-01T10:01:00Z"
        assert b["advance_seconds"] == 0.0
        assert b["blocker"] is None

    def test_plan_scales_with_count(self, client):
        sid = create_session(client)
        ladder(client, sid)
        body = plan(client, sid, 4).json()
        # 4th buffered reading is b@10:01:00.
        assert body["target_watermark"] == "2026-01-01T10:01:00.000001Z"
        sources = by_source(body)
        assert sources["a"]["required"] == "2026-01-01T10:01:10.000001Z"
        assert sources["a"]["advance_seconds"] == pytest.approx(30.000001)
        assert sources["a"]["blocker"] == "below_required"
        assert sources["b"]["advance_seconds"] == pytest.approx(10.000001)
        assert sources["b"]["blocker"] == "below_required"

    def test_tied_nth_reading_uses_same_target(self, client):
        sid = create_session(client)
        ladder(client, sid)
        # The 2nd buffered reading ties the 1st on observed_at: same target.
        assert (
            plan(client, sid, 2).json()["target_watermark"]
            == "2026-01-01T10:00:30.000001Z"
        )

    def test_published_and_quarantined_are_excluded(self, client):
        sid = create_session(client)
        ladder(client, sid)
        # Late arrival behind the published watermark (10:00:30) -> quarantined.
        r = report(client, sid, [
            {"source_id": "a", "seq": 9,
             "observed_at": "2026-01-01T10:00:10Z", "value": 99}])
        assert r.json()["quarantined"] == 1
        body = plan(client, sid, 1).json()
        # Still exactly the 4 buffered readings (2 published + 1 quarantined
        # never enter the computation).
        assert body["buffered"] == 4
        assert body["target_watermark"] == "2026-01-01T10:00:30.000001Z"

    def test_unreported_source_has_null_current(self, client):
        sid = create_session(client, sources=("a", "b", "c"))
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 2},
        ])
        body = plan(client, sid, 1).json()
        assert body["target_watermark"] == "2026-01-01T10:00:00.000001Z"
        assert body["attainable"] is True
        sources = by_source(body)
        c = sources["c"]
        assert c["current"] is None
        assert c["required"] == "2026-01-01T10:00:10.000001Z"
        assert c["advance_seconds"] is None
        assert c["blocker"] == "not_reported"
        # Reported sources get an ordinary advance suggestion.
        assert sources["a"]["advance_seconds"] == pytest.approx(10.000001)
        assert sources["a"]["blocker"] == "below_required"

    def test_sealed_source_below_required_is_unattainable(self, client):
        sid = create_session(client, lateness=5.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 2},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 3},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 4},
        ])
        # The two :00 readings publish; a@:20 and b@:20 stay buffered.
        r = seal(client, sid, "a", "2026-01-01T10:00:20Z")
        assert r.status_code == 200, r.text
        body = plan(client, sid, 1).json()
        assert body["target_watermark"] == "2026-01-01T10:00:20.000001Z"
        # a's sealed clock (10:00:20) can never reach required (10:00:25.000001).
        assert body["attainable"] is False
        assert "a" in body["reason"]
        sources = by_source(body)
        a = sources["a"]
        assert a["current"] == "2026-01-01T10:00:20Z"
        assert a["required"] == "2026-01-01T10:00:25.000001Z"
        # A sealed source is never advised to advance.
        assert a["advance_seconds"] is None
        assert a["blocker"] == "sealed_below_required"
        b = sources["b"]
        assert b["advance_seconds"] == pytest.approx(5.000001)
        assert b["blocker"] == "below_required"

    def test_sealed_source_meeting_required_stays_attainable(self, client):
        sid = create_session(client, lateness=5.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 2},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 3},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 4},
        ])
        # Sealing a at 10:00:30 leaves its clock above the required 10:00:25.000001.
        seal(client, sid, "a", "2026-01-01T10:00:30Z")
        body = plan(client, sid, 1).json()
        assert body["attainable"] is True
        assert body["reason"] is None
        a = by_source(body)["a"]
        assert a["current"] == "2026-01-01T10:00:30Z"
        assert a["advance_seconds"] == 0.0
        assert a["blocker"] is None

    def test_following_the_plan_releases_exactly_the_prefix(self, client):
        sid = create_session(client)
        ladder(client, sid)
        body = plan(client, sid, 2).json()
        a = by_source(body)["a"]
        assert a["advance_seconds"] == pytest.approx(0.000001)
        # Advance source a's clock by exactly the suggested microsecond.
        r = report(client, sid, [
            {"source_id": "a", "seq": 4,
             "observed_at": "2026-01-01T10:00:40.000001Z", "value": 7}])
        assert r.json()["published_now"] == 2
        stream = client.get(f"/sessions/{sid}/stream").json()["readings"]
        assert [(x["source_id"], x["seq"]) for x in stream] == [
            ("a", 1), ("b", 1), ("a", 2), ("b", 2)]

    def test_plan_is_read_only(self, client):
        sid = create_session(client)
        ladder(client, sid)
        before = client.get(f"/sessions/{sid}").json()
        assert plan(client, sid, 1).status_code == 200
        assert plan(client, sid, 4).status_code == 200
        after = client.get(f"/sessions/{sid}").json()
        assert before == after
        stream = client.get(f"/sessions/{sid}/stream").json()
        assert len(stream["readings"]) == 2


class TestReleasePlanErrors:
    def test_closed_session_returns_409(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 2},
        ])
        seal(client, sid, "a", "2026-01-01T10:00:00Z")
        seal(client, sid, "b", "2026-01-01T10:00:00Z")
        assert client.get(f"/sessions/{sid}").json()["closed"] is True
        r = plan(client, sid, 1)
        assert r.status_code == 409
        assert "closed" in r.json()["detail"]

    def test_unknown_session_returns_404(self, client):
        assert plan(client, "nope", 1).status_code == 404

    @pytest.mark.parametrize("count", [0, -1, 101, "abc"])
    def test_count_out_of_range_returns_422(self, client, count):
        sid = create_session(client)
        ladder(client, sid)
        assert plan(client, sid, count).status_code == 422

    def test_count_is_required(self, client):
        sid = create_session(client)
        assert client.get(f"/sessions/{sid}/release-plan").status_code == 422

    def test_count_exceeding_buffered_returns_422(self, client):
        sid = create_session(client)
        ladder(client, sid)
        r = plan(client, sid, 5)
        assert r.status_code == 422
        assert "buffered" in r.json()["detail"]

    def test_empty_buffer_returns_422(self, client):
        sid = create_session(client)
        assert plan(client, sid, 1).status_code == 422


class TestReleasePlanConcurrency:
    def test_plan_concurrent_with_reports_never_500s(self, client):
        sid = create_session(client)
        ladder(client, sid)
        outcomes = {"plans": [], "reports": []}
        barrier = threading.Barrier(2)

        def do_plans():
            barrier.wait()
            for _ in range(30):
                outcomes["plans"].append(plan(client, sid, 1).status_code)

        def do_reports():
            barrier.wait()
            for i in range(30):
                outcomes["reports"].append(report(client, sid, [
                    {"source_id": "a", "seq": 100 + i,
                     "observed_at": f"2026-01-01T10:02:{i:02d}Z", "value": i},
                ]).status_code)

        t1 = threading.Thread(target=do_plans)
        t2 = threading.Thread(target=do_reports)
        t1.start(); t2.start(); t1.join(); t2.join()

        # Every plan ran against one consistent snapshot: always a clean 200,
        # never a 500 from a mid-write read.
        assert outcomes["plans"] == [200] * 30
        assert outcomes["reports"] == [200] * 30
