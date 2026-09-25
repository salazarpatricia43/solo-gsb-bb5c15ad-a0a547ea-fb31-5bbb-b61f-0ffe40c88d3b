"""End-to-end tests for published-stream consumer checkpoints."""
from __future__ import annotations

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
        c.db_path = db_path  # type: ignore[attr-defined]
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
    return client.post(
        f"/sessions/{sid}/readings", json={"readings": readings}
    )


def seal(client, sid, source, final_observed_at):
    return client.post(
        f"/sessions/{sid}/sources/{source}/seal",
        json={"final_observed_at": final_observed_at},
    )


def make_checkpoint(client, sid, name):
    return client.post(f"/sessions/{sid}/checkpoints", json={"name": name})


def retention(client, sid, cutoff, dry_run=False):
    return client.post(
        f"/sessions/{sid}/retention",
        json={"cutoff": cutoff, "dry_run": dry_run},
    )


def keys(rows):
    return [(x["source_id"], x["seq"]) for x in rows]


def full_stream(client, sid, limit=1000):
    out, cursor = [], None
    while True:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        page = client.get(f"/sessions/{sid}/stream", params=params).json()
        out.extend(page["readings"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


def full_checkpoint_stream(client, sid, name, limit=1000):
    out, cursor = [], None
    while True:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        page = client.get(
            f"/sessions/{sid}/checkpoints/{name}/stream", params=params
        ).json()
        out.extend(page["readings"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


# Deterministic publish ladder with lateness 0: reporting a reading for the
# currently-slowest source beyond the other source's max releases the other
# source's readings (strictly behind the new watermark).
def publish_ladder(client, sid):
    # wm undefined until both sources report; nothing published yet.
    report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 1},
        {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": 2},
    ])
    # slowest b@:10 -> wm :10; a@:00 publishes.
    report(client, sid, [
        {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z", "value": 3}])
    # slowest a@:20 -> wm :20; b@:10 publishes (a@:20 stays on the boundary).
    report(client, sid, [
        {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:30Z", "value": 4}])
    # slowest b@:30 -> wm :30; a@:20 publishes.
    report(client, sid, [
        {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T00:00:40Z", "value": 5}])
    # slowest a@:40 -> wm :40; b@:30 publishes.
    report(client, sid, [
        {"source_id": "b", "seq": 3, "observed_at": "2026-01-01T00:00:50Z", "value": 6}])


class TestCreateCheckpoint:
    def test_empty_stream_checkpoint(self, client):
        sid = create_session(client)
        r = make_checkpoint(client, sid, "cp")
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["session_id"] == sid
        assert body["name"] == "cp"
        assert body["stream_offset"] == 0
        assert body["last_observed_at"] is None
        assert body["last_source_id"] is None
        assert body["last_seq"] is None
        assert body["published_watermark"] is None
        assert body["created_at"]

    def test_snapshots_position_and_watermark_in_one_transaction(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        # exactly one reading published: a seq 1 @ :00, watermark :10
        state = client.get(f"/sessions/{sid}").json()
        wm = state["published_watermark"]
        assert wm == "2026-01-01T00:00:10Z"

        body = make_checkpoint(client, sid, "cp").json()
        assert body["stream_offset"] == 1
        assert body["last_observed_at"] == "2026-01-01T00:00:00Z"
        assert body["last_source_id"] == "a"
        assert body["last_seq"] == 1
        assert body["published_watermark"] == wm

    def test_idempotent_same_name_same_position(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        second = make_checkpoint(client, sid, "cp")
        assert second.status_code == 200
        assert second.json() == first.json()

    def test_idempotent_when_only_watermark_moves(self, client):
        # The published watermark may advance while nothing new is published
        # (readings sitting on/beyond the boundary).  Position equality is what
        # defines idempotency; the original snapshot (watermark included) is
        # returned unchanged.
        sid = create_session(client, lateness=10.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:20Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:30Z"},
        ])  # wm :10; a@:20 sits beyond it -> still buffered, 0 published
        first = make_checkpoint(client, sid, "cp")
        assert first.json()["stream_offset"] == 0
        assert first.json()["published_watermark"] == "2026-01-01T00:00:10Z"
        # Slowest clock moves to :30 -> wm :20; a@:20 sits exactly on the new
        # boundary, so it still is not published (position unchanged).
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:40Z"}])
        state = client.get(f"/sessions/{sid}").json()
        assert state["published_watermark"] == "2026-01-01T00:00:20Z"
        second = make_checkpoint(client, sid, "cp")
        assert second.status_code == 200
        assert second.json() == first.json()

    def test_conflict_when_position_changed(self, client):
        sid = create_session(client, sources=("a", "b"), lateness=0.0)
        # get exactly one reading published
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        assert first.json()["stream_offset"] == 1

        # advance: b@:10 publishes
        report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:30Z"}])
        r = make_checkpoint(client, sid, "cp")
        assert r.status_code == 409
        assert "stream_offset" in r.json()["detail"]
        # the original checkpoint survives untouched
        listed = client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"]
        assert len(listed) == 1
        assert listed[0]["stream_offset"] == 1
        assert listed[0]["last_seq"] == 1
        assert listed[0]["created_at"] == first.json()["created_at"]

    def test_names_scoped_per_session(self, client):
        s1 = create_session(client, session_id="s1")
        s2 = create_session(client, sources=("a",), lateness=0.0, session_id="s2")
        assert make_checkpoint(client, s1, "cp").status_code == 201
        # same name in another session is independent
        assert make_checkpoint(client, s2, "cp").status_code == 201

    def test_create_on_closed_session_snapshots_final_position(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:01Z"},
        ])
        seal(client, sid, "a", "2026-01-01T00:00:00Z")
        seal(client, sid, "b", "2026-01-01T00:00:01Z")
        state = client.get(f"/sessions/{sid}").json()
        assert state["closed"] is True

        # Checkpoints can still be created on a closed session; they snapshot
        # the final position, and re-creating the name stays idempotent.
        r = make_checkpoint(client, sid, "after-close")
        assert r.status_code == 201
        body = r.json()
        assert body["stream_offset"] == 2
        assert body["published_watermark"] == state["published_watermark"]
        assert make_checkpoint(client, sid, "after-close").status_code == 200
        assert client.get(
            f"/sessions/{sid}/checkpoints/after-close/stream"
        ).json()["readings"] == []

    def test_create_unknown_session_404(self, client):
        r = make_checkpoint(client, "nope", "cp")
        assert r.status_code == 404

    @pytest.mark.parametrize("name", ["", " ", "bad name", "-lead", ".lead",
                                      "_lead", "x" * 129, "na/me", "na?me"])
    def test_invalid_name_422(self, client, name):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/checkpoints", json={"name": name})
        assert r.status_code == 422, r.text

    def test_missing_name_field_422(self, client):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/checkpoints", json={})
        assert r.status_code == 422


class TestListCheckpoints:
    def test_list_empty(self, client):
        sid = create_session(client)
        body = client.get(f"/sessions/{sid}/checkpoints").json()
        assert body == {"session_id": sid, "checkpoints": []}

    def test_list_returns_all_sorted(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        for name in ("zeta", "alpha", "mid"):
            assert make_checkpoint(client, sid, name).status_code == 201
        body = client.get(f"/sessions/{sid}/checkpoints").json()
        names = [c["name"] for c in body["checkpoints"]]
        # sorted by (created_at, name); creation was sequential so it is also
        # the declaration order -- assert the documented ordering directly
        assert names == sorted(
            names, key=lambda n: (
                next(c["created_at"] for c in body["checkpoints"] if c["name"] == n),
                n,
            )
        )
        assert set(names) == {"zeta", "alpha", "mid"}
        # each listed row carries the full snapshot
        assert all("stream_offset" in c and "published_watermark" in c
                   for c in body["checkpoints"])

    def test_list_unknown_session_404(self, client):
        assert client.get("/sessions/nope/checkpoints").status_code == 404


class TestDeleteCheckpoint:
    def test_delete_existing(self, client):
        sid = create_session(client)
        make_checkpoint(client, sid, "cp")
        r = client.delete(f"/sessions/{sid}/checkpoints/cp")
        assert r.status_code == 204
        assert r.content == b""
        assert client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"] == []
        # deleting again is a 404
        assert client.delete(f"/sessions/{sid}/checkpoints/cp").status_code == 404

    def test_delete_keeps_other_names(self, client):
        sid = create_session(client)
        make_checkpoint(client, sid, "cp1")
        make_checkpoint(client, sid, "cp2")
        assert client.delete(f"/sessions/{sid}/checkpoints/cp1").status_code == 204
        names = [c["name"] for c in
                 client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"]]
        assert names == ["cp2"]

    def test_delete_unknown_session_404(self, client):
        assert client.delete("/sessions/nope/checkpoints/cp").status_code == 404

    def test_deleted_name_can_be_recreated(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        make_checkpoint(client, sid, "cp")
        client.delete(f"/sessions/{sid}/checkpoints/cp")
        assert make_checkpoint(client, sid, "cp").status_code == 201


class TestContinueFromCheckpoint:
    def test_resume_returns_readings_after_position_only(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        # published order: a@:00/1, b@:10/1, a@:20/2, b@:30/2
        all_rows = full_stream(client, sid)
        assert keys(all_rows) == [("a", 1), ("b", 1), ("a", 2), ("b", 2)]

        cp = make_checkpoint(client, sid, "cp").json()
        assert cp["stream_offset"] == 4

        # publish one more: a@:40 is still buffered after the ladder (final
        # watermark :40, and strict-less-than publishing keeps it there).  Move
        # both clocks to :50 so the slowest-source watermark passes :40.
        report(client, sid, [
            {"source_id": "a", "seq": 4, "observed_at": "2026-01-01T00:00:50Z"},
            {"source_id": "b", "seq": 4, "observed_at": "2026-01-01T00:00:50Z"}])
        after = full_stream(client, sid)
        assert keys(after) == keys(all_rows) + [("a", 3)]

        body = client.get(f"/sessions/{sid}/checkpoints/cp/stream").json()
        assert keys(body["readings"]) == [("a", 3)]
        assert body["next_cursor"] is None
        # response embeds the immutable checkpoint snapshot
        assert body["checkpoint"] == cp
        # ...and the session's *current* watermark
        state = client.get(f"/sessions/{sid}").json()
        assert body["published_watermark"] == state["published_watermark"]

    def test_resume_tail_equals_full_stream_tail(self, client):
        sid = create_session(client, lateness=0.0)
        # checkpoint after the first published reading
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        make_checkpoint(client, sid, "cp")
        publish_ladder(client, sid)  # extra reports; duplicates are no-ops

        full = full_stream(client, sid)
        tail = full_checkpoint_stream(client, sid, "cp")
        assert tail == full[1:]
        assert keys(tail)[0] == ("b", 1)

    def test_resume_from_empty_checkpoint_returns_everything(self, client):
        sid = create_session(client, lateness=0.0)
        cp = make_checkpoint(client, sid, "start").json()
        assert cp["stream_offset"] == 0
        publish_ladder(client, sid)
        tail = full_checkpoint_stream(client, sid, "start")
        assert tail == full_stream(client, sid)
        assert keys(tail) == [("a", 1), ("b", 1), ("a", 2), ("b", 2)]

    def test_resume_pagination_uses_same_keyset_order(self, client):
        sid = create_session(client, sources=("a", "b"), lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        make_checkpoint(client, sid, "cp")  # after a@:00
        publish_ladder(client, sid)
        # walk one reading per page
        paged = full_checkpoint_stream(client, sid, "cp", limit=1)
        assert keys(paged) == [("b", 1), ("a", 2), ("b", 2)]
        # page boundaries carry the same cursor format as /stream: resume
        # exactly once and check the first page metadata.
        first = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream", params={"limit": 1}
        ).json()
        assert len(first["readings"]) == 1
        assert first["readings"][0]["source_id"] == "b"
        second = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream",
            params={"limit": 1, "cursor": first["next_cursor"]},
        ).json()
        assert keys(second["readings"]) == [("a", 2)]

    def test_checkpoint_immutable_after_later_publishes_and_close(self, client):
        sid = create_session(client, lateness=10.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:11Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:40Z", "value": 2},
        ])  # slowest a@:11 -> wm :01, nothing released yet
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:25Z", "value": 3}])
        # slowest a@:25 -> wm :15; a@:11 publishes
        cp = make_checkpoint(client, sid, "cp").json()
        assert cp["stream_offset"] == 1

        # close the session: tail publish releases a@:25 and b@:40
        seal(client, sid, "a", "2026-01-01T00:00:25Z")
        sealed = seal(client, sid, "b", "2026-01-01T00:00:40Z")
        assert sealed.json()["closed"] is True

        state = client.get(f"/sessions/{sid}").json()
        # the checkpoint snapshot did not move
        body = client.get(f"/sessions/{sid}/checkpoints/cp/stream").json()
        assert body["checkpoint"] == cp
        # resume still means "after a@:11", so exactly the later readings
        assert keys(body["readings"]) == [("a", 2), ("b", 1)]
        # the current (terminal) watermark is exposed separately
        assert body["published_watermark"] == state["published_watermark"]
        assert body["published_watermark"] != cp["published_watermark"]

        # paging after close is exhaustive and stable across repeats
        assert full_checkpoint_stream(client, sid, "cp") == body["readings"]

    def test_resume_unknown_checkpoint_404(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/checkpoints/nope/stream")
        assert r.status_code == 404

    def test_resume_unknown_session_404(self, client):
        assert client.get("/sessions/nope/checkpoints/cp/stream").status_code == 404

    def test_resume_malformed_cursor_400(self, client):
        sid = create_session(client)
        make_checkpoint(client, sid, "cp")
        r = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream",
            params={"cursor": "%%%not-base64%%%"},
        )
        assert r.status_code == 400
        assert "cursor" in r.json()["detail"]

    def test_cursor_at_or_before_checkpoint_rejected(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)  # 4 readings published
        make_checkpoint(client, sid, "cp")  # position = reading #4
        # a /stream cursor after reading #3 (limit 3 -> next_cursor keyset #3)
        page = client.get(f"/sessions/{sid}/stream", params={"limit": 3}).json()
        cursor_before = page["next_cursor"]
        assert cursor_before is not None
        r = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream",
            params={"cursor": cursor_before},
        )
        assert r.status_code == 400
        assert "checkpoint" in r.json()["detail"]

        # a cursor positioned exactly at the checkpoint keyset is also invalid
        page4 = client.get(f"/sessions/{sid}/stream", params={"limit": 4}).json()
        assert page4["next_cursor"] is None
        # build the exact keyset cursor via a later publish: easier to encode
        # reading #4's keyset straight from its stored representation.
        from app.cursor import encode_cursor

        fourth = page4["readings"][-1]
        from app.time_utils import store_event_time, parse_event_time

        exact = encode_cursor((
            store_event_time(parse_event_time(fourth["observed_at"])),
            fourth["source_id"],
            fourth["seq"],
        ))
        r = client.get(
            f"/sessions/{sid}/checkpoints/cp/stream", params={"cursor": exact}
        )
        assert r.status_code == 400

    def test_limit_validation(self, client):
        sid = create_session(client)
        make_checkpoint(client, sid, "cp")
        assert client.get(
            f"/sessions/{sid}/checkpoints/cp/stream", params={"limit": 0}
        ).status_code == 422
        assert client.get(
            f"/sessions/{sid}/checkpoints/cp/stream", params={"limit": 1001}
        ).status_code == 422

    def test_quarantined_readings_never_resumed(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])  # wm :10
        make_checkpoint(client, sid, "cp")
        # far too late -> quarantine, must never appear in the resumed stream
        late = report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T00:00:01Z", "value": 9}])
        assert late.json()["quarantined"] == 1
        body = client.get(f"/sessions/{sid}/checkpoints/cp/stream").json()
        assert body["readings"] == []
        q = client.get(f"/sessions/{sid}/quarantine").json()["readings"]
        assert [(x["source_id"], x["seq"]) for x in q] == [("a", 9)]

    def test_tie_break_order_preserved_on_resume(self, client):
        sid = create_session(client, sources=("a", "b", "c"), lateness=0.0)
        report(client, sid, [
            {"source_id": "c", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:01Z"},
        ])
        make_checkpoint(client, sid, "cp")  # empty stream (wm :00 boundary)
        seal(client, sid, "a", "2026-01-01T00:00:02Z")
        seal(client, sid, "b", "2026-01-01T00:00:02Z")
        seal(client, sid, "c", "2026-01-01T00:00:02Z")
        tail = full_checkpoint_stream(client, sid, "cp", limit=2)
        assert keys(tail) == [("a", 1), ("a", 2), ("c", 1), ("b", 1)]


class TestCheckpointConcurrency:
    def test_concurrent_same_name_creates_are_linearised(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)
        outcomes = {}
        barrier = threading.Barrier(2)

        def create(tag):
            barrier.wait()
            outcomes[tag] = make_checkpoint(client, sid, "cp").status_code

        t1 = threading.Thread(target=create, args=("one",))
        t2 = threading.Thread(target=create, args=("two",))
        t1.start(); t2.start(); t1.join(); t2.join()

        # Exactly one create wins (201); the serialised follower sees the same
        # position and gets the idempotent 200 -- never a 409 or a 500.
        assert sorted(outcomes.values()) == [200, 201]
        listed = client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"]
        assert [c["name"] for c in listed] == ["cp"]


class TestCheckpointRetryAfterRetention:
    """Same-name retries must not be fooled by retention cleanup.

    Idempotency is decided by the session's publish marker, which advances
    only when readings are actually published.  Retention deleting the rows
    a checkpoint covered (or any later cleanup) never moves it, so a retry
    without intervening publishes returns the original checkpoint; a genuine
    publish in between still conflicts, and the 409 sticks even though the
    marker can never be moved back by cleanup.
    """

    def test_retry_after_covered_readings_cleaned_returns_original(self, client):
        sid = create_session(client, lateness=0.0)
        # publish a@:00 and b@:10; a@:20 stays buffered on the :20 watermark
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": 2},
        ])  # wm :00 -> nothing published (boundary)
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z", "value": 3}])
        # wm :10 -> a@:00 published
        report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:30Z", "value": 4}])
        # wm :20 -> b@:10 published; a@:20 buffered on the boundary
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        assert first.json()["stream_offset"] == 2

        # Retention deletes exactly the two rows the checkpoint covered; the
        # published stream is now empty, so comparing against the current
        # rows would see a *different* position even though nothing was
        # published since the checkpoint.
        deleted = retention(client, sid, "2026-12-31T00:00:00Z").json()
        assert deleted["deleted"] == 2
        assert client.get(f"/sessions/{sid}/stream").json()["readings"] == []

        second = make_checkpoint(client, sid, "cp")
        assert second.status_code == 200
        # the original checkpoint, with its original creation time
        assert second.json() == first.json()
        assert second.json()["created_at"] == first.json()["created_at"]
        assert second.json()["stream_offset"] == 2

    def test_retry_after_repeated_cleanup_rounds(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)  # 4 published: a@:00, b@:10, a@:20, b@:30
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        assert first.json()["stream_offset"] == 4

        # Two cleanup rounds eventually remove every covered row.
        assert retention(client, sid, "2026-01-01T00:00:15Z").json()["deleted"] == 2
        assert retention(client, sid, "2026-12-31T00:00:00Z").json()["deleted"] == 2
        assert client.get(f"/sessions/{sid}/stream").json()["readings"] == []

        second = make_checkpoint(client, sid, "cp")
        assert second.status_code == 200
        assert second.json() == first.json()

    def test_retry_after_cleanup_and_watermark_only_advance(self, client):
        # Cleanup plus a watermark move that publishes nothing: still an
        # idempotent retry returning the original watermark snapshot.
        sid = create_session(client, lateness=10.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:20Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:30Z"},
        ])  # wm :10, nothing published
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:45Z"},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:50Z"},
        ])  # wm :35 -> a@:20 and b@:30 published; a@:45/b@:50 buffered
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        assert first.json()["stream_offset"] == 2
        assert first.json()["published_watermark"] == "2026-01-01T00:00:35Z"

        assert retention(client, sid, "2026-12-31T00:00:00Z").json()["deleted"] == 2
        assert client.get(f"/sessions/{sid}/stream").json()["readings"] == []

        # The watermark advances to :40 but a@:45 stays beyond it: nothing
        # new is published, so the retry is still idempotent.
        report(client, sid, [
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T00:00:55Z"}])
        state = client.get(f"/sessions/{sid}").json()
        assert state["published_watermark"] == "2026-01-01T00:00:40Z"

        second = make_checkpoint(client, sid, "cp")
        assert second.status_code == 200
        assert second.json() == first.json()
        assert second.json()["published_watermark"] == "2026-01-01T00:00:35Z"

    def test_conflict_when_published_since_despite_cleanup(self, client):
        sid = create_session(client, lateness=0.0)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:00:00Z"},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z"},
        ])  # wm :00 -> nothing published (boundary)
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z"}])
        # wm :10 -> a@:00 published
        report(client, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:30Z"}])
        # wm :20 -> b@:10 published; a@:20 buffered on the boundary
        first = make_checkpoint(client, sid, "cp")
        assert first.status_code == 201
        assert first.json()["stream_offset"] == 2

        # Cleanup empties the published stream...
        assert retention(client, sid, "2026-12-31T00:00:00Z").json()["deleted"] == 2
        assert client.get(f"/sessions/{sid}/stream").json()["readings"] == []
        # ...but a genuine publish afterwards still advances the marker.
        report(client, sid, [
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T00:00:40Z"}])
        assert keys(full_stream(client, sid)) == [("a", 2)]

        r = make_checkpoint(client, sid, "cp")
        assert r.status_code == 409
        # the original checkpoint survives untouched
        listed = client.get(f"/sessions/{sid}/checkpoints").json()["checkpoints"]
        assert len(listed) == 1
        assert listed[0]["stream_offset"] == 2
        assert listed[0]["created_at"] == first.json()["created_at"]

    def test_retention_never_moves_the_publish_marker(self, client):
        sid = create_session(client, lateness=0.0)
        publish_ladder(client, sid)  # 4 published
        make_checkpoint(client, sid, "cp")

        def markers():
            conn = sqlite3.connect(client.db_path)
            try:
                session_marker = conn.execute(
                    "SELECT published_seq FROM sessions WHERE session_id = ?",
                    (sid,),
                ).fetchone()[0]
                checkpoint_marker = conn.execute(
                    "SELECT published_seq FROM checkpoints"
                    " WHERE session_id = ? AND name = 'cp'",
                    (sid,),
                ).fetchone()[0]
                return session_marker, checkpoint_marker
            finally:
                conn.close()

        before = markers()
        assert before == (4, 4)
        assert retention(client, sid, "2026-12-31T00:00:00Z").json()["deleted"] == 4
        assert client.get(f"/sessions/{sid}/stream").json()["readings"] == []
        # the cleanup removed every published row but neither marker moved
        assert markers() == before
        assert make_checkpoint(client, sid, "cp").status_code == 200


class TestPublishMarkerMigration:
    def test_v5_backfills_marker_and_preserves_checkpoints(
        self, tmp_path, monkeypatch
    ):
        from app import database
        from app.database import utc_now
        from app.time_utils import store_event_time

        # A database as a v4-era release would have left it: base schema plus
        # migrations 1-4 applied and recorded, live session data, checkpoints
        # without the publish marker.
        db_path = str(tmp_path / "legacy.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(database.SCHEMA_V0)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
            now = store_event_time(utc_now())
            for version in (1, 2, 3, 4):
                database.MIGRATIONS[version](conn, now)
            conn.execute(
                "INSERT INTO sessions (session_id, source_ids,"
                " allowed_lateness_seconds, published_watermark, created_at)"
                " VALUES (?, ?, 0, ?, ?)",
                ("legacy-s", '["a","b"]', "2026-01-01T00:00:30.000000Z", now),
            )
            for src in ("a", "b"):
                conn.execute(
                    "INSERT INTO session_sources (session_id, source_id,"
                    " max_observed_at, first_seen_at) VALUES (?, ?, ?, ?)",
                    ("legacy-s", src, "2026-01-01T00:00:30.000000Z", now),
                )
            # four published readings: a@:00/1, b@:10/1, a@:20/2, b@:30/2
            for src, seq, ts in (
                ("a", 1, "2026-01-01T00:00:00.000000Z"),
                ("b", 1, "2026-01-01T00:00:10.000000Z"),
                ("a", 2, "2026-01-01T00:00:20.000000Z"),
                ("b", 2, "2026-01-01T00:00:30.000000Z"),
            ):
                conn.execute(
                    "INSERT INTO readings (session_id, source_id, seq,"
                    " observed_at, payload, status, first_received_at,"
                    " published_watermark) VALUES (?, ?, ?, ?, ?, 'published',"
                    " ?, ?)",
                    ("legacy-s", src, seq, ts, str(seq), now,
                     "2026-01-01T00:00:30.000000Z"),
                )
            # checkpoints: one behind the frontier, one at it, one empty-stream
            for name, offset, ts, src, seq in (
                ("cp-behind", 2, "2026-01-01T00:00:10.000000Z", "b", 1),
                ("cp-frontier", 4, "2026-01-01T00:00:30.000000Z", "b", 2),
                ("cp-empty", 0, None, None, None),
            ):
                conn.execute(
                    "INSERT INTO checkpoints (session_id, name, created_at,"
                    " stream_offset, last_observed_at, last_source_id, last_seq,"
                    " published_watermark) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    ("legacy-s", name, now, offset, ts, src, seq,
                     "2026-01-01T00:00:30.000000Z"),
                )
            conn.commit()
        finally:
            conn.close()

        # Startup upgrade applies exactly the pending migration and backfills
        # the marker without touching the captured positions.
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            versions = [r[0] for r in conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version")]
            assert versions == [1, 2, 3, 4, 5]
            assert database.LATEST_VERSION == 5
            session_marker = conn.execute(
                "SELECT published_seq FROM sessions WHERE session_id = 'legacy-s'"
            ).fetchone()[0]
            assert session_marker == 4
            markers = dict(conn.execute(
                "SELECT name, published_seq FROM checkpoints"
                " WHERE session_id = 'legacy-s'").fetchall())
            # at the frontier: shares the session marker -> retry idempotent
            assert markers["cp-frontier"] == 4
            # behind the frontier: strictly smaller -> retry keeps 409
            assert markers["cp-behind"] == 2
            # empty-stream checkpoint on a non-empty stream: also smaller
            assert markers["cp-empty"] == 0
            # captured positions (resume boundaries) are untouched
            positions = {
                row[0]: (row[1], row[2], row[3], row[4])
                for row in conn.execute(
                    "SELECT name, stream_offset, last_observed_at,"
                    " last_source_id, last_seq FROM checkpoints"
                    " WHERE session_id = 'legacy-s'")
            }
            assert positions["cp-behind"] == (
                2, "2026-01-01T00:00:10.000000Z", "b", 1)
            assert positions["cp-frontier"] == (
                4, "2026-01-01T00:00:30.000000Z", "b", 2)
            assert positions["cp-empty"] == (0, None, None, None)
        finally:
            conn.close()

        # Re-running init must not re-apply v5.
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 5"
            ).fetchone()[0] == 1
        finally:
            conn.close()

        # The upgraded database serves checkpoint traffic with the new
        # semantics end to end.
        monkeypatch.setenv("SENSOR_DB_PATH", db_path)
        from app import main

        main.app.state.db_path = db_path
        with TestClient(main.app) as c:
            # nothing was published after cp-frontier: idempotent retry
            first = c.post(
                "/sessions/legacy-s/checkpoints", json={"name": "cp-frontier"})
            assert first.status_code == 200
            assert first.json()["stream_offset"] == 4
            assert first.json()["last_seq"] == 2
            # readings were published after the other two: still 409
            for name in ("cp-behind", "cp-empty"):
                r = c.post("/sessions/legacy-s/checkpoints", json={"name": name})
                assert r.status_code == 409
            # resume boundaries are preserved exactly
            body = c.get("/sessions/legacy-s/checkpoints/cp-behind/stream").json()
            assert keys(body["readings"]) == [("a", 2), ("b", 2)]
            body = c.get("/sessions/legacy-s/checkpoints/cp-empty/stream").json()
            assert keys(body["readings"]) == [
                ("a", 1), ("b", 1), ("a", 2), ("b", 2)]

            # Once the earlier checkpoints are gone, cleanup removes the rows
            # cp-frontier covered; its retry stays idempotent (the fix).
            assert c.delete(
                "/sessions/legacy-s/checkpoints/cp-empty").status_code == 204
            assert c.delete(
                "/sessions/legacy-s/checkpoints/cp-behind").status_code == 204
            deleted = c.post(
                "/sessions/legacy-s/retention",
                json={"cutoff": "2026-12-31T00:00:00Z", "dry_run": False},
            ).json()
            assert deleted["deleted"] == 4
            assert c.get("/sessions/legacy-s/stream").json()["readings"] == []
            again = c.post(
                "/sessions/legacy-s/checkpoints", json={"name": "cp-frontier"})
            assert again.status_code == 200
            assert again.json() == first.json()


class TestCheckpointMigration:
    def test_v3_applies_to_legacy_database(self, tmp_path, monkeypatch):
        from app import database
        from app.time_utils import store_event_time
        from app.database import utc_now

        db_path = str(tmp_path / "legacy.db")
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(database.SCHEMA_V0)
            now = store_event_time(utc_now())
            conn.execute(
                "INSERT INTO sessions (session_id, source_ids,"
                " allowed_lateness_seconds, created_at) VALUES (?, ?, 0, ?)",
                ("legacy-s", '["a","b"]', now),
            )
            conn.commit()
        finally:
            conn.close()

        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            versions = [r[0] for r in conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version")]
            assert versions == [1, 2, 3, 4, 5]
            names = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert "checkpoints" in names
            cols = {r[1] for r in conn.execute("PRAGMA table_info(checkpoints)")}
            assert cols == {
                "session_id", "name", "created_at", "stream_offset",
                "last_observed_at", "last_source_id", "last_seq",
                "published_watermark", "published_seq",
            }
        finally:
            conn.close()

        # Re-running init must not re-apply v3.
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 3"
            ).fetchone()[0] == 1
        finally:
            conn.close()

        # The upgraded database serves checkpoint traffic end to end.
        monkeypatch.setenv("SENSOR_DB_PATH", db_path)
        from app import main

        main.app.state.db_path = db_path
        with TestClient(main.app) as c:
            r = c.post("/sessions/legacy-s/checkpoints", json={"name": "cp"})
            assert r.status_code == 201, r.text
            assert r.json()["stream_offset"] == 0
            assert c.get(
                "/sessions/legacy-s/checkpoints/cp/stream"
            ).status_code == 200
