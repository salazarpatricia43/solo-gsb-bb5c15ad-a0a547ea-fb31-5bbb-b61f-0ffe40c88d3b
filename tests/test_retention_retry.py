"""End-to-end tests for dedup of retries after retention cleanup.

Retention deletes published/quarantined rows outright.  Every row it
actually deletes leaves a tombstone (key + canonical-payload digest) written
in the delete's own transaction, so a later retry of the same
``(source_id, seq)`` keeps its dedup identity instead of looking like a
brand-new key: same payload -> idempotent duplicate (nothing restored, no
watermark movement, no new alerts), different payload -> 409, mixed batches
roll back atomically.
"""
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


def create_session(client, sources=("a", "b"), lateness=0.0, session_id="s"):
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


def retention(client, sid, cutoff, dry_run=None):
    body = {"cutoff": cutoff}
    if dry_run is not None:
        body["dry_run"] = dry_run
    return client.post(f"/sessions/{sid}/retention", json=body)


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


def tombstones(client, sid=None):
    conn = sqlite3.connect(client.db_path)
    try:
        if sid is None:
            rows = conn.execute(
                "SELECT session_id, source_id, seq, payload_digest"
                " FROM retention_tombstones ORDER BY source_id, seq"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT session_id, source_id, seq, payload_digest"
                " FROM retention_tombstones WHERE session_id = ?"
                " ORDER BY source_id, seq",
                (sid,),
            ).fetchall()
        return rows
    finally:
        conn.close()


T0 = "2026-01-01T00:00:00Z"


def publish_two(client, sid):
    """Publish a@:00/1 and b@:10/1; watermark ends at :20."""
    report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1},
        {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z", "value": 2},
    ])  # wm :00 -> nothing strictly before it
    report(client, sid, [
        {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z", "value": 3}])
    # wm :10 -> a@:00 published
    report(client, sid, [
        {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T00:00:30Z", "value": 4}])
    # wm :20 -> b@:10 published; a@:20 and b@:30 stay buffered
    assert keys(full_stream(client, sid)) == [("a", 1), ("b", 1)]


class TestRetryAfterCleanup:
    def test_deleted_published_retry_is_duplicate_not_requarantined(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        deleted = retention(client, sid, "2026-01-01T00:00:05Z", False).json()
        assert deleted["deleted"] == 1  # a@:00/1 is gone
        assert keys(full_stream(client, sid)) == [("b", 1)]

        # The exact retry must NOT re-enter quarantine behind the watermark
        # (the reported bug): it is an idempotent duplicate.
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1}])
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["duplicates"] == 1
        assert body["quarantined"] == 0
        assert body["published_now"] == 0
        # nothing is restored: the stream and the quarantine stay as after
        # the cleanup, and the watermark does not move
        assert keys(full_stream(client, sid)) == [("b", 1)]
        assert client.get(f"/sessions/{sid}/quarantine").json()["readings"] == []
        state = client.get(f"/sessions/{sid}").json()
        assert state["published_watermark"] == "2026-01-01T00:00:20Z"

    def test_deleted_quarantined_retry_is_duplicate(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        late = report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T00:00:05Z",
             "value": 9}])
        assert late.json()["quarantined"] == 1
        deleted = retention(client, sid, "2026-01-01T00:00:15Z", False).json()
        # a@:00 + b@:10 published and the a@:05 quarantined row are all gone
        assert deleted["deleted"] == 3
        assert client.get(f"/sessions/{sid}/quarantine").json()["readings"] == []

        r = report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T00:00:05Z",
             "value": 9}])
        assert r.status_code == 200, r.text
        assert r.json()["duplicates"] == 1
        assert r.json()["quarantined"] == 0
        # the quarantine listing is not repopulated
        assert client.get(f"/sessions/{sid}/quarantine").json()["readings"] == []

    def test_same_key_different_payload_after_cleanup_is_409(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        retention(client, sid, "2026-01-01T00:00:05Z", False)

        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 999}])
        assert r.status_code == 409, r.text
        assert keys(full_stream(client, sid)) == [("b", 1)]
        assert client.get(f"/sessions/{sid}/quarantine").json()["readings"] == []

    def test_mixed_batch_conflict_rolls_everything_back(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        retention(client, sid, "2026-01-01T00:00:05Z", False)
        before = client.get(f"/sessions/{sid}").json()

        # One brand-new reading plus one conflicting retry of a deleted key:
        # the conflict must roll the whole batch back.
        r = report(client, sid, [
            {"source_id": "a", "seq": 5, "observed_at": "2026-01-01T00:00:30Z",
             "value": 50},
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 999},
        ])
        assert r.status_code == 409, r.text
        after = client.get(f"/sessions/{sid}").json()
        # the new key was not admitted: source clocks, watermark and stream
        # are exactly as before the failed batch
        assert after == before
        assert keys(full_stream(client, sid)) == [("b", 1)]

    def test_retry_after_source_sealed_is_duplicate_not_rejected(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        retention(client, sid, "2026-01-01T00:00:05Z", False)
        r = seal(client, sid, "a", "2026-01-01T00:00:20Z")
        assert r.status_code == 200, r.text

        # The reported bug: after sealing, the exact retry of the deleted
        # reading was rejected as a "new key".  It is a duplicate.
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1}])
        assert r.status_code == 200, r.text
        assert r.json()["duplicates"] == 1
        # a different payload still conflicts, and a genuinely new key is
        # still rejected as sealed
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 2}])
        assert r.status_code == 409
        r = report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": T0, "value": 1}])
        assert r.status_code == 409

    def test_retry_after_session_closed_is_duplicate_not_rejected(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        retention(client, sid, "2026-01-01T00:00:05Z", False)
        assert seal(client, sid, "a", "2026-01-01T00:00:20Z").status_code == 200
        assert seal(client, sid, "b", "2026-01-01T00:00:30Z").status_code == 200
        assert client.get(f"/sessions/{sid}").json()["closed"] is True

        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1}])
        assert r.status_code == 200, r.text
        assert r.json()["duplicates"] == 1
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 2}])
        assert r.status_code == 409

    def test_retry_does_not_advance_clocks_or_watermark(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        retention(client, sid, "2026-01-01T00:00:05Z", False)
        before = client.get(f"/sessions/{sid}").json()

        # Same key + same payload but a *later* observed_at: dedup is keyed
        # on (source_id, seq) + payload, so this is still a duplicate and
        # must not push the source clock or the watermark forward.
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T00:05:00Z",
             "value": 1}])
        assert r.status_code == 200, r.text
        assert r.json()["duplicates"] == 1
        after = client.get(f"/sessions/{sid}").json()
        assert after == before

    def test_retry_does_not_retrigger_alerts(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        rule = client.post(f"/sessions/{sid}/alert-rules", json={
            "rule_id": "a-low", "source_id": "a", "direction": "below",
            "enter_threshold": 100, "recover_threshold": 200,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert rule.status_code == 201, rule.text
        history_before = client.get(f"/sessions/{sid}/alerts/history").json()
        current_before = client.get(f"/sessions/{sid}/alerts/current").json()
        assert len(history_before["events"]) == 1  # a@:00 entered

        retention(client, sid, "2026-01-01T00:00:05Z", False)
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1}])
        assert r.json()["duplicates"] == 1
        # alert state and history are byte-identical: no replay, no new events
        assert client.get(f"/sessions/{sid}/alerts/history").json() == history_before
        assert client.get(f"/sessions/{sid}/alerts/current").json() == current_before

    def test_checkpoint_resume_unchanged_by_cleanup_and_retry(self, client):
        sid = create_session(client)
        publish_two(client, sid)  # stream a@:00, b@:10; wm :20
        report(client, sid, [
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T00:00:40Z",
             "value": 5}])
        # wm :30 -> a@:20 published; stream a@:00, b@:10, a@:20
        cp = client.post(f"/sessions/{sid}/checkpoints", json={"name": "cp"})
        assert cp.status_code == 201
        report(client, sid, [
            {"source_id": "b", "seq": 3, "observed_at": "2026-01-01T00:00:40Z",
             "value": 6}])
        # wm :40 -> b@:30 published
        resume_before = keys(full_checkpoint_stream(client, sid, "cp"))
        assert resume_before == [("b", 2)]

        deleted = retention(client, sid, "2026-01-01T00:00:25Z", False).json()
        # a@:00, b@:10 and the boundary a@:20 are all deletable
        assert deleted["deleted"] == 3
        # retry every deleted key with its original payload
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z",
             "value": 2},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z",
             "value": 3},
        ])
        assert r.status_code == 200, r.text
        assert r.json()["duplicates"] == 3
        # checkpoint resume and live stream are exactly as after the cleanup
        assert keys(full_checkpoint_stream(client, sid, "cp")) == resume_before
        assert keys(full_stream(client, sid)) == [("b", 2)]

    def test_retry_uses_canonical_payload_normalisation(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0,
             "value": {"temp": 21.5, "tags": ["x", "y"]}},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T00:00:10Z",
             "value": 2},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T00:00:20Z",
             "value": 3}])
        # wm :10 -> a@:00 published
        assert keys(full_stream(client, sid)) == [("a", 1)]
        retention(client, sid, "2026-01-01T00:00:05Z", False)

        # Same JSON value, different key order on the wire -> canonical form
        # matches the tombstone digest -> duplicate.
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0,
             "value": {"tags": ["x", "y"], "temp": 21.5}}])
        assert r.status_code == 200, r.text
        assert r.json()["duplicates"] == 1
        # A semantically different value conflicts.
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0,
             "value": {"temp": 21.5, "tags": ["x", "z"]}}])
        assert r.status_code == 409

    def test_tombstones_are_scoped_to_their_session(self, client):
        create_session(client, session_id="s1")
        create_session(client, session_id="s2")
        publish_two(client, "s1")
        retention(client, "s1", "2026-01-01T00:00:05Z", False)
        assert len(tombstones(client, "s1")) == 1

        # The same (source_id, seq, payload) in another session is unrelated.
        r = report(client, "s2", [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1}])
        assert r.status_code == 200, r.text
        assert r.json()["duplicates"] == 0
        assert len(tombstones(client, "s2")) == 0


class TestTombstoneWrites:
    def test_tombstone_written_for_each_deleted_row(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T00:00:05Z",
             "value": 9}])  # quarantined (behind wm :20)
        deleted = retention(client, sid, "2026-01-01T00:00:15Z", False).json()
        assert deleted["deleted"] == 3  # a@:00, b@:10 published + a@:05 quarantined
        rows = tombstones(client, sid)
        assert [(r[1], r[2]) for r in rows] == [("a", 1), ("a", 9), ("b", 1)]
        assert all(r[3] for r in rows)  # every row carries a payload digest

    def test_dry_run_and_retained_rows_write_no_tombstones(self, client):
        sid = create_session(client)
        publish_two(client, sid)  # stream a@:00, b@:10
        # checkpoint boundary b@:10, then one more publish
        client.post(f"/sessions/{sid}/checkpoints", json={"name": "cp"})
        report(client, sid, [
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T00:00:40Z",
             "value": 5}])
        # wm :30 -> a@:20 published; stream a@:00, b@:10, a@:20
        # dry run only reports; a@:20 is retained (keyset after the boundary)
        body = retention(client, sid, "2026-01-01T00:00:25Z", True).json()
        assert body["deletable"] == 2
        assert body["retained"] == 1
        assert tombstones(client, sid) == []
        # execute: only the actually deleted rows leave tombstones
        body = retention(client, sid, "2026-01-01T00:00:25Z", False).json()
        assert body["deleted"] == 2
        assert [(r[1], r[2]) for r in tombstones(client, sid)] == [
            ("a", 1), ("b", 1)]
        # the retained row is still there and dedups through the live row
        assert keys(full_stream(client, sid)) == [("a", 2)]

    def test_failed_delete_writes_no_tombstones(self, client):
        sid = create_session(client)
        publish_two(client, sid)

        conn = sqlite3.connect(client.db_path)
        try:
            conn.execute(
                "CREATE TRIGGER fail_retention DELETE ON readings"
                " BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END")
            conn.commit()
        finally:
            conn.close()

        r = retention(client, sid, "2026-01-01T00:00:05Z", False)
        assert r.status_code == 500, r.text
        # delete and tombstone roll back together
        assert tombstones(client, sid) == []
        assert keys(full_stream(client, sid)) == [("a", 1), ("b", 1)]

        conn = sqlite3.connect(client.db_path)
        try:
            conn.execute("DROP TRIGGER fail_retention")
            conn.commit()
        finally:
            conn.close()
        assert retention(client, sid, "2026-01-01T00:00:05Z", False).json()["deleted"] == 1
        assert [(r[1], r[2]) for r in tombstones(client, sid)] == [("a", 1)]

    def test_retention_repeat_is_idempotent_and_keeps_tombstones(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        first = retention(client, sid, "2026-01-01T00:00:05Z", False).json()
        assert first["deleted"] == 1
        second = retention(client, sid, "2026-01-01T00:00:05Z", False).json()
        assert second["deleted"] == 0
        # the tombstone survives further cleanups: retries stay duplicates
        assert len(tombstones(client, sid)) == 1
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1}])
        assert r.json()["duplicates"] == 1

    def test_concurrent_identical_retries_both_count_as_duplicates(self, client):
        sid = create_session(client)
        publish_two(client, sid)
        retention(client, sid, "2026-01-01T00:00:05Z", False)

        outcomes = []
        barrier = threading.Barrier(2)

        def run():
            barrier.wait()
            r = report(client, sid, [
                {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1}])
            outcomes.append((r.status_code, r.json()["duplicates"]))

        t1 = threading.Thread(target=run)
        t2 = threading.Thread(target=run)
        t1.start(); t2.start(); t1.join(); t2.join()
        assert sorted(outcomes) == [(200, 1), (200, 1)]
        assert keys(full_stream(client, sid)) == [("b", 1)]
        assert client.get(f"/sessions/{sid}/quarantine").json()["readings"] == []


class TestTombstoneMigrationUpgrade:
    def test_v4_applies_to_legacy_v3_database(self, tmp_path, monkeypatch):
        from app import database
        from app.database import utc_now
        from app.time_utils import store_event_time

        # A database as a v3-era release would have left it: base schema plus
        # migrations 1-3 applied and recorded, live session data, no
        # tombstone table.
        db_path = str(tmp_path / "legacy.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(database.SCHEMA_V0)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
            now = store_event_time(utc_now())
            for version in (1, 2, 3):
                database.MIGRATIONS[version](conn, now)
            conn.execute(
                "INSERT INTO sessions (session_id, source_ids,"
                " allowed_lateness_seconds, created_at) VALUES (?, ?, 0, ?)",
                ("legacy-s", '["a","b"]', now),
            )
            conn.commit()
        finally:
            conn.close()

        # Startup upgrade applies exactly the pending migration.
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            versions = [r[0] for r in conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version")]
            assert versions == [1, 2, 3, 4, 5]
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(retention_tombstones)")}
            assert cols == {
                "session_id", "source_id", "seq", "payload_digest", "deleted_at",
            }
            # legacy data untouched
            assert conn.execute(
                "SELECT session_id FROM sessions WHERE session_id = 'legacy-s'"
            ).fetchone() is not None
        finally:
            conn.close()

        # Re-running init must not re-apply v4.
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 4"
            ).fetchone()[0] == 1
        finally:
            conn.close()

        # The upgraded database serves the cleanup/retry flow end to end.
        monkeypatch.setenv("SENSOR_DB_PATH", db_path)
        from app import main

        main.app.state.db_path = db_path
        with TestClient(main.app) as c:
            c.db_path = db_path  # type: ignore[attr-defined]
            sid = create_session(c, session_id="upgraded-s")
            publish_two(c, sid)
            assert retention(
                c, sid, "2026-01-01T00:00:05Z", False).json()["deleted"] == 1
            r = report(c, sid, [
                {"source_id": "a", "seq": 1, "observed_at": T0, "value": 1}])
            assert r.status_code == 200, r.text
            assert r.json()["duplicates"] == 1
