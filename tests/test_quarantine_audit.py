"""End-to-end tests for the read-only quarantine impact audit."""
from __future__ import annotations

import json
import sqlite3

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
    return client.post(f"/sessions/{sid}/readings", json={"readings": readings})


def seal(client, sid, source, final_observed_at):
    return client.post(
        f"/sessions/{sid}/sources/{source}/seal",
        json={"final_observed_at": final_observed_at},
    )


def audit(client, sid, **body):
    return client.post(f"/sessions/{sid}/quarantine-audit", json=body)


def index_items(items):
    return {(q["source_id"], q["seq"]): q for q in items}


def setup_five_row_stream(client, sid="s"):
    """Publish five readings in deterministic order:

    a@10:00:00, b@10:00:00, a@10:00:10, a@10:00:20, b@10:00:20
    (published watermark 10:00:30; the clock-advancing reports a@40/b@40
    stay buffered), then quarantine four late readings:

    a#90@10:00:05 (head-middle), b#90@10:00:10 (source tie),
    a#91@10:00:15 (middle), a#92@10:00:25 (tail).
    """
    create_session(client, session_id=sid)
    r = report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": "a1"},
        {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": "b1"},
        {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:10Z", "value": "a2"},
        {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T10:00:20Z", "value": "a3"},
        {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": "b2"},
        {"source_id": "a", "seq": 100, "observed_at": "2026-01-01T10:00:40Z", "value": "ac"},
        {"source_id": "b", "seq": 100, "observed_at": "2026-01-01T10:00:40Z", "value": "bc"},
    ])
    assert r.json()["published_total"] == 5, r.text

    late = [
        {"source_id": "a", "seq": 90, "observed_at": "2026-01-01T10:00:05Z", "value": "q05"},
        {"source_id": "b", "seq": 90, "observed_at": "2026-01-01T10:00:10Z", "value": "q10b"},
        {"source_id": "a", "seq": 91, "observed_at": "2026-01-01T10:00:15Z", "value": "q15"},
        {"source_id": "a", "seq": 92, "observed_at": "2026-01-01T10:00:25Z", "value": "q25"},
    ]
    r = report(client, sid, late)
    assert r.json()["quarantined"] == 4, r.text
    return sid


def checkpoint(client, sid, name):
    return client.post(f"/sessions/{sid}/checkpoints", json={"name": name})


class TestQuarantineAuditPlacement:
    def test_insertion_between_two_neighbours(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        r = audit(c, sid)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["session_id"] == sid
        assert body["source_id"] is None
        assert body["from"] is None
        assert body["to"] is None
        assert body["limit"] == 100
        assert body["matched"] == 4
        assert body["returned"] == 4
        assert body["has_more"] is False

        items = index_items(body["readings"])

        # a#90@10:00:05 slots between b@10:00:00 and a@10:00:10 -> position 2.
        q = items[("a", 90)]
        assert q["observed_at"] == "2026-01-01T10:00:05Z"
        assert q["value"] == "q05"
        assert q["reason"] == "behind_published_watermark"
        assert q["quarantined_watermark"] == "2026-01-01T10:00:30Z"
        assert q["insert_position"] == 2
        assert q["stream_size"] == 5
        assert q["prev_key"] == {
            "observed_at": "2026-01-01T10:00:00Z", "source_id": "b", "seq": 1}
        assert q["next_key"] == {
            "observed_at": "2026-01-01T10:00:10Z", "source_id": "a", "seq": 2}
        assert q["gap_prev_seconds"] == pytest.approx(5.0)
        assert q["gap_next_seconds"] == pytest.approx(5.0)

        # a#91@10:00:15 slots between a@10:00:10 and a@10:00:20 -> position 3.
        q = items[("a", 91)]
        assert q["insert_position"] == 3
        assert q["prev_key"] == {
            "observed_at": "2026-01-01T10:00:10Z", "source_id": "a", "seq": 2}
        assert q["next_key"] == {
            "observed_at": "2026-01-01T10:00:20Z", "source_id": "a", "seq": 3}
        assert q["gap_prev_seconds"] == pytest.approx(5.0)
        assert q["gap_next_seconds"] == pytest.approx(5.0)

    def test_tie_breaks_on_source_id_then_seq(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        items = index_items(audit(c, sid).json()["readings"])

        # b#90@10:00:10 ties published a#2@10:00:10; 'a' < 'b', so it lands
        # right after a#2 and before a@10:00:20 -> position 3.
        q = items[("b", 90)]
        assert q["insert_position"] == 3
        assert q["prev_key"] == {
            "observed_at": "2026-01-01T10:00:10Z", "source_id": "a", "seq": 2}
        assert q["next_key"] == {
            "observed_at": "2026-01-01T10:00:20Z", "source_id": "a", "seq": 3}
        assert q["gap_prev_seconds"] == pytest.approx(0.0)
        assert q["gap_next_seconds"] == pytest.approx(10.0)

    def test_head_insertion_has_no_predecessor(self, client):
        c, _ = client
        sid = create_session(c)
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:10Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:10Z", "value": 2},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 3},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 4},
        ])
        # Watermark 10:00:20 publishes the two :10 readings; :05 is late.
        r = report(c, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T10:00:05Z", "value": 9}])
        assert r.json()["quarantined"] == 1
        q = audit(c, sid).json()["readings"][0]
        assert q["insert_position"] == 0
        assert q["prev_key"] is None
        assert q["next_key"] == {
            "observed_at": "2026-01-01T10:00:10Z", "source_id": "a", "seq": 1}
        assert q["gap_prev_seconds"] is None
        assert q["gap_next_seconds"] == pytest.approx(5.0)

    def test_tail_insertion_has_no_successor(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        q = index_items(audit(c, sid).json()["readings"])[("a", 92)]
        # a@10:00:25 sorts after b@10:00:20 -> end of the five-row stream.
        assert q["insert_position"] == 5
        assert q["prev_key"] == {
            "observed_at": "2026-01-01T10:00:20Z", "source_id": "b", "seq": 2}
        assert q["next_key"] is None
        assert q["gap_prev_seconds"] == pytest.approx(5.0)
        assert q["gap_next_seconds"] is None

    def test_empty_published_stream_has_no_neighbours(self, client):
        # Under normal invariants a quarantined reading implies at least one
        # earlier publish, so an empty stream is only reachable by removing
        # the published rows directly; the audit must still place the reading
        # at position 0 with neither neighbour.
        c, db_path = client
        sid = setup_five_row_stream(c)
        with sqlite3.connect(db_path) as raw:
            raw.execute(
                "UPDATE readings SET status = 'buffered', published_watermark = NULL"
                " WHERE session_id = ? AND status = 'published'",
                (sid,),
            )
            raw.commit()
        q = audit(c, sid).json()["readings"][0]
        assert q["stream_size"] == 0
        assert q["insert_position"] == 0
        assert q["prev_key"] is None
        assert q["next_key"] is None
        assert q["gap_prev_seconds"] is None
        assert q["gap_next_seconds"] is None

    def test_results_are_ordered_by_timeline_keyset(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        keys = [(q["observed_at"], q["source_id"], q["seq"])
                for q in audit(c, sid).json()["readings"]]
        assert keys == sorted(keys)
        assert keys == [
            ("2026-01-01T10:00:05Z", "a", 90),
            ("2026-01-01T10:00:10Z", "b", 90),
            ("2026-01-01T10:00:15Z", "a", 91),
            ("2026-01-01T10:00:25Z", "a", 92),
        ]


class TestQuarantineAuditFilters:
    def test_filter_by_source_id(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        body = audit(c, sid, source_id="a").json()
        assert body["source_id"] == "a"
        assert body["matched"] == 3
        assert [(q["source_id"], q["seq"]) for q in body["readings"]] == [
            ("a", 90), ("a", 91), ("a", 92)]

    def test_inclusive_from_to_bounds(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        body = audit(c, sid, **{
            "from": "2026-01-01T10:00:10Z",
            "to": "2026-01-01T10:00:15Z",
        }).json()
        assert body["from"] == "2026-01-01T10:00:10Z"
        assert body["to"] == "2026-01-01T10:00:15Z"
        assert [(q["observed_at"], q["source_id"]) for q in body["readings"]] == [
            ("2026-01-01T10:00:10Z", "b"),
            ("2026-01-01T10:00:15Z", "a"),
        ]

    def test_from_to_are_normalised_to_utc(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        body = audit(c, sid, **{
            "from": "2026-01-01T12:00:10+02:00",
            "to": "2026-01-01T10:00:10Z",
        }).json()
        assert body["from"] == "2026-01-01T10:00:10Z"
        assert body["matched"] == 1
        assert body["readings"][0]["source_id"] == "b"

    def test_filters_combine(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        body = audit(c, sid, source_id="a", **{
            "from": "2026-01-01T10:00:10Z"}).json()
        assert body["matched"] == 2
        assert [(q["source_id"], q["seq"]) for q in body["readings"]] == [
            ("a", 91), ("a", 92)]

    def test_empty_result(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        body = audit(c, sid, **{"from": "2027-01-01T00:00:00Z"}).json()
        assert body["matched"] == 0
        assert body["returned"] == 0
        assert body["has_more"] is False
        assert body["readings"] == []

    def test_limit_caps_page_but_reports_all_matches(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        body = audit(c, sid, limit=2).json()
        assert body["matched"] == 4
        assert body["returned"] == 2
        assert body["has_more"] is True
        assert [(q["observed_at"], q["source_id"], q["seq"])
                for q in body["readings"]] == [
            ("2026-01-01T10:00:05Z", "a", 90),
            ("2026-01-01T10:00:10Z", "b", 90),
        ]

    def test_filtering_is_stable_across_pages(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        # Re-filtered windows compose in the same stable timeline order with
        # no overlap (order is (observed_at, source_id, seq), bounds inclusive).
        first = audit(c, sid, limit=2, to="2026-01-01T10:00:10Z").json()["readings"]
        second = audit(c, sid, limit=2, **{
            "from": "2026-01-01T10:00:15Z",
            "to": "2026-01-01T10:00:15Z",
        }).json()["readings"]
        third = audit(c, sid, limit=2, **{
            "from": "2026-01-01T10:00:25Z"}).json()["readings"]
        composed = [(q["observed_at"], q["source_id"], q["seq"])
                    for q in first + second + third]
        assert composed == [
            ("2026-01-01T10:00:05Z", "a", 90),
            ("2026-01-01T10:00:10Z", "b", 90),
            ("2026-01-01T10:00:15Z", "a", 91),
            ("2026-01-01T10:00:25Z", "a", 92),
        ]


class TestQuarantineAuditCheckpoints:
    def _staged_stream_with_checkpoints(self, c, sid):
        """Stream a@00,b@00,a@20,b@20,a@30,b@30 published (WM=30).

        cp-early captured offset 2 (last key b@00); cp-mid captured offset 4
        (last key b@20).
        """
        create_session(c, session_id=sid, lateness=10.0)
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 2},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 3},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 4},
        ])
        assert checkpoint(c, sid, "cp-early").json()["stream_offset"] == 2
        report(c, sid, [
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T10:00:30Z", "value": 5},
            {"source_id": "b", "seq": 3, "observed_at": "2026-01-01T10:00:30Z", "value": 6},
            {"source_id": "a", "seq": 4, "observed_at": "2026-01-01T10:00:40Z", "value": 7},
            {"source_id": "b", "seq": 4, "observed_at": "2026-01-01T10:00:40Z", "value": 8},
        ])
        assert checkpoint(c, sid, "cp-mid").json()["stream_offset"] == 4

    def test_earliest_affected_checkpoint_names_earliest_position(self, client):
        c, _ = client
        sid = "s"
        self._staged_stream_with_checkpoints(c, sid)
        # Late readings: @05 (slot 2, after cp-early key b@00),
        # @25 (slot 4, strictly after both checkpoint keys), and
        # a#50@00 (slot 1, before cp-early's key b@00).
        r = report(c, sid, [
            {"source_id": "a", "seq": 90, "observed_at": "2026-01-01T10:00:05Z", "value": 1},
            {"source_id": "a", "seq": 91, "observed_at": "2026-01-01T10:00:25Z", "value": 2},
            {"source_id": "a", "seq": 50, "observed_at": "2026-01-01T10:00:00Z", "value": 3},
        ])
        assert r.json()["quarantined"] == 3, r.text
        items = index_items(audit(c, sid).json()["readings"])
        # @05 at slot 2: resume from cp-early (captured b@00) would gain it;
        # cp-mid captured b@20 and is already past it. Earliest = cp-early.
        q = items[("a", 90)]
        assert q["insert_position"] == 2
        assert q["earliest_affected_checkpoint"] == "cp-early"
        # @25 at slot 4: both checkpoints precede it; earliest still cp-early.
        q = items[("a", 91)]
        assert q["insert_position"] == 4
        assert q["earliest_affected_checkpoint"] == "cp-early"
        # a#50@00 at slot 1 sorts before cp-early's captured key b@00: no
        # captured checkpoint resume would return it.
        q = items[("a", 50)]
        assert q["insert_position"] == 1
        assert q["earliest_affected_checkpoint"] is None

    def test_checkpoint_exactly_at_slot_boundary_is_not_affected(self, client):
        c, _ = client
        sid = "s"
        create_session(c, session_id=sid)
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 2},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 3},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 4},
        ])
        assert checkpoint(c, sid, "cp-at2").json()["stream_offset"] == 2
        # b#50@00 ties the captured last key's event time but sorts before
        # source 'b'; the checkpoint has already passed the slot.
        r = report(c, sid, [
            {"source_id": "a", "seq": 50, "observed_at": "2026-01-01T09:59:50Z",
             "value": 9}])
        assert r.json()["quarantined"] == 1
        q = audit(c, sid).json()["readings"][0]
        assert q["insert_position"] == 0
        assert q["earliest_affected_checkpoint"] is None

    def test_empty_stream_checkpoint_is_always_affected(self, client):
        c, _ = client
        sid = "s"
        create_session(c, session_id=sid)
        assert checkpoint(c, sid, "cp-empty").json()["stream_offset"] == 0
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:10Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:10Z", "value": 2},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 3},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 4},
        ])
        r = report(c, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T10:00:05Z",
             "value": 9}])
        assert r.json()["quarantined"] == 1
        q = audit(c, sid).json()["readings"][0]
        # Resuming from an empty-stream checkpoint starts at the beginning.
        assert q["earliest_affected_checkpoint"] == "cp-empty"

    def test_no_checkpoints_means_none_affected(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        for q in audit(c, sid).json()["readings"]:
            assert q["earliest_affected_checkpoint"] is None

    def test_earliest_affected_uses_keyset_not_shrunken_offset(self, client):
        # After a retention round the offset of a new checkpoint can be
        # smaller than an older one's; the audit must still name the older
        # checkpoint whose immutable keyset precedes the insertion slot.
        c, _ = client
        sid = "s"
        create_session(c, session_id=sid, lateness=0.0)
        # Ladder up to a@10:01:00: five rows published; b@10:00:50 and
        # a@10:01:00 remain buffered on the watermark.
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:10Z", "value": 2},
        ])
        report(c, sid, [
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:20Z", "value": 3}])
        report(c, sid, [
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 4}])
        report(c, sid, [
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T10:00:40Z", "value": 5}])
        report(c, sid, [
            {"source_id": "b", "seq": 3, "observed_at": "2026-01-01T10:00:50Z", "value": 6}])
        report(c, sid, [
            {"source_id": "a", "seq": 4, "observed_at": "2026-01-01T10:01:00Z", "value": 7}])
        # cp-a: offset 5, keyset a@10:00:40.
        cp_a = checkpoint(c, sid, "cp-a")
        assert cp_a.json()["stream_offset"] == 5
        assert cp_a.json()["last_observed_at"] == "2026-01-01T10:00:40Z"
        # First retention: the five covered rows disappear; buffered rows are
        # no candidates and stay buffered, so the published stream is empty.
        r = c.post(f"/sessions/{sid}/retention", json={
            "cutoff": "2026-12-31T00:00:00Z", "dry_run": False})
        assert r.json()["deleted"] == 5
        # Push both clocks: buffered b@10:00:50 and a@10:01:00 publish; the new
        # pair stays buffered.  Two published rows, both after cp-a's keyset.
        report(c, sid, [
            {"source_id": "a", "seq": 5, "observed_at": "2026-01-01T10:01:20Z", "value": 8},
            {"source_id": "b", "seq": 5, "observed_at": "2026-01-01T10:01:30Z", "value": 9},
        ])
        # cp-b: offset 2 on the re-grown stream, a later keyset than cp-a
        # despite the smaller offset.
        cp_b = checkpoint(c, sid, "cp-b")
        assert cp_b.json()["stream_offset"] == 2
        assert cp_b.json()["last_observed_at"] == "2026-01-01T10:01:00Z"
        # A late reading slotted after cp-a's keyset but before cp-b's: only
        # cp-a's resume would gain it regardless of the inverted offsets.
        r = report(c, sid, [
            {"source_id": "a", "seq": 90, "observed_at": "2026-01-01T10:00:45Z",
             "value": 10}])
        assert r.json()["quarantined"] == 1
        items = index_items(audit(c, sid).json()["readings"])
        assert items[("a", 90)]["earliest_affected_checkpoint"] == "cp-a"
        # A later slot follows both keysets: cp-a is still the earliest.
        r = report(c, sid, [
            {"source_id": "a", "seq": 91, "observed_at": "2026-01-01T10:01:05Z",
             "value": 11}])
        assert r.json()["quarantined"] == 1
        items = index_items(audit(c, sid).json()["readings"])
        assert items[("a", 91)]["earliest_affected_checkpoint"] == "cp-a"


class TestQuarantineAuditErrors:
    def test_unknown_session_returns_404(self, client):
        c, _ = client
        r = audit(c, "nope")
        assert r.status_code == 404
        assert "nope" in r.json()["detail"]

    @pytest.mark.parametrize("limit", [0, -1, 1001, "100", 1.5, True, False])
    def test_bad_limit_returns_400(self, client, limit):
        c, _ = client
        sid = create_session(c)
        r = audit(c, sid, limit=limit)
        assert r.status_code == 400
        assert "limit" in r.json()["detail"]

    @pytest.mark.parametrize("ts", ["not-a-time", "", 123, True])
    def test_bad_from_returns_400(self, client, ts):
        c, _ = client
        sid = create_session(c)
        assert audit(c, sid, **{"from": ts}).status_code == 400

    @pytest.mark.parametrize("ts", ["not-a-time", "", 123])
    def test_bad_to_returns_400(self, client, ts):
        c, _ = client
        sid = create_session(c)
        assert audit(c, sid, to=ts).status_code == 400

    def test_from_after_to_returns_400(self, client):
        c, _ = client
        sid = create_session(c)
        r = audit(c, sid, **{
            "from": "2026-01-01T10:00:01Z",
            "to": "2026-01-01T10:00:00Z",
        })
        assert r.status_code == 400
        assert "from" in r.json()["detail"]

    @pytest.mark.parametrize("source_id", ["", "   ", 5, True, ["a"]])
    def test_bad_source_id_returns_400(self, client, source_id):
        c, _ = client
        sid = create_session(c)
        assert audit(c, sid, source_id=source_id).status_code == 400

    def test_undeclared_source_id_returns_400(self, client):
        c, _ = client
        sid = create_session(c, sources=("a", "b"))
        r = audit(c, sid, source_id="c")
        assert r.status_code == 400
        assert "c" in r.json()["detail"]

    def test_body_must_be_object(self, client):
        c, _ = client
        sid = create_session(c)
        r = c.post(
            f"/sessions/{sid}/quarantine-audit",
            content=json.dumps(["nope"]),
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400
        assert "object" in r.json()["detail"]

    def test_body_must_be_valid_json(self, client):
        c, _ = client
        sid = create_session(c)
        r = c.post(
            f"/sessions/{sid}/quarantine-audit",
            content="{not json",
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400

    def test_empty_object_body_is_defaults(self, client):
        c, _ = client
        sid = create_session(c)
        body = audit(c, sid).json()
        assert body["limit"] == 100
        assert body["matched"] == 0


class TestQuarantineAuditReadOnly:
    def test_audit_changes_nothing(self, client):
        c, _ = client
        sid = setup_five_row_stream(c)
        checkpoint(c, sid, "cp-early")

        session_before = c.get(f"/sessions/{sid}").json()
        stream_before = c.get(f"/sessions/{sid}/stream").json()
        quarantine_before = c.get(f"/sessions/{sid}/quarantine").json()
        checkpoints_before = c.get(f"/sessions/{sid}/checkpoints").json()

        for kwargs in ({}, {"source_id": "a"}, {"limit": 2}, {
            "from": "2026-01-01T10:00:10Z",
            "to": "2026-01-01T10:00:30Z",
        }):
            assert audit(c, sid, **kwargs).status_code == 200

        assert c.get(f"/sessions/{sid}").json() == session_before
        assert c.get(f"/sessions/{sid}/stream").json() == stream_before
        assert c.get(f"/sessions/{sid}/quarantine").json() == quarantine_before
        assert c.get(f"/sessions/{sid}/checkpoints").json() == checkpoints_before

    def test_audit_works_on_closed_session(self, client):
        c, _ = client
        sid = create_session(c)
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:10Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:10Z", "value": 2},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 3},
            {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 4},
        ])
        r = report(c, sid, [
            {"source_id": "a", "seq": 9, "observed_at": "2026-01-01T10:00:05Z", "value": 9}])
        assert r.json()["quarantined"] == 1
        seal(c, sid, "a", "2026-01-01T10:01:00Z")
        seal(c, sid, "b", "2026-01-01T10:01:00Z")
        assert c.get(f"/sessions/{sid}").json()["closed"] is True
        # The audit stays available against the frozen published stream.
        r = audit(c, sid)
        assert r.status_code == 200, r.text
        q = r.json()["readings"][0]
        assert q["insert_position"] == 0
        assert q["next_key"]["observed_at"] == "2026-01-01T10:00:10Z"
