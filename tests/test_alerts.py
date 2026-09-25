"""Behaviour tests for the post-publish threshold alert state machines."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

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


BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def at(second: int) -> str:
    return (BASE + timedelta(seconds=second)).strftime("%Y-%m-%dT%H:%M:%SZ")


def report(client, sid, readings):
    return client.post(f"/sessions/{sid}/readings", json={"readings": readings})


def push(client, sid, second, values, seq, sources=("a", "b")):
    """Report one reading per source at ``second``.

    ``values`` is a scalar applied to every source or a ``{source: value}``
    mapping.  With zero allowed lateness the new watermark equals the slowest
    source's max event time, so readings reported here sit on the boundary
    and stay buffered until a later report pushes the watermark past them.
    """
    rows = []
    for s in sources:
        v = values.get(s) if isinstance(values, dict) else values
        n = seq.get(s, 0) + 1
        seq[s] = n
        rows.append({"source_id": s, "seq": n, "observed_at": at(second), "value": v})
    return report(client, sid, rows)


def feed(client, sid, values, seq, *, sources=("a", "b"), benign=0):
    """Report source-a test ``values`` at consecutive event-time seconds.

    Every other source reports a benign value at the same second so the
    watermark tracks a's clock: after batch i the watermark equals second i,
    hence a reading reported at second i publishes on the *next* batch and the
    final value is still buffered when this call returns.  Use
    :func:`end_feed` to publish it without feeding the machines an extra
    reading.
    """
    start = seq.get("_t", 0)
    for i, value in enumerate(values, start=1):
        second = start + i
        rows = [{"source_id": "a", "seq": seq.get("a", 0) + 1,
                 "observed_at": at(second), "value": value}]
        seq["a"] = seq.get("a", 0) + 1
        for s in sources:
            if s == "a":
                continue
            n = seq.get(s, 0) + 1
            seq[s] = n
            rows.append({"source_id": s, "seq": n,
                         "observed_at": at(second), "value": benign})
        report(client, sid, rows)
    seq["_t"] = start + len(values)


def end_feed(client, sid, seq, *, sources=("a", "b"), benign=0):
    """Publish the last buffered source-a reading.

    A trailing sentinel (``null``) advances a's event-time clock by one
    second, which pushes the watermark past the previous reading; the sentinel
    itself then sits on the new boundary and stays buffered, so it is never
    evaluated by an alert machine.
    """
    second = seq.get("_t", 0) + 1
    rows = [{"source_id": "a", "seq": seq.get("a", 0) + 1,
             "observed_at": at(second), "value": None}]
    seq["a"] = seq.get("a", 0) + 1
    for s in sources:
        if s == "a":
            continue
        n = seq.get(s, 0) + 1
        seq[s] = n
        rows.append({"source_id": s, "seq": n,
                     "observed_at": at(second), "value": benign})
    report(client, sid, rows)
    seq["_t"] = second


def create_session(client, sources=("a", "b"), lateness=0.0):
    r = client.post("/sessions", json={
        "source_ids": list(sources),
        "allowed_lateness_seconds": lateness,
    })
    assert r.status_code == 201, r.text
    return r.json()["session_id"]


def make_rule(
    client,
    sid,
    source_id="a",
    *,
    direction="above",
    enter=10.0,
    recover=5.0,
    enter_n=1,
    recover_n=1,
    value_key=None,
    rule_id=None,
):
    body = {
        "source_id": source_id,
        "direction": direction,
        "enter_threshold": enter,
        "recover_threshold": recover,
        "enter_consecutive": enter_n,
        "recover_consecutive": recover_n,
    }
    if value_key is not None:
        body["value_key"] = value_key
    if rule_id is not None:
        body["rule_id"] = rule_id
    r = client.post(f"/sessions/{sid}/alert-rules", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def history(client, sid, **params):
    """Page through the whole event history with the given query parameters."""
    out = []
    cursor = None
    for _ in range(100):
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        page = client.get(f"/sessions/{sid}/alerts/history", params=q).json()
        out.extend(page["events"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


def current(client, sid, **params):
    return client.get(f"/sessions/{sid}/alerts/current", params=params).json()["alerts"]


class TestRuleValidation:
    def test_unknown_source_400(self, client):
        sid = create_session(client, sources=["a", "b"])
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "zzz", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 5,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 400
        assert "unknown source" in r.json()["detail"]

    def test_bad_hysteresis_above_400(self, client):
        sid = create_session(client)
        # recover must be strictly below enter for 'above'
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 10,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 400
        assert "hysteresis" in r.json()["detail"]
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 12,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 400

    def test_bad_hysteresis_below_400(self, client):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "below",
            "enter_threshold": 0, "recover_threshold": 0,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 400
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "below",
            "enter_threshold": 0, "recover_threshold": -1,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 400

    def test_structural_errors_422(self, client):
        sid = create_session(client)
        # bad direction
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "sideways",
            "enter_threshold": 10, "recover_threshold": 5,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 422
        # consecutive must be >= 1
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 5,
            "enter_consecutive": 0, "recover_consecutive": 1,
        })
        assert r.status_code == 422
        # missing threshold
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "above",
            "recover_threshold": 5,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 422
        # NaN / infinity are rejected as non-finite
        for bad in ("NaN", "Infinity", "-Infinity"):
            r = client.post(f"/sessions/{sid}/alert-rules", json={
                "source_id": "a", "direction": "above",
                "enter_threshold": bad, "recover_threshold": 5,
                "enter_consecutive": 1, "recover_consecutive": 1,
            })
            assert r.status_code == 422, bad
        # blank source / value_key
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "  ", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 5,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 422
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 5,
            "enter_consecutive": 1, "recover_consecutive": 1,
            "value_key": "  ",
        })
        assert r.status_code == 422

    def test_rule_on_unknown_session_404(self, client):
        r = client.post("/sessions/nope/alert-rules", json={
            "source_id": "a", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 5,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 404

    def test_duplicate_rule_id_409(self, client):
        sid = create_session(client)
        make_rule(client, sid, rule_id="r1")
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "rule_id": "r1", "source_id": "b", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 5,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 409

    def test_list_and_get_rule(self, client):
        sid = create_session(client)
        make_rule(client, sid, source_id="a", rule_id="ra")
        make_rule(client, sid, source_id="b", enter=20, recover=15, rule_id="rb")
        rules = client.get(f"/sessions/{sid}/alert-rules").json()["rules"]
        assert {r["rule_id"] for r in rules} == {"ra", "rb"}
        one = client.get(f"/sessions/{sid}/alert-rules/ra")
        assert one.status_code == 200
        assert one.json()["source_id"] == "a"
        assert client.get(f"/sessions/{sid}/alert-rules/missing").status_code == 404


class TestStateMachine:
    def test_enter_on_single_breach_and_query_current(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid)["rule_id"]
        seq: dict = {}
        # a@:01 breaches; b@:01 -> wm :01, a@:01 on the boundary is buffered
        push(client, sid, 1, {"a": 11, "b": 1}, seq)
        assert current(client, sid) == []
        # benign pair at :02 publishes a@:01 -> entered
        push(client, sid, 2, {"a": 0, "b": 0}, seq)
        alerts = current(client, sid)
        assert len(alerts) == 1
        alert = alerts[0]
        assert alert["rule_id"] == rid
        assert alert["state"] == "active"
        assert alert["entered"]["seq"] == 1
        assert alert["entered"]["observed_at"] == at(1)
        assert alert["entered"]["value"] == 11.0
        assert alert["entered"]["threshold"] == 10.0
        events = history(client, sid)
        assert [(e["event_type"], e["source_id"]) for e in events] == [("entered", "a")]

    def test_strict_threshold_equality_does_not_breach(self, client):
        sid = create_session(client)
        make_rule(client, sid, enter=10.0)
        seq: dict = {}
        push(client, sid, 1, {"a": 10, "b": 1}, seq)
        push(client, sid, 2, {"a": 0, "b": 0}, seq)
        assert current(client, sid) == []
        assert history(client, sid) == []

    def test_enter_then_recover_lifecycle(self, client):
        sid = create_session(client)
        make_rule(client, sid, enter=10, recover=5, enter_n=1, recover_n=1)
        seq: dict = {}
        # machine sees, in order: breach, band value, breach, boundary, safe
        feed(client, sid, [11, 7, 11, 5, 4], seq)
        end_feed(client, sid, seq)

        events = history(client, sid)
        # newest first: entered on the first reading, recovered only at 4
        assert [e["event_type"] for e in events] == ["recovered", "entered"]
        ent, rec = events[1], events[0]
        assert ent["observed_at"] == at(1)
        assert ent["value"] == 11.0 and ent["threshold"] == 10.0
        assert rec["source_id"] == "a"
        assert rec["observed_at"] == at(5)
        assert rec["value"] == 4.0
        assert rec["threshold"] == 5.0
        assert current(client, sid) == []

    def test_consecutive_counts_require_unbroken_runs(self, client):
        sid = create_session(client)
        make_rule(client, sid, enter=10, recover=5, enter_n=3, recover_n=2)
        seq: dict = {}
        # two breaches, then a normal value resets the enter streak,
        # then an unbroken run of three triggers the enter
        feed(client, sid, [11, 11, 1, 11, 11, 11], seq)
        end_feed(client, sid, seq)
        alerts = current(client, sid)
        assert len(alerts) == 1
        assert alerts[0]["entered"]["observed_at"] == at(6)
        assert alerts[0]["entered"]["value"] == 11.0

        # recovery needs two safe readings in a row; a band value in the
        # middle resets the streak, then two consecutive safe values clear it
        feed(client, sid, [4, 7, 4, 3], seq)
        end_feed(client, sid, seq)
        assert current(client, sid) == []
        assert [e["event_type"] for e in history(client, sid)] == [
            "recovered", "entered"
        ]

    def test_below_direction(self, client):
        sid = create_session(client)
        make_rule(
            client, sid, direction="below",
            enter=0, recover=2, enter_n=2, recover_n=1,
        )
        seq: dict = {}
        # a breach, then a band value (>= 0 but not strictly > 2) resets;
        # two consecutive breaches then enter
        feed(client, sid, [-1, 1, -5, -5], seq, benign=100)
        end_feed(client, sid, seq, benign=100)
        alerts = current(client, sid)
        assert len(alerts) == 1
        assert alerts[0]["entered"]["observed_at"] == at(4)
        assert alerts[0]["entered"]["value"] == -5.0
        # equality with the recover threshold does not recover; the next safe
        # value (strictly above 2) clears the alert
        feed(client, sid, [2, 3], seq, benign=100)
        end_feed(client, sid, seq, benign=100)
        assert current(client, sid) == []
        events = history(client, sid)
        assert [e["event_type"] for e in events] == ["recovered", "entered"]
        # the second feed resumes at second 6 (value 2); 3 is at second 7
        assert events[0]["observed_at"] == at(7)

    def test_non_numeric_and_missing_value_reset_without_event(self, client):
        sid = create_session(client)
        make_rule(client, sid, enter=10, recover=5, enter_n=2)
        seq: dict = {}
        feed(client, sid, [11, "hot", True, None, 12, 12], seq)
        end_feed(client, sid, seq)
        # only the final unbroken pair (12, 12) satisfies enter_consecutive=2
        alerts = current(client, sid)
        assert len(alerts) == 1
        assert alerts[0]["entered"]["observed_at"] == at(6)
        assert [e["event_type"] for e in history(client, sid)] == ["entered"]

    def test_value_key_extracted_from_object_payload(self, client):
        sid = create_session(client)
        make_rule(client, sid, enter=100, recover=50, value_key="temp")
        seq: dict = {}
        feed(client, sid, [
            {"temp": 90, "unit": "C"},
            {"other": 1},          # missing key -> streak reset
            {"temp": 101},
        ], seq)
        end_feed(client, sid, seq)
        alerts = current(client, sid)
        assert len(alerts) == 1
        assert alerts[0]["entered"]["observed_at"] == at(3)
        assert alerts[0]["entered"]["value"] == 101.0


class TestPublishedOnly:
    def test_quarantined_late_reading_never_drives_alerts(self, client):
        sid = create_session(client, lateness=0.0)
        make_rule(client, sid, enter=10, recover=5, enter_n=1, recover_n=1)
        # establish a published watermark at :10 with benign values
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(10), "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": at(10), "value": 1},
        ])
        assert current(client, sid) == []
        # a late reading behind the published watermark would breach, but it is
        # diverted to quarantine and must never touch alert state
        r = report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": at(1), "value": 999},
        ]).json()
        assert r["quarantined"] == 1
        assert current(client, sid) == []
        assert history(client, sid) == []
        q = client.get(f"/sessions/{sid}/quarantine").json()["readings"]
        assert len(q) == 1 and q[0]["seq"] == 9

    def test_buffered_readings_do_not_advance_until_published(self, client):
        sid = create_session(client)
        make_rule(client, sid, enter=10)
        # a breaches at :01 but b has not reported -> nothing published
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(1), "value": 11},
        ])
        assert current(client, sid) == []
        # b reports :01 -> watermark :01, a@:01 on the boundary still buffered
        report(client, sid, [
            {"source_id": "b", "seq": 1, "observed_at": at(1), "value": 0},
        ])
        assert current(client, sid) == []
        # both advance to :02 -> a@:01 is strictly before the new watermark
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": at(2), "value": 0},
            {"source_id": "b", "seq": 2, "observed_at": at(2), "value": 0},
        ])
        alerts = current(client, sid)
        assert len(alerts) == 1
        assert alerts[0]["entered"]["observed_at"] == at(1)

    def test_duplicate_reports_do_not_retrigger(self, client):
        sid = create_session(client)
        make_rule(client, sid)
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 0}, seq)
        push(client, sid, 2, {"a": 0, "b": 0}, seq)
        assert len(history(client, sid)) == 1
        # re-post an identical earlier reading: duplicate, no extra events
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(1), "value": 11},
        ]).json()
        assert r["duplicates"] == 1
        assert len(history(client, sid)) == 1
        rule = client.get(f"/sessions/{sid}/alert-rules").json()["rules"][0]
        assert rule["state"] == "active"


class TestBackfill:
    def test_rule_created_after_publication_replays_history(self, client):
        sid = create_session(client)
        seq: dict = {}
        # publish normal, breach, breach for source a (the trailing null
        # sentinel stays buffered and is not part of the replayed history)
        feed(client, sid, [1, 11, 12], seq)
        end_feed(client, sid, seq)
        assert current(client, sid) == []
        # enter_n=2 replays: the last two readings are consecutive breaches
        created = make_rule(client, sid, enter=10, recover=5, enter_n=2)
        assert created["state"] == "active"
        alerts = current(client, sid)
        assert len(alerts) == 1
        assert alerts[0]["entered"]["observed_at"] == at(3)
        assert alerts[0]["entered"]["value"] == 12.0
        events = history(client, sid)
        assert len(events) == 1 and events[0]["event_type"] == "entered"

    def test_backfill_replays_full_lifecycle(self, client):
        sid = create_session(client)
        seq: dict = {}
        feed(client, sid, [11, 11, 4], seq)
        end_feed(client, sid, seq)
        created = make_rule(client, sid)
        # entered at reading 1, recovered at reading 3 -> normal, both events
        assert created["state"] == "normal"
        assert [e["event_type"] for e in history(client, sid)] == [
            "recovered", "entered"
        ]
        assert current(client, sid) == []

    def test_backfill_ignores_quarantined_readings(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(10), "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": at(10), "value": 1},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": at(1), "value": 999},
        ])
        created = make_rule(client, sid, enter=10)
        assert created["state"] == "normal"
        assert history(client, sid) == []


class TestPerSource:
    def test_rules_are_independent_per_source(self, client):
        sid = create_session(client, sources=["a", "b", "c"])
        ra = make_rule(client, sid, source_id="a", rule_id="ra")["rule_id"]
        rb = make_rule(client, sid, source_id="b", enter=20, recover=10,
                       rule_id="rb")["rule_id"]
        seq: dict = {}
        # a breaches its own threshold (11>10); b breaches its own (21>20)
        push(client, sid, 1, {"a": 11, "b": 21, "c": 0}, seq,
             sources=("a", "b", "c"))
        push(client, sid, 2, {"a": 0, "b": 0, "c": 0}, seq,
             sources=("a", "b", "c"))
        alerts = current(client, sid)
        assert {x["rule_id"] for x in alerts} == {ra, rb}
        # filter by source
        assert [x["rule_id"] for x in current(client, sid, source_id="a")] == [ra]
        # unknown source filter -> 400
        r = client.get(f"/sessions/{sid}/alerts/current", params={"source_id": "zzz"})
        assert r.status_code == 400

    def test_multiple_rules_on_same_source_evaluate_independently(self, client):
        sid = create_session(client)
        make_rule(client, sid, enter=10, recover=5, rule_id="hi")
        make_rule(client, sid, direction="below", enter=0, recover=2, rule_id="lo")
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 0}, seq)
        push(client, sid, 2, {"a": 0, "b": 0}, seq)
        assert {x["rule_id"] for x in current(client, sid)} == {"hi"}


class TestHistoryPagination:
    def test_keyset_pagination_newest_first_and_filters(self, client):
        sid = create_session(client)
        make_rule(client, sid, enter=10, recover=5)
        seq: dict = {}
        # 3 enter/recover cycles (the trailing null sentinel is not evaluated)
        feed(client, sid, [11, 4, 11, 4, 11, 4], seq)
        end_feed(client, sid, seq)

        all_events = history(client, sid, limit=2)
        assert len(all_events) == 6
        ids = [e["id"] for e in all_events]
        assert ids == sorted(ids, reverse=True)
        assert len(set(ids)) == 6  # no duplicates across pages

        entered = history(client, sid, event_type="entered")
        assert len(entered) == 3
        assert all(e["event_type"] == "entered" for e in entered)
        recovered = client.get(
            f"/sessions/{sid}/alerts/history", params={"event_type": "recovered"}
        ).json()["events"]
        assert len(recovered) == 3

    def test_bad_event_type_400(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/alerts/history",
                       params={"event_type": "exploded"})
        assert r.status_code == 400

    def test_bad_cursor_400(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/alerts/history",
                       params={"cursor": "%%%garbage"})
        assert r.status_code == 400

    def test_unknown_rule_filter_404(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/alerts/history",
                       params={"rule_id": "ghost"})
        assert r.status_code == 404

    def test_history_unknown_session_404(self, client):
        assert client.get("/sessions/nope/alerts/history").status_code == 404
        assert client.get("/sessions/nope/alerts/current").status_code == 404
        assert client.get("/sessions/nope/alert-rules").status_code == 404


class TestHugeIntegerValues:
    """JSON integers too large for a finite double (e.g. 10**400).

    They are stored, published and deduplicated like any other reading, but
    they are invalid *alert* values: they reset the streak without producing
    enter/recover events, and they never crash the publish transaction or a
    rule-creation replay.
    """

    BIG = 10**400

    def test_stored_published_and_deduped_without_rules(self, client):
        sid = create_session(client)
        seq: dict = {}
        push(client, sid, 1, {"a": self.BIG, "b": 0}, seq)
        # The batch that publishes the huge-integer reading commits in full.
        r = push(client, sid, 2, {"a": 1, "b": 1}, seq)
        assert r.status_code == 200
        assert r.json()["published_now"] == 2
        stream = client.get(f"/sessions/{sid}/stream").json()["readings"]
        assert [(x["source_id"], x["seq"]) for x in stream] == [("a", 1), ("b", 1)]
        # The payload round-trips as the original JSON integer.
        assert stream[0]["value"] == self.BIG
        # Same key + same payload -> idempotent duplicate (dedup by payload).
        r = report(client, sid, [{"source_id": "a", "seq": 1,
                                  "observed_at": at(1), "value": self.BIG}])
        assert r.status_code == 200
        assert r.json()["duplicates"] == 1
        # Same key + different payload -> 409.
        r = report(client, sid, [{"source_id": "a", "seq": 1,
                                  "observed_at": at(1), "value": 10**399}])
        assert r.status_code == 409

    def test_resets_enter_streak_without_entering(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, enter=10, recover=5,
                        enter_n=2, recover_n=1)["rule_id"]
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 0}, seq)
        push(client, sid, 2, {"a": self.BIG, "b": 0}, seq)  # publishes t1: streak 1
        r = push(client, sid, 3, {"a": 11, "b": 0}, seq)    # publishes t2: reset
        assert r.status_code == 200 and r.json()["published_now"] == 2
        push(client, sid, 4, {"a": 11, "b": 0}, seq)        # publishes t3: streak 1
        rule = client.get(f"/sessions/{sid}/alert-rules/{rid}").json()
        # The huge integer broke the run: 11, BIG, 11 is not two consecutive
        # breaches, so the rule never entered.
        assert rule["state"] == "normal"
        assert rule["streak"] == 1
        assert current(client, sid) == []
        assert history(client, sid) == []
        # One more consecutive breach completes the pair and enters.
        push(client, sid, 5, {"a": 0, "b": 0}, seq)         # publishes t4: entered
        assert [x["rule_id"] for x in current(client, sid)] == [rid]

    def test_resets_recovery_streak_without_recovering(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, enter=10, recover=5,
                        enter_n=1, recover_n=2)["rule_id"]
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 0}, seq)
        push(client, sid, 2, {"a": 1, "b": 0}, seq)          # publishes t1: entered
        push(client, sid, 3, {"a": self.BIG, "b": 0}, seq)   # publishes t2: rec. streak 1
        push(client, sid, 4, {"a": 1, "b": 0}, seq)          # publishes t3: reset
        rule = client.get(f"/sessions/{sid}/alert-rules/{rid}").json()
        # The huge integer is not a recovery reading: still active, streak 0.
        assert rule["state"] == "active"
        assert rule["streak"] == 0
        push(client, sid, 5, {"a": 1, "b": 0}, seq)          # publishes t4: streak 1
        push(client, sid, 6, {"a": 1, "b": 0}, seq)          # publishes t5: recovered
        rule = client.get(f"/sessions/{sid}/alert-rules/{rid}").json()
        assert rule["state"] == "normal"
        assert [e["event_type"] for e in history(client, sid)] == ["recovered", "entered"]

    def test_huge_int_under_value_key(self, client):
        sid = create_session(client)
        rid = make_rule(client, sid, enter=10, recover=5,
                        enter_n=1, value_key="temp")["rule_id"]
        seq: dict = {}
        push(client, sid, 1, {"a": {"temp": self.BIG}, "b": 0}, seq)
        push(client, sid, 2, {"a": {"temp": 1}, "b": 0}, seq)  # publishes t1
        rule = client.get(f"/sessions/{sid}/alert-rules/{rid}").json()
        assert rule["state"] == "normal"
        assert rule["streak"] == 0
        assert current(client, sid) == []

    def test_late_rule_replay_resets_on_huge_int(self, client):
        # Publish first, create the rule afterwards: the replayed machine
        # matches the state it would have had from the start.
        sid = create_session(client)
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 0}, seq)
        push(client, sid, 2, {"a": self.BIG, "b": 0}, seq)
        push(client, sid, 3, {"a": 11, "b": 0}, seq)
        push(client, sid, 4, {"a": 0, "b": 0}, seq)  # publishes t1..t3
        rule = make_rule(client, sid, enter=10, recover=5, enter_n=2)
        # Replay: 11 (streak 1), BIG (reset), 11 (streak 1) -> no enter.
        assert rule["state"] == "normal"
        assert rule["streak"] == 1
        assert history(client, sid) == []
