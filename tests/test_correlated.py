"""Behaviour tests for cross-source correlated alert rules."""
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


def create_session(client, sources=("a", "b"), lateness=0.0, session_id=None):
    body = {"source_ids": list(sources), "allowed_lateness_seconds": lateness}
    if session_id:
        body["session_id"] = session_id
    r = client.post("/sessions", json=body)
    assert r.status_code == 201, r.text
    return r.json()["session_id"]


def make_corr_rule(client, sid, conditions, rule_id=None):
    body = {"conditions": conditions}
    if rule_id is not None:
        body["rule_id"] = rule_id
    r = client.post(f"/sessions/sid/correlated-rules".replace("sid", sid), json=body)
    assert r.status_code == 201, r.text
    return r.json()


def cond(source, direction="above", threshold=10.0, window=5.0, value_key=None):
    c = {"source_id": source, "direction": direction, "threshold": threshold,
         "window_seconds": window}
    if value_key is not None:
        c["value_key"] = value_key
    return c


def push(client, sid, second, values, seq, sources):
    """Report one reading per source at event-time ``second``.

    ``values`` maps each source to its value (defaults to a benign 0).  With
    zero allowed lateness a reading at second i publishes when the next batch
    (second i+1) advances the watermark past it; the final batch stays
    buffered on the boundary.
    """
    rows = []
    for s in sources:
        n = seq.get(s, 0) + 1
        seq[s] = n
        rows.append({"source_id": s, "seq": n, "observed_at": at(second),
                     "value": values.get(s, 0)})
    return report(client, sid, rows)


def current(client, sid):
    return client.get(f"/sessions/{sid}/correlated-alerts/current").json()["alerts"]


def history(client, sid, **params):
    """Page through the full correlated event history."""
    out = []
    cursor = None
    for _ in range(100):
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        page = client.get(f"/sessions/{sid}/correlated-alerts/history", params=q).json()
        out.extend(page["events"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


class TestRuleValidation:
    def test_create_rule_with_two_conditions(self, client):
        sid = create_session(client)
        rule = make_corr_rule(client, sid, [cond("a"), cond("b", threshold=20)])
        assert rule["state"] == "waiting"
        assert rule["correlated_since"] is None
        assert [c["position"] for c in rule["conditions"]] == [0, 1]
        assert [c["source_id"] for c in rule["conditions"]] == ["a", "b"]
        assert all(c["last_hit"] is None for c in rule["conditions"])

    def test_three_and_four_sources_accepted(self, client):
        sid3 = create_session(client, sources=["a", "b", "c"])
        r = client.post(f"/sessions/{sid3}/correlated-rules",
                        json={"conditions": [cond(s) for s in ("a", "b", "c")]})
        assert r.status_code == 201, r.text
        sid4 = create_session(client, sources=["a", "b", "c", "d"], session_id="s4")
        r = client.post(f"/sessions/{sid4}/correlated-rules",
                        json={"conditions": [cond(s) for s in ("a", "b", "c", "d")]})
        assert r.status_code == 201, r.text

    def test_too_few_or_too_many_conditions_422(self, client):
        sid = create_session(client, sources=["a", "b", "c", "d", "e"])
        r = client.post(f"/sessions/{sid}/correlated-rules",
                        json={"conditions": [cond("a")]})
        assert r.status_code == 422
        r = client.post(f"/sessions/{sid}/correlated-rules", json={"conditions": [
            cond(s) for s in ("a", "b", "c", "d", "e")
        ]})
        assert r.status_code == 422

    def test_duplicate_sources_422(self, client):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/correlated-rules", json={"conditions": [
            cond("a", threshold=10), cond("a", threshold=20),
        ]})
        assert r.status_code == 422
        assert "distinct" in r.text

    def test_unknown_source_400(self, client):
        sid = create_session(client)
        r = client.post(f"/sessions/{sid}/correlated-rules",
                        json={"conditions": [cond("a"), cond("zzz")]})
        assert r.status_code == 400
        assert "unknown source" in r.json()["detail"]

    def test_unknown_session_404(self, client):
        r = client.post("/sessions/nope/correlated-rules",
                        json={"conditions": [cond("a"), cond("b")]})
        assert r.status_code == 404

    def test_structural_errors_422(self, client):
        sid = create_session(client)
        base = {"conditions": [
            {"source_id": "a", "direction": "above", "threshold": 10,
             "window_seconds": 5},
            {"source_id": "b", "direction": "above", "threshold": 20,
             "window_seconds": 5},
        ]}
        # bad direction
        bad = {"conditions": [dict(base["conditions"][0], direction="sideways"),
                              base["conditions"][1]]}
        assert client.post(f"/sessions/{sid}/correlated-rules", json=bad).status_code == 422
        # non-positive / non-finite window
        bad = {"conditions": [dict(base["conditions"][0], window_seconds=0),
                              base["conditions"][1]]}
        assert client.post(f"/sessions/{sid}/correlated-rules", json=bad).status_code == 422
        # NaN threshold
        bad = {"conditions": [dict(base["conditions"][0], threshold="NaN"),
                              base["conditions"][1]]}
        assert client.post(f"/sessions/{sid}/correlated-rules", json=bad).status_code == 422
        # missing threshold
        bad = {"conditions": [
            {"source_id": "a", "direction": "above", "window_seconds": 5},
            base["conditions"][1],
        ]}
        assert client.post(f"/sessions/{sid}/correlated-rules", json=bad).status_code == 422
        # blank value_key
        bad = {"conditions": [dict(base["conditions"][0], value_key="  "),
                              base["conditions"][1]]}
        assert client.post(f"/sessions/{sid}/correlated-rules", json=bad).status_code == 422

    def test_duplicate_rule_id_409(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid, [cond("a"), cond("b")], rule_id="r1")
        r = client.post(f"/sessions/{sid}/correlated-rules", json={
            "rule_id": "r1",
            "conditions": [cond("a", threshold=1), cond("b", threshold=2)],
        })
        assert r.status_code == 409

    def test_list_and_get_rule(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid, [cond("a"), cond("b", threshold=20)], rule_id="r1")
        make_corr_rule(
            client, sid,
            [cond("a", direction="below", threshold=-5, window=10),
             cond("b", threshold=30, window=2)],
            rule_id="r2",
        )
        rules = client.get(f"/sessions/{sid}/correlated-rules").json()["rules"]
        assert {r["rule_id"] for r in rules} == {"r1", "r2"}
        one = client.get(f"/sessions/{sid}/correlated-rules/r1").json()
        assert [c["source_id"] for c in one["conditions"]] == ["a", "b"]
        assert one["conditions"][0]["window_seconds"] == 5.0
        assert client.get(f"/sessions/{sid}/correlated-rules/missing").status_code == 404
        assert client.get("/sessions/nope/correlated-rules").status_code == 404


class TestCorrelation:
    def test_fires_when_all_sources_breach_within_window(self, client):
        sid = create_session(client)
        rule = make_corr_rule(
            client, sid,
            [cond("a", threshold=10, window=10), cond("b", threshold=20, window=10)],
        )
        rid = rule["rule_id"]
        seq: dict = {}
        # a breaches at t1; it alone cannot correlate
        push(client, sid, 1, {"a": 11, "b": 0}, seq, ("a", "b"))
        assert current(client, sid) == []
        # boundary push publishes t1; still only a has hit
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert current(client, sid) == []
        # b breaches at t3; t3 boundary push at t4 publishes it: a's hit (t1)
        # is 2s old <= 10s window -> correlation completes on b@t3
        push(client, sid, 3, {"a": 0, "b": 21}, seq, ("a", "b"))
        assert current(client, sid) == []
        push(client, sid, 4, {"a": 0, "b": 0}, seq, ("a", "b"))
        alerts = current(client, sid)
        assert len(alerts) == 1
        alert = alerts[0]
        assert alert["rule_id"] == rid
        entered = alert["correlated_entered"]
        assert entered["event_type"] == "correlated_entered"
        assert entered["completed_by"] == {
            "source_id": "b", "seq": 3, "observed_at": at(3),
        }
        hits = {(h["source_id"]): h for h in entered["conditions"]}
        assert hits["a"]["observed_at"] == at(1) and hits["a"]["value"] == 11.0
        assert hits["b"]["observed_at"] == at(3) and hits["b"]["value"] == 21.0
        events = history(client, sid)
        assert len(events) == 1
        assert events[0]["id"] == entered["id"]

    def test_three_source_correlation_merge_order(self, client):
        sid = create_session(client, sources=("a", "b", "c"))
        make_corr_rule(client, sid, [
            cond("a", threshold=10, window=30),
            cond("b", threshold=20, window=30),
            cond("c", threshold=30, window=30),
        ])
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 0, "c": 0}, seq, ("a", "b", "c"))
        push(client, sid, 2, {"a": 0, "b": 22, "c": 0}, seq, ("a", "b", "c"))
        push(client, sid, 3, {"a": 0, "b": 0, "c": 33}, seq, ("a", "b", "c"))
        assert current(client, sid) == []
        # publish up through t3
        push(client, sid, 4, {"a": 0, "b": 0, "c": 0}, seq, ("a", "b", "c"))
        alerts = current(client, sid)
        assert len(alerts) == 1
        assert alerts[0]["correlated_entered"]["completed_by"]["source_id"] == "c"

    def test_strict_threshold_equality_is_not_a_hit(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=30),
                       cond("b", threshold=20, window=30)])
        seq: dict = {}
        # exactly on the thresholds: no breach (strict comparison)
        push(client, sid, 1, {"a": 10, "b": 20}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert current(client, sid) == []
        assert history(client, sid) == []
        rule = client.get(f"/sessions/{sid}/correlated-rules").json()["rules"][0]
        assert all(c["last_hit"] is None for c in rule["conditions"])

    def test_below_direction(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid, [
            cond("a", direction="below", threshold=0, window=30),
            cond("b", direction="below", threshold=-10, window=30),
        ])
        seq: dict = {}
        push(client, sid, 1, {"a": -1, "b": 100}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": -11}, seq, ("a", "b"))
        push(client, sid, 3, {"a": 0, "b": 0}, seq, ("a", "b"))
        alerts = current(client, sid)
        assert len(alerts) == 1
        hits = {h["source_id"]: h for h in alerts[0]["correlated_entered"]["conditions"]}
        assert hits["a"]["value"] == -1.0 and hits["b"]["value"] == -11.0

    def test_window_boundary_is_inclusive(self, client):
        sid = create_session(client)
        # window exactly 2 seconds: a@t1 is age exactly 2 when b@t3 evaluates
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=2),
                       cond("b", threshold=20, window=2)])
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 0}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        push(client, sid, 3, {"a": 0, "b": 21}, seq, ("a", "b"))
        push(client, sid, 4, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert len(current(client, sid)) == 1

    def test_hit_just_outside_window_blocks_then_refreshes(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=2),
                       cond("b", threshold=20, window=2)])
        seq: dict = {}
        # a hits at t1; b hits at t4 -> age 3 > 2, no correlation
        push(client, sid, 1, {"a": 11, "b": 0}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        push(client, sid, 4, {"a": 0, "b": 21}, seq, ("a", "b"))
        push(client, sid, 5, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert current(client, sid) == []
        assert history(client, sid) == []
        # a refreshes its hit at t6 while b's t4 hit is age 2 (inclusive)
        push(client, sid, 6, {"a": 12, "b": 0}, seq, ("a", "b"))
        push(client, sid, 7, {"a": 0, "b": 0}, seq, ("a", "b"))
        alerts = current(client, sid)
        assert len(alerts) == 1
        entered = alerts[0]["correlated_entered"]
        assert entered["completed_by"]["source_id"] == "a"
        hits = {h["source_id"]: h for h in entered["conditions"]}
        assert hits["a"]["observed_at"] == at(6) and hits["b"]["observed_at"] == at(4)

    def test_per_condition_windows(self, client):
        sid = create_session(client)
        # a has a wide window, b a narrow one: b's stale hit expires first
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=30),
                       cond("b", threshold=20, window=1)])
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 21}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert len(current(client, sid)) == 1
        # time passes without fresh breaches; at t4 the b hit is age 3 > 1
        push(client, sid, 3, {"a": 0, "b": 0}, seq, ("a", "b"))
        push(client, sid, 4, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert current(client, sid) == []
        rule = client.get(f"/sessions/{sid}/correlated-rules").json()["rules"][0]
        assert rule["state"] == "waiting"
        # exactly one historical event from the earlier correlation
        assert len(history(client, sid)) == 1

    def test_non_participating_source_readings_expire_windows(self, client):
        sid = create_session(client, sources=("a", "b", "c"))
        make_corr_rule(client, sid, [
            cond("a", threshold=10, window=1),
            cond("b", threshold=20, window=1),
        ])
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 21, "c": 0}, seq, ("a", "b", "c"))
        push(client, sid, 2, {"a": 0, "b": 0, "c": 0}, seq, ("a", "b", "c"))
        assert len(current(client, sid)) == 1
        # only the unrelated source c advances event time past the window
        push(client, sid, 3, {"c": 0}, seq, ("a", "b", "c"))
        push(client, sid, 4, {"c": 0}, seq, ("a", "b", "c"))
        assert current(client, sid) == []

    def test_no_duplicate_event_while_still_correlated(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=30),
                       cond("b", threshold=20, window=30)])
        seq: dict = {}
        # both breach together at t1 and keep breaching through t4
        for s in range(1, 5):
            push(client, sid, s, {"a": 11, "b": 21}, seq, ("a", "b"))
        push(client, sid, 5, {"a": 0, "b": 0}, seq, ("a", "b"))
        events = history(client, sid)
        assert len(events) == 1
        assert events[0]["completed_by"]["observed_at"] == at(1)

    def test_rearm_requires_a_waiting_interlude(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=2),
                       cond("b", threshold=20, window=2)])
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 21}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert len(history(client, sid)) == 1
        # gap: hits expire (t4, t5), then both breach again at t6
        push(client, sid, 4, {"a": 0, "b": 0}, seq, ("a", "b"))
        push(client, sid, 5, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert current(client, sid) == []
        push(client, sid, 6, {"a": 12, "b": 22}, seq, ("a", "b"))
        push(client, sid, 7, {"a": 0, "b": 0}, seq, ("a", "b"))
        events = history(client, sid)
        assert len(events) == 2  # one event per correlation episode only
        assert [e["completed_by"]["observed_at"] for e in events] == [at(6), at(1)]
        assert len(current(client, sid)) == 1

    def test_value_key_per_condition(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid, [
            cond("a", threshold=10, window=30, value_key="temp"),
            cond("b", threshold=100, window=30, value_key="pressure"),
        ])
        seq: dict = {}
        push(client, sid, 1, {
            "a": {"temp": 11, "unit": "C"}, "b": {"pressure": 0},
        }, seq, ("a", "b"))
        push(client, sid, 2, {"a": {"other": 1}, "b": {"pressure": 101}},
             seq, ("a", "b"))
        push(client, sid, 3, {"a": {}, "b": {}}, seq, ("a", "b"))
        alerts = current(client, sid)
        assert len(alerts) == 1
        hits = {h["source_id"]: h for h in alerts[0]["correlated_entered"]["conditions"]}
        assert hits["a"]["value"] == 11.0 and hits["b"]["value"] == 101.0
        assert hits["a"]["observed_at"] == at(1)  # t2 object lacked 'temp'

    def test_non_numeric_values_never_hit(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=30),
                       cond("b", threshold=20, window=30)])
        seq: dict = {}
        push(client, sid, 1, {"a": "hot", "b": True}, seq, ("a", "b"))
        push(client, sid, 2, {"a": None, "b": 21}, seq, ("a", "b"))
        push(client, sid, 3, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert current(client, sid) == []
        assert history(client, sid) == []


class TestPublishedOnly:
    def test_quarantined_readings_never_correlate(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=60),
                       cond("b", threshold=20, window=60)])
        # establish a published watermark at t10
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(10), "value": 0},
            {"source_id": "b", "seq": 1, "observed_at": at(10), "value": 0},
        ])
        # a late breach pair behind the published watermark is quarantined
        r = report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": at(1), "value": 999},
            {"source_id": "b", "seq": 9, "observed_at": at(1), "value": 999},
        ]).json()
        assert r["quarantined"] == 2
        assert current(client, sid) == []
        assert history(client, sid) == []

    def test_buffered_readings_do_not_correlate_until_published(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=60),
                       cond("b", threshold=20, window=60)])
        # both breach at t1; the watermark equals t1, so they sit on the
        # boundary and stay buffered -- no correlation yet
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(1), "value": 11},
            {"source_id": "b", "seq": 1, "observed_at": at(1), "value": 21},
        ])
        assert current(client, sid) == []
        # a benign t2 batch moves the watermark to t2 and publishes t1
        report(client, sid, [
            {"source_id": "a", "seq": 2, "observed_at": at(2), "value": 0},
            {"source_id": "b", "seq": 2, "observed_at": at(2), "value": 0},
        ])
        alerts = current(client, sid)
        assert len(alerts) == 1
        assert alerts[0]["correlated_entered"]["completed_by"]["observed_at"] == at(1)

    def test_duplicate_reports_do_not_retrigger(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=30),
                       cond("b", threshold=20, window=30)])
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 21}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert len(history(client, sid)) == 1
        r = report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(1), "value": 11},
            {"source_id": "b", "seq": 1, "observed_at": at(1), "value": 21},
        ]).json()
        assert r["duplicates"] == 2
        assert len(history(client, sid)) == 1

    def test_independent_from_per_source_alert_rules(self, client):
        sid = create_session(client)
        # a per-source rule and a correlated rule coexist
        r = client.post(f"/sessions/{sid}/alert-rules", json={
            "source_id": "a", "direction": "above",
            "enter_threshold": 10, "recover_threshold": 5,
            "enter_consecutive": 1, "recover_consecutive": 1,
        })
        assert r.status_code == 201
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=30),
                       cond("b", threshold=20, window=30)])
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 21}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert len(current(client, sid)) == 1
        single = client.get(f"/sessions/{sid}/alerts/current").json()["alerts"]
        assert len(single) == 1 and single[0]["source_id"] == "a"


class TestBackfill:
    def test_rule_created_after_publication_replays_merged_stream(self, client):
        sid = create_session(client)
        seq: dict = {}
        # a and b breach at t1 within a small window; publish via t2, and the
        # trailing t2 boundary readings stay buffered (never published).
        push(client, sid, 1, {"a": 11, "b": 21}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        assert current(client, sid) == []
        created = make_corr_rule(
            client, sid,
            [cond("a", threshold=10, window=30), cond("b", threshold=20, window=30)],
        )
        assert created["state"] == "correlated"
        assert created["correlated_since"]["observed_at"] == at(1)
        alerts = current(client, sid)
        assert len(alerts) == 1
        events = history(client, sid)
        assert len(events) == 1
        hits = {h["source_id"]: h for h in events[0]["conditions"]}
        assert hits["a"]["observed_at"] == at(1) and hits["b"]["observed_at"] == at(1)

    def test_backfill_respects_windows_and_expiry(self, client):
        sid = create_session(client)
        seq: dict = {}
        # a breaches at t1; b only breaches at t8 -> a's 2s window lapsed,
        # then benign readings at t10 leave the rule waiting
        push(client, sid, 1, {"a": 11, "b": 0}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        push(client, sid, 8, {"a": 0, "b": 21}, seq, ("a", "b"))
        push(client, sid, 9, {"a": 0, "b": 0}, seq, ("a", "b"))
        push(client, sid, 10, {"a": 0, "b": 0}, seq, ("a", "b"))
        created = make_corr_rule(
            client, sid,
            [cond("a", threshold=10, window=2), cond("b", threshold=20, window=2)],
        )
        assert created["state"] == "waiting"
        assert history(client, sid) == []
        # the stale hits are still visible on the conditions
        by_source = {c["source_id"]: c for c in created["conditions"]}
        assert by_source["a"]["last_hit"]["observed_at"] == at(1)
        assert by_source["b"]["last_hit"]["observed_at"] == at(8)

    def test_backfill_ignores_quarantined_readings(self, client):
        sid = create_session(client)
        report(client, sid, [
            {"source_id": "a", "seq": 1, "observed_at": at(10), "value": 0},
            {"source_id": "b", "seq": 1, "observed_at": at(10), "value": 0},
        ])
        report(client, sid, [
            {"source_id": "a", "seq": 9, "observed_at": at(1), "value": 999},
            {"source_id": "b", "seq": 9, "observed_at": at(1), "value": 999},
        ])
        created = make_corr_rule(
            client, sid,
            [cond("a", threshold=10, window=60), cond("b", threshold=20, window=60)],
        )
        assert created["state"] == "waiting"
        assert history(client, sid) == []
        assert all(c["last_hit"] is None for c in created["conditions"])

    def test_backfill_checkpoint_then_live_continuation(self, client):
        sid = create_session(client)
        seq: dict = {}
        # publish a breach on a only (t1, published by the benign t2 pair)
        push(client, sid, 1, {"a": 11, "b": 0}, seq, ("a", "b"))
        push(client, sid, 2, {"a": 0, "b": 0}, seq, ("a", "b"))
        created = make_corr_rule(
            client, sid,
            [cond("a", threshold=10, window=60), cond("b", threshold=20, window=60)],
        )
        assert created["state"] == "waiting"
        # live continuation: b breaches at t3 (published at t4); the replay
        # checkpoint must not be revisited and the fresh hit completes once
        push(client, sid, 3, {"a": 0, "b": 21}, seq, ("a", "b"))
        push(client, sid, 4, {"a": 0, "b": 0}, seq, ("a", "b"))
        events = history(client, sid)
        assert len(events) == 1
        assert events[0]["completed_by"]["observed_at"] == at(3)
        assert len(current(client, sid)) == 1


class TestHistoryPagination:
    def test_keyset_pagination_newest_first(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid,
                      [cond("a", threshold=10, window=2),
                       cond("b", threshold=20, window=2)])
        seq: dict = {}
        # three short correlation episodes separated by gaps: t1, t6, t11
        for base in (1, 6, 11):
            push(client, sid, base, {"a": 11, "b": 21}, seq, ("a", "b"))
            push(client, sid, base + 1, {"a": 0, "b": 0}, seq, ("a", "b"))
            push(client, sid, base + 3, {"a": 0, "b": 0}, seq, ("a", "b"))
        all_events = history(client, sid, limit=2)
        assert len(all_events) == 3
        ids = [e["id"] for e in all_events]
        assert ids == sorted(ids, reverse=True)
        assert len(set(ids)) == 3
        times = [e["completed_by"]["observed_at"] for e in all_events]
        assert times == [at(11), at(6), at(1)]

    def test_filter_by_rule(self, client):
        sid = create_session(client, sources=("a", "b", "c"))
        make_corr_rule(client, sid, [
            cond("a", threshold=10, window=30), cond("b", threshold=20, window=30),
        ], rule_id="r1")
        make_corr_rule(client, sid, [
            cond("a", threshold=10, window=30), cond("c", threshold=30, window=30),
        ], rule_id="r2")
        seq: dict = {}
        push(client, sid, 1, {"a": 11, "b": 21, "c": 31}, seq, ("a", "b", "c"))
        push(client, sid, 2, {"a": 0, "b": 0, "c": 0}, seq, ("a", "b", "c"))
        assert len(history(client, sid)) == 2
        r1_events = history(client, sid, rule_id="r1")
        assert len(r1_events) == 1 and r1_events[0]["rule_id"] == "r1"

    def test_bad_cursor_400(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/correlated-alerts/history",
                       params={"cursor": "%%%garbage"})
        assert r.status_code == 400

    def test_unknown_rule_filter_404(self, client):
        sid = create_session(client)
        r = client.get(f"/sessions/{sid}/correlated-alerts/history",
                       params={"rule_id": "ghost"})
        assert r.status_code == 404

    def test_history_unknown_session_404(self, client):
        assert client.get("/sessions/nope/correlated-alerts/history").status_code == 404
        assert client.get("/sessions/nope/correlated-alerts/current").status_code == 404
        assert client.get("/sessions/nope/correlated-rules").status_code == 404


class TestMigrations:
    def test_legacy_database_is_upgraded_in_place(self, tmp_path, monkeypatch):
        import sqlite3

        from app import database
        from app.time_utils import store_event_time
        from app.database import utc_now

        db_path = str(tmp_path / "legacy.db")
        # Build a database with only the pre-migration (v0) schema and some
        # data in it, as an older release would have left behind.
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

        # Startup upgrade: migrations apply once and create the new tables
        # without touching existing data (migration 2 also adds the
        # source-sealing / session-closing columns).
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            versions = [r[0] for r in conn.execute(
                "SELECT version FROM schema_migrations ORDER BY version")]
            assert versions == [1, 2, 3, 4, 5]
            row = conn.execute(
                "SELECT session_id FROM sessions WHERE session_id = 'legacy-s'"
            ).fetchone()
            assert row is not None
            names = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
            }
            assert {"correlated_rules", "correlated_conditions",
                    "correlated_events"} <= names
            cols = {r[1] for r in conn.execute("PRAGMA table_info(sessions)")}
            assert {"status", "closed_at"} <= cols
            cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(session_sources)")}
            assert {"final_observed_at", "sealed_at"} <= cols
            # Legacy sessions upgrade open, with every source unsealed.
            row = conn.execute(
                "SELECT status, closed_at FROM sessions WHERE session_id = 'legacy-s'"
            ).fetchone()
            assert row[0] == "open" and row[1] is None
        finally:
            conn.close()

        # Re-running init is a no-op (no duplicate application).
        database.init_db(db_path)
        conn = sqlite3.connect(db_path)
        try:
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 1"
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 2"
            ).fetchone()[0] == 1
        finally:
            conn.close()

        # And the upgraded database serves live traffic end to end.
        monkeypatch.setenv("SENSOR_DB_PATH", db_path)
        from app import main

        main.app.state.db_path = db_path
        with TestClient(main.app) as c:
            r = c.post("/sessions/legacy-s/correlated-rules", json={
                "conditions": [
                    {"source_id": "a", "direction": "above",
                     "threshold": 10, "window_seconds": 30},
                    {"source_id": "b", "direction": "above",
                     "threshold": 20, "window_seconds": 30},
                ]
            })
            assert r.status_code == 201, r.text
