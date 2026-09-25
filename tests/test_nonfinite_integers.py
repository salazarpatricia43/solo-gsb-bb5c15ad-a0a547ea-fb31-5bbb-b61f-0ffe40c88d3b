"""Boundary tests for JSON integers that cannot become finite doubles.

The reporting API accepts arbitrary JSON values, and JSON integers have
arbitrary precision in Python, so a value like ``10**400`` is stored and
published normally -- but ``float(10**400)`` raises ``OverflowError``.  The
alert evaluation must treat such an integer as an *invalid alert value*
(streak reset / no hit) instead of aborting the publish transaction and
rolling the whole batch back, both for live advancement and for the history
replay performed when a rule is created after the fact.
"""
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
HUGE = 10**400  # parses as a Python int; float(HUGE) raises OverflowError


def at(second: int) -> str:
    return (BASE + timedelta(seconds=second)).strftime("%Y-%m-%dT%H:%M:%SZ")


def report(client, sid, rows):
    return client.post(f"/sessions/{sid}/readings", json={"readings": rows})


def row(source, seq, second, value):
    return {"source_id": source, "seq": seq, "observed_at": at(second), "value": value}


def create_session(client, sources=("a", "b"), lateness=0.0, session_id=None):
    body = {"source_ids": list(sources), "allowed_lateness_seconds": lateness}
    if session_id is not None:
        body["session_id"] = session_id
    r = client.post("/sessions", json=body)
    assert r.status_code == 201, r.text
    return r.json()["session_id"]


def make_alert_rule(client, sid, source_id="a", **overrides):
    body = {
        "source_id": source_id,
        "direction": "above",
        "enter_threshold": 10.0,
        "recover_threshold": 5.0,
        "enter_consecutive": 1,
        "recover_consecutive": 1,
    }
    body.update(overrides)
    r = client.post(f"/sessions/{sid}/alert-rules", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def current_alerts(client, sid):
    return client.get(f"/sessions/{sid}/alerts/current").json()["alerts"]


def alert_history(client, sid):
    out = []
    cursor = None
    for _ in range(100):
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        page = client.get(f"/sessions/{sid}/alerts/history", params=params).json()
        out.extend(page["events"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


def make_corr_rule(client, sid, conditions, rule_id=None):
    body = {"conditions": conditions}
    if rule_id is not None:
        body["rule_id"] = rule_id
    r = client.post(f"/sessions/{sid}/correlated-rules", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def cond(source, direction="above", threshold=10.0, window=5.0, value_key=None):
    c = {"source_id": source, "direction": direction,
         "threshold": threshold, "window_seconds": window}
    if value_key is not None:
        c["value_key"] = value_key
    return c


def corr_history(client, sid):
    out = []
    cursor = None
    for _ in range(100):
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        page = client.get(
            f"/sessions/{sid}/correlated-alerts/history", params=params
        ).json()
        out.extend(page["events"])
        cursor = page["next_cursor"]
        if not cursor:
            return out


def publish_pairs(client, sid, a_values, *, sentinel=None):
    """Publish paired (a, b=0) readings at consecutive event-time seconds.

    With zero allowed lateness a reading at second i publishes once the
    watermark passes it, so a final paired sentinel at ``last+1`` flushes the
    last target second; the sentinel's a value defaults to null (invalid for
    every machine) and b's 0 never breaches an ``above 10`` condition.
    Returns ``(next_a_seq, next_b_seq, sentinel_second)`` so callers can keep
    reporting afterwards without colliding on keys.
    """
    seq = {"a": 0, "b": 0}
    seconds = sorted(a_values)
    flush_second = seconds[-1] + 1
    for second in [*seconds, flush_second]:
        seq["a"] += 1
        seq["b"] += 1
        value = a_values[second] if second in a_values else sentinel
        r = report(client, sid, [
            row("a", seq["a"], second, value),
            row("b", seq["b"], second, 0),
        ])
        assert r.status_code == 200, r.text
    return seq["a"] + 1, seq["b"] + 1, flush_second


# ---------------------------------------------------------------------------
# storage / publishing never reject the value
# ---------------------------------------------------------------------------

class TestHugeIntegerStorage:
    def test_publishes_normally_without_any_rule(self, client):
        sid = create_session(client)
        r = report(client, sid, [row("a", 1, 1, HUGE), row("b", 1, 2, 0)])
        assert r.status_code == 200, r.text
        # The second batch pushes the watermark past both readings.
        r = report(client, sid, [row("a", 2, 3, 0), row("b", 2, 3, 0)])
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["published_now"] >= 2

        stream = client.get(f"/sessions/{sid}/stream", params={"limit": 100}).json()
        values = [x["value"] for x in stream["readings"]]
        # The integer round-trips verbatim through storage and the API.
        assert HUGE in values

    def test_same_key_retry_uses_original_payload_dedup(self, client):
        sid = create_session(client)
        report(client, sid, [row("a", 1, 1, HUGE), row("b", 1, 2, 0)])
        r = report(client, sid, [row("a", 1, 1, HUGE)])
        assert r.status_code == 200, r.text
        assert r.json()["duplicates"] == 1

        # Same key, different payload still conflicts and rolls the batch back.
        r = report(client, sid, [
            row("a", 1, 1, HUGE + 1), row("b", 2, 3, 0),
        ])
        assert r.status_code == 409
        follow = report(client, sid, [row("b", 2, 3, 0)])
        # The conflicting batch was atomic: b seq=2 was not written.
        assert follow.json()["duplicates"] == 0

    def test_tail_publish_on_seal_does_not_raise(self, client):
        # The original failure mode: the huge integer sits buffered while no
        # watermark move publishes it, then sealing the last source tail-flushes
        # every buffered reading inside one transaction -- the alert evaluation
        # raised OverflowError and rolled the seal back.
        sid = create_session(client)
        make_alert_rule(client, sid)
        make_corr_rule(client, sid, [cond("a"), cond("b")])

        assert report(client, sid, [
            row("a", 1, 1, HUGE), row("b", 1, 1, 0),
        ]).status_code == 200

        r = client.post(f"/sessions/{sid}/sources/a/seal",
                        json={"final_observed_at": at(1)})
        assert r.status_code == 200, r.text
        r = client.post(f"/sessions/{sid}/sources/b/seal",
                        json={"final_observed_at": at(1)})
        assert r.status_code == 200, r.text
        assert r.json()["closed"] is True


# ---------------------------------------------------------------------------
# per-source state machine
# ---------------------------------------------------------------------------

class TestSingleSourceHugeInteger:
    def test_breaks_normal_streak_does_not_enter(self, client):
        sid = create_session(client)
        make_alert_rule(client, sid, enter_consecutive=2)
        # 11, <huge>, 11: the two breaches are never *consecutive*.
        publish_pairs(client, sid, {1: 11, 2: HUGE, 3: 11})
        assert current_alerts(client, sid) == []
        assert alert_history(client, sid) == []

    def test_breaks_recovery_streak_does_not_recover(self, client):
        sid = create_session(client)
        make_alert_rule(client, sid, recover_consecutive=2)
        publish_pairs(client, sid, {
            1: 11,       # enter (active)
            2: HUGE,     # invalid: recovery streak reset, still active
            3: 1,        # recovery streak 1
            4: HUGE,     # reset again, still active
            5: 1,        # recovery streak 1 (not 2)
        })
        alerts = current_alerts(client, sid)
        assert len(alerts) == 1
        assert [e["event_type"] for e in alert_history(client, sid)] == ["entered"]

        # Two genuinely consecutive recovery readings finally clear it.
        r = report(client, sid, [row("a", 7, 7, 1), row("b", 7, 7, 0)])
        assert r.status_code == 200, r.text
        r = report(client, sid, [row("a", 8, 8, 1), row("b", 8, 8, 0)])
        assert r.status_code == 200, r.text
        r = report(client, sid, [row("a", 9, 9, None), row("b", 9, 9, 0)])
        assert r.status_code == 200, r.text
        assert current_alerts(client, sid) == []
        assert [e["event_type"] for e in alert_history(client, sid)] == [
            "recovered", "entered",
        ]

    def test_negative_huge_integer_does_not_enter_below_rule(self, client):
        sid = create_session(client)
        make_alert_rule(
            client, sid,
            direction="below", enter_threshold=-100.0, recover_threshold=-50.0,
            enter_consecutive=1,
        )
        # Mathematically far below -100, but not representable as a finite
        # double: an invalid value, not a breach.
        publish_pairs(client, sid, {1: -(10**400)})
        assert current_alerts(client, sid) == []
        assert alert_history(client, sid) == []

    def test_huge_integer_inside_value_key_object_is_invalid(self, client):
        sid = create_session(client)
        make_alert_rule(client, sid, value_key="temp", enter_consecutive=1)
        publish_pairs(client, sid, {1: {"temp": HUGE}})
        assert current_alerts(client, sid) == []
        assert alert_history(client, sid) == []

    def test_replay_when_rule_created_afterwards_matches_live_result(self, client):
        sid = create_session(client)
        # No rule exists while the values are published.
        publish_pairs(client, sid, {1: 11, 2: HUGE, 3: 11})
        # enter_consecutive=2: replay must skip the huge value and stay normal.
        rule = make_alert_rule(client, sid, enter_consecutive=2)
        assert rule["state"] == "normal"
        # The t3 breach starts a fresh streak after the huge value reset it;
        # one more finite breach would be needed to enter.
        assert rule["streak"] == 1
        assert alert_history(client, sid) == []

    def test_replay_enters_on_finite_value_preceding_huge_one(self, client):
        sid = create_session(client)
        publish_pairs(client, sid, {1: 11, 2: HUGE})
        rule = make_alert_rule(client, sid, enter_consecutive=1)
        assert rule["state"] == "active"
        events = alert_history(client, sid)
        assert [e["event_type"] for e in events] == ["entered"]
        assert events[0]["value"] == 11.0


# ---------------------------------------------------------------------------
# cross-source correlated rules
# ---------------------------------------------------------------------------

class TestCorrelatedHugeInteger:
    def test_huge_value_is_not_recorded_as_hit(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid, [
            cond("a", window=3.0), cond("b", window=100.0),
        ])
        # a=11 at t1 (hit); t2: b=11 while a's hit is fresh -> correlated.
        assert report(client, sid, [
            row("a", 1, 1, 11), row("b", 1, 1, 0),
        ]).status_code == 200
        assert report(client, sid, [
            row("a", 2, 2, 0), row("b", 2, 2, 11),
        ]).status_code == 200
        assert report(client, sid, [
            row("a", 3, 3, 0), row("b", 3, 3, 0),
        ]).status_code == 200
        events = corr_history(client, sid)
        assert len(events) == 1
        assert events[0]["completed_by"]["seq"] == 2  # b@2

        # a's hit ages out; at t7 a reports the huge integer and b hits again.
        # The huge value must not refresh a's hit, so no new correlation fires.
        assert report(client, sid, [
            row("a", 4, 6, 0), row("b", 4, 6, 11),
        ]).status_code == 200
        assert report(client, sid, [
            row("a", 5, 7, HUGE), row("b", 5, 7, 11),
        ]).status_code == 200
        assert report(client, sid, [
            row("a", 6, 8, 0), row("b", 6, 8, 0),
        ]).status_code == 200

        rule = client.get(f"/sessions/{sid}/correlated-rules").json()["rules"][0]
        a_condition = rule["conditions"][0]
        # a's last hit is still the finite t1 reading, never the t7 huge one.
        assert a_condition["last_hit"]["observed_at"] == at(1)
        assert a_condition["last_hit"]["value"] == 11.0
        assert rule["state"] == "waiting"
        assert len(corr_history(client, sid)) == 1

        # A fresh finite a hit can complete the correlation again normally.
        assert report(client, sid, [
            row("a", 7, 9, 11), row("b", 7, 9, 11),
        ]).status_code == 200
        assert report(client, sid, [
            row("a", 8, 10, 0), row("b", 8, 10, 0),
        ]).status_code == 200
        events = corr_history(client, sid)
        assert len(events) == 2
        assert events[0]["completed_by"] == {
            "source_id": "a", "seq": 7, "observed_at": at(9),
        }

    def test_existing_hit_still_ages_on_its_own_event_time_window(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid, [
            cond("a", window=3.0), cond("b", window=100.0),
        ])
        # a hits once at t1; the huge value at t2 does not refresh it.
        # b then hits at t10 with a far-younger window: a is long expired,
        # so processing the huge reading strictly by event time must leave the
        # rule waiting (it neither preserves nor refreshes a's hit).
        report(client, sid, [row("a", 1, 1, 11), row("b", 1, 1, 0)])
        report(client, sid, [row("a", 2, 2, HUGE), row("b", 2, 2, 0)])
        report(client, sid, [row("a", 3, 10, 0), row("b", 3, 10, 11)])
        report(client, sid, [row("a", 4, 11, 0), row("b", 4, 11, 0)])
        assert corr_history(client, sid) == []
        rule = client.get(f"/sessions/{sid}/correlated-rules").json()["rules"][0]
        assert rule["state"] == "waiting"

    def test_huge_value_in_value_key_object_is_not_a_hit(self, client):
        sid = create_session(client)
        make_corr_rule(client, sid, [
            cond("a", window=100.0, value_key="temp"),
            cond("b", window=100.0),
        ])
        publish_pairs(client, sid, {1: {"temp": HUGE}, 2: {"temp": 0}})
        rule = client.get(f"/sessions/{sid}/correlated-rules").json()["rules"][0]
        assert rule["conditions"][0]["last_hit"] is None
        assert corr_history(client, sid) == []

    def test_replay_when_rule_created_afterwards_skips_huge_value(self, client):
        sid = create_session(client)
        # b hits at t1, a only ever reports the huge integer at t2.
        report(client, sid, [row("a", 1, 1, 0), row("b", 1, 1, 11)])
        report(client, sid, [row("a", 2, 2, HUGE), row("b", 2, 2, 11)])
        report(client, sid, [row("a", 3, 3, 0), row("b", 3, 3, 0)])
        rule = make_corr_rule(client, sid, [
            cond("a", window=100.0), cond("b", window=100.0),
        ])
        assert rule["state"] == "waiting"
        assert rule["conditions"][0]["last_hit"] is None
        assert corr_history(client, sid) == []

    def test_replay_finds_correlation_around_huge_value(self, client):
        sid = create_session(client)
        # Finite hits on both sources at t1; a's t2 huge reading is a no-op
        # hit-wise and must not stop the replay from seeing the t1 correlation.
        report(client, sid, [row("a", 1, 1, 11), row("b", 1, 1, 11)])
        report(client, sid, [row("a", 2, 2, HUGE), row("b", 2, 2, 0)])
        report(client, sid, [row("a", 3, 3, 0), row("b", 3, 3, 0)])
        rule = make_corr_rule(client, sid, [
            cond("a", window=100.0), cond("b", window=100.0),
        ])
        assert rule["state"] == "correlated"
        events = corr_history(client, sid)
        assert len(events) == 1
        snapshot = {c["source_id"]: c for c in events[0]["conditions"]}
        assert snapshot["a"]["observed_at"] == at(1)
        assert snapshot["a"]["value"] == 11.0


# ---------------------------------------------------------------------------
# other read-only analytics share the same extraction helper
# ---------------------------------------------------------------------------

class TestAnalyticsHugeInteger:
    def test_exposure_treats_huge_integer_as_unknown(self, client):
        sid = create_session(client, sources=("a", "b"))
        # Published a readings: HUGE@t1 (unknown), then 10@t2 and 10@t3.
        publish_pairs(client, sid, {1: HUGE, 2: 10.0, 3: 10.0})
        r = client.get(f"/sessions/{sid}/exposure", params={
            "source_id": "a", "from": at(1), "to": at(3), "threshold": "10",
        })
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["above_threshold_seconds"] == 0.0
        assert body["covered_seconds"] == 1.0
        assert body["unknown_seconds"] == 1.0
        assert body["ratio_above"] == 0.0

    def test_local_spikes_skips_huge_integer(self, client):
        sid = create_session(client, sources=("a", "b"))
        publish_pairs(client, sid, {1: HUGE, 2: 10.0, 3: 10.0, 4: 10.0})
        r = client.get(f"/sessions/{sid}/local-spikes", params={
            "source_id": "a", "from": at(0), "to": at(10),
            "window_seconds": "100", "sensitivity": "3",
        })
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["readings_count"] == 4
        assert body["valid_count"] == 3
        assert body["spikes"] == []
