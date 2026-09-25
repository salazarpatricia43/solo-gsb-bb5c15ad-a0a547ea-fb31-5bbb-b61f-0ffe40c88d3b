"""End-to-end tests for the read-only per-source gap endpoint."""
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
    return client.post(
        f"/sessions/{sid}/readings", json={"readings": readings}
    )


def seal(client, sid, source, final_observed_at):
    return client.post(
        f"/sessions/{sid}/sources/{source}/seal",
        json={"final_observed_at": final_observed_at},
    )


def gaps(client, sid, **params):
    return client.get(f"/sessions/{sid}/source-gaps", params=params)


def gap_keys(body):
    return [
        (
            g["source_id"],
            (g["start_key"]["observed_at"], g["start_key"]["seq"]),
            (g["end_key"]["observed_at"], g["end_key"]["seq"]),
            g["gap_seconds"],
        )
        for g in body["gaps"]
    ]


def setup_timeline(client, sid="s"):
    """Publish, per source (lateness 10s):

    a: 10:00:00(#1), 10:00:10(#2), 10:00:40(#3), 10:00:50(#4)
       -> adjacent gaps 10, **30**, 10
    b: 10:00:00(#1), 10:00:05(#2), 10:00:25(#3)
       -> adjacent gaps 5, **20**

    The far-later clock reports a#99/b#99 @10:02:00 drive the watermark to
    10:01:50, so all seven target readings are published while the two
    clock reports themselves remain buffered (never part of the gaps).
    """
    create_session(client, session_id=sid)
    r = report(client, sid, [
        {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
        {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:10Z", "value": 1},
        {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T10:00:40Z", "value": 1},
        {"source_id": "a", "seq": 4, "observed_at": "2026-01-01T10:00:50Z", "value": 1},
        {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
        {"source_id": "b", "seq": 2, "observed_at": "2026-01-01T10:00:05Z", "value": 1},
        {"source_id": "b", "seq": 3, "observed_at": "2026-01-01T10:00:25Z", "value": 1},
        {"source_id": "a", "seq": 99, "observed_at": "2026-01-01T10:02:00Z", "value": 1},
        {"source_id": "b", "seq": 99, "observed_at": "2026-01-01T10:02:00Z", "value": 1},
    ])
    assert r.json()["published_total"] == 7, r.text
    return sid


class TestSourceGapDetection:
    def test_finds_gaps_strictly_over_threshold(self, client):
        c, _ = client
        sid = setup_timeline(c)
        body = gaps(c, sid, max_gap_seconds=15).json()
        assert body["matched"] == 2
        # Global order by start observed_at: b's gap starts at :05, a's at :10.
        assert gap_keys(body) == [
            ("b", ("2026-01-01T10:00:05Z", 2), ("2026-01-01T10:00:25Z", 3), 20.0),
            ("a", ("2026-01-01T10:00:10Z", 2), ("2026-01-01T10:00:40Z", 3), 30.0),
        ]
        for g in body["gaps"]:
            assert g["start_key"]["source_id"] == g["source_id"]
            assert g["end_key"]["source_id"] == g["source_id"]

    def test_threshold_is_strict_greater_than(self, client):
        c, _ = client
        sid = setup_timeline(c)
        # gap == 30 is NOT reported when the threshold is exactly 30.
        body = gaps(c, sid, max_gap_seconds=30).json()
        assert [g["source_id"] for g in body["gaps"]] == []
        assert body["matched"] == 0
        # 29.999999 still strictly below the 30s gap, so a's gap appears;
        # b's 20s gap does not exceed such a threshold.
        body = gaps(c, sid, max_gap_seconds="29.999999").json()
        assert [g["source_id"] for g in body["gaps"]] == ["a"]
        # gap == 20 likewise excluded at threshold 20 (b drops out).
        body = gaps(c, sid, max_gap_seconds=20).json()
        assert [g["source_id"] for g in body["gaps"]] == ["a"]

    def test_equal_event_times_are_zero_gap_not_gap(self, client):
        c, _ = client
        sid = create_session(c)
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "a", "seq": 3, "observed_at": "2026-01-01T10:00:00.500000Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            # Far-later clock pair: publishes everything up to :00:50 while
            # these clock reports themselves stay buffered.
            {"source_id": "a", "seq": 99, "observed_at": "2026-01-01T10:01:00Z", "value": 1},
            {"source_id": "b", "seq": 99, "observed_at": "2026-01-01T10:01:00Z", "value": 1},
        ])
        body = gaps(c, sid, max_gap_seconds=0).json()
        # Only the strictly-positive 0.5s gap qualifies; the two equal-time
        # readings (adjacent via seq tie-break) have gap 0 and are excluded.
        assert gap_keys(body) == [
            ("a", ("2026-01-01T10:00:00Z", 2),
             ("2026-01-01T10:00:00.5Z", 3), 0.5),
        ]

    def test_microsecond_precision_is_exact(self, client):
        c, _ = client
        sid = create_session(c)
        report(c, sid, [
            {"source_id": "a", "seq": 1,
             "observed_at": "2026-01-01T10:00:00.000000Z", "value": 1},
            {"source_id": "a", "seq": 2,
             "observed_at": "2026-01-01T10:00:00.000001Z", "value": 1},
            {"source_id": "b", "seq": 1,
             "observed_at": "2026-01-01T10:00:00.000000Z", "value": 1},
            {"source_id": "a", "seq": 99,
             "observed_at": "2026-01-01T10:01:00Z", "value": 1},
            {"source_id": "b", "seq": 99,
             "observed_at": "2026-01-01T10:01:00Z", "value": 1},
        ])
        # A 1-microsecond gap must be detected, not rounded away.
        body = gaps(c, sid, max_gap_seconds=0).json()
        assert body["gaps"][0]["gap_seconds"] == pytest.approx(0.000001)
        assert body["matched"] == 1
        # And must be excluded exactly at its own width.
        body = gaps(c, sid, max_gap_seconds=0.000001).json()
        assert body["gaps"] == []

    def test_sub_microsecond_threshold_is_not_rounded_up(self, client):
        c, _ = client
        sid = create_session(c)
        report(c, sid, [
            {"source_id": "a", "seq": 1,
             "observed_at": "2026-01-01T10:00:00.000000Z", "value": 1},
            {"source_id": "a", "seq": 2,
             "observed_at": "2026-01-01T10:00:00.000001Z", "value": 1},
            {"source_id": "a", "seq": 3,
             "observed_at": "2026-01-01T10:00:00.000003Z", "value": 1},
            {"source_id": "b", "seq": 1,
             "observed_at": "2026-01-01T10:00:00.000000Z", "value": 1},
            {"source_id": "a", "seq": 99,
             "observed_at": "2026-01-01T10:01:00Z", "value": 1},
            {"source_id": "b", "seq": 99,
             "observed_at": "2026-01-01T10:01:00Z", "value": 1},
        ])
        # Threshold 1.5us: the 2us gap (:01 -> :03) strictly exceeds it and
        # must be reported; the 1us gap (:00 -> :01) does not.  Rounding the
        # threshold to 2us would wrongly miss the 2us gap.
        body = gaps(c, sid, max_gap_seconds="0.0000015").json()
        assert body["matched"] == 1
        assert gap_keys(body) == [
            ("a", ("2026-01-01T10:00:00.000001Z", 2),
             ("2026-01-01T10:00:00.000003Z", 3), 0.000002),
        ]
        # Exactly at the 2us width the gap is excluded (strictly greater).
        body = gaps(c, sid, max_gap_seconds="0.000002").json()
        assert body["gaps"] == []
        assert body["matched"] == 0

    def test_source_filter(self, client):
        c, _ = client
        sid = setup_timeline(c)
        body = gaps(c, sid, max_gap_seconds=15, source_id="a").json()
        assert body["source_id"] == "a"
        assert gap_keys(body) == [
            ("a", ("2026-01-01T10:00:10Z", 2), ("2026-01-01T10:00:40Z", 3), 30.0),
        ]
        body = gaps(c, sid, max_gap_seconds=15, source_id="b").json()
        assert gap_keys(body) == [
            ("b", ("2026-01-01T10:00:05Z", 2), ("2026-01-01T10:00:25Z", 3), 20.0),
        ]
        # A source with no qualifying gaps.
        body = gaps(c, sid, max_gap_seconds=1000, source_id="b").json()
        assert body["gaps"] == []
        assert body["matched"] == 0

    def test_inclusive_from_to_window(self, client):
        c, _ = client
        sid = setup_timeline(c)
        # Window [10:00:10, 10:00:40] on a: readings #2@:10, #3@:40 are both
        # endpoints (inclusive) -> their 30s gap is wholly inside.
        body = gaps(
            c, sid, max_gap_seconds=15, source_id="a",
            **{"from": "2026-01-01T10:00:10Z", "to": "2026-01-01T10:00:40Z"},
        ).json()
        assert body["from"] == "2026-01-01T10:00:10Z"
        assert body["to"] == "2026-01-01T10:00:40Z"
        assert gap_keys(body) == [
            ("a", ("2026-01-01T10:00:10Z", 2), ("2026-01-01T10:00:40Z", 3), 30.0),
        ]

    def test_gap_straddling_boundary_is_not_reported(self, client):
        c, _ = client
        sid = setup_timeline(c)
        # from=:15 excludes a#2@:10; the first in-window reading is a#3@:40,
        # so the :10->:40 gap straddles the boundary and must not appear.
        body = gaps(
            c, sid, max_gap_seconds=15, source_id="a",
            **{"from": "2026-01-01T10:00:15Z"},
        ).json()
        assert body["gaps"] == []
        assert body["matched"] == 0
        # to=:35 excludes a#3@:40; last in-window reading is a#2@:10 -> same.
        body = gaps(
            c, sid, max_gap_seconds=15, source_id="a",
            **{"to": "2026-01-01T10:00:35Z"},
        ).json()
        assert body["gaps"] == []
        assert body["matched"] == 0

    def test_time_bounds_normalise_offsets(self, client):
        c, _ = client
        sid = setup_timeline(c)
        # 10:00:10+00:00 == 10:00:10Z, written with a +02:00 offset for :12.
        body = gaps(
            c, sid, max_gap_seconds=15, source_id="a",
            **{"from": "2026-01-01T12:00:10+02:00", "to": "2026-01-01T10:00:40Z"},
        ).json()
        assert body["from"] == "2026-01-01T10:00:10Z"
        assert len(body["gaps"]) == 1

    def test_only_published_readings_participate(self, client):
        c, _ = client
        sid = create_session(c)
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            # Buffered (watermark not defined yet): must never create a gap.
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-02T00:00:00Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
        ])
        body = gaps(c, sid, max_gap_seconds=0).json()
        assert body["gaps"] == []
        assert body["matched"] == 0

    def test_quarantined_readings_do_not_create_or_split_gaps(self, client):
        c, _ = client
        sid = setup_timeline(c)
        # A late reading inside a's 30s gap is quarantined; the published gap
        # a#2@:10 -> a#3@:40 must survive unchanged.
        r = report(c, sid, [
            {"source_id": "a", "seq": 50, "observed_at": "2026-01-01T10:00:12Z", "value": 9},
        ])
        assert r.json()["quarantined"] == 1
        body = gaps(c, sid, max_gap_seconds=15, source_id="a").json()
        assert gap_keys(body) == [
            ("a", ("2026-01-01T10:00:10Z", 2), ("2026-01-01T10:00:40Z", 3), 30.0),
        ]

    def test_works_on_closed_session(self, client):
        c, _ = client
        sid = create_session(c)
        report(c, sid, [
            {"source_id": "a", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
            {"source_id": "a", "seq": 2, "observed_at": "2026-01-01T10:00:30Z", "value": 1},
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1},
        ])
        seal(c, sid, "a", "2026-01-01T10:00:30Z")
        seal(c, sid, "b", "2026-01-01T10:00:00Z")
        state = c.get(f"/sessions/{sid}").json()
        assert state["closed"] is True
        body = gaps(c, sid, max_gap_seconds=10, source_id="a").json()
        assert gap_keys(body) == [
            ("a", ("2026-01-01T10:00:00Z", 1), ("2026-01-01T10:00:30Z", 2), 30.0),
        ]

    def test_empty_result(self, client):
        c, _ = client
        sid = create_session(c)
        body = gaps(c, sid, max_gap_seconds=10).json()
        assert body["gaps"] == []
        assert body["matched"] == 0
        assert body["next_cursor"] is None


class TestSourceGapPagination:
    def _many_gaps_session(self, c, sid="m"):
        """Source a: six readings 100s apart -> five gaps; source b: one.

        Both sources are sealed so the final seal tail-publishes every
        buffered reading (the session ends closed with all seven published).
        """
        create_session(c, sources=("a", "b"), session_id=sid)
        rows = [
            {"source_id": "a", "seq": i, "observed_at": t, "value": 1}
            for i, t in enumerate(
                ["2026-01-01T10:00:00Z", "2026-01-01T10:01:40Z",
                 "2026-01-01T10:03:20Z", "2026-01-01T10:05:00Z",
                 "2026-01-01T10:06:40Z", "2026-01-01T10:08:20Z"],
                start=1,
            )
        ]
        rows.append(
            {"source_id": "b", "seq": 1, "observed_at": "2026-01-01T10:00:00Z", "value": 1}
        )
        r = report(c, sid, rows)
        assert r.status_code == 200, r.text
        seal(c, sid, "a", "2026-01-01T10:08:20Z")
        seal(c, sid, "b", "2026-01-01T10:00:00Z")
        r = c.get(f"/sessions/{sid}").json()
        assert r["closed"] is True
        assert r["published_watermark"] is not None
        return sid

    def test_limit_and_next_cursor(self, client):
        c, _ = client
        sid = self._many_gaps_session(c)
        page1 = gaps(c, sid, max_gap_seconds=10, limit=2).json()
        assert page1["matched"] == 5
        assert len(page1["gaps"]) == 2
        assert page1["next_cursor"]
        starts = [g["start_key"]["seq"] for g in page1["gaps"]]
        assert starts == [1, 2]

        page2 = gaps(
            c, sid, max_gap_seconds=10, limit=2, cursor=page1["next_cursor"]
        ).json()
        assert [g["start_key"]["seq"] for g in page2["gaps"]] == [3, 4]
        assert page2["matched"] == 5
        assert page2["next_cursor"]

        page3 = gaps(
            c, sid, max_gap_seconds=10, limit=2, cursor=page2["next_cursor"]
        ).json()
        assert [g["start_key"]["seq"] for g in page3["gaps"]] == [5]
        assert page3["next_cursor"] is None

    def test_pagination_covers_every_gap_once(self, client):
        c, _ = client
        sid = self._many_gaps_session(c)
        seen = []
        cursor = None
        for _ in range(10):
            params = {"max_gap_seconds": 10, "limit": 2}
            if cursor:
                params["cursor"] = cursor
            body = gaps(c, sid, **params).json()
            seen.extend((g["source_id"], g["start_key"]["seq"]) for g in body["gaps"])
            cursor = body["next_cursor"]
            if cursor is None:
                break
        assert seen == [("a", 1), ("a", 2), ("a", 3), ("a", 4), ("a", 5)]

    def test_cursor_remains_stable_with_filter(self, client):
        c, _ = client
        sid = self._many_gaps_session(c)
        # A cursor paging source a must not suddenly include other sources
        # (there is only a here, but check filter+cursor interplay explicitly).
        page1 = gaps(
            c, sid, max_gap_seconds=10, source_id="a", limit=3
        ).json()
        page2 = gaps(
            c, sid, max_gap_seconds=10, source_id="a", limit=3,
            cursor=page1["next_cursor"],
        ).json()
        assert {g["source_id"] for g in page1["gaps"] + page2["gaps"]} == {"a"}
        assert [g["start_key"]["seq"] for g in page2["gaps"]] == [4, 5]

    def test_malformed_cursor_is_400(self, client):
        c, _ = client
        sid = setup_timeline(c)
        r = gaps(c, sid, max_gap_seconds=15, cursor="not-a-cursor!!")
        assert r.status_code == 400
        # A structurally valid cursor with the wrong arity is also 400.
        from app.cursor import encode_cursor
        r = gaps(c, sid, max_gap_seconds=15, cursor=encode_cursor(("x",)))
        assert r.status_code == 400
        # Correct arity but wrong value types is also a malformed cursor.
        r = gaps(c, sid, max_gap_seconds=15,
                 cursor=encode_cursor(("2026-01-01T10:00:00.000000Z", 1, "x")))
        assert r.status_code == 400

    def test_limit_bounds(self, client):
        c, _ = client
        sid = setup_timeline(c)
        assert gaps(c, sid, max_gap_seconds=15, limit=0).status_code == 400
        assert gaps(c, sid, max_gap_seconds=15, limit=1001).status_code == 400
        assert gaps(c, sid, max_gap_seconds=15, limit="1.5").status_code == 400
        assert gaps(c, sid, max_gap_seconds=15, limit="abc").status_code == 400
        assert gaps(c, sid, max_gap_seconds=15, limit=" ").status_code == 400
        ok = gaps(c, sid, max_gap_seconds=15, limit=1)
        assert ok.status_code == 200
        assert ok.json()["limit"] == 1


class TestSourceGapErrors:
    def test_unknown_session_404(self, client):
        c, _ = client
        r = gaps(c, "nope", max_gap_seconds=10)
        assert r.status_code == 404

    def test_unknown_source_400(self, client):
        c, _ = client
        sid = setup_timeline(c)
        r = gaps(c, sid, max_gap_seconds=15, source_id="zzz")
        assert r.status_code == 400
        assert "zzz" in r.json()["detail"]

    def test_blank_source_400(self, client):
        c, _ = client
        sid = setup_timeline(c)
        r = gaps(c, sid, max_gap_seconds=15, source_id="   ")
        assert r.status_code == 400

    def test_missing_max_gap_400(self, client):
        c, _ = client
        sid = setup_timeline(c)
        r = c.get(f"/sessions/{sid}/source-gaps")
        assert r.status_code == 400

    def test_bad_max_gap_values_400(self, client):
        c, _ = client
        sid = setup_timeline(c)
        for bad in ("abc", "", " ", "-1", "nan", "inf", "-inf"):
            r = gaps(c, sid, max_gap_seconds=bad)
            assert r.status_code == 400, bad

    def test_huge_finite_threshold_finds_nothing(self, client):
        c, _ = client
        sid = setup_timeline(c)
        # A finite threshold that overflows integers when scaled to
        # microseconds must not crash the SQLite binding; the threshold just
        # can never be exceeded.
        r = gaps(c, sid, max_gap_seconds="9007199254740993")
        assert r.status_code == 200
        assert r.json()["gaps"] == []
        assert r.json()["matched"] == 0
        r = gaps(c, sid, max_gap_seconds="1e302")
        assert r.status_code == 200
        assert r.json()["gaps"] == []

    def test_zero_threshold_allowed(self, client):
        c, _ = client
        sid = setup_timeline(c)
        r = gaps(c, sid, max_gap_seconds=0)
        assert r.status_code == 200
        assert r.json()["matched"] == 5  # every positive adjacent gap

    def test_bad_time_bounds_400(self, client):
        c, _ = client
        sid = setup_timeline(c)
        r = gaps(c, sid, max_gap_seconds=15, **{"from": "not-a-time"})
        assert r.status_code == 400
        r = gaps(c, sid, max_gap_seconds=15, **{"to": "2026-13-99T00:00:00Z"})
        assert r.status_code == 400

    def test_from_after_to_400(self, client):
        c, _ = client
        sid = setup_timeline(c)
        r = gaps(
            c, sid, max_gap_seconds=15,
            **{"from": "2026-01-01T10:00:40Z", "to": "2026-01-01T10:00:10Z"},
        )
        assert r.status_code == 400


class TestSourceGapReadOnly:
    def test_call_changes_nothing(self, client):
        c, db_path = client
        sid = setup_timeline(c)

        def snapshot():
            with sqlite3.connect(db_path) as raw:
                raw.row_factory = sqlite3.Row
                tables = {}
                for name in (
                    "sessions", "session_sources", "readings", "alert_rules",
                    "alert_events", "correlated_rules", "correlated_conditions",
                    "correlated_events", "checkpoints",
                ):
                    tables[name] = [
                        dict(r) for r in raw.execute(f"SELECT * FROM {name}")
                    ]
                return tables

        before = snapshot()
        for _ in range(3):
            r = gaps(c, sid, max_gap_seconds=15)
            assert r.status_code == 200
            r = gaps(
                c, sid, max_gap_seconds=15, source_id="a",
                **{"from": "2026-01-01T10:00:00Z", "to": "2026-01-01T10:01:00Z"},
                limit=1,
            )
            assert r.status_code == 200
        after = snapshot()
        assert after == before

    def test_watermark_unchanged(self, client):
        c, _ = client
        sid = setup_timeline(c)
        before = c.get(f"/sessions/{sid}").json()
        gaps(c, sid, max_gap_seconds=1)
        after = c.get(f"/sessions/{sid}").json()
        assert after == before

    def test_does_not_touch_alerts_or_checkpoints(self, client):
        c, _ = client
        sid = setup_timeline(c)
        # Create a rule and a checkpoint before the read.
        r = c.post(
            f"/sessions/{sid}/alert-rules",
            json={
                "rule_id": "r1", "source_id": "a", "direction": "above",
                "enter_threshold": 30, "recover_threshold": 25,
                "enter_consecutive": 1, "recover_consecutive": 1,
            },
        )
        assert r.status_code == 201
        r = c.post(f"/sessions/{sid}/checkpoints", json={"name": "cp1"})
        assert r.status_code in (200, 201)
        rule_before = c.get(f"/sessions/{sid}/alert-rules/r1").json()
        cp_before = c.get(f"/sessions/{sid}/checkpoints").json()
        stream_before = c.get(f"/sessions/{sid}/stream?limit=1000").json()

        gaps(c, sid, max_gap_seconds=0, limit=1)

        assert c.get(f"/sessions/{sid}/alert-rules/r1").json() == rule_before
        assert c.get(f"/sessions/{sid}/checkpoints").json() == cp_before
        assert c.get(f"/sessions/{sid}/stream?limit=1000").json() == stream_before
