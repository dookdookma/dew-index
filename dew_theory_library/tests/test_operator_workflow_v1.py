from __future__ import annotations

import json
from pathlib import Path

import pytest

from dewlib.digest import generate_daily_digest
from dewlib.ledger_db import connect_db
from dewlib.ledger_store import LedgerStore
from dewlib.recommend_review import list_queue, set_status
from dewlib.scan_db import ScanDB
from dewlib.scan_runtime import run_scan
from dewlib.signal_tune import tune_signal
from dewlib.util import atomic_write_json, atomic_write_jsonl, read_json


def _write_minimal_chunk_corpus(data_dir: Path) -> str:
    chunk_id = "1122334455667788:1:0"
    (data_dir / "index").mkdir(parents=True, exist_ok=True)
    row = {
        "chunk_id": chunk_id,
        "doc_id": "1122334455667788",
        "theorist": "Virilio",
        "title": "Speed and Politics",
        "source_path": "Virilio/SpeedAndPolitics.pdf",
        "ocr_path": "Virilio/SpeedAndPolitics.pdf",
        "page_start": 1,
        "page_end": 1,
        "text_hash": "abc123abc123abc1",
        "text": "Dromology links speed, logistics, and political power.",
    }
    atomic_write_jsonl(data_dir / "index" / "meta.jsonl", [row])
    atomic_write_jsonl(data_dir / "chunks.jsonl", [row])
    return chunk_id


def _write_rss_fixture(path: Path, title: str) -> None:
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Fixture Feed</title>
    <item>
      <guid>fixture-1</guid>
      <title>{title}</title>
      <link>https://example.com/fixture-1</link>
      <description>Dromology appears in this summary.</description>
      <pubDate>Thu, 05 Mar 2026 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(xml, encoding="utf-8")


def test_recommendation_queue_and_status_transition_with_event(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "ledger.sqlite3"
    scan_db = ScanDB(db_path=db_path)
    scan_db.initialize()

    run = scan_db.create_scan_run(
        inputs={"feeds": [], "options": {"cadence": "morning"}, "item_count": 0},
        created_by="test",
        ts="2026-03-05T14:00:00+00:00",
    )
    recommendation = scan_db.create_recommendation(
        scan_run_id=run["scan_run_id"],
        kind="watch",
        title="Queue candidate",
        body="Paper recommendation.",
        confidence=0.7,
        status="proposed",
        created_by="test",
        ts="2026-03-05T14:01:00+00:00",
    )

    with connect_db(db_path) as conn:
        queue = list_queue(conn, status="proposed", limit=50, since_ts=None)
        assert queue["status"] == "proposed"
        assert len(queue["items"]) == 1
        assert queue["items"][0]["recommendation_id"] == recommendation["recommendation_id"]
        assert queue["items"][0]["run_ts"] == run["ts"]

        updated = set_status(
            conn,
            recommendation_id=recommendation["recommendation_id"],
            new_status="accepted",
            actor="operator",
            note="reviewed",
        )
        conn.commit()
        assert updated["status"] == "accepted"

    with connect_db(db_path) as conn:
        row = conn.execute(
            "SELECT status FROM recommendations WHERE recommendation_id = ?",
            (recommendation["recommendation_id"],),
        ).fetchone()
        assert row is not None
        assert row["status"] == "accepted"

        event_row = conn.execute(
            """
            SELECT payload_json FROM events
            WHERE event_type = 'recommendation.status_change' AND entity_id = ?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (recommendation["recommendation_id"],),
        ).fetchone()
        assert event_row is not None
        payload = json.loads(event_row["payload_json"])
        assert payload["before"]["status"] == "proposed"
        assert payload["after"]["status"] == "accepted"
        assert payload["actor"] == "operator"
        assert payload["note"] == "reviewed"

        with pytest.raises(ValueError):
            set_status(
                conn,
                recommendation_id=recommendation["recommendation_id"],
                new_status="rejected",
                actor="operator",
                note=None,
            )


def test_daily_digest_aggregation_sections(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "ledger.sqlite3"
    out_root = tmp_path / "out"
    scan_db = ScanDB(db_path=db_path)
    scan_db.initialize()
    store = LedgerStore(db_path=db_path, data_dir=tmp_path / "data")
    store.initialize()

    signal = store.create_signal(
        name="Digest Signal",
        description="Digest test signal",
        universe={"scope": "test"},
        spec={
            "kind": "rss_keyword_count",
            "keywords": ["dromology"],
            "threshold": 1,
            "window_items": 50,
            "match_fields": ["title", "summary"],
            "case_sensitive": False,
        },
        status="active",
        created_by="test",
    )

    run_specs = [
        ("morning", "2026-03-05T13:00:00+00:00", "accepted", 2),
        ("midday", "2026-03-05T17:00:00+00:00", "proposed", 1),
        ("close", "2026-03-05T21:00:00+00:00", "rejected", 0),
    ]

    run_ids: list[str] = []
    for cadence, ts, recommendation_status, failed_feeds in run_specs:
        run = scan_db.create_scan_run(
            inputs={"feeds": [], "options": {"cadence": cadence, "feed_set": "core"}, "item_count": 3},
            created_by="test",
            ts=ts,
        )
        run_ids.append(run["scan_run_id"])
        report_file = out_root / "scans" / f"{cadence}.md"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        report_file.write_text(f"# {cadence}\n", encoding="utf-8")
        scan_db.update_scan_run_report_path(run["scan_run_id"], report_file.as_posix())

        feeds = []
        for index in range(max(1, failed_feeds)):
            failed = index < failed_feeds
            feeds.append(
                {
                    "id": f"{cadence}-feed-{index}",
                    "url": f"file:///tmp/{cadence}-{index}.xml",
                    "fetch_ok": not failed,
                    "http_status": None,
                    "bytes": 128,
                    "parse_ok": not failed,
                    "item_count": 1 if not failed else 0,
                    "error": "fetch_error: missing file" if failed else None,
                    "elapsed_ms": 3,
                }
            )
        health_payload = {
            "scan_run_id": run["scan_run_id"],
            "ts": ts,
            "summary": {
                "feeds_total": len(feeds),
                "feeds_ok": sum(1 for row in feeds if row["fetch_ok"] and row["parse_ok"]),
                "feeds_failed": sum(1 for row in feeds if not (row["fetch_ok"] and row["parse_ok"])),
                "items_total": sum(int(row["item_count"]) for row in feeds),
            },
            "feeds": feeds,
        }
        health_path = report_file.with_name(f"{report_file.stem}_feeds_health.json")
        atomic_write_json(health_path, health_payload)
        sidecar = {
            "scan_run_id": run["scan_run_id"],
            "ts": ts,
            "cadence": cadence,
            "feed_set": "core",
            "report_path": report_file.as_posix(),
            "signals_evaluated": 1,
            "signals_triggered": 1,
            "triggered": [],
            "recommendations": [],
            "feeds_health_path": health_path.as_posix(),
            **health_payload["summary"],
        }
        atomic_write_json(report_file.with_suffix(".json"), sidecar)

        scan_db.create_observation(
            scan_run_id=run["scan_run_id"],
            signal_id=signal["signal_id"],
            signal_version=signal["version"],
            metric={"kind": "rss_keyword_count", "match_count": 3, "threshold": 1},
            triggered=True,
            context={"matched_items": []},
            created_by="test",
            ts=ts,
        )
        scan_db.create_recommendation(
            scan_run_id=run["scan_run_id"],
            kind="watch",
            title=f"{cadence} recommendation",
            body="digest rec",
            confidence=0.6,
            status=recommendation_status,
            created_by="test",
            ts=ts,
        )

    result = generate_daily_digest(
        ledger_db_path=db_path,
        out_dir=out_root / "digests",
        date="2026-03-05",
        tz_name="America/New_York",
        cadences=["morning", "midday", "close"],
    )
    markdown = Path(result["markdown_path"]).read_text(encoding="utf-8")
    assert "# DEW Digest — 2026-03-05" in markdown
    assert "## Runs Included" in markdown
    assert "## Triggered Signals" in markdown
    assert "## Recommendations" in markdown
    assert "## Feed Health Summary" in markdown
    assert "Total failed feeds: **3**" in markdown

    json_payload = read_json(Path(result["json_path"]))
    assert len(json_payload["runs"]) == 3
    assert json_payload["feed_health"]["total_failed_feeds"] == 3


def test_scan_run_writes_feed_health_and_exposes_summary(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    db_path = data_dir / "ledger.sqlite3"
    out_dir = tmp_path / "out" / "scans"
    _write_minimal_chunk_corpus(data_dir)

    store = LedgerStore(db_path=db_path, data_dir=data_dir)
    store.initialize()
    store.create_signal(
        name="Feed Health Signal",
        description="signal for feed health test",
        universe={"scope": "test"},
        spec={
            "kind": "rss_keyword_count",
            "keywords": ["dromology"],
            "threshold": 1,
            "window_items": 50,
            "match_fields": ["title", "summary"],
            "case_sensitive": False,
        },
        status="active",
        created_by="test",
    )

    fixture = tmp_path / "fixtures" / "health_feed.xml"
    _write_rss_fixture(fixture, "Dromology appears in logistics")
    missing_feed = tmp_path / "fixtures" / "missing.xml"

    result = run_scan(
        ledger_db_path=db_path,
        feed_sources=[
            {"id": "ok_feed", "url": str(fixture)},
            {"id": "bad_feed", "url": str(missing_feed)},
        ],
        run_options={"out_dir": str(out_dir), "cadence": "morning", "max_items": 100},
        created_by="scanner_test",
    )

    assert result["signals_evaluated"] == 1
    assert "feeds_health_path" in result
    assert result["feeds_total"] == 2
    assert result["feeds_failed"] == 1

    feeds_health_path = Path(result["feeds_health_path"])
    assert feeds_health_path.exists()
    feeds_payload = read_json(feeds_health_path)
    assert feeds_payload["summary"]["feeds_total"] == 2
    assert feeds_payload["summary"]["feeds_failed"] == 1

    sidecar = read_json(Path(result["report_json_path"]))
    assert sidecar["feeds_health_path"] == result["feeds_health_path"]
    assert sidecar["feeds_total"] == 2
    assert sidecar["feeds_failed"] == 1


def test_signal_tune_creates_new_version_and_event(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "ledger.sqlite3"
    store = LedgerStore(db_path=db_path, data_dir=tmp_path / "data")
    store.initialize()
    signal = store.create_signal(
        name="Tuneable Signal",
        description="For tune helper test",
        universe={"scope": "test"},
        spec={
            "kind": "rss_keyword_count",
            "feeds": ["all"],
            "keywords": ["dromology", "latency"],
            "window_items": 50,
            "threshold": 2,
            "match_fields": ["title", "summary"],
            "case_sensitive": False,
        },
        status="active",
        created_by="test",
    )

    result = tune_signal(
        ledger_db_path=db_path,
        signal_id=signal["signal_id"],
        from_version=1,
        set_overrides={
            "threshold": "4",
            "window_items": "120",
            "keywords_add": "export controls",
            "keywords_remove": "latency",
        },
        created_by="operator",
        feeds_registry_path=tmp_path / "data" / "feeds.json",
    )

    assert result["from_version"] == 1
    assert result["to_version"] == 2
    assert result["after_spec"]["threshold"] == 4
    assert result["after_spec"]["window_items"] == 120
    assert "export controls" in result["after_spec"]["keywords"]
    assert "latency" not in result["after_spec"]["keywords"]

    with connect_db(db_path) as conn:
        row = conn.execute(
            "SELECT spec_json FROM signals WHERE signal_id = ? AND version = 2",
            (signal["signal_id"],),
        ).fetchone()
        assert row is not None
        spec = json.loads(row["spec_json"])
        assert int(spec["threshold"]) == 4

        event_row = conn.execute(
            """
            SELECT payload_json FROM events
            WHERE event_type = 'signal.clone_patch' AND entity_id = ?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (f"{signal['signal_id']}:2",),
        ).fetchone()
        assert event_row is not None
        payload = json.loads(event_row["payload_json"])
        assert payload["from_version"] == 1
        assert payload["to_version"] == 2
