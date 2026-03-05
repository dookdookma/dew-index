from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from dewlib.ledger_db import connect_db, migrate_ledger_db
from dewlib.ledger_store import LedgerStore
from dewlib.scan_config import ensure_signal_pack_file, seed_feeds_registry
from dewlib.scan_db import ScanDB
from dewlib.scan_runtime import run_scan
from dewlib.util import atomic_write_jsonl, read_json


def _write_minimal_chunk_corpus(data_dir: Path) -> str:
    chunk_id = "cafebabecafebabe:1:0"
    (data_dir / "index").mkdir(parents=True, exist_ok=True)
    atomic_write_jsonl(
        data_dir / "index" / "meta.jsonl",
        [
            {
                "chunk_id": chunk_id,
                "doc_id": "cafebabecafebabe",
                "theorist": "Virilio",
                "title": "Speed and Politics",
                "source_path": "Virilio/SpeedAndPolitics.pdf",
                "ocr_path": "Virilio/SpeedAndPolitics.pdf",
                "page_start": 1,
                "page_end": 1,
                "text_hash": "abc123abc123abc1",
                "text": "Dromology links speed, logistics, and political power.",
            }
        ],
    )
    atomic_write_jsonl(
        data_dir / "chunks.jsonl",
        [
            {
                "chunk_id": chunk_id,
                "doc_id": "cafebabecafebabe",
                "theorist": "Virilio",
                "title": "Speed and Politics",
                "source_path": "Virilio/SpeedAndPolitics.pdf",
                "ocr_path": "Virilio/SpeedAndPolitics.pdf",
                "page_start": 1,
                "page_end": 1,
                "text_hash": "abc123abc123abc1",
                "text": "Dromology links speed, logistics, and political power.",
            }
        ],
    )
    return chunk_id


def _write_rss_fixture(path: Path) -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Local Feed</title>
    <item>
      <guid>item-1</guid>
      <title>Dromology pressure in logistics corridors</title>
      <link>https://example.com/item-1</link>
      <description>A brief note on dromology and supply chain acceleration.</description>
      <pubDate>Thu, 05 Mar 2026 10:00:00 GMT</pubDate>
    </item>
    <item>
      <guid>item-2</guid>
      <title>Dromology update and speed politics</title>
      <link>https://example.com/item-2</link>
      <description>Another dromology discussion.</description>
      <pubDate>Thu, 05 Mar 2026 11:00:00 GMT</pubDate>
    </item>
    <item>
      <guid>item-3</guid>
      <title>Infrastructure dromology watch</title>
      <link>https://example.com/item-3</link>
      <description>Dromology appears in transport narratives.</description>
      <pubDate>Thu, 05 Mar 2026 12:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(xml, encoding="utf-8")


def _count_rows(db_path: Path, table: str) -> int:
    with connect_db(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
        assert row is not None
        return int(row["n"])


def test_migration_v2_to_v3_preserves_data(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    db_path = data_dir / "ledger.sqlite3"
    store = LedgerStore(db_path=db_path, data_dir=data_dir)
    store.initialize()
    concept = store.create_concept(name="Migration Concept", status="proposed", created_by="test")

    with connect_db(db_path) as conn:
        conn.execute("UPDATE meta SET value = '2' WHERE key = 'schema_version'")
        conn.execute("DROP TABLE IF EXISTS recommendations")
        conn.commit()

    migrated = migrate_ledger_db(db_path)
    assert migrated["schema_version"] == "3"
    assert "recommendations" in migrated["tables"]

    with connect_db(db_path) as conn:
        row = conn.execute(
            "SELECT concept_id FROM concepts WHERE concept_id = ?",
            (concept["concept_id"],),
        ).fetchone()
        assert row is not None


def test_seed_scripts_idempotent(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "ledger.sqlite3"
    feeds_path = data_dir / "feeds.json"
    pack_path = data_dir / "signal_pack_v1.json"

    # create baseline files using module helpers
    first_registry = seed_feeds_registry(feeds_path)
    ensure_signal_pack_file(pack_path)
    assert len(first_registry["feeds"]) >= 10

    # run feed seeding script twice
    subprocess.run(
        [sys.executable, str(repo_root / "scripts" / "seed_feeds.py"), "--feeds-registry", str(feeds_path)],
        check=True,
    )
    subprocess.run(
        [sys.executable, str(repo_root / "scripts" / "seed_feeds.py"), "--feeds-registry", str(feeds_path)],
        check=True,
    )
    merged_registry = read_json(feeds_path)
    assert len(merged_registry["feeds"]) >= len(first_registry["feeds"])
    assert "core" in merged_registry["feed_sets"]

    # run signal-pack seed twice; second run should not change signal row count
    subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "seed_signal_pack.py"),
            "--ledger-db",
            str(db_path),
            "--signal-pack",
            str(pack_path),
            "--created-by",
            "test_seed",
        ],
        check=True,
    )
    signal_count_1 = _count_rows(db_path, "signals")
    concept_count_1 = _count_rows(db_path, "concepts")

    subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "seed_signal_pack.py"),
            "--ledger-db",
            str(db_path),
            "--signal-pack",
            str(pack_path),
            "--created-by",
            "test_seed",
        ],
        check=True,
    )
    signal_count_2 = _count_rows(db_path, "signals")
    concept_count_2 = _count_rows(db_path, "concepts")

    assert signal_count_2 == signal_count_1
    assert concept_count_2 == concept_count_1


def test_scan_run_creates_recommendations_and_sidecar(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    db_path = data_dir / "ledger.sqlite3"
    out_dir = tmp_path / "out" / "scans"
    chunk_id = _write_minimal_chunk_corpus(data_dir)

    store = LedgerStore(db_path=db_path, data_dir=data_dir)
    store.initialize()
    citation = store.create_citation_from_chunk(chunk_id=chunk_id, created_by="test")
    concept = store.create_concept(
        name="Virilio Dromology",
        description="Operationalized dromology concept",
        status="approved",
        created_by="test",
    )
    store.link_concept_citations(
        concept_id=concept["concept_id"],
        citation_ids=[citation["citation_id"]],
        status="approved",
        created_by="test",
    )
    signal = store.create_signal(
        name="Dromology RSS Spike",
        description="Monitors dromology mentions in feeds.",
        universe={"scope": "news"},
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
    store.link_concept_signal(
        concept_id=concept["concept_id"],
        signal_id=signal["signal_id"],
        signal_version=signal["version"],
        claim="Rising dromology mentions map to acceleration risk.",
        confidence=0.8,
        status="approved",
        created_by="test",
    )

    rss_fixture = tmp_path / "fixtures" / "sample_feed.xml"
    _write_rss_fixture(rss_fixture)

    result = run_scan(
        ledger_db_path=db_path,
        feed_sources=[{"id": "fixture", "url": str(rss_fixture)}],
        run_options={"out_dir": str(out_dir), "max_items": 200, "cadence": "morning", "feed_set": "core"},
        created_by="scanner_test",
    )

    assert result["signals_evaluated"] == 1
    assert result["signals_triggered"] == 1
    assert result["triggered"][0]["signal_id"] == signal["signal_id"]
    assert result["triggered"][0]["metric"]["match_count"] >= 3
    assert result["triggered"][0]["explain"]["concepts"]
    assert len(result["recommendations"]) >= 2

    report_path = Path(result["report_path"])
    assert report_path.exists()
    report_text = report_path.read_text(encoding="utf-8")
    assert "Cadence: **morning**" in report_text
    assert "## Recommendations" in report_text
    assert "No execution performed; recommendations are non-binding." in report_text

    sidecar_path = Path(result["report_json_path"])
    assert sidecar_path.exists()
    sidecar = read_json(sidecar_path)
    assert sidecar["scan_run_id"] == result["scan_run_id"]
    assert sidecar["recommendations"]

    scan_db = ScanDB(db_path=db_path)
    run_payload = scan_db.get_scan_run(result["scan_run_id"])
    assert run_payload is not None
    observations = run_payload["observations"]
    assert len(observations) == 1
    assert observations[0]["triggered"] == 1
    assert run_payload["recommendations"]

