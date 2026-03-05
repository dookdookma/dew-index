from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3

from .util import ensure_dir

BASE_SCHEMA_VERSION = "1"
SCHEMA_VERSION = "3"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path: Path) -> sqlite3.Connection:
    ensure_dir(db_path.parent)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _create_v1_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS citations (
          citation_id TEXT PRIMARY KEY,
          chunk_id TEXT NOT NULL UNIQUE,
          doc_id TEXT NOT NULL,
          theorist TEXT NOT NULL,
          title TEXT NOT NULL,
          source_path TEXT NOT NULL,
          ocr_path TEXT NOT NULL,
          page_start INTEGER NOT NULL,
          page_end INTEGER NOT NULL,
          text_hash TEXT NOT NULL,
          quote TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_citations_doc_id ON citations(doc_id);
        CREATE INDEX IF NOT EXISTS idx_citations_theorist ON citations(theorist);

        CREATE TABLE IF NOT EXISTS concepts (
          concept_id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          description TEXT,
          tags_json TEXT,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT,
          updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS signals (
          signal_id TEXT NOT NULL,
          version INTEGER NOT NULL,
          name TEXT NOT NULL,
          description TEXT,
          universe_json TEXT NOT NULL,
          spec_json TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT,
          PRIMARY KEY (signal_id, version)
        );

        CREATE TABLE IF NOT EXISTS concept_citations (
          concept_id TEXT NOT NULL,
          citation_id TEXT NOT NULL,
          weight REAL DEFAULT 1.0,
          note TEXT,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT,
          PRIMARY KEY (concept_id, citation_id),
          FOREIGN KEY (concept_id) REFERENCES concepts(concept_id),
          FOREIGN KEY (citation_id) REFERENCES citations(citation_id)
        );

        CREATE TABLE IF NOT EXISTS concept_signals (
          concept_id TEXT NOT NULL,
          signal_id TEXT NOT NULL,
          signal_version INTEGER NOT NULL,
          claim TEXT NOT NULL,
          confidence REAL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL,
          created_by TEXT,
          PRIMARY KEY (concept_id, signal_id, signal_version),
          FOREIGN KEY (concept_id) REFERENCES concepts(concept_id),
          FOREIGN KEY (signal_id, signal_version) REFERENCES signals(signal_id, version)
        );

        CREATE TABLE IF NOT EXISTS events (
          event_id TEXT PRIMARY KEY,
          ts TEXT NOT NULL,
          actor TEXT,
          event_type TEXT NOT NULL,
          entity_type TEXT NOT NULL,
          entity_id TEXT NOT NULL,
          payload_json TEXT NOT NULL
        );
        """
    )


def _create_v2_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scan_runs (
          scan_run_id TEXT PRIMARY KEY,
          ts TEXT NOT NULL,
          inputs_json TEXT NOT NULL,
          notes TEXT,
          report_path TEXT,
          created_by TEXT,
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS observations (
          observation_id TEXT PRIMARY KEY,
          scan_run_id TEXT NOT NULL,
          signal_id TEXT NOT NULL,
          signal_version INTEGER NOT NULL,
          ts TEXT NOT NULL,
          metric_json TEXT NOT NULL,
          triggered INTEGER NOT NULL,
          context_json TEXT NOT NULL,
          created_by TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY (scan_run_id) REFERENCES scan_runs(scan_run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_obs_scan_run ON observations(scan_run_id);
        CREATE INDEX IF NOT EXISTS idx_obs_signal ON observations(signal_id, signal_version);
        CREATE INDEX IF NOT EXISTS idx_obs_triggered ON observations(triggered);
        """
    )


def _create_v3_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS recommendations (
          recommendation_id TEXT PRIMARY KEY,
          scan_run_id TEXT NOT NULL,
          ts TEXT NOT NULL,
          kind TEXT NOT NULL,
          title TEXT NOT NULL,
          body TEXT NOT NULL,
          confidence REAL,
          related_signal_ids_json TEXT,
          related_observation_ids_json TEXT,
          status TEXT NOT NULL,
          created_by TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY (scan_run_id) REFERENCES scan_runs(scan_run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_reco_scan_run ON recommendations(scan_run_id);
        CREATE INDEX IF NOT EXISTS idx_reco_status ON recommendations(status);
        """
    )


def list_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [str(row["name"]) for row in rows]


def get_schema_version(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    if row is None:
        return None
    return str(row["value"])


def migrate_schema(conn: sqlite3.Connection) -> str:
    _create_v1_tables(conn)
    current = get_schema_version(conn)
    if current is None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('schema_version', ?)",
            (BASE_SCHEMA_VERSION,),
        )
        current = BASE_SCHEMA_VERSION

    if current == "1":
        _create_v2_tables(conn)
        conn.execute("UPDATE meta SET value = ? WHERE key = 'schema_version'", ("2",))
        current = "2"

    if current == "2":
        _create_v2_tables(conn)
        _create_v3_tables(conn)
        conn.execute("UPDATE meta SET value = ? WHERE key = 'schema_version'", ("3",))
        current = "3"

    if current == "3":
        _create_v2_tables(conn)
        _create_v3_tables(conn)
    else:
        raise RuntimeError(f"Unsupported ledger schema_version: {current}")

    return current


def migrate_ledger_db(db_path: Path) -> dict:
    with connect_db(db_path) as conn:
        schema_version = migrate_schema(conn)
        conn.commit()
        tables = list_tables(conn)
    return {
        "db_path": str(db_path),
        "schema_version": schema_version,
        "tables": tables,
    }


def initialize_ledger_db(db_path: Path) -> dict:
    return migrate_ledger_db(db_path)
