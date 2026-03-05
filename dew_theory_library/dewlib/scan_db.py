from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from .ledger_db import connect_db, initialize_ledger_db, utc_now_iso


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _from_json(value: str | None, fallback: object) -> object:
    if value is None:
        return fallback
    return json.loads(value)


class ScanDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def initialize(self) -> dict:
        return initialize_ledger_db(self.db_path)

    def _connect(self) -> sqlite3.Connection:
        return connect_db(self.db_path)

    def _log_event(
        self,
        conn: sqlite3.Connection,
        actor: str | None,
        event_type: str,
        entity_type: str,
        entity_id: str,
        payload: object,
    ) -> None:
        conn.execute(
            """
            INSERT INTO events(event_id, ts, actor, event_type, entity_type, entity_id, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid4()),
                utc_now_iso(),
                actor,
                event_type,
                entity_type,
                entity_id,
                _to_json(payload),
            ),
        )

    def create_scan_run(
        self,
        inputs: object,
        notes: str | None = None,
        created_by: str | None = None,
        ts: str | None = None,
    ) -> dict:
        scan_run_id = str(uuid4())
        now = utc_now_iso()
        ts_value = ts or now
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO scan_runs(scan_run_id, ts, inputs_json, notes, report_path, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (scan_run_id, ts_value, _to_json(inputs), notes, None, created_by, now),
            )
            row = conn.execute("SELECT * FROM scan_runs WHERE scan_run_id = ?", (scan_run_id,)).fetchone()
            assert row is not None
            payload = self._scan_run_from_row(row)
            self._log_event(
                conn,
                actor=created_by,
                event_type="scan_run.create",
                entity_type="scan_run",
                entity_id=scan_run_id,
                payload=payload,
            )
            conn.commit()
            return payload

    def update_scan_run_report_path(self, scan_run_id: str, report_path: str) -> dict:
        with self._connect() as conn:
            conn.execute(
                "UPDATE scan_runs SET report_path = ? WHERE scan_run_id = ?",
                (report_path, scan_run_id),
            )
            row = conn.execute("SELECT * FROM scan_runs WHERE scan_run_id = ?", (scan_run_id,)).fetchone()
            if row is None:
                raise KeyError(f"scan_run not found: {scan_run_id}")
            conn.commit()
            return self._scan_run_from_row(row)

    def create_observation(
        self,
        scan_run_id: str,
        signal_id: str,
        signal_version: int,
        metric: object,
        triggered: bool,
        context: object,
        created_by: str | None = None,
        ts: str | None = None,
    ) -> dict:
        observation_id = str(uuid4())
        now = utc_now_iso()
        ts_value = ts or now
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO observations(
                  observation_id, scan_run_id, signal_id, signal_version, ts,
                  metric_json, triggered, context_json, created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observation_id,
                    scan_run_id,
                    signal_id,
                    int(signal_version),
                    ts_value,
                    _to_json(metric),
                    1 if triggered else 0,
                    _to_json(context),
                    created_by,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM observations WHERE observation_id = ?",
                (observation_id,),
            ).fetchone()
            assert row is not None
            payload = self._observation_from_row(row)
            self._log_event(
                conn,
                actor=created_by,
                event_type="observation.create",
                entity_type="observation",
                entity_id=observation_id,
                payload=payload,
            )
            conn.commit()
            return payload

    def _signal_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "signal_id": row["signal_id"],
            "version": int(row["version"]),
            "name": row["name"],
            "description": row["description"],
            "universe": _from_json(row["universe_json"], {}),
            "spec": _from_json(row["spec_json"], {}),
            "status": row["status"],
            "created_at": row["created_at"],
            "created_by": row["created_by"],
        }

    def load_active_signals_latest(self, pinned_versions: dict[str, int] | None = None) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM signals
                WHERE status = 'active'
                ORDER BY signal_id ASC, version DESC
                """
            ).fetchall()

            latest_by_signal: dict[str, sqlite3.Row] = {}
            for row in rows:
                latest_by_signal.setdefault(str(row["signal_id"]), row)

            if pinned_versions:
                for signal_id in sorted(pinned_versions):
                    pinned_version = int(pinned_versions[signal_id])
                    pinned = conn.execute(
                        """
                        SELECT * FROM signals
                        WHERE signal_id = ? AND version = ? AND status = 'active'
                        """,
                        (signal_id, pinned_version),
                    ).fetchone()
                    if pinned is None:
                        raise KeyError(f"Pinned active signal not found: {signal_id}:{pinned_version}")
                    latest_by_signal[signal_id] = pinned

            signals = [self._signal_from_row(row) for row in latest_by_signal.values()]
            return sorted(
                signals,
                key=lambda row: (row["name"].lower(), row["signal_id"], row["version"]),
            )

    def _scan_run_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "scan_run_id": row["scan_run_id"],
            "ts": row["ts"],
            "inputs_json": _from_json(row["inputs_json"], {}),
            "notes": row["notes"],
            "report_path": row["report_path"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    def _observation_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "observation_id": row["observation_id"],
            "scan_run_id": row["scan_run_id"],
            "signal_id": row["signal_id"],
            "signal_version": int(row["signal_version"]),
            "ts": row["ts"],
            "metric_json": _from_json(row["metric_json"], {}),
            "triggered": int(row["triggered"]),
            "context_json": _from_json(row["context_json"], {}),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    def list_scan_runs(self, limit: int = 20) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM scan_runs
                ORDER BY ts DESC, scan_run_id DESC
                LIMIT ?
                """,
                (max(1, min(int(limit), 500)),),
            ).fetchall()
            return [self._scan_run_from_row(row) for row in rows]

    def get_scan_run(self, scan_run_id: str) -> dict | None:
        with self._connect() as conn:
            run_row = conn.execute(
                "SELECT * FROM scan_runs WHERE scan_run_id = ?",
                (scan_run_id,),
            ).fetchone()
            if run_row is None:
                return None

            obs_rows = conn.execute(
                """
                SELECT * FROM observations
                WHERE scan_run_id = ?
                ORDER BY triggered DESC, signal_id ASC, signal_version ASC, observation_id ASC
                """,
                (scan_run_id,),
            ).fetchall()
            return {
                "scan_run": self._scan_run_from_row(run_row),
                "observations": [self._observation_from_row(row) for row in obs_rows],
                "recommendations": self._recommendations_for_run(conn, scan_run_id),
            }

    def _recommendation_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "recommendation_id": row["recommendation_id"],
            "scan_run_id": row["scan_run_id"],
            "ts": row["ts"],
            "kind": row["kind"],
            "title": row["title"],
            "body": row["body"],
            "confidence": float(row["confidence"]) if row["confidence"] is not None else None,
            "related_signal_ids_json": _from_json(row["related_signal_ids_json"], []),
            "related_observation_ids_json": _from_json(row["related_observation_ids_json"], []),
            "status": row["status"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    def _recommendations_for_run(self, conn: sqlite3.Connection, scan_run_id: str) -> list[dict]:
        rows = conn.execute(
            """
            SELECT * FROM recommendations
            WHERE scan_run_id = ?
            ORDER BY ts ASC, recommendation_id ASC
            """,
            (scan_run_id,),
        ).fetchall()
        return [self._recommendation_from_row(row) for row in rows]

    def create_recommendation(
        self,
        scan_run_id: str,
        kind: str,
        title: str,
        body: str,
        confidence: float | None = None,
        related_signal_ids: list[dict] | None = None,
        related_observation_ids: list[str] | None = None,
        status: str = "proposed",
        created_by: str | None = None,
        ts: str | None = None,
    ) -> dict:
        if status not in {"proposed", "accepted", "rejected", "superseded"}:
            raise ValueError(f"Invalid recommendation status: {status}")
        if confidence is not None and (confidence < 0.0 or confidence > 1.0):
            raise ValueError("confidence must be within [0, 1]")

        recommendation_id = str(uuid4())
        now = utc_now_iso()
        ts_value = ts or now
        with self._connect() as conn:
            run = conn.execute(
                "SELECT scan_run_id FROM scan_runs WHERE scan_run_id = ?",
                (scan_run_id,),
            ).fetchone()
            if run is None:
                raise KeyError(f"scan_run not found: {scan_run_id}")

            conn.execute(
                """
                INSERT INTO recommendations(
                  recommendation_id, scan_run_id, ts, kind, title, body, confidence,
                  related_signal_ids_json, related_observation_ids_json, status, created_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    recommendation_id,
                    scan_run_id,
                    ts_value,
                    kind,
                    title,
                    body,
                    confidence,
                    _to_json(related_signal_ids or []),
                    _to_json(related_observation_ids or []),
                    status,
                    created_by,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM recommendations WHERE recommendation_id = ?",
                (recommendation_id,),
            ).fetchone()
            assert row is not None
            payload = self._recommendation_from_row(row)
            self._log_event(
                conn,
                actor=created_by,
                event_type="recommendation.create",
                entity_type="recommendation",
                entity_id=recommendation_id,
                payload=payload,
            )
            conn.commit()
            return payload

    def list_recommendations(
        self,
        scan_run_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        query = "SELECT * FROM recommendations WHERE 1=1"
        params: list[object] = []
        if scan_run_id:
            query += " AND scan_run_id = ?"
            params.append(scan_run_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY ts DESC, recommendation_id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 500)))

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
            return [self._recommendation_from_row(row) for row in rows]

    def update_recommendation_status(
        self,
        recommendation_id: str,
        status: str,
        actor: str | None = None,
    ) -> dict:
        if status not in {"proposed", "accepted", "rejected", "superseded"}:
            raise ValueError(f"Invalid recommendation status: {status}")

        allowed = {
            "proposed": {"accepted", "rejected", "superseded"},
            "accepted": {"superseded"},
            "rejected": {"superseded"},
            "superseded": set(),
        }
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM recommendations WHERE recommendation_id = ?",
                (recommendation_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"recommendation not found: {recommendation_id}")
            before = self._recommendation_from_row(row)
            if status != before["status"] and status not in allowed.get(before["status"], set()):
                raise ValueError(
                    f"Invalid recommendation status transition: {before['status']} -> {status}"
                )
            conn.execute(
                "UPDATE recommendations SET status = ? WHERE recommendation_id = ?",
                (status, recommendation_id),
            )
            updated_row = conn.execute(
                "SELECT * FROM recommendations WHERE recommendation_id = ?",
                (recommendation_id,),
            ).fetchone()
            assert updated_row is not None
            payload = self._recommendation_from_row(updated_row)
            self._log_event(
                conn,
                actor=actor,
                event_type="recommendation.status_change",
                entity_type="recommendation",
                entity_id=recommendation_id,
                payload={"before": {"status": before["status"]}, "after": {"status": payload["status"]}},
            )
            conn.commit()
            return payload
