from __future__ import annotations

import json
import sqlite3
from uuid import uuid4

from .ledger_db import utc_now_iso


RECOMMENDATION_STATUSES = {"proposed", "accepted", "rejected", "superseded"}
RECOMMENDATION_TRANSITIONS = {
    "proposed": {"accepted", "rejected", "superseded"},
    "accepted": {"superseded"},
    "rejected": {"superseded"},
    "superseded": set(),
}


def _to_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def list_queue(
    conn: sqlite3.Connection,
    status: str = "proposed",
    limit: int = 50,
    since_ts: str | None = None,
) -> dict:
    if status not in RECOMMENDATION_STATUSES:
        raise ValueError(f"Invalid recommendation status: {status}")

    params: list[object] = [status]
    query = """
        SELECT
          r.recommendation_id,
          r.scan_run_id,
          r.ts,
          r.kind,
          r.title,
          r.confidence,
          r.status,
          sr.ts AS run_ts,
          sr.report_path
        FROM recommendations r
        LEFT JOIN scan_runs sr ON sr.scan_run_id = r.scan_run_id
        WHERE r.status = ?
    """
    if since_ts:
        query += " AND r.ts >= ?"
        params.append(since_ts)
    query += " ORDER BY r.ts DESC, r.recommendation_id DESC LIMIT ?"
    params.append(max(1, min(int(limit), 500)))

    rows = conn.execute(query, tuple(params)).fetchall()
    items = [
        {
            "recommendation_id": row["recommendation_id"],
            "scan_run_id": row["scan_run_id"],
            "ts": row["ts"],
            "kind": row["kind"],
            "title": row["title"],
            "confidence": float(row["confidence"]) if row["confidence"] is not None else None,
            "status": row["status"],
            "run_ts": row["run_ts"],
            "report_path": row["report_path"],
        }
        for row in rows
    ]
    return {"status": status, "items": items}


def set_status(
    conn: sqlite3.Connection,
    recommendation_id: str,
    new_status: str,
    actor: str,
    note: str | None = None,
) -> dict:
    if new_status not in RECOMMENDATION_STATUSES:
        raise ValueError(f"Invalid recommendation status: {new_status}")
    row = conn.execute(
        "SELECT recommendation_id, status FROM recommendations WHERE recommendation_id = ?",
        (recommendation_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"recommendation not found: {recommendation_id}")

    old_status = str(row["status"])
    if new_status != old_status and new_status not in RECOMMENDATION_TRANSITIONS.get(old_status, set()):
        raise ValueError(f"Invalid recommendation status transition: {old_status} -> {new_status}")

    conn.execute(
        "UPDATE recommendations SET status = ? WHERE recommendation_id = ?",
        (new_status, recommendation_id),
    )
    payload = {
        "before": {"status": old_status},
        "after": {"status": new_status},
        "actor": actor,
        "note": note,
    }
    conn.execute(
        """
        INSERT INTO events(event_id, ts, actor, event_type, entity_type, entity_id, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid4()),
            utc_now_iso(),
            actor,
            "recommendation.status_change",
            "recommendation",
            recommendation_id,
            _to_json(payload),
        ),
    )
    return {"recommendation_id": recommendation_id, "status": new_status}

