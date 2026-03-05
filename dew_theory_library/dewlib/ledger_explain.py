from __future__ import annotations

import json
import sqlite3


class ExplainError(Exception):
    pass


def _parse_json(value: str) -> object:
    return json.loads(value)


def _load_signal(conn: sqlite3.Connection, signal_id: str, version: int | None) -> dict | None:
    if version is None:
        row = conn.execute(
            """
            SELECT * FROM signals
            WHERE signal_id = ?
            ORDER BY version DESC
            LIMIT 1
            """,
            (signal_id,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM signals WHERE signal_id = ? AND version = ?",
            (signal_id, version),
        ).fetchone()
    if row is None:
        return None
    return {
        "signal_id": row["signal_id"],
        "version": int(row["version"]),
        "name": row["name"],
        "description": row["description"],
        "universe_json": _parse_json(row["universe_json"]),
        "spec_json": _parse_json(row["spec_json"]),
        "status": row["status"],
        "created_at": row["created_at"],
        "created_by": row["created_by"],
    }


def _status_mode(status_filter: str | None) -> str:
    return (status_filter or "approved+active").strip().lower()


def explain_signal(
    conn: sqlite3.Connection,
    signal_id: str,
    version: int | None = None,
    status_filter: str | None = None,
) -> dict:
    mode = _status_mode(status_filter)
    if mode not in {"approved+active", "all"}:
        raise ExplainError(f"Unsupported status_filter: {status_filter}")

    signal = _load_signal(conn, signal_id=signal_id, version=version)
    if signal is None:
        raise ExplainError(f"Signal not found: {signal_id}")

    concept_links = conn.execute(
        """
        SELECT
          cs.concept_id,
          cs.signal_id,
          cs.signal_version,
          cs.claim,
          cs.confidence,
          cs.status AS claim_status,
          c.name,
          c.description,
          c.status AS concept_status
        FROM concept_signals cs
        JOIN concepts c ON c.concept_id = cs.concept_id
        WHERE cs.signal_id = ? AND cs.signal_version = ?
        ORDER BY
          CASE WHEN cs.status = 'approved' THEN 0 ELSE 1 END,
          cs.confidence DESC,
          c.name ASC
        """,
        (signal["signal_id"], signal["version"]),
    ).fetchall()

    concepts_payload: list[dict] = []
    for row in concept_links:
        if mode == "approved+active":
            if signal["status"] != "active":
                continue
            if row["claim_status"] != "approved":
                continue
            if row["concept_status"] != "approved":
                continue

        citation_rows = conn.execute(
            """
            SELECT
              cc.status AS link_status,
              ci.citation_id,
              ci.doc_id,
              ci.theorist,
              ci.title,
              ci.source_path,
              ci.page_start,
              ci.page_end,
              ci.chunk_id,
              ci.quote
            FROM concept_citations cc
            JOIN citations ci ON ci.citation_id = cc.citation_id
            WHERE cc.concept_id = ?
            ORDER BY ci.doc_id, ci.page_start, ci.chunk_id
            """,
            (row["concept_id"],),
        ).fetchall()

        citations = []
        for citation in citation_rows:
            if mode == "approved+active" and citation["link_status"] != "approved":
                continue
            citations.append(
                {
                    "citation_id": citation["citation_id"],
                    "doc_id": citation["doc_id"],
                    "theorist": citation["theorist"],
                    "title": citation["title"],
                    "source_path": citation["source_path"],
                    "page_start": int(citation["page_start"]),
                    "page_end": int(citation["page_end"]),
                    "chunk_id": citation["chunk_id"],
                    "quote": citation["quote"],
                }
            )

        concepts_payload.append(
            {
                "concept": {
                    "concept_id": row["concept_id"],
                    "name": row["name"],
                    "description": row["description"],
                    "status": row["concept_status"],
                },
                "claim": {
                    "claim": row["claim"],
                    "confidence": row["confidence"],
                    "status": row["claim_status"],
                },
                "citations": citations,
            }
        )

    concepts_payload = sorted(
        concepts_payload,
        key=lambda item: (
            0 if item["claim"]["status"] == "approved" else 1,
            -(float(item["claim"]["confidence"]) if item["claim"]["confidence"] is not None else -1.0),
            item["concept"]["name"],
        ),
    )
    return {
        "signal": {
            "signal_id": signal["signal_id"],
            "version": signal["version"],
            "name": signal["name"],
            "spec_json": signal["spec_json"],
            "status": signal["status"],
        },
        "concepts": concepts_payload,
    }

