from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .manifest import load_manifest
from .util import atomic_write_json, read_json


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _triage_by_doc(data_dir: Path, triage_report: dict | None) -> dict[str, dict]:
    if triage_report is not None:
        return {row["doc_id"]: row for row in triage_report.get("docs", [])}
    triage_path = data_dir / "ocr_triage.json"
    if not triage_path.exists():
        return {}
    payload = read_json(triage_path)
    return {row["doc_id"]: row for row in payload.get("docs", [])}


def build_canonical_registry(
    data_dir: Path,
    manifest_path: Path | None = None,
    triage_report: dict | None = None,
    output_path: Path | None = None,
) -> dict:
    manifest = manifest_path or (data_dir / "manifest.jsonl")
    rows = load_manifest(manifest)
    triage_map = _triage_by_doc(data_dir=data_dir, triage_report=triage_report)

    docs_by_theorist: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        doc_id = row["doc_id"]
        triage_row = triage_map.get(doc_id, {})
        docs_by_theorist[row.get("theorist", "")].append(
            {
                "doc_id": doc_id,
                "title": row.get("title", ""),
                "source_path": row.get("source_path", ""),
                "quality_score": int(triage_row.get("quality_score", 0)),
                "doc_id_collision": bool(triage_row.get("doc_id_collision", False)),
            }
        )

    theorists_rows: list[dict] = []
    for theorist in sorted(docs_by_theorist):
        ranked = sorted(
            docs_by_theorist[theorist],
            key=lambda row: (-row["quality_score"], row["title"], row["source_path"], row["doc_id"]),
        )
        candidates: list[dict] = []
        for row in ranked[: min(3, len(ranked))]:
            note = "auto-ranked by quality_score"
            if row["doc_id_collision"]:
                note += "; doc_id collision detected in manifest"
            candidates.append(
                {
                    "doc_id": row["doc_id"],
                    "title": row["title"],
                    "source_path": row["source_path"],
                    "status": "candidate",
                    "notes": note,
                }
            )
        theorists_rows.append(
            {
                "theorist": theorist,
                "candidates": candidates,
            }
        )

    payload = {
        "generated_at": _utc_now_iso(),
        "theorists": theorists_rows,
    }
    if output_path is not None:
        atomic_write_json(output_path, payload)
    return payload

