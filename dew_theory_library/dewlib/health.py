from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .manifest import load_manifest
from .util import atomic_write_json


NONEMPTY_RATIO_THRESHOLD = 0.60
AVG_CHARS_THRESHOLD = 200.0
NO_OCR_HEURISTIC_THRESHOLD = 50.0


def _evaluate_doc(row: dict, ocr_dir: Path) -> dict:
    page_count = int(row.get("page_count") or 0)
    nonempty_pages = int(row.get("nonempty_pages") or 0)
    avg_chars = float(row.get("avg_chars_per_page") or 0.0)
    nonempty_ratio = (nonempty_pages / page_count) if page_count else 0.0

    flags: list[str] = []
    if nonempty_ratio < NONEMPTY_RATIO_THRESHOLD:
        flags.append("low_nonempty_ratio")
    if avg_chars < AVG_CHARS_THRESHOLD:
        flags.append("low_avg_chars_per_page")

    ocr_pdf = ocr_dir / row["ocr_path"]
    if (not ocr_pdf.exists()) and avg_chars < NO_OCR_HEURISTIC_THRESHOLD:
        flags.append("no_ocr_output")

    return {
        "doc_id": row["doc_id"],
        "theorist": row["theorist"],
        "title": row["title"],
        "source_path": row["source_path"],
        "ocr_path": row["ocr_path"],
        "page_count": page_count,
        "nonempty_pages": nonempty_pages,
        "nonempty_ratio": nonempty_ratio,
        "avg_chars_per_page": avg_chars,
        "flags": sorted(set(flags)),
    }


def build_health_report(manifest_path: Path, ocr_dir: Path, output_path: Path) -> dict:
    rows = load_manifest(manifest_path)
    docs = [_evaluate_doc(row, ocr_dir=ocr_dir) for row in rows]
    docs_sorted = sorted(
        docs,
        key=lambda row: (
            0 if row["flags"] else 1,
            row["nonempty_ratio"],
            row["avg_chars_per_page"],
            row["doc_id"],
        ),
    )
    flagged = [doc for doc in docs_sorted if doc["flags"]]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thresholds": {
            "nonempty_ratio_lt": NONEMPTY_RATIO_THRESHOLD,
            "avg_chars_per_page_lt": AVG_CHARS_THRESHOLD,
            "no_ocr_output_avg_chars_lt": NO_OCR_HEURISTIC_THRESHOLD,
        },
        "summary": {
            "total_docs": len(docs_sorted),
            "flagged_docs": len(flagged),
        },
        "docs": docs_sorted,
    }
    atomic_write_json(output_path, report)
    return report


def render_health_table(docs: list[dict]) -> str:
    lines = [
        "doc_id            theorist       ratio  avg_chars  flags",
        "---------------------------------------------------------------",
    ]
    for doc in docs:
        flags = ",".join(doc["flags"]) if doc["flags"] else "-"
        lines.append(
            f"{doc['doc_id'][:16]:16} "
            f"{doc['theorist'][:13]:13} "
            f"{doc['nonempty_ratio']:.2f}  "
            f"{doc['avg_chars_per_page']:.1f}     "
            f"{flags}"
        )
    return "\n".join(lines)
