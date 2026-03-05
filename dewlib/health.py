from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .manifest import load_manifest
from .util import write_json


NONEMPTY_RATIO_THRESHOLD = 0.60
AVG_CHARS_THRESHOLD = 200.0


def evaluate_record(row: dict) -> dict:
    page_count = int(row.get("page_count", 0) or 0)
    nonempty_pages = int(row.get("nonempty_pages", 0) or 0)
    avg_chars = float(row.get("avg_chars_per_page", 0.0) or 0.0)
    nonempty_ratio = (nonempty_pages / page_count) if page_count else 0.0
    flags: list[str] = []
    if nonempty_ratio < NONEMPTY_RATIO_THRESHOLD:
        flags.append("low_nonempty_ratio")
    if avg_chars < AVG_CHARS_THRESHOLD:
        flags.append("low_avg_chars_per_page")
    return {
        "doc_id": row["doc_id"],
        "theorist": row["theorist"],
        "title": row["title"],
        "source_path": row["source_path"],
        "page_count": page_count,
        "nonempty_pages": nonempty_pages,
        "nonempty_ratio": nonempty_ratio,
        "avg_chars_per_page": avg_chars,
        "flags": flags,
        "flagged": bool(flags),
    }


def format_health_table(report_rows: list[dict]) -> str:
    headers = [
        ("doc_id", 18),
        ("theorist", 14),
        ("ratio", 8),
        ("avg_chars", 10),
        ("flags", 35),
    ]
    line = " ".join(name.ljust(width) for name, width in headers)
    sep = "-" * len(line)
    body = [line, sep]
    for row in report_rows:
        flags = ",".join(row["flags"]) if row["flags"] else "-"
        body.append(
            f"{row['doc_id'][:18].ljust(18)} "
            f"{row['theorist'][:14].ljust(14)} "
            f"{row['nonempty_ratio']:.2f}".ljust(8)
            + " "
            + f"{row['avg_chars_per_page']:.1f}".ljust(10)
            + " "
            + flags[:35]
        )
    return "\n".join(body)


def generate_health_report(manifest_path: Path, output_path: Path) -> dict:
    rows = load_manifest(manifest_path)
    docs = [evaluate_record(row) for row in rows]
    docs_sorted = sorted(
        docs,
        key=lambda row: (
            row["nonempty_ratio"],
            row["avg_chars_per_page"],
            row["doc_id"],
        ),
    )
    flagged = [row for row in docs_sorted if row["flagged"]]
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thresholds": {
            "nonempty_ratio_lt": NONEMPTY_RATIO_THRESHOLD,
            "avg_chars_per_page_lt": AVG_CHARS_THRESHOLD,
        },
        "summary": {
            "total_docs": len(docs_sorted),
            "flagged_docs": len(flagged),
        },
        "docs": docs_sorted,
    }
    write_json(output_path, report)
    return report
