from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import shutil

from .manifest import load_manifest
from .util import atomic_write_json, atomic_write_text, read_json

TIER_1_UNUSABLE_ZERO_TEXT = "TIER_1_UNUSABLE_ZERO_TEXT"
TIER_2_NEEDS_REMEDIATION_LOW_TEXT = "TIER_2_NEEDS_REMEDIATION_LOW_TEXT"
TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE = "TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE"
TIER_4_STRONG_RETRIEVAL_READY = "TIER_4_STRONG_RETRIEVAL_READY"

TIER_ORDER = {
    TIER_1_UNUSABLE_ZERO_TEXT: 1,
    TIER_2_NEEDS_REMEDIATION_LOW_TEXT: 2,
    TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE: 3,
    TIER_4_STRONG_RETRIEVAL_READY: 4,
}

NONEMPTY_MIN_CHARS = 40
MIXED_MIN_AVG_CHARS = 600.0
MIXED_MIN_ZERO_RATIO = 0.45
LOW_TEXT_NONEMPTY_RATIO_THRESHOLD = 0.60
LOW_TEXT_AVG_CHARS_THRESHOLD = 200.0
STRONG_NONEMPTY_RATIO_THRESHOLD = 0.90
STRONG_AVG_CHARS_THRESHOLD = 600.0

OCR_EXECUTABLES = ["ocrmypdf", "tesseract", "gs", "qpdf", "unpaper", "pngquant"]
OCR_FULL_REQUIRED = ["ocrmypdf", "tesseract", "gs", "qpdf", "unpaper", "pngquant"]
OCR_LIGHT_REQUIRED = ["ocrmypdf", "tesseract", "gs", "qpdf"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_health_flags(health_report_path: Path) -> dict[str, list[str]]:
    if not health_report_path.exists():
        return {}
    report = read_json(health_report_path)
    flags_by_doc: dict[str, list[str]] = {}
    for row in report.get("docs", []):
        flags_by_doc[row.get("doc_id", "")] = sorted(set(row.get("flags", [])))
    return flags_by_doc


def _manifest_collisions(rows: list[dict]) -> tuple[list[dict], set[str]]:
    by_doc: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_doc[row["doc_id"]].append(row)

    collisions: list[dict] = []
    colliding_doc_ids: set[str] = set()
    for doc_id, entries in sorted(by_doc.items(), key=lambda item: item[0]):
        if len(entries) <= 1:
            continue
        colliding_doc_ids.add(doc_id)
        normalized_entries = sorted(
            [
                {
                    "theorist": entry.get("theorist", ""),
                    "title": entry.get("title", ""),
                    "source_path": entry.get("source_path", ""),
                }
                for entry in entries
            ],
            key=lambda entry: (
                entry["theorist"],
                entry["title"],
                entry["source_path"],
            ),
        )
        collisions.append(
            {
                "doc_id": doc_id,
                "entries": normalized_entries,
                "severity": "HIGH",
            }
        )
    return collisions, colliding_doc_ids


def _mixed_content_nonempty_min(page_count: int) -> int:
    return 100 if page_count >= 200 else 50


def _is_mixed_content(page_count: int, nonempty_pages: int, avg_chars: float, zero_ratio: float) -> bool:
    return (
        nonempty_pages >= _mixed_content_nonempty_min(page_count)
        and avg_chars >= MIXED_MIN_AVG_CHARS
        and zero_ratio >= MIXED_MIN_ZERO_RATIO
    )


def _load_page_lengths(pages_path: Path) -> tuple[int, list[int]]:
    payload = read_json(pages_path)
    page_rows = payload.get("pages", [])
    lengths: list[int] = []
    if isinstance(page_rows, list):
        for row in page_rows:
            text = ""
            if isinstance(row, dict):
                text = str(row.get("text") or "")
            lengths.append(len(text))
    declared_count = int(payload.get("page_count") or 0)
    page_count = declared_count or len(lengths)
    if len(lengths) < page_count:
        lengths.extend([0] * (page_count - len(lengths)))
    elif len(lengths) > page_count:
        page_count = len(lengths)
    return page_count, lengths


def _classify_doc(doc: dict) -> str:
    if not doc["has_pages_json"]:
        return TIER_1_UNUSABLE_ZERO_TEXT
    if doc["nonempty_pages"] == 0 or doc["avg_chars_per_page"] == 0.0:
        return TIER_1_UNUSABLE_ZERO_TEXT
    if _is_mixed_content(
        page_count=doc["page_count"],
        nonempty_pages=doc["nonempty_pages"],
        avg_chars=doc["avg_chars_per_page"],
        zero_ratio=doc["zero_ratio"],
    ):
        return TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE
    if (
        doc["nonempty_pages"] > 0
        and (
            doc["nonempty_ratio"] < LOW_TEXT_NONEMPTY_RATIO_THRESHOLD
            or doc["avg_chars_per_page"] < LOW_TEXT_AVG_CHARS_THRESHOLD
        )
    ):
        return TIER_2_NEEDS_REMEDIATION_LOW_TEXT
    if (
        doc["nonempty_ratio"] >= STRONG_NONEMPTY_RATIO_THRESHOLD
        and doc["avg_chars_per_page"] >= STRONG_AVG_CHARS_THRESHOLD
    ):
        return TIER_4_STRONG_RETRIEVAL_READY
    # Intermediate-quality docs are still retrieval-usable; classify as retrieval-ready.
    return TIER_4_STRONG_RETRIEVAL_READY


def _clamp(value: int, lo: int, hi: int) -> int:
    return max(lo, min(value, hi))


def _quality_score(doc: dict, tier: str) -> int:
    if not doc["has_pages_json"]:
        return 0

    if tier == TIER_1_UNUSABLE_ZERO_TEXT:
        text_signal = min(doc["avg_chars_per_page"] / LOW_TEXT_AVG_CHARS_THRESHOLD, 1.0)
        ratio_signal = min(doc["nonempty_ratio"], 1.0)
        score = int(round(10.0 * ((text_signal + ratio_signal) / 2.0)))
        score = _clamp(score, 0, 10)
    elif tier == TIER_2_NEEDS_REMEDIATION_LOW_TEXT:
        ratio_part = min(max(doc["nonempty_ratio"], 0.0), LOW_TEXT_NONEMPTY_RATIO_THRESHOLD)
        ratio_part = ratio_part / LOW_TEXT_NONEMPTY_RATIO_THRESHOLD
        avg_part = min(max(doc["avg_chars_per_page"], 0.0), LOW_TEXT_AVG_CHARS_THRESHOLD)
        avg_part = avg_part / LOW_TEXT_AVG_CHARS_THRESHOLD
        score = 20 + int(round(25.0 * ((ratio_part + avg_part) / 2.0)))
        score = _clamp(score, 20, 45)
    elif tier == TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE:
        expected_nonempty = _mixed_content_nonempty_min(doc["page_count"])
        nonempty_part = min(doc["nonempty_pages"] / max(expected_nonempty, 1), 1.0)
        avg_part = min(doc["avg_chars_per_page"] / 1600.0, 1.0)
        zero_part = min(doc["zero_ratio"] / 0.70, 1.0)
        score = 55 + int(round(20.0 * ((nonempty_part + avg_part + zero_part) / 3.0)))
        score = _clamp(score, 55, 75)
    else:
        ratio_floor = LOW_TEXT_NONEMPTY_RATIO_THRESHOLD
        ratio_part = (min(max(doc["nonempty_ratio"], ratio_floor), 1.0) - ratio_floor) / (
            1.0 - ratio_floor
        )
        avg_part = min(doc["avg_chars_per_page"] / 2200.0, 1.0)
        score = 80 + int(round(20.0 * ((ratio_part + avg_part) / 2.0)))
        score = _clamp(score, 80, 100)

    if doc["doc_id_collision"]:
        score = max(0, score - 20)
    return score


def _ocr_tooling_report() -> dict:
    executable_presence = {tool: bool(shutil.which(tool)) for tool in OCR_EXECUTABLES}
    full_feasible = all(executable_presence[tool] for tool in OCR_FULL_REQUIRED)
    light_feasible = all(executable_presence[tool] for tool in OCR_LIGHT_REQUIRED)

    if full_feasible:
        recommended_profile = "FULL"
        recommendation = "FULL profile is feasible (includes unpaper + pngquant)."
    elif light_feasible:
        recommended_profile = "LIGHT"
        recommendation = "LIGHT profile is feasible (--deskew --rotate-pages --optimize 0)."
    else:
        recommended_profile = "NONE"
        recommendation = "OCR profile prerequisites are incomplete in PATH."

    return {
        "executables": executable_presence,
        "profiles": {
            "FULL": {
                "feasible": full_feasible,
                "requires": OCR_FULL_REQUIRED,
                "description": "Requires unpaper + pngquant in addition to core OCR tools.",
            },
            "LIGHT": {
                "feasible": light_feasible,
                "requires": OCR_LIGHT_REQUIRED,
                "description": "Uses --deskew --rotate-pages --optimize 0 (no unpaper/pngquant).",
            },
        },
        "recommended_profile": recommended_profile,
        "recommendation": recommendation,
    }


def build_ocr_triage_report(
    library_root: Path,
    data_dir: Path,
    manifest_path: Path | None = None,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
) -> dict:
    manifest = manifest_path or (data_dir / "manifest.jsonl")
    pages_dir = data_dir / "pages"
    ocr_dir = data_dir / "ocr"
    health_report_path = data_dir / "health_report.json"

    rows = load_manifest(manifest)
    collisions, colliding_doc_ids = _manifest_collisions(rows)
    health_flags = _load_health_flags(health_report_path)

    docs: list[dict] = []
    for row in sorted(rows, key=lambda item: (item.get("source_path", ""), item["doc_id"])):
        doc_id = row["doc_id"]
        pages_path = pages_dir / f"{doc_id}.json"
        has_pages_json = pages_path.exists()

        if has_pages_json:
            page_count, lengths = _load_page_lengths(pages_path)
            total_chars = sum(lengths)
            nonempty_pages = sum(1 for length in lengths if length >= NONEMPTY_MIN_CHARS)
            zero_pages = sum(1 for length in lengths if length == 0)
            small_pages = sum(1 for length in lengths if 1 <= length <= 39)
            avg_chars_per_page = (total_chars / page_count) if page_count else 0.0
        else:
            page_count = int(row.get("page_count") or 0)
            nonempty_pages = int(row.get("nonempty_pages") or 0)
            avg_chars_per_page = float(row.get("avg_chars_per_page") or 0.0)
            zero_pages = max(page_count - nonempty_pages, 0)
            small_pages = 0

        nonempty_ratio = (nonempty_pages / page_count) if page_count else 0.0
        zero_ratio = (zero_pages / page_count) if page_count else 0.0

        ocr_rel = row.get("ocr_path") or row.get("source_path", "")
        has_ocr_output = bool(ocr_rel) and (ocr_dir / ocr_rel).exists()
        doc = {
            "doc_id": doc_id,
            "theorist": row.get("theorist", ""),
            "title": row.get("title", ""),
            "source_path": row.get("source_path", ""),
            "ocr_path": ocr_rel,
            "page_count": page_count,
            "nonempty_pages": nonempty_pages,
            "nonempty_ratio": nonempty_ratio,
            "avg_chars_per_page": avg_chars_per_page,
            "zero_pages": zero_pages,
            "zero_ratio": zero_ratio,
            "small_pages": small_pages,
            "has_ocr_output": has_ocr_output,
            "has_pages_json": has_pages_json,
            "doc_id_collision": doc_id in colliding_doc_ids,
            "current_health_flags": health_flags.get(doc_id, []),
        }

        triage_class = _classify_doc(doc)
        if not has_pages_json:
            triage_class = TIER_1_UNUSABLE_ZERO_TEXT
        quality_score = _quality_score(doc, triage_class)
        if not has_pages_json:
            quality_score = 0

        docs.append(
            {
                **doc,
                "triage_class": triage_class,
                "quality_score": quality_score,
            }
        )

    docs_sorted = sorted(
        docs,
        key=lambda row: (
            TIER_ORDER[row["triage_class"]],
            row["quality_score"],
            row["theorist"],
            row["title"],
            row["doc_id"],
        ),
    )

    tier_counts = {
        tier: sum(1 for row in docs_sorted if row["triage_class"] == tier)
        for tier in (
            TIER_1_UNUSABLE_ZERO_TEXT,
            TIER_2_NEEDS_REMEDIATION_LOW_TEXT,
            TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE,
            TIER_4_STRONG_RETRIEVAL_READY,
        )
    }
    remediation_priorities = [
        row
        for row in docs_sorted
        if row["triage_class"] in {TIER_1_UNUSABLE_ZERO_TEXT, TIER_2_NEEDS_REMEDIATION_LOW_TEXT}
    ]
    tooling = _ocr_tooling_report()

    report = {
        "generated_at": _utc_now_iso(),
        "library_root": str(library_root),
        "data_dir": str(data_dir),
        "thresholds": {
            "nonempty_min_chars": NONEMPTY_MIN_CHARS,
            "tier2_nonempty_ratio_lt": LOW_TEXT_NONEMPTY_RATIO_THRESHOLD,
            "tier2_avg_chars_per_page_lt": LOW_TEXT_AVG_CHARS_THRESHOLD,
            "tier3_zero_ratio_gte": MIXED_MIN_ZERO_RATIO,
            "tier3_avg_chars_per_page_gte": MIXED_MIN_AVG_CHARS,
            "tier4_nonempty_ratio_gte": STRONG_NONEMPTY_RATIO_THRESHOLD,
            "tier4_avg_chars_per_page_gte": STRONG_AVG_CHARS_THRESHOLD,
        },
        "ocr_tooling": tooling,
        "summary": {
            "total_docs": len(docs_sorted),
            "collisions": len(collisions),
            "tier_counts": tier_counts,
            "remediation_count": len(remediation_priorities),
        },
        "collisions": collisions,
        "remediation_priorities": [
            {
                "doc_id": row["doc_id"],
                "theorist": row["theorist"],
                "title": row["title"],
                "source_path": row["source_path"],
                "triage_class": row["triage_class"],
                "quality_score": row["quality_score"],
            }
            for row in remediation_priorities
        ],
        "docs": docs_sorted,
    }

    if output_json_path is not None:
        atomic_write_json(output_json_path, report)
    if output_md_path is not None:
        atomic_write_text(output_md_path, render_ocr_triage_markdown(report))
    return report


def render_ocr_triage_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append("# DEW Corpus OCR Triage")
    lines.append("")
    lines.append(f"- Generated at: `{report['generated_at']}`")
    lines.append(f"- Total docs: **{report['summary']['total_docs']}**")
    lines.append(f"- Collisions: **{report['summary']['collisions']}**")
    lines.append("")
    lines.append("## OCR Tool Availability")
    for tool, present in report["ocr_tooling"]["executables"].items():
        status = "yes" if present else "no"
        lines.append(f"- `{tool}`: **{status}**")
    lines.append("")
    lines.append("### OCR Profiles")
    lines.append(
        f"- Recommended profile: **{report['ocr_tooling']['recommended_profile']}** "
        f"({report['ocr_tooling']['recommendation']})"
    )
    lines.append("- FULL profile requires `unpaper` + `pngquant` plus core OCR tools.")
    lines.append("- LIGHT profile uses `--deskew --rotate-pages --optimize 0`.")
    lines.append("")
    lines.append("## Tier Summary")
    lines.append("| Tier | Count |")
    lines.append("| --- | ---: |")
    for tier, count in report["summary"]["tier_counts"].items():
        lines.append(f"| `{tier}` | {count} |")
    lines.append("")

    lines.append("## Collisions")
    if not report["collisions"]:
        lines.append("- No repeated `doc_id` collisions detected in manifest.")
    else:
        for collision in report["collisions"]:
            lines.append(f"- **{collision['doc_id']}** severity={collision['severity']}")
            for entry in collision["entries"]:
                lines.append(
                    f"  - {entry['theorist']} | {entry['title']} | `{entry['source_path']}`"
                )
    lines.append("")

    lines.append("## Top Remediation Priorities")
    lines.append("- Ordered by tier (Tier 1 then Tier 2) and lowest quality score first.")
    lines.append("- Tier 3 mixed-content docs are excluded from remediation priority.")
    priorities = report.get("remediation_priorities", [])
    if not priorities:
        lines.append("- None")
    else:
        for row in priorities[:20]:
            lines.append(
                f"- `{row['doc_id']}` | {row['theorist']} | {row['title']} | "
                f"{row['triage_class']} | score={row['quality_score']} | `{row['source_path']}`"
            )
    lines.append("")
    lines.append("## Notes")
    lines.append(
        "- `TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE` is treated as usable and not an OCR failure."
    )
    return "\n".join(lines) + "\n"

