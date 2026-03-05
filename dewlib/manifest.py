from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .util import read_jsonl, sha256_file, to_posix, write_jsonl


def iter_pdf_files(library_dir: Path) -> Iterable[Path]:
    for path in sorted(library_dir.rglob("*.pdf"), key=lambda p: to_posix(p)):
        if path.is_file():
            yield path


def load_manifest(manifest_path: Path) -> list[dict]:
    return read_jsonl(manifest_path)


def save_manifest(manifest_path: Path, records: list[dict]) -> None:
    ordered = sorted(records, key=lambda row: row["source_path"])
    write_jsonl(manifest_path, ordered)


def build_manifest(
    library_dir: Path,
    data_dir: Path,
    manifest_path: Path | None = None,
) -> list[dict]:
    if not library_dir.exists():
        raise FileNotFoundError(
            f"Library directory not found: {library_dir}. "
            "Create it or pass --library-dir."
        )

    manifest_file = manifest_path or (data_dir / "manifest.jsonl")
    existing = {
        row["doc_id"]: row
        for row in load_manifest(manifest_file)
        if isinstance(row, dict) and "doc_id" in row
    }

    records: list[dict] = []
    for pdf in iter_pdf_files(library_dir):
        source_sha = sha256_file(pdf)
        doc_id = source_sha[:16]
        relative = pdf.relative_to(library_dir)
        rel_posix = to_posix(relative)
        theorist = relative.parts[0] if len(relative.parts) > 1 else "unknown"
        title = pdf.stem
        mtime = int(pdf.stat().st_mtime)
        old = existing.get(doc_id, {})
        record = {
            "doc_id": doc_id,
            "source_path": rel_posix,
            "ocr_path": to_posix(data_dir / "ocr" / relative),
            "theorist": theorist,
            "title": title,
            "source_sha256": source_sha,
            "mtime": mtime,
            "page_count": int(old.get("page_count", 0) or 0),
            "nonempty_pages": int(old.get("nonempty_pages", 0) or 0),
            "avg_chars_per_page": float(old.get("avg_chars_per_page", 0.0) or 0.0),
        }
        records.append(record)

    save_manifest(manifest_file, records)
    return records


def update_manifest_stats(manifest_path: Path, stats_by_doc: dict[str, dict]) -> list[dict]:
    rows = load_manifest(manifest_path)
    updated: list[dict] = []
    for row in rows:
        stats = stats_by_doc.get(row["doc_id"])
        if stats:
            row["page_count"] = int(stats.get("page_count", row.get("page_count", 0)))
            row["nonempty_pages"] = int(
                stats.get("nonempty_pages", row.get("nonempty_pages", 0))
            )
            row["avg_chars_per_page"] = float(
                stats.get("avg_chars_per_page", row.get("avg_chars_per_page", 0.0))
            )
        updated.append(row)
    save_manifest(manifest_path, updated)
    return updated
