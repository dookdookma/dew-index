from __future__ import annotations

from pathlib import Path

from pypdf import PdfReader

from .manifest import load_manifest, update_manifest_stats
from .util import ensure_dir, normalize_page_text, read_json, write_json


def _select_input_pdf(record: dict, library_dir: Path, prefer_ocr: bool) -> Path:
    source_pdf = library_dir / record["source_path"]
    ocr_pdf = Path(record["ocr_path"])
    if prefer_ocr and ocr_pdf.exists():
        return ocr_pdf
    return source_pdf


def _should_skip_extract(output_json: Path, input_pdf: Path, force: bool) -> bool:
    if force or not output_json.exists():
        return False
    return output_json.stat().st_mtime >= input_pdf.stat().st_mtime


def extract_doc_pages(
    record: dict,
    library_dir: Path,
    data_dir: Path,
    prefer_ocr: bool = True,
    force: bool = False,
) -> dict:
    pages_dir = data_dir / "pages"
    ensure_dir(pages_dir)
    output_json = pages_dir / f"{record['doc_id']}.json"
    input_pdf = _select_input_pdf(record, library_dir, prefer_ocr=prefer_ocr)
    if not input_pdf.exists():
        raise FileNotFoundError(f"Input PDF missing for doc {record['doc_id']}: {input_pdf}")

    if _should_skip_extract(output_json, input_pdf, force):
        cached = read_json(output_json)
        return {
            "doc_id": record["doc_id"],
            "page_count": int(cached.get("page_count", 0)),
            "nonempty_pages": int(cached.get("nonempty_pages", 0)),
            "avg_chars_per_page": float(cached.get("avg_chars_per_page", 0.0)),
            "status": "skipped",
        }

    reader = PdfReader(str(input_pdf))
    pages: list[dict] = []
    total_chars = 0
    nonempty_pages = 0
    for idx, page in enumerate(reader.pages, start=1):
        raw = page.extract_text() or ""
        text = normalize_page_text(raw)
        if text:
            nonempty_pages += 1
        total_chars += len(text)
        pages.append({"page": idx, "text": text})

    page_count = len(pages)
    avg_chars = (total_chars / page_count) if page_count else 0.0
    payload = {
        "doc_id": record["doc_id"],
        "theorist": record["theorist"],
        "title": record["title"],
        "source_path": record["source_path"],
        "ocr_path": record["ocr_path"],
        "page_count": page_count,
        "nonempty_pages": nonempty_pages,
        "avg_chars_per_page": avg_chars,
        "pages": pages,
    }
    write_json(output_json, payload)

    return {
        "doc_id": record["doc_id"],
        "page_count": page_count,
        "nonempty_pages": nonempty_pages,
        "avg_chars_per_page": avg_chars,
        "status": "extracted",
    }


def extract_pages_batch(
    manifest_path: Path,
    library_dir: Path,
    data_dir: Path,
    prefer_ocr: bool = True,
    force: bool = False,
) -> dict:
    rows = load_manifest(manifest_path)
    stats_by_doc: dict[str, dict] = {}
    summary = {"total": len(rows), "extracted": 0, "skipped": 0}

    for row in rows:
        stats = extract_doc_pages(
            row,
            library_dir=library_dir,
            data_dir=data_dir,
            prefer_ocr=prefer_ocr,
            force=force,
        )
        stats_by_doc[row["doc_id"]] = stats
        summary[stats["status"]] += 1

    update_manifest_stats(manifest_path, stats_by_doc)
    return summary
