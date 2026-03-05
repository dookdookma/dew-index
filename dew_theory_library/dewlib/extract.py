from __future__ import annotations

from pathlib import Path

from .manifest import load_manifest, update_manifest_stats
from .util import atomic_write_json, ensure_dir, normalize_page_text, read_json


def _open_pdf(path: Path):
    try:
        import fitz
    except ImportError as exc:
        raise RuntimeError(
            "PyMuPDF (fitz) is required for extraction. Install 'pymupdf'."
        ) from exc
    return fitz.open(path)


def _input_pdf_path(record: dict, library_root: Path, ocr_dir: Path) -> Path:
    ocr_pdf = ocr_dir / record["ocr_path"]
    if ocr_pdf.exists():
        return ocr_pdf
    return library_root / record["source_path"]


def _extract_one(record: dict, library_root: Path, pages_dir: Path, ocr_dir: Path, force: bool) -> dict:
    source_pdf = library_root / record["source_path"]
    if not source_pdf.exists():
        raise FileNotFoundError(f"Missing source PDF: {source_pdf}")

    ensure_dir(pages_dir)
    output_path = pages_dir / f"{record['doc_id']}.json"
    input_pdf = _input_pdf_path(record, library_root=library_root, ocr_dir=ocr_dir)

    if (not force) and output_path.exists() and output_path.stat().st_mtime >= input_pdf.stat().st_mtime:
        cached = read_json(output_path)
        return {
            "doc_id": record["doc_id"],
            "page_count": int(cached.get("page_count", 0)),
            "nonempty_pages": int(cached.get("nonempty_pages", 0)),
            "avg_chars_per_page": float(cached.get("avg_chars_per_page", 0.0)),
            "status": "skipped",
        }

    pages: list[dict] = []
    nonempty_pages = 0
    total_chars = 0

    with _open_pdf(input_pdf) as doc:
        for i in range(doc.page_count):
            page = doc.load_page(i)
            text = normalize_page_text(page.get_text("text"))
            text_len = len(text)
            if text_len >= 40:
                nonempty_pages += 1
            total_chars += text_len
            pages.append({"page": i + 1, "text": text})

    page_count = len(pages)
    avg_chars = float(total_chars / page_count) if page_count else 0.0
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
    atomic_write_json(output_path, payload)
    return {
        "doc_id": record["doc_id"],
        "page_count": page_count,
        "nonempty_pages": nonempty_pages,
        "avg_chars_per_page": avg_chars,
        "status": "extracted",
    }


def extract_all_pages(
    manifest_path: Path,
    library_root: Path,
    pages_dir: Path,
    ocr_dir: Path,
    force: bool = False,
) -> dict:
    rows = load_manifest(manifest_path)
    stats_by_doc_id: dict[str, dict] = {}
    summary = {"total": len(rows), "extracted": 0, "skipped": 0}

    for row in rows:
        stats = _extract_one(
            row,
            library_root=library_root,
            pages_dir=pages_dir,
            ocr_dir=ocr_dir,
            force=force,
        )
        stats_by_doc_id[row["doc_id"]] = stats
        summary[stats["status"]] += 1

    update_manifest_stats(manifest_path, stats_by_doc_id)
    return summary
