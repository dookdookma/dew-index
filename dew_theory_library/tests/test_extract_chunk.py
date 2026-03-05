from __future__ import annotations

import re
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from dewlib.chunk import build_chunk_corpus
from dewlib.extract import extract_all_pages
from dewlib.manifest import build_manifest, load_manifest
from dewlib.util import read_jsonl


def _make_pdf(path: Path, pages: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    for text in pages:
        c.drawString(72, 720, text)
        c.showPage()
    c.save()


def test_chunk_id_and_page_provenance(tmp_path: Path) -> None:
    library_root = tmp_path / "library"
    data_dir = tmp_path / "data"
    manifest_path = data_dir / "manifest.jsonl"
    pages_dir = data_dir / "pages"
    chunks_path = data_dir / "chunks.jsonl"
    ocr_dir = data_dir / "ocr"

    p1 = "alpha " * 60
    p2 = "beta " * 80
    _make_pdf(library_root / "TheoristB" / "sample.pdf", [p1, p2])

    build_manifest(library_root, manifest_path)
    extract_all_pages(manifest_path, library_root, pages_dir, ocr_dir)
    build_chunk_corpus(
        manifest_path=manifest_path,
        pages_dir=pages_dir,
        chunks_path=chunks_path,
        target_chars=100,
        overlap=20,
        min_chars=40,
    )

    manifest = load_manifest(manifest_path)
    doc_id = manifest[0]["doc_id"]
    rows = read_jsonl(chunks_path)
    assert rows
    pattern = re.compile(rf"^{doc_id}:(1|2):\d+$")
    for row in rows:
        assert pattern.match(row["chunk_id"])
        assert row["page_start"] == row["page_end"]
        assert row["source_path"] == "TheoristB/sample.pdf"
        assert row["ocr_path"] == "TheoristB/sample.pdf"
        assert len(row["text_hash"]) == 16
