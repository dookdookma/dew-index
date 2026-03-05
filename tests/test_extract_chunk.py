from __future__ import annotations

import re
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from dewlib.chunk import build_chunks
from dewlib.extract import extract_pages_batch
from dewlib.manifest import build_manifest, load_manifest
from dewlib.util import read_jsonl


def make_pdf(path: Path, pages: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    for text in pages:
        c.drawString(72, 720, text)
        c.showPage()
    c.save()


def test_chunk_ids_and_page_provenance(tmp_path: Path) -> None:
    library_dir = tmp_path / "library"
    data_dir = tmp_path / "data"
    text_page1 = "alpha " * 60
    text_page2 = "beta " * 70
    pdf_path = library_dir / "TheoristA" / "work.pdf"
    make_pdf(pdf_path, [text_page1, text_page2])

    build_manifest(library_dir=library_dir, data_dir=data_dir)
    manifest_path = data_dir / "manifest.jsonl"
    extract_pages_batch(
        manifest_path=manifest_path,
        library_dir=library_dir,
        data_dir=data_dir,
        prefer_ocr=False,
    )
    build_chunks(
        manifest_path=manifest_path,
        data_dir=data_dir,
        target_chars=120,
        overlap=30,
        min_chars=40,
    )

    manifest = load_manifest(manifest_path)
    doc_id = manifest[0]["doc_id"]
    chunks = read_jsonl(data_dir / "chunks.jsonl")
    assert chunks
    pattern = re.compile(rf"^{doc_id}:(1|2):\d+$")
    for chunk in chunks:
        assert pattern.match(chunk["chunk_id"])
        assert chunk["page_start"] in (1, 2)
        assert chunk["page_end"] == chunk["page_start"]
