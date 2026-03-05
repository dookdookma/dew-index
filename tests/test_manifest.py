from __future__ import annotations

import hashlib
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from dewlib.manifest import build_manifest


def make_pdf(path: Path, pages: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    for text in pages:
        c.drawString(72, 720, text)
        c.showPage()
    c.save()


def test_manifest_doc_id_stable_from_source_sha(tmp_path: Path) -> None:
    library_dir = tmp_path / "library"
    data_dir = tmp_path / "data"
    pdf_path = library_dir / "Tester" / "sample.pdf"
    make_pdf(pdf_path, ["Page one text", "Page two text"])

    first = build_manifest(library_dir=library_dir, data_dir=data_dir)
    second = build_manifest(library_dir=library_dir, data_dir=data_dir)
    assert len(first) == 1
    assert len(second) == 1

    source_bytes = pdf_path.read_bytes()
    full_sha = hashlib.sha256(source_bytes).hexdigest()
    assert first[0]["source_sha256"] == full_sha
    assert first[0]["doc_id"] == full_sha[:16]
    assert second[0]["doc_id"] == first[0]["doc_id"]
