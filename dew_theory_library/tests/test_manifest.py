from __future__ import annotations

import hashlib
from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from dewlib.manifest import build_manifest


def _make_pdf(path: Path, pages: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    for text in pages:
        c.drawString(72, 720, text)
        c.showPage()
    c.save()


def test_doc_id_is_stable_from_source_bytes(tmp_path: Path) -> None:
    library_root = tmp_path / "library"
    manifest_path = tmp_path / "data" / "manifest.jsonl"
    pdf_path = library_root / "TheoristA" / "test.pdf"
    _make_pdf(pdf_path, ["page one", "page two"])

    first = build_manifest(library_root, manifest_path)
    second = build_manifest(library_root, manifest_path)

    assert len(first) == 1
    assert len(second) == 1
    source_sha = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    assert first[0]["source_sha256"] == source_sha
    assert first[0]["doc_id"] == source_sha[:16]
    assert second[0]["doc_id"] == first[0]["doc_id"]
