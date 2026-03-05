from __future__ import annotations

from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from dewlib.chunk import build_chunks
from dewlib.extract import extract_pages_batch
from dewlib.index import build_index
from dewlib.manifest import build_manifest, load_manifest
from dewlib.search import SearchEngine


def make_pdf(path: Path, pages: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    for text in pages:
        c.drawString(72, 720, text)
        c.showPage()
    c.save()


def test_hash_embedder_hybrid_search_returns_expected_chunk(tmp_path: Path) -> None:
    library_dir = tmp_path / "library"
    data_dir = tmp_path / "data"
    page1 = "This page discusses media archaeology and communication systems."
    page2 = (
        "Unique phrase: synthetic cybernetic resonance protocol appears here "
        "with detailed explanation."
    )
    pdf_path = library_dir / "Wiener" / "cybernetics.pdf"
    make_pdf(pdf_path, [page1, page2])

    build_manifest(library_dir=library_dir, data_dir=data_dir)
    manifest_path = data_dir / "manifest.jsonl"
    extract_pages_batch(
        manifest_path=manifest_path,
        library_dir=library_dir,
        data_dir=data_dir,
        prefer_ocr=False,
    )
    build_chunks(manifest_path=manifest_path, data_dir=data_dir)
    build_index(data_dir=data_dir, backend="hash", dim=256)

    engine = SearchEngine(data_dir=data_dir)
    results = engine.search("synthetic cybernetic resonance protocol", top_k=5)
    assert results
    top = results[0]
    assert top["theorist"] == "Wiener"
    assert top["page_start"] == 2

    manifest = load_manifest(manifest_path)
    doc_id = manifest[0]["doc_id"]
    assert top["doc_id"] == doc_id

    chunk = engine.get_chunk(top["chunk_id"])
    assert chunk is not None
    assert "synthetic cybernetic resonance protocol" in chunk["text"].lower()
