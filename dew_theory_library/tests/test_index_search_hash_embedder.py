from __future__ import annotations

from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from dewlib.chunk import build_chunk_corpus
from dewlib.extract import extract_all_pages
from dewlib.index import build_hybrid_index, load_index_artifacts
from dewlib.manifest import build_manifest
from dewlib.search import SearchService
from dewlib.util import read_jsonl


def _make_pdf(path: Path, pages: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    for text in pages:
        c.drawString(72, 720, text)
        c.showPage()
    c.save()


def test_index_alignment_and_search_determinism(tmp_path: Path) -> None:
    library_root = tmp_path / "library"
    data_dir = tmp_path / "data"
    manifest_path = data_dir / "manifest.jsonl"
    pages_dir = data_dir / "pages"
    chunks_path = data_dir / "chunks.jsonl"
    ocr_dir = data_dir / "ocr"
    index_dir = data_dir / "index"

    _make_pdf(
        library_root / "Wiener" / "cybernetics.pdf",
        [
            "Intro text about systems and communication." * 8,
            "Unique phrase synthetic cybernetic resonance protocol appears here." * 8,
        ],
    )

    build_manifest(library_root, manifest_path)
    extract_all_pages(manifest_path, library_root, pages_dir, ocr_dir)
    build_chunk_corpus(manifest_path, pages_dir, chunks_path)
    build_hybrid_index(chunks_path, index_dir, embedder="hash", dim=256)

    chunks = read_jsonl(chunks_path)
    loaded = load_index_artifacts(index_dir)
    assert len(loaded["meta"]) == len(chunks)
    assert len(loaded["tokenized"]) == len(chunks)
    assert loaded["faiss_index"].ntotal == len(chunks)

    service = SearchService(data_dir=data_dir)
    query = "synthetic cybernetic resonance protocol"
    first = service.search(query, top_k=5)
    second = service.search(query, top_k=5)
    assert [row["chunk_id"] for row in first] == [row["chunk_id"] for row in second]
    assert first
    assert "synthetic cybernetic resonance protocol" in first[0]["excerpt"].lower()
