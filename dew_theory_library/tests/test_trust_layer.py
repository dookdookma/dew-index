from __future__ import annotations

from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from dewlib.chunk import build_chunk_corpus
from dewlib.extract import extract_all_pages
from dewlib.index import build_hybrid_index
from dewlib.manifest import build_manifest
from dewlib.triage import (
    TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE,
    build_ocr_triage_report,
)
from dewlib.util import atomic_write_jsonl
from dewlib.validate import build_retrieval_validation_report


def _make_mixed_pdf(path: Path, total_pages: int = 100, text_pages: int = 50) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    dense_text = "Signal processing and mediation theory with repeated synthetic vocabulary."
    for page_index in range(total_pages):
        if page_index < text_pages:
            text_obj = c.beginText(36, 780)
            for _ in range(24):
                text_obj.textLine(dense_text)
            c.drawText(text_obj)
        c.showPage()
    c.save()


def _make_simple_pdf(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(path), pagesize=letter)
    text_obj = c.beginText(72, 760)
    for _ in range(10):
        text_obj.textLine(text)
    c.drawText(text_obj)
    c.showPage()
    c.save()


def test_triage_mixed_content_pdf_is_tier3(tmp_path: Path) -> None:
    library_root = tmp_path / "library"
    data_dir = tmp_path / "data"
    manifest_path = data_dir / "manifest.jsonl"

    _make_mixed_pdf(library_root / "Wiener" / "mixed_compilation.pdf", total_pages=100, text_pages=50)

    build_manifest(library_root, manifest_path)
    extract_all_pages(
        manifest_path=manifest_path,
        library_root=library_root,
        pages_dir=data_dir / "pages",
        ocr_dir=data_dir / "ocr",
    )

    report = build_ocr_triage_report(
        library_root=library_root,
        data_dir=data_dir,
        manifest_path=manifest_path,
    )
    assert report["docs"]
    doc = report["docs"][0]
    assert doc["triage_class"] == TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE
    assert doc["zero_ratio"] >= 0.45
    assert 55 <= doc["quality_score"] <= 75


def test_collision_detection_from_manifest_fixture(tmp_path: Path) -> None:
    library_root = tmp_path / "library"
    data_dir = tmp_path / "data"
    manifest_path = data_dir / "manifest.jsonl"
    library_root.mkdir(parents=True, exist_ok=True)

    duplicate_id = "deadbeefdeadbeef"
    rows = [
        {
            "doc_id": duplicate_id,
            "source_sha256": "deadbeef" * 8,
            "source_path": "Virilio/Virilio_A.pdf",
            "ocr_path": "Virilio/Virilio_A.pdf",
            "theorist": "Virilio",
            "title": "Virilio A",
            "mtime": 0.0,
        },
        {
            "doc_id": duplicate_id,
            "source_sha256": "deadbeef" * 8,
            "source_path": "Virilio/Virilio_B.pdf",
            "ocr_path": "Virilio/Virilio_B.pdf",
            "theorist": "Virilio",
            "title": "Virilio B",
            "mtime": 0.0,
        },
    ]
    atomic_write_jsonl(manifest_path, rows)

    report = build_ocr_triage_report(
        library_root=library_root,
        data_dir=data_dir,
        manifest_path=manifest_path,
    )
    assert len(report["collisions"]) == 1
    collision = report["collisions"][0]
    assert collision["doc_id"] == duplicate_id
    assert collision["severity"] == "HIGH"
    assert len(collision["entries"]) == 2
    assert all(doc["doc_id_collision"] for doc in report["docs"])


def test_retrieval_validation_is_deterministic(tmp_path: Path) -> None:
    library_root = tmp_path / "library"
    data_dir = tmp_path / "data"
    manifest_path = data_dir / "manifest.jsonl"
    pages_dir = data_dir / "pages"
    chunks_path = data_dir / "chunks.jsonl"
    index_dir = data_dir / "index"

    _make_simple_pdf(
        library_root / "Wiener" / "cybernetics.pdf",
        "Cybernetics studies communication and control with feedback loops.",
    )
    _make_simple_pdf(
        library_root / "McLuhan" / "media.pdf",
        "The medium is the message and media reshape sensory balance.",
    )

    build_manifest(library_root, manifest_path)
    extract_all_pages(manifest_path, library_root, pages_dir, data_dir / "ocr")
    build_chunk_corpus(manifest_path, pages_dir, chunks_path)
    build_hybrid_index(chunks_path, index_dir, embedder="hash", dim=256)

    triage = build_ocr_triage_report(
        library_root=library_root,
        data_dir=data_dir,
        manifest_path=manifest_path,
    )
    registry = [
        {
            "query_id": "wiener_feedback_exact",
            "query_text": "cybernetics communication control feedback",
            "theorist": "Wiener",
            "query_type": "exact",
            "expected_terms": ["cybernetics", "feedback"],
            "notes": "Determinism fixture query.",
        },
        {
            "query_id": "cross_media_control",
            "query_text": "medium message control communication",
            "query_type": "cross_theorist",
            "expected_terms": ["medium", "message", "control"],
            "notes": "Cross fixture query.",
        },
    ]

    first = build_retrieval_validation_report(
        data_dir=data_dir,
        query_registry=registry,
        triage_report=triage,
        top_k=5,
    )
    second = build_retrieval_validation_report(
        data_dir=data_dir,
        query_registry=registry,
        triage_report=triage,
        top_k=5,
    )

    assert first["query_count"] == second["query_count"]
    assert first["overall"]["average_retrieval_quality_score"] == second["overall"][
        "average_retrieval_quality_score"
    ]
    first_scores = {
        row["query_id"]: row["metrics"]["retrieval_quality_score"] for row in first["per_query"]
    }
    second_scores = {
        row["query_id"]: row["metrics"]["retrieval_quality_score"] for row in second["per_query"]
    }
    assert first_scores == second_scores
    first_top_chunks = {
        row["query_id"]: [item["chunk_id"] for item in row["results"]]
        for row in first["per_query"]
    }
    second_top_chunks = {
        row["query_id"]: [item["chunk_id"] for item in row["results"]]
        for row in second["per_query"]
    }
    assert first_top_chunks == second_top_chunks
