"""DEW Theory Library v1."""

from .chunk import build_chunk_corpus
from .canon import build_canonical_registry
from .extract import extract_all_pages
from .health import build_health_report
from .index import build_hybrid_index
from .manifest import build_manifest
from .search import SearchService
from .triage import build_ocr_triage_report
from .validate import build_retrieval_validation_report

__all__ = [
    "SearchService",
    "build_canonical_registry",
    "build_chunk_corpus",
    "build_health_report",
    "build_hybrid_index",
    "build_manifest",
    "build_ocr_triage_report",
    "build_retrieval_validation_report",
    "extract_all_pages",
]

__version__ = "0.1.0"
