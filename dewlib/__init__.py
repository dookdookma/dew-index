"""DEW theory library package."""

from .chunk import build_chunks
from .extract import extract_pages_batch
from .health import generate_health_report
from .index import build_index
from .manifest import build_manifest, load_manifest
from .search import SearchEngine

__all__ = [
    "SearchEngine",
    "build_chunks",
    "build_index",
    "build_manifest",
    "extract_pages_batch",
    "generate_health_report",
    "load_manifest",
]

__version__ = "0.1.0"
