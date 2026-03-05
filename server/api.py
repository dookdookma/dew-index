from __future__ import annotations

import os
from functools import lru_cache

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from dewlib.config import PathConfig
from dewlib.health import AVG_CHARS_THRESHOLD, NONEMPTY_RATIO_THRESHOLD
from dewlib.manifest import load_manifest
from dewlib.search import SearchEngine

app = FastAPI(title="DEW Theory Library API", version="0.1.0")


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1)
    theorist: str | None = None
    top_k: int = Field(default=10, ge=1, le=100)


def _paths() -> PathConfig:
    library_dir = os.getenv("DEW_LIBRARY_DIR") or None
    data_dir = os.getenv("DEW_DATA_DIR", "data")
    manifest_path = os.getenv("DEW_MANIFEST_PATH") or None
    return PathConfig.resolve(
        library_dir=library_dir,
        data_dir=data_dir,
        manifest_path=manifest_path,
    )


@lru_cache(maxsize=1)
def _engine() -> SearchEngine:
    cfg = _paths()
    return SearchEngine(data_dir=cfg.data_dir)


@lru_cache(maxsize=1)
def _manifest_map() -> dict[str, dict]:
    cfg = _paths()
    rows = load_manifest(cfg.manifest_path)
    return {row["doc_id"]: row for row in rows}


def _doc_health_flags(doc: dict) -> list[str]:
    page_count = int(doc.get("page_count", 0) or 0)
    nonempty_pages = int(doc.get("nonempty_pages", 0) or 0)
    avg_chars = float(doc.get("avg_chars_per_page", 0.0) or 0.0)
    ratio = (nonempty_pages / page_count) if page_count else 0.0
    flags: list[str] = []
    if ratio < NONEMPTY_RATIO_THRESHOLD:
        flags.append("low_nonempty_ratio")
    if avg_chars < AVG_CHARS_THRESHOLD:
        flags.append("low_avg_chars_per_page")
    return flags


@app.post("/search")
def post_search(request: SearchRequest) -> list[dict]:
    try:
        engine = _engine()
    except Exception as exc:  # pragma: no cover - defensive API surface
        raise HTTPException(status_code=503, detail=f"Search index unavailable: {exc}") from exc
    return engine.search(query=request.query, theorist=request.theorist, top_k=request.top_k)


@app.get("/chunk/{chunk_id}")
def get_chunk(chunk_id: str) -> dict:
    try:
        engine = _engine()
    except Exception as exc:  # pragma: no cover - defensive API surface
        raise HTTPException(status_code=503, detail=f"Search index unavailable: {exc}") from exc

    chunk = engine.get_chunk(chunk_id)
    if not chunk:
        raise HTTPException(status_code=404, detail=f"Chunk not found: {chunk_id}")
    return {
        "chunk_id": chunk["chunk_id"],
        "doc_id": chunk["doc_id"],
        "theorist": chunk["theorist"],
        "title": chunk["title"],
        "page_start": chunk["page_start"],
        "page_end": chunk["page_end"],
        "text": chunk.get("text", ""),
    }


@app.get("/doc/{doc_id}")
def get_doc(doc_id: str) -> dict:
    doc = _manifest_map().get(doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail=f"Document not found: {doc_id}")
    return {
        "doc_id": doc["doc_id"],
        "theorist": doc["theorist"],
        "title": doc["title"],
        "source_path": doc["source_path"],
        "ocr_path": doc["ocr_path"],
        "page_count": doc["page_count"],
        "nonempty_pages": doc["nonempty_pages"],
        "avg_chars_per_page": doc["avg_chars_per_page"],
        "health_flags": _doc_health_flags(doc),
    }

@app.get("/health/index")
def health_index() -> dict:
    cfg = _paths()
    data_dir = cfg.data_dir
    checks = {
        "data_dir": data_dir,
        "manifest_path": cfg.manifest_path,
        "exists_manifest": os.path.exists(cfg.manifest_path),
        "exists_chunks": os.path.exists(os.path.join(data_dir, "chunks.jsonl")),
        "exists_bm25": os.path.exists(os.path.join(data_dir, "index", "bm25_tokens.json")),
        "exists_faiss": os.path.exists(os.path.join(data_dir, "index", "faiss.index")),
        "exists_meta": os.path.exists(os.path.join(data_dir, "index", "meta.jsonl")),
    }
    return checks
