from __future__ import annotations

from pathlib import Path

from .util import read_jsonl


def _index_meta_path(data_dir: Path) -> Path:
    return data_dir / "index" / "meta.jsonl"


def _chunks_path(data_dir: Path) -> Path:
    return data_dir / "chunks.jsonl"


def first_chunk_id(data_dir: Path) -> str | None:
    meta_rows = read_jsonl(_index_meta_path(data_dir))
    if meta_rows:
        return str(meta_rows[0].get("chunk_id") or "")
    chunk_rows = read_jsonl(_chunks_path(data_dir))
    if chunk_rows:
        return str(chunk_rows[0].get("chunk_id") or "")
    return None


def _find_by_chunk_id(path: Path, chunk_id: str) -> dict | None:
    for row in read_jsonl(path):
        if row.get("chunk_id") == chunk_id:
            return row
    return None


def resolve_chunk_provenance(data_dir: Path, chunk_id: str) -> dict | None:
    meta_row = _find_by_chunk_id(_index_meta_path(data_dir), chunk_id)
    chunk_row = _find_by_chunk_id(_chunks_path(data_dir), chunk_id)
    if meta_row is None and chunk_row is None:
        return None

    source = meta_row or chunk_row or {}
    chunk_source = chunk_row or {}

    quote = str(chunk_source.get("text") or source.get("text") or "")
    text_hash = str(chunk_source.get("text_hash") or source.get("text_hash") or "")

    return {
        "chunk_id": chunk_id,
        "doc_id": str(source.get("doc_id") or ""),
        "theorist": str(source.get("theorist") or ""),
        "title": str(source.get("title") or ""),
        "source_path": str(source.get("source_path") or ""),
        "ocr_path": str(source.get("ocr_path") or ""),
        "page_start": int(source.get("page_start") or chunk_source.get("page_start") or 0),
        "page_end": int(source.get("page_end") or chunk_source.get("page_end") or 0),
        "text_hash": text_hash,
        "quote": quote,
    }

