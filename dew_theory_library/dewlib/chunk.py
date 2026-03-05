from __future__ import annotations

from pathlib import Path

from .manifest import load_manifest
from .util import atomic_write_jsonl, chunk_text_with_overlap, read_json, sha256_text_16


def _latest_mtime(paths: list[Path]) -> float:
    mtimes = [path.stat().st_mtime for path in paths if path.exists()]
    return max(mtimes) if mtimes else 0.0


def build_chunk_corpus(
    manifest_path: Path,
    pages_dir: Path,
    chunks_path: Path,
    target_chars: int = 1200,
    overlap: int = 200,
    min_chars: int = 40,
    force: bool = False,
) -> dict:
    manifest_rows = load_manifest(manifest_path)
    page_files = [pages_dir / f"{row['doc_id']}.json" for row in manifest_rows]

    if (
        (not force)
        and chunks_path.exists()
        and chunks_path.stat().st_mtime >= _latest_mtime(page_files)
    ):
        return {"status": "skipped", "docs": len(manifest_rows), "chunks": 0}

    chunks: list[dict] = []
    for doc in manifest_rows:
        page_path = pages_dir / f"{doc['doc_id']}.json"
        if not page_path.exists():
            continue
        payload = read_json(page_path)
        for page in payload.get("pages", []):
            page_num = int(page["page"])
            text = (page.get("text") or "").strip()
            if len(text) < min_chars:
                continue
            parts = chunk_text_with_overlap(text, target_chars=target_chars, overlap=overlap)
            for k, part in enumerate(parts):
                chunks.append(
                    {
                        "chunk_id": f"{doc['doc_id']}:{page_num}:{k}",
                        "doc_id": doc["doc_id"],
                        "theorist": doc["theorist"],
                        "title": doc["title"],
                        "source_path": doc["source_path"],
                        "ocr_path": doc["ocr_path"],
                        "page_start": page_num,
                        "page_end": page_num,
                        "text": part,
                        "text_hash": sha256_text_16(part),
                    }
                )

    atomic_write_jsonl(chunks_path, chunks)
    return {"status": "built", "docs": len(manifest_rows), "chunks": len(chunks)}
