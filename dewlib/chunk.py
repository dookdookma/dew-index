from __future__ import annotations

from pathlib import Path

from .manifest import load_manifest
from .util import chunk_text_with_overlap, read_json, write_jsonl


def _latest_mtime(paths: list[Path]) -> float:
    if not paths:
        return 0.0
    return max(path.stat().st_mtime for path in paths if path.exists())


def _should_skip_chunk_build(chunks_path: Path, pages_files: list[Path], force: bool) -> bool:
    if force or not chunks_path.exists():
        return False
    return chunks_path.stat().st_mtime >= _latest_mtime(pages_files)


def build_chunks(
    manifest_path: Path,
    data_dir: Path,
    target_chars: int = 1200,
    overlap: int = 200,
    min_chars: int = 40,
    force: bool = False,
) -> dict:
    chunks_path = data_dir / "chunks.jsonl"
    manifest_rows = load_manifest(manifest_path)
    pages_files = [data_dir / "pages" / f"{row['doc_id']}.json" for row in manifest_rows]

    if _should_skip_chunk_build(chunks_path, pages_files, force=force):
        return {"status": "skipped", "docs": len(manifest_rows), "chunks": 0}

    rows: list[dict] = []
    for doc in manifest_rows:
        pages_path = data_dir / "pages" / f"{doc['doc_id']}.json"
        if not pages_path.exists():
            continue
        payload = read_json(pages_path)
        for page in payload.get("pages", []):
            page_num = int(page["page"])
            text = (page.get("text") or "").strip()
            if len(text) < min_chars:
                continue
            parts = chunk_text_with_overlap(text, target_chars=target_chars, overlap=overlap)
            for k, part in enumerate(parts):
                rows.append(
                    {
                        "chunk_id": f"{doc['doc_id']}:{page_num}:{k}",
                        "doc_id": doc["doc_id"],
                        "theorist": doc["theorist"],
                        "title": doc["title"],
                        "page_start": page_num,
                        "page_end": page_num,
                        "text": part,
                    }
                )

    write_jsonl(chunks_path, rows)
    return {"status": "built", "docs": len(manifest_rows), "chunks": len(rows)}
