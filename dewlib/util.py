from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable


TOKEN_RE = re.compile(r"[a-z0-9]+")


def detect_library_dir() -> Path:
    for candidate in ("library", "dew_theory_library"):
        path = Path(candidate)
        if path.exists() and path.is_dir():
            return path
    return Path("library")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def to_posix(path: Path | str) -> str:
    return str(path).replace("\\", "/")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    with NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=path.parent) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        tmp.write("\n")
    Path(tmp.name).replace(path)


def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    ensure_dir(path.parent)
    with NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=path.parent) as tmp:
        for row in rows:
            tmp.write(json.dumps(row, ensure_ascii=False))
            tmp.write("\n")
    Path(tmp.name).replace(path)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def normalize_page_text(text: str) -> str:
    cleaned = (text or "").replace("\x00", "")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for raw in cleaned.split("\n"):
        line = re.sub(r"\s+", " ", raw).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall((text or "").lower())


def chunk_text_with_overlap(text: str, target_chars: int, overlap: int) -> list[str]:
    if not text:
        return []
    if len(text) <= target_chars:
        return [text]
    step = max(1, target_chars - overlap)
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + target_chars)
        piece = text[start:end].strip()
        if piece:
            chunks.append(piece)
        if end >= len(text):
            break
        start += step
    return chunks
