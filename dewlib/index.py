from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
from rank_bm25 import BM25Okapi

from .util import read_json, read_jsonl, tokenize, write_json, write_jsonl


class HashEmbedder:
    backend = "hash"

    def __init__(self, dim: int = 384) -> None:
        self.dim = dim

    def encode(self, texts: list[str]) -> np.ndarray:
        vectors = np.zeros((len(texts), self.dim), dtype=np.float32)
        for row_idx, text in enumerate(texts):
            tokens = tokenize(text)
            if not tokens:
                continue
            for token in tokens:
                digest = hashlib.blake2b(token.encode("utf-8"), digest_size=16).digest()
                idx = int.from_bytes(digest[:8], "big") % self.dim
                sign = 1.0 if digest[8] % 2 == 0 else -1.0
                vectors[row_idx, idx] += sign
        return normalize_rows(vectors)

    def to_config(self) -> dict:
        return {"backend": self.backend, "dim": self.dim}


class SentenceTransformersEmbedder:
    backend = "sentence_transformers"

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        allow_download: bool = False,
    ) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers backend requested but package is not installed. "
                "Install optional dependency or use --backend hash."
            ) from exc

        self.model_name = model_name
        self.model = SentenceTransformer(
            model_name,
            local_files_only=not allow_download,
        )
        self.dim = int(self.model.get_sentence_embedding_dimension())

    def encode(self, texts: list[str]) -> np.ndarray:
        vectors = self.model.encode(
            texts,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return np.asarray(vectors, dtype=np.float32)

    def to_config(self) -> dict:
        return {
            "backend": self.backend,
            "dim": self.dim,
            "model_name": self.model_name,
        }


def normalize_rows(vectors: np.ndarray) -> np.ndarray:
    if vectors.size == 0:
        return vectors.astype(np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return (vectors / norms).astype(np.float32)


def create_embedder(
    backend: str = "hash",
    dim: int = 384,
    model_name: str = "all-MiniLM-L6-v2",
    allow_download: bool = False,
):
    if backend == "hash":
        return HashEmbedder(dim=dim)
    if backend == "sentence_transformers":
        return SentenceTransformersEmbedder(
            model_name=model_name,
            allow_download=allow_download,
        )
    raise ValueError(f"Unsupported embedder backend: {backend}")


def _faiss_module():
    try:
        import faiss
    except ImportError as exc:
        raise RuntimeError(
            "faiss-cpu is required to build/search the vector index. "
            "Install faiss-cpu and retry."
        ) from exc
    return faiss


def _index_files(index_dir: Path) -> dict[str, Path]:
    return {
        "meta": index_dir / "meta.jsonl",
        "faiss": index_dir / "faiss.index",
        "bm25": index_dir / "bm25_tokens.json",
        "embedder": index_dir / "embedder.json",
    }


def _embedder_config_matches(embedder_path: Path, expected: dict) -> bool:
    if not embedder_path.exists():
        return False
    current = read_json(embedder_path)
    if current.get("backend") != expected.get("backend"):
        return False
    if int(current.get("dim", -1)) != int(expected.get("dim", -2)):
        return False
    if current.get("backend") == "sentence_transformers":
        return current.get("model_name") == expected.get("model_name")
    return True


def _is_index_fresh(chunks_path: Path, files: dict[str, Path], embedder_cfg: dict) -> bool:
    if not chunks_path.exists():
        return False
    if not all(path.exists() for path in files.values()):
        return False
    if not _embedder_config_matches(files["embedder"], embedder_cfg):
        return False
    oldest_output = min(path.stat().st_mtime for path in files.values())
    return oldest_output >= chunks_path.stat().st_mtime


def build_index(
    data_dir: Path,
    backend: str = "hash",
    dim: int = 384,
    model_name: str = "all-MiniLM-L6-v2",
    allow_download: bool = False,
    force: bool = False,
) -> dict:
    chunks_path = data_dir / "chunks.jsonl"
    if not chunks_path.exists():
        raise FileNotFoundError(f"Chunks file not found: {chunks_path}")

    index_dir = data_dir / "index"
    index_dir.mkdir(parents=True, exist_ok=True)
    files = _index_files(index_dir)

    embedder = create_embedder(
        backend=backend,
        dim=dim,
        model_name=model_name,
        allow_download=allow_download,
    )
    embedder_cfg = embedder.to_config()
    if not force and _is_index_fresh(chunks_path, files, embedder_cfg):
        return {"status": "skipped", "chunks": 0, "backend": backend}

    chunks = read_jsonl(chunks_path)
    texts = [row.get("text", "") for row in chunks]
    tokenized = [tokenize(text) for text in texts]

    vectors = embedder.encode(texts)
    vectors = normalize_rows(vectors)
    faiss = _faiss_module()
    index = faiss.IndexFlatIP(vectors.shape[1] if vectors.size else embedder_cfg["dim"])
    if vectors.size:
        index.add(vectors.astype(np.float32))
    faiss.write_index(index, str(files["faiss"]))

    write_jsonl(files["meta"], chunks)
    write_json(files["bm25"], {"tokenized": tokenized})
    write_json(files["embedder"], embedder_cfg)

    return {"status": "built", "chunks": len(chunks), "backend": backend}


def load_index(data_dir: Path) -> dict:
    files = _index_files(data_dir / "index")
    faiss = _faiss_module()
    meta = read_jsonl(files["meta"])
    tokenized = read_json(files["bm25"])["tokenized"]
    embedder_cfg = read_json(files["embedder"])
    vector_index = faiss.read_index(str(files["faiss"]))
    bm25 = BM25Okapi(tokenized) if tokenized else None
    return {
        "meta": meta,
        "bm25": bm25,
        "vector_index": vector_index,
        "embedder_cfg": embedder_cfg,
    }
