from __future__ import annotations

import hashlib
from dataclasses import dataclass

import numpy as np

from .util import tokenize


def normalize_vectors(vectors: np.ndarray) -> np.ndarray:
    if vectors.size == 0:
        return vectors.astype(np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return (vectors / norms).astype(np.float32)


@dataclass
class EmbedderSpec:
    backend: str
    dim: int
    model_name: str | None


class HashEmbedder:
    backend = "hash"

    def __init__(self, dim: int = 384) -> None:
        self.dim = dim
        self.model_name = None

    def encode(self, texts: list[str]) -> np.ndarray:
        vectors = np.zeros((len(texts), self.dim), dtype=np.float32)
        for row_i, text in enumerate(texts):
            for token in tokenize(text):
                digest = hashlib.blake2b(token.encode("utf-8"), digest_size=16).digest()
                idx = int.from_bytes(digest[:8], "big") % self.dim
                sign = 1.0 if digest[8] % 2 == 0 else -1.0
                vectors[row_i, idx] += sign
        return normalize_vectors(vectors)


class SentenceTransformersEmbedder:
    backend = "sentence_transformers"

    def __init__(self, model_name: str, allow_download: bool = False) -> None:
        from sentence_transformers import SentenceTransformer

        self.model_name = model_name
        self.model = SentenceTransformer(model_name, local_files_only=not allow_download)
        self.dim = int(self.model.get_sentence_embedding_dimension())

    def encode(self, texts: list[str]) -> np.ndarray:
        arr = self.model.encode(texts, show_progress_bar=False, normalize_embeddings=True)
        return np.asarray(arr, dtype=np.float32)


def build_embedder(
    backend: str = "hash",
    dim: int = 384,
    model_name: str = "all-MiniLM-L6-v2",
    allow_download: bool = False,
    fallback_to_hash: bool = True,
):
    normalized_backend = {
        "hash": "hash",
        "st": "sentence_transformers",
        "sentence_transformers": "sentence_transformers",
    }.get(backend, backend)

    if normalized_backend == "hash":
        return HashEmbedder(dim=dim), EmbedderSpec(backend="hash", dim=dim, model_name=None)

    if normalized_backend == "sentence_transformers":
        try:
            embedder = SentenceTransformersEmbedder(
                model_name=model_name,
                allow_download=allow_download,
            )
            return embedder, EmbedderSpec(
                backend="sentence_transformers",
                dim=embedder.dim,
                model_name=model_name,
            )
        except Exception:
            if not fallback_to_hash:
                raise
            hash_embedder = HashEmbedder(dim=dim)
            return hash_embedder, EmbedderSpec(backend="hash", dim=dim, model_name=None)

    raise ValueError(f"Unsupported embedder backend: {backend}")
