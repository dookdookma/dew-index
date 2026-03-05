from __future__ import annotations

from pathlib import Path

import numpy as np

from .index import create_embedder, load_index, normalize_rows
from .util import tokenize


class SearchEngine:
    def __init__(
        self,
        data_dir: Path,
        allow_download: bool = False,
    ) -> None:
        self.data_dir = data_dir
        loaded = load_index(data_dir)
        self.meta: list[dict] = loaded["meta"]
        self.bm25 = loaded["bm25"]
        self.vector_index = loaded["vector_index"]
        self.embedder_cfg = loaded["embedder_cfg"]
        self.embedder = create_embedder(
            backend=self.embedder_cfg["backend"],
            dim=int(self.embedder_cfg.get("dim", 384)),
            model_name=self.embedder_cfg.get("model_name", "all-MiniLM-L6-v2"),
            allow_download=allow_download,
        )
        self._chunk_map = {row["chunk_id"]: row for row in self.meta}

    def get_chunk(self, chunk_id: str) -> dict | None:
        return self._chunk_map.get(chunk_id)

    def search(self, query: str, theorist: str | None = None, top_k: int = 10) -> list[dict]:
        if not query.strip():
            return []
        size = len(self.meta)
        if size == 0:
            return []

        candidate_k = min(size, max(top_k * 20, 100))
        tokens = tokenize(query)
        if tokens and self.bm25 is not None:
            bm25_scores = self.bm25.get_scores(tokens)
        else:
            bm25_scores = np.zeros(size, dtype=float)
        bm25_order = np.argsort(-bm25_scores)[:candidate_k]

        query_vec = self.embedder.encode([query])
        query_vec = normalize_rows(np.asarray(query_vec, dtype=np.float32))
        vector_scores, vector_indices = self.vector_index.search(query_vec, candidate_k)
        vector_pairs = list(zip(vector_indices[0].tolist(), vector_scores[0].tolist()))

        fused: dict[int, float] = {}
        offset = 60.0
        for rank, idx in enumerate(bm25_order.tolist(), start=1):
            if bm25_scores[idx] <= 0:
                continue
            row = self.meta[idx]
            if theorist and row.get("theorist") != theorist:
                continue
            fused[idx] = fused.get(idx, 0.0) + 1.0 / (offset + rank)

        for rank, (idx, _score) in enumerate(vector_pairs, start=1):
            if idx < 0:
                continue
            row = self.meta[idx]
            if theorist and row.get("theorist") != theorist:
                continue
            fused[idx] = fused.get(idx, 0.0) + 1.0 / (offset + rank)

        ranked = sorted(
            fused.items(),
            key=lambda item: (-item[1], self.meta[item[0]]["chunk_id"]),
        )
        results: list[dict] = []
        for idx, score in ranked[:top_k]:
            row = self.meta[idx]
            results.append(
                {
                    "score": float(score),
                    "chunk_id": row["chunk_id"],
                    "doc_id": row["doc_id"],
                    "theorist": row["theorist"],
                    "title": row["title"],
                    "page_start": row["page_start"],
                    "page_end": row["page_end"],
                    "excerpt": row.get("text", "")[:600],
                }
            )
        return results
