from __future__ import annotations

from pathlib import Path

import numpy as np
from rank_bm25 import BM25Okapi

from .embed import build_embedder, normalize_vectors
from .index import load_index_artifacts
from .util import read_json, tokenize


class SearchService:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        loaded = load_index_artifacts(data_dir / "index")
        self.meta: list[dict] = loaded["meta"]
        self.tokenized: list[list[str]] = loaded["tokenized"]
        self.faiss_index = loaded["faiss_index"]
        self.embedder_info: dict = loaded["embedder"]

        self.bm25 = BM25Okapi(self.tokenized) if self.tokenized else None
        self.embedder, _spec = build_embedder(
            backend=self.embedder_info["backend"],
            dim=int(self.embedder_info["dim"]),
            model_name=self.embedder_info.get("model_name") or "all-MiniLM-L6-v2",
            allow_download=False,
            fallback_to_hash=True,
        )
        self._chunk_map = {row["chunk_id"]: row for row in self.meta}

    def get_chunk(self, chunk_id: str) -> dict | None:
        return self._chunk_map.get(chunk_id)

    def get_doc_pages(self, doc_id: str, start: int, end: int) -> list[dict]:
        pages_file = self.data_dir / "pages" / f"{doc_id}.json"
        if not pages_file.exists():
            return []
        payload = read_json(pages_file)
        rows = []
        for page in payload.get("pages", []):
            page_num = int(page["page"])
            if start <= page_num <= end:
                rows.append({"page": page_num, "text": page.get("text", "")})
        return rows

    def search(
        self,
        query: str,
        theorist: str | None = None,
        top_k: int = 8,
        bm25_k: int = 200,
        vector_k: int = 200,
    ) -> list[dict]:
        if not query.strip() or not self.meta:
            return []

        total = len(self.meta)
        bm25_k = min(total, max(top_k * 10, bm25_k))
        vector_k = min(total, max(top_k * 10, vector_k))

        query_tokens = tokenize(query)
        query_token_set = set(query_tokens)
        overlap_scores = np.zeros(total, dtype=float)
        if query_token_set:
            for i, tokens in enumerate(self.tokenized):
                overlap_scores[i] = float(len(query_token_set.intersection(tokens)))

        bm25_scores = (
            self.bm25.get_scores(query_tokens)
            if (self.bm25 is not None and query_tokens)
            else np.zeros(total, dtype=float)
        )
        bm25_indices = np.argsort(-bm25_scores)[:bm25_k].tolist()

        qvec = self.embedder.encode([query])
        qvec = normalize_vectors(np.asarray(qvec, dtype=np.float32))
        vec_scores, vec_indices = self.faiss_index.search(qvec, vector_k)
        vec_indices_list = vec_indices[0].tolist()

        fused: dict[int, float] = {}
        k = 60.0
        max_bm25 = float(np.max(bm25_scores)) if bm25_scores.size else 0.0
        max_overlap = float(np.max(overlap_scores)) if overlap_scores.size else 0.0

        for rank, idx in enumerate(bm25_indices, start=1):
            if idx < 0:
                continue
            score = fused.get(idx, 0.0) + (1.0 / (k + rank))
            if max_bm25 > 0:
                score += 0.35 * float(bm25_scores[idx] / max_bm25)
            if max_overlap > 0:
                score += 0.25 * float(overlap_scores[idx] / max_overlap)
            fused[idx] = score

        for rank, idx in enumerate(vec_indices_list, start=1):
            if idx < 0:
                continue
            fused[idx] = fused.get(idx, 0.0) + (1.0 / (k + rank))

        ranked = sorted(
            fused.items(),
            key=lambda item: (-item[1], self.meta[item[0]]["chunk_id"]),
        )

        results: list[dict] = []
        for idx, score in ranked:
            row = self.meta[idx]
            if theorist and row["theorist"] != theorist:
                continue
            results.append(
                {
                    "score": float(score),
                    "chunk_id": row["chunk_id"],
                    "doc_id": row["doc_id"],
                    "theorist": row["theorist"],
                    "title": row["title"],
                    "source_path": row["source_path"],
                    "ocr_path": row["ocr_path"],
                    "page_start": row["page_start"],
                    "page_end": row["page_end"],
                    "excerpt": row.get("text", "")[:600],
                    "embedder_backend": self.embedder_info["backend"],
                }
            )
            if len(results) >= top_k:
                break

        return results
