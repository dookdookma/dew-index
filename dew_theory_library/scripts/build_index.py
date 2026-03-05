from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.config import Paths
from dewlib.index import build_hybrid_index


def main() -> int:
    parser = argparse.ArgumentParser(description="Build BM25 + FAISS hybrid index.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument(
        "--embedder",
        choices=["hash", "st", "sentence_transformers"],
        default="hash",
    )
    parser.add_argument("--dim", type=int, default=384)
    parser.add_argument("--model-name", default="all-MiniLM-L6-v2")
    parser.add_argument("--allow-download", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    summary = build_hybrid_index(
        chunks_path=paths.chunks_path,
        index_dir=paths.index_dir,
        embedder=args.embedder,
        dim=args.dim,
        model_name=args.model_name,
        allow_download=args.allow_download,
        force=args.force,
    )
    print(
        f"Index {summary['status']}: backend={summary['backend']} chunks={summary['chunks']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
