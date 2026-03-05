from __future__ import annotations

import argparse

from dewlib.config import PathConfig
from dewlib.index import build_index


def main() -> int:
    parser = argparse.ArgumentParser(description="Build hybrid index artifacts under data/index.")
    parser.add_argument("--data-dir", default="data", help="Data output directory.")
    parser.add_argument(
        "--backend",
        choices=["hash", "sentence_transformers"],
        default="hash",
        help="Embedding backend.",
    )
    parser.add_argument("--dim", type=int, default=384, help="Hash embedder dimension.")
    parser.add_argument(
        "--model-name",
        default="all-MiniLM-L6-v2",
        help="Sentence-transformers model name.",
    )
    parser.add_argument(
        "--allow-download",
        action="store_true",
        help="Allow sentence-transformers to download model files.",
    )
    parser.add_argument("--force", action="store_true", help="Force rebuild.")
    args = parser.parse_args()

    cfg = PathConfig.resolve(data_dir=args.data_dir)
    summary = build_index(
        data_dir=cfg.data_dir,
        backend=args.backend,
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
