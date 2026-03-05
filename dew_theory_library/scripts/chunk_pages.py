from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.chunk import build_chunk_corpus
from dewlib.config import Paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Create page-bounded chunk corpus.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--target-chars", type=int, default=1200)
    parser.add_argument("--overlap", type=int, default=200)
    parser.add_argument("--min-chars", type=int, default=40)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    summary = build_chunk_corpus(
        manifest_path=paths.manifest_path,
        pages_dir=paths.pages_dir,
        chunks_path=paths.chunks_path,
        target_chars=args.target_chars,
        overlap=args.overlap,
        min_chars=args.min_chars,
        force=args.force,
    )
    print(
        f"Chunking {summary['status']}: docs={summary['docs']} chunks={summary['chunks']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
