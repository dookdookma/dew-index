from __future__ import annotations

import argparse

from dewlib.chunk import build_chunks
from dewlib.config import PathConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="Build citation-safe chunks from page JSON files.")
    parser.add_argument("--library-dir", default=None, help="Input library directory.")
    parser.add_argument("--data-dir", default="data", help="Data output directory.")
    parser.add_argument("--manifest-path", default=None, help="Manifest path override.")
    parser.add_argument("--target-chars", type=int, default=1200)
    parser.add_argument("--overlap", type=int, default=200)
    parser.add_argument("--min-chars", type=int, default=40)
    parser.add_argument("--force", action="store_true", help="Force rebuild of chunks.")
    args = parser.parse_args()

    cfg = PathConfig.resolve(
        library_dir=args.library_dir,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    summary = build_chunks(
        manifest_path=cfg.manifest_path,
        data_dir=cfg.data_dir,
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
