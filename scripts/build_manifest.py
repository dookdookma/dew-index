from __future__ import annotations

import argparse

from dewlib.config import PathConfig
from dewlib.manifest import build_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Build data/manifest.jsonl from PDF library.")
    parser.add_argument("--library-dir", default=None, help="Input library directory.")
    parser.add_argument("--data-dir", default="data", help="Data output directory.")
    parser.add_argument("--manifest-path", default=None, help="Manifest path override.")
    args = parser.parse_args()

    cfg = PathConfig.resolve(
        library_dir=args.library_dir,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    rows = build_manifest(cfg.library_dir, cfg.data_dir, manifest_path=cfg.manifest_path)
    print(f"Manifest written: {cfg.manifest_path} ({len(rows)} docs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
