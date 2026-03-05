from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.config import Paths
from dewlib.manifest import build_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Build data/manifest.jsonl from library PDFs.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    rows = build_manifest(paths.library_root, paths.manifest_path)
    print(f"Manifest written: {paths.manifest_path} ({len(rows)} docs)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
