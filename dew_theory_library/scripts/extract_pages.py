from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.config import Paths
from dewlib.extract import extract_all_pages


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract per-page text from PDFs.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    summary = extract_all_pages(
        manifest_path=paths.manifest_path,
        library_root=paths.library_root,
        pages_dir=paths.pages_dir,
        ocr_dir=paths.ocr_dir,
        force=args.force,
    )
    print(
        f"Extract complete: total={summary['total']} "
        f"extracted={summary['extracted']} skipped={summary['skipped']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
