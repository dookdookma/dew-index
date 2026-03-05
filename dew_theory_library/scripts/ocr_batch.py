from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.config import Paths
from dewlib.ocr import run_ocr_batch


def main() -> int:
    parser = argparse.ArgumentParser(description="Run OCRmyPDF in batch mode.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--force-ocr", action="store_true")
    parser.add_argument("--lang", default=None)
    parser.add_argument("--jobs", type=int, default=None)
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    summary = run_ocr_batch(
        manifest_path=paths.manifest_path,
        library_root=paths.library_root,
        ocr_dir=paths.ocr_dir,
        jobs=args.jobs,
        force_ocr=args.force_ocr,
        lang=args.lang,
    )

    if not summary["available"]:
        print("WARNING: OCRmyPDF not found in PATH. Skipping OCR step.")
        return 0

    print(
        f"OCR complete: total={summary['total']} ocrd={summary['ocrd']} "
        f"skipped={summary['skipped']} failed={summary['failed']}"
    )
    for err in summary["errors"][:20]:
        print(f"ERROR: {err}")
    return 1 if summary["failed"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
