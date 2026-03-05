from __future__ import annotations

import argparse

from dewlib.config import PathConfig
from dewlib.extract import extract_pages_batch


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract page text into data/pages/*.json.")
    parser.add_argument("--library-dir", default=None, help="Input library directory.")
    parser.add_argument("--data-dir", default="data", help="Data output directory.")
    parser.add_argument("--manifest-path", default=None, help="Manifest path override.")
    parser.add_argument(
        "--prefer-ocr",
        dest="prefer_ocr",
        action="store_true",
        help="Prefer OCR PDF when available (default).",
    )
    parser.add_argument(
        "--no-prefer-ocr",
        dest="prefer_ocr",
        action="store_false",
        help="Always extract directly from source PDFs.",
    )
    parser.set_defaults(prefer_ocr=True)
    parser.add_argument("--force", action="store_true", help="Force re-extraction.")
    args = parser.parse_args()

    cfg = PathConfig.resolve(
        library_dir=args.library_dir,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    summary = extract_pages_batch(
        manifest_path=cfg.manifest_path,
        library_dir=cfg.library_dir,
        data_dir=cfg.data_dir,
        prefer_ocr=args.prefer_ocr,
        force=args.force,
    )
    print(
        "Extraction complete: "
        f"total={summary['total']} extracted={summary['extracted']} skipped={summary['skipped']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
