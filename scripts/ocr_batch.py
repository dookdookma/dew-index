from __future__ import annotations

import argparse

from dewlib.config import PathConfig
from dewlib.ocr import run_ocr_batch


def main() -> int:
    parser = argparse.ArgumentParser(description="Run incremental OCR over manifest PDFs.")
    parser.add_argument("--library-dir", default=None, help="Input library directory.")
    parser.add_argument("--data-dir", default="data", help="Data output directory.")
    parser.add_argument("--manifest-path", default=None, help="Manifest path override.")
    parser.add_argument("--jobs", type=int, default=None, help="OCR worker count.")
    parser.add_argument("--force-ocr", action="store_true", help="Force OCR even if up-to-date.")
    parser.add_argument("--lang", default=None, help="OCR language code (e.g. eng).")
    args = parser.parse_args()

    cfg = PathConfig.resolve(
        library_dir=args.library_dir,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    try:
        summary = run_ocr_batch(
            cfg.manifest_path,
            library_dir=cfg.library_dir,
            jobs=args.jobs,
            force_ocr=args.force_ocr,
            lang=args.lang,
        )
    except RuntimeError as exc:
        print(str(exc))
        return 2

    print(
        f"OCR complete: total={summary['total']} ocrd={summary['ocrd']} skipped={summary['skipped']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
