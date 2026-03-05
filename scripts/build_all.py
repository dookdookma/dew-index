from __future__ import annotations

import argparse

from dewlib.chunk import build_chunks
from dewlib.config import PathConfig
from dewlib.extract import extract_pages_batch
from dewlib.health import generate_health_report
from dewlib.index import build_index
from dewlib.manifest import build_manifest
from dewlib.ocr import ocr_available, run_ocr_batch


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full DEW library pipeline.")
    parser.add_argument("--library-dir", default=None, help="Input library directory.")
    parser.add_argument("--data-dir", default="data", help="Data output directory.")
    parser.add_argument("--manifest-path", default=None, help="Manifest path override.")
    parser.add_argument("--skip-ocr", action="store_true", help="Skip OCR step.")
    parser.add_argument("--force-ocr", action="store_true", help="Force OCR re-run.")
    parser.add_argument("--lang", default=None, help="OCR language code.")
    parser.add_argument("--ocr-jobs", type=int, default=None, help="OCR worker count.")
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
    parser.add_argument("--allow-download", action="store_true")
    args = parser.parse_args()

    cfg = PathConfig.resolve(
        library_dir=args.library_dir,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )

    manifest_rows = build_manifest(cfg.library_dir, cfg.data_dir, manifest_path=cfg.manifest_path)
    print(f"[manifest] docs={len(manifest_rows)}")

    if args.skip_ocr:
        print("[ocr] skipped by flag")
    elif ocr_available():
        ocr_summary = run_ocr_batch(
            cfg.manifest_path,
            library_dir=cfg.library_dir,
            jobs=args.ocr_jobs,
            force_ocr=args.force_ocr,
            lang=args.lang,
        )
        print(
            f"[ocr] total={ocr_summary['total']} ocrd={ocr_summary['ocrd']} skipped={ocr_summary['skipped']}"
        )
    else:
        print("[ocr] OCRmyPDF not installed; skipping OCR")

    extract_summary = extract_pages_batch(
        manifest_path=cfg.manifest_path,
        library_dir=cfg.library_dir,
        data_dir=cfg.data_dir,
        prefer_ocr=not args.skip_ocr,
        force=False,
    )
    print(
        f"[extract] total={extract_summary['total']} "
        f"extracted={extract_summary['extracted']} skipped={extract_summary['skipped']}"
    )

    chunk_summary = build_chunks(
        manifest_path=cfg.manifest_path,
        data_dir=cfg.data_dir,
    )
    print(f"[chunk] status={chunk_summary['status']} chunks={chunk_summary['chunks']}")

    index_summary = build_index(
        data_dir=cfg.data_dir,
        backend=args.backend,
        dim=args.dim,
        model_name=args.model_name,
        allow_download=args.allow_download,
    )
    print(
        f"[index] status={index_summary['status']} backend={index_summary['backend']} "
        f"chunks={index_summary['chunks']}"
    )

    health = generate_health_report(cfg.manifest_path, cfg.health_report_path)
    print(
        f"[health] flagged={health['summary']['flagged_docs']} total={health['summary']['total_docs']} "
        f"path={cfg.health_report_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
