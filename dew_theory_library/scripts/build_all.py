from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.chunk import build_chunk_corpus
from dewlib.config import Paths
from dewlib.extract import extract_all_pages
from dewlib.health import build_health_report
from dewlib.index import build_hybrid_index
from dewlib.manifest import build_manifest
from dewlib.ocr import run_ocr_batch


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full DEW Theory Library pipeline.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--force-ocr", action="store_true")
    parser.add_argument("--lang", default=None)
    parser.add_argument("--jobs", type=int, default=None)
    parser.add_argument("--skip-ocr", action="store_true")
    parser.add_argument(
        "--embedder",
        choices=["hash", "st", "sentence_transformers"],
        default="hash",
    )
    parser.add_argument("--dim", type=int, default=384)
    parser.add_argument("--model-name", default="all-MiniLM-L6-v2")
    parser.add_argument("--allow-download", action="store_true")
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )

    rows = build_manifest(paths.library_root, paths.manifest_path)
    print(f"[manifest] docs={len(rows)}")

    if args.skip_ocr:
        print("[ocr] skipped by --skip-ocr")
    else:
        ocr = run_ocr_batch(
            manifest_path=paths.manifest_path,
            library_root=paths.library_root,
            ocr_dir=paths.ocr_dir,
            jobs=args.jobs,
            force_ocr=(args.force or args.force_ocr),
            lang=args.lang,
        )
        if not ocr["available"]:
            print("[ocr] OCRmyPDF not found; skipping OCR")
        else:
            print(
                f"[ocr] total={ocr['total']} ocrd={ocr['ocrd']} "
                f"skipped={ocr['skipped']} failed={ocr['failed']}"
            )
            for err in ocr["errors"][:20]:
                print(f"[ocr][error] {err}")

    extracted = extract_all_pages(
        manifest_path=paths.manifest_path,
        library_root=paths.library_root,
        pages_dir=paths.pages_dir,
        ocr_dir=paths.ocr_dir,
        force=args.force,
    )
    print(
        f"[extract] total={extracted['total']} extracted={extracted['extracted']} "
        f"skipped={extracted['skipped']}"
    )

    chunked = build_chunk_corpus(
        manifest_path=paths.manifest_path,
        pages_dir=paths.pages_dir,
        chunks_path=paths.chunks_path,
        force=args.force,
    )
    print(f"[chunk] status={chunked['status']} chunks={chunked['chunks']}")

    indexed = build_hybrid_index(
        chunks_path=paths.chunks_path,
        index_dir=paths.index_dir,
        embedder=args.embedder,
        dim=args.dim,
        model_name=args.model_name,
        allow_download=args.allow_download,
        force=args.force,
    )
    print(
        f"[index] status={indexed['status']} backend={indexed['backend']} chunks={indexed['chunks']}"
    )

    health = build_health_report(
        manifest_path=paths.manifest_path,
        ocr_dir=paths.ocr_dir,
        output_path=paths.health_report_path,
    )
    print(
        f"[health] flagged={health['summary']['flagged_docs']} "
        f"total={health['summary']['total_docs']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
