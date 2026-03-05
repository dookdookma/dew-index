from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.config import Paths
from dewlib.triage import build_ocr_triage_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Build deterministic OCR/corpus triage reports.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    output_json = paths.data_dir / "ocr_triage.json"
    output_md = paths.data_dir / "ocr_triage.md"

    report = build_ocr_triage_report(
        library_root=paths.library_root,
        data_dir=paths.data_dir,
        manifest_path=paths.manifest_path,
        output_json_path=output_json,
        output_md_path=output_md,
    )
    print(
        f"Triage saved: {output_json} and {output_md} "
        f"(docs={report['summary']['total_docs']} collisions={report['summary']['collisions']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

