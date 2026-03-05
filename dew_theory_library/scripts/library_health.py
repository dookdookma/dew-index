from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.config import Paths
from dewlib.health import build_health_report, render_health_table


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate OCR/extraction health report.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    report = build_health_report(
        manifest_path=paths.manifest_path,
        ocr_dir=paths.ocr_dir,
        output_path=paths.health_report_path,
    )
    print(render_health_table(report["docs"]))
    print(
        f"\nHealth report saved: {paths.health_report_path} "
        f"(flagged={report['summary']['flagged_docs']} total={report['summary']['total_docs']})"
    )
    if args.strict and report["summary"]["flagged_docs"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
