from __future__ import annotations

import argparse

from dewlib.config import PathConfig
from dewlib.health import format_health_table, generate_health_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate OCR/library health report.")
    parser.add_argument("--library-dir", default=None, help="Input library directory.")
    parser.add_argument("--data-dir", default="data", help="Data output directory.")
    parser.add_argument("--manifest-path", default=None, help="Manifest path override.")
    parser.add_argument("--strict", action="store_true", help="Exit with code 1 if any doc is flagged.")
    args = parser.parse_args()

    cfg = PathConfig.resolve(
        library_dir=args.library_dir,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )
    report = generate_health_report(cfg.manifest_path, cfg.health_report_path)
    docs = report["docs"]
    print(format_health_table(docs))
    print(
        f"\nHealth report: {cfg.health_report_path} "
        f"(flagged={report['summary']['flagged_docs']} total={report['summary']['total_docs']})"
    )

    if args.strict and report["summary"]["flagged_docs"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
