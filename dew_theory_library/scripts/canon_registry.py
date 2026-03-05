from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.canon import build_canonical_registry
from dewlib.config import Paths
from dewlib.triage import build_ocr_triage_report
from dewlib.util import read_json


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate canonical candidate registry scaffold.")
    parser.add_argument("--library-root", default="library")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--manifest-path", default=None)
    args = parser.parse_args()

    paths = Paths.from_args(
        library_root=args.library_root,
        data_dir=args.data_dir,
        manifest_path=args.manifest_path,
    )

    triage_json = paths.data_dir / "ocr_triage.json"
    triage_md = paths.data_dir / "ocr_triage.md"
    if triage_json.exists():
        triage_report = read_json(triage_json)
    else:
        triage_report = build_ocr_triage_report(
            library_root=paths.library_root,
            data_dir=paths.data_dir,
            manifest_path=paths.manifest_path,
            output_json_path=triage_json,
            output_md_path=triage_md,
        )

    output_json = paths.data_dir / "canonical_registry.json"
    payload = build_canonical_registry(
        data_dir=paths.data_dir,
        manifest_path=paths.manifest_path,
        triage_report=triage_report,
        output_path=output_json,
    )
    print(f"Canonical registry saved: {output_json} (theorists={len(payload['theorists'])})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

