#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.digest import generate_daily_digest


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate DEW daily digest from scan runs.")
    parser.add_argument("--ledger-db", default="data/ledger.sqlite3")
    parser.add_argument("--date", default=None, help="Local date in YYYY-MM-DD (default: today in --tz).")
    parser.add_argument("--tz", default="America/New_York")
    parser.add_argument("--cadences", default="morning,midday,close")
    parser.add_argument("--out-dir", default="out/digests")
    args = parser.parse_args()

    cadences = [part.strip() for part in args.cadences.split(",") if part.strip()]
    result = generate_daily_digest(
        ledger_db_path=args.ledger_db,
        out_dir=args.out_dir,
        date=args.date,
        tz_name=args.tz,
        cadences=cadences,
    )
    print(f"date={result['date']} runs={result['run_count']}")
    print(f"markdown={result['markdown_path']}")
    print(f"json={result['json_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
