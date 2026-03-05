#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.signal_tune import parse_set_overrides, tune_signal


def main() -> int:
    parser = argparse.ArgumentParser(description="Clone+patch an rss_keyword_count signal version.")
    parser.add_argument("--ledger-db", default="data/ledger.sqlite3")
    parser.add_argument("--signal-id", required=True)
    parser.add_argument("--from-version", type=int, default=None)
    parser.add_argument("--set", dest="set_pairs", action="append", default=[], help="key=value")
    parser.add_argument("--created-by", default="operator")
    parser.add_argument("--feeds-registry", default="data/feeds.json")
    args = parser.parse_args()

    if not args.set_pairs:
        raise SystemExit("At least one --set key=value override is required.")

    overrides = parse_set_overrides(args.set_pairs)
    result = tune_signal(
        ledger_db_path=args.ledger_db,
        signal_id=args.signal_id,
        from_version=args.from_version,
        set_overrides=overrides,
        created_by=args.created_by,
        feeds_registry_path=args.feeds_registry,
    )

    before = result["before_spec"]
    after = result["after_spec"]
    print(
        f"signal {result['signal_id']} v{result['from_version']} -> v{result['to_version']} "
        f"threshold: {before.get('threshold')}->{after.get('threshold')} "
        f"window_items: {before.get('window_items')}->{after.get('window_items')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
