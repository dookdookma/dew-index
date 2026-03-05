from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.scan_config import load_feeds_file, load_feeds_registry, resolve_feed_set_to_sources
from dewlib.scan_runtime import run_scan


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DEW Scanner v1 (observe + explain + report).")
    parser.add_argument("--ledger-db", default="data/ledger.sqlite3")
    parser.add_argument("--feeds-file", default=None)
    parser.add_argument("--feed", action="append", default=[])
    parser.add_argument("--feed-set", default=None)
    parser.add_argument("--feeds-registry", default="data/feeds.json")
    parser.add_argument(
        "--cadence",
        choices=["morning", "midday", "close", "weekly", "ad_hoc"],
        default="ad_hoc",
    )
    parser.add_argument("--out-dir", default="out/scans")
    parser.add_argument("--created-by", default="scanner")
    parser.add_argument("--max-items", type=int, default=200)
    args = parser.parse_args()

    feeds: list[dict | str] = []
    feed_sets_map: dict[str, list[str]] = {}
    if args.feed_set:
        registry = load_feeds_registry(Path(args.feeds_registry))
        feeds.extend(resolve_feed_set_to_sources(registry, args.feed_set))
        feed_sets_map = {
            name: [str(value) for value in values]
            for name, values in dict(registry.get("feed_sets") or {}).items()
        }

    if args.feeds_file:
        feeds.extend(load_feeds_file(Path(args.feeds_file)))
    if args.feed:
        feeds.extend(args.feed)
    if not feeds:
        raise SystemExit("No feeds provided. Use --feed, --feeds-file, or --feed-set.")

    result = run_scan(
        ledger_db_path=args.ledger_db,
        feed_sources=feeds,
        run_options={
            "out_dir": args.out_dir,
            "max_items": args.max_items,
            "cadence": args.cadence,
            "feed_set": args.feed_set,
            "feed_sets_map": feed_sets_map,
        },
        created_by=args.created_by,
    )
    print(
        f"scan_run_id={result['scan_run_id']} "
        f"evaluated={result['signals_evaluated']} "
        f"triggered={result['signals_triggered']}"
    )
    print(f"report_path={result['report_path']}")
    print(f"report_json_path={result['report_json_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
