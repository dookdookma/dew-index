#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.ledger_db import connect_db
from dewlib.recommend_review import list_queue


def main() -> int:
    parser = argparse.ArgumentParser(description="Print recommendation review queue.")
    parser.add_argument("--ledger-db", default="data/ledger.sqlite3")
    parser.add_argument("--status", default="proposed")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--since-ts", default=None)
    args = parser.parse_args()

    with connect_db(Path(args.ledger_db)) as conn:
        payload = list_queue(
            conn=conn,
            status=args.status,
            limit=args.limit,
            since_ts=args.since_ts,
        )

    print(f"status={payload['status']} count={len(payload['items'])}")
    for item in payload["items"]:
        print(
            f"{item['ts']} {item['recommendation_id']} [{item['kind']}] "
            f"{item['title']} run={item['scan_run_id']} status={item['status']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
