from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.scan_config import seed_feeds_registry


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed or merge feeds registry JSON.")
    parser.add_argument("--feeds-registry", default="data/feeds.json")
    args = parser.parse_args()

    path = Path(args.feeds_registry)
    payload = seed_feeds_registry(path)
    print(f"Feeds registry written: {path}")
    print(f"Feed count: {len(payload.get('feeds', []))}")
    print(f"Feed sets: {', '.join(sorted((payload.get('feed_sets') or {}).keys()))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

