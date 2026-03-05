from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.ledger_db import initialize_ledger_db


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize DEW Evidence Ledger SQLite database.")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--db-path", default=None)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path) if args.db_path else (data_dir / "ledger.sqlite3")
    result = initialize_ledger_db(db_path)
    print(f"Ledger DB initialized: {result['db_path']}")
    print(f"Schema version: {result['schema_version']}")
    print("Tables:")
    for table in result["tables"]:
        print(f"- {table}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

