from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import uvicorn

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve DEW Evidence Ledger FastAPI app.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8788)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--db-path", default=None)
    args = parser.parse_args()

    os.environ["DEW_DATA_DIR"] = args.data_dir
    if args.db_path:
        os.environ["DEW_LEDGER_DB_PATH"] = args.db_path
    else:
        os.environ["DEW_LEDGER_DB_PATH"] = str(Path(args.data_dir) / "ledger.sqlite3")

    uvicorn.run("server.ledger_api:app", host=args.host, port=args.port, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

