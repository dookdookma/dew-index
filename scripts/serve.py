from __future__ import annotations

import argparse
import os

import uvicorn


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve the DEW theory library FastAPI app.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--library-dir", default=None)
    parser.add_argument("--manifest-path", default=None)
    args = parser.parse_args()

    os.environ["DEW_DATA_DIR"] = args.data_dir
    if args.library_dir:
        os.environ["DEW_LIBRARY_DIR"] = args.library_dir
    if args.manifest_path:
        os.environ["DEW_MANIFEST_PATH"] = args.manifest_path

    uvicorn.run("server.api:app", host=args.host, port=args.port, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
