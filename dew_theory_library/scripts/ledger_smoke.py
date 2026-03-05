from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.ledger_import import first_chunk_id
from dewlib.ledger_store import LedgerStore


def main() -> int:
    parser = argparse.ArgumentParser(description="Run ledger smoke workflow against local corpus.")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--db-path", default=None)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    db_path = Path(args.db_path) if args.db_path else (data_dir / "ledger.sqlite3")
    store = LedgerStore(db_path=db_path, data_dir=data_dir)
    store.initialize()

    chunk_id = first_chunk_id(data_dir)
    if not chunk_id:
        raise SystemExit("No chunk_id available in data/index/meta.jsonl or data/chunks.jsonl")

    actor = "ledger_smoke"
    run_id = uuid4().hex[:8]
    citation = store.create_citation_from_chunk(chunk_id=chunk_id, created_by=actor)
    concept = store.create_concept(
        name=f"smoke-concept-{run_id}",
        description="Smoke-test concept for evidence ledger.",
        tags=["smoke", "test"],
        status="approved",
        created_by=actor,
    )
    store.link_concept_citations(
        concept_id=concept["concept_id"],
        citation_ids=[citation["citation_id"]],
        weight=1.0,
        note="smoke link",
        status="approved",
        created_by=actor,
    )
    signal = store.create_signal(
        name=f"smoke-signal-{run_id}",
        description="Smoke-test signal",
        universe={"symbol_scope": "test"},
        spec={
            "metric": "doc_mention_count",
            "trigger": ">=1 mention",
            "sources": ["ledger_smoke"],
            "cadence": "daily",
            "action_template": "review manually",
        },
        status="active",
        created_by=actor,
    )
    store.link_concept_signal(
        concept_id=concept["concept_id"],
        signal_id=signal["signal_id"],
        signal_version=signal["version"],
        claim="If the referenced chunk appears in monitoring context, investigate the signal.",
        confidence=0.8,
        status="approved",
        created_by=actor,
    )
    explained = store.explain_signal(signal_id=signal["signal_id"], version=signal["version"])

    compact = {
        "signal": explained["signal"],
        "concept_count": len(explained["concepts"]),
        "first_concept": explained["concepts"][0]["concept"]["name"] if explained["concepts"] else None,
        "first_citation_chunk": (
            explained["concepts"][0]["citations"][0]["chunk_id"]
            if explained["concepts"] and explained["concepts"][0]["citations"]
            else None
        ),
    }
    print(json.dumps(compact, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

