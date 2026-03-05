from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dewlib.ledger_db import connect_db
from dewlib.ledger_store import LedgerStore
from dewlib.scan_config import ensure_signal_pack_file


def _get_concept_by_name(db_path: Path, name: str) -> dict | None:
    with connect_db(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM concepts WHERE name = ?",
            (name,),
        ).fetchone()
        if row is None:
            return None
        return {
            "concept_id": row["concept_id"],
            "name": row["name"],
            "status": row["status"],
        }


def _get_latest_signal_by_name(db_path: Path, name: str) -> dict | None:
    with connect_db(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM signals WHERE name = ? ORDER BY version DESC LIMIT 1",
            (name,),
        ).fetchone()
        if row is None:
            return None
        return {
            "signal_id": row["signal_id"],
            "version": int(row["version"]),
            "name": row["name"],
            "description": row["description"],
            "universe": json.loads(row["universe_json"]),
            "spec": json.loads(row["spec_json"]),
            "status": row["status"],
        }


def _link_exists(db_path: Path, concept_id: str, signal_id: str, signal_version: int) -> bool:
    with connect_db(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1 FROM concept_signals
            WHERE concept_id = ? AND signal_id = ? AND signal_version = ?
            """,
            (concept_id, signal_id, int(signal_version)),
        ).fetchone()
    return row is not None


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed signal pack into ledger database (idempotent).")
    parser.add_argument("--ledger-db", default="data/ledger.sqlite3")
    parser.add_argument("--signal-pack", default="data/signal_pack_v1.json")
    parser.add_argument("--created-by", default="seed_signal_pack")
    args = parser.parse_args()

    db_path = Path(args.ledger_db)
    pack_path = Path(args.signal_pack)
    pack = ensure_signal_pack_file(pack_path)

    store = LedgerStore(db_path=db_path, data_dir=db_path.parent)
    init = store.initialize()

    concepts_created = 0
    concepts_status_updated = 0
    signals_created = 0
    signals_versions_bumped = 0
    signal_status_updated = 0
    links_created = 0
    links_updated = 0

    concept_ids_by_name: dict[str, str] = {}
    for concept_spec in pack.get("concepts", []):
        name = str(concept_spec["name"])
        existing = _get_concept_by_name(db_path, name)
        if existing is None:
            created = store.create_concept(
                name=name,
                description=concept_spec.get("description"),
                tags=list(concept_spec.get("tags") or []),
                status=str(concept_spec.get("status") or "proposed"),
                created_by=args.created_by,
            )
            concept_ids_by_name[name] = created["concept_id"]
            concepts_created += 1
            continue

        concept_ids_by_name[name] = existing["concept_id"]
        desired_status = str(concept_spec.get("status") or existing["status"])
        if desired_status != existing["status"]:
            try:
                store.update_concept_status(
                    concept_id=existing["concept_id"],
                    new_status=desired_status,
                    actor=args.created_by,
                )
                concepts_status_updated += 1
            except Exception:
                pass

    for signal_spec in pack.get("signals", []):
        signal_name = str(signal_spec["name"])
        desired_spec = signal_spec.get("spec") or {}
        desired_universe = signal_spec.get("universe") or {}
        desired_description = signal_spec.get("description")
        desired_status = str(signal_spec.get("status") or "proposed")

        latest = _get_latest_signal_by_name(db_path, signal_name)
        signal_row: dict
        if latest is None:
            signal_row = store.create_signal(
                name=signal_name,
                description=desired_description,
                universe=desired_universe,
                spec=desired_spec,
                status=desired_status,
                created_by=args.created_by,
            )
            signals_created += 1
        else:
            same_spec = json.dumps(latest["spec"], sort_keys=True) == json.dumps(desired_spec, sort_keys=True)
            if not same_spec:
                signal_row = store.clone_signal(
                    signal_id=latest["signal_id"],
                    from_version=latest["version"],
                    patch_json={
                        "description": desired_description,
                        "universe": desired_universe,
                        "spec": desired_spec,
                        "status": desired_status,
                    },
                    created_by=args.created_by,
                )
                signals_versions_bumped += 1
            else:
                signal_row = latest
                if desired_status != latest["status"]:
                    try:
                        signal_row = store.update_signal_status(
                            signal_id=latest["signal_id"],
                            version=latest["version"],
                            new_status=desired_status,
                            actor=args.created_by,
                        )
                        signal_status_updated += 1
                    except Exception:
                        pass

        for link in signal_spec.get("concept_links", []):
            concept_name = str(link.get("concept") or "")
            concept_id = concept_ids_by_name.get(concept_name)
            if not concept_id:
                continue
            existed = _link_exists(
                db_path=db_path,
                concept_id=concept_id,
                signal_id=signal_row["signal_id"],
                signal_version=int(signal_row["version"]),
            )
            store.link_concept_signal(
                concept_id=concept_id,
                signal_id=signal_row["signal_id"],
                signal_version=int(signal_row["version"]),
                claim=str(link.get("claim") or ""),
                confidence=link.get("confidence"),
                status=str(link.get("status") or "proposed"),
                created_by=args.created_by,
            )
            if existed:
                links_updated += 1
            else:
                links_created += 1

    print(f"Ledger schema version: {init['schema_version']}")
    print(
        "Signal pack summary: "
        f"concepts_created={concepts_created} "
        f"concepts_status_updated={concepts_status_updated} "
        f"signals_created={signals_created} "
        f"signals_versions_bumped={signals_versions_bumped} "
        f"signal_status_updated={signal_status_updated} "
        f"links_created={links_created} "
        f"links_updated={links_updated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

