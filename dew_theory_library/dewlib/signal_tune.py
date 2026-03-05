from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from .ledger_db import connect_db, utc_now_iso
from .scan_config import load_feeds_registry


SUPPORTED_KEYS = {"threshold", "window_items", "keywords_add", "keywords_remove", "feeds_set", "status"}


def parse_set_overrides(pairs: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Invalid --set argument (expected key=value): {pair}")
        key, value = pair.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key not in SUPPORTED_KEYS:
            raise ValueError(f"Unsupported tune key: {key}")
        parsed[key] = value
    return parsed


def _event_insert(
    conn: sqlite3.Connection,
    actor: str | None,
    event_type: str,
    entity_type: str,
    entity_id: str,
    payload: object,
) -> None:
    conn.execute(
        """
        INSERT INTO events(event_id, ts, actor, event_type, entity_type, entity_id, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid4()),
            utc_now_iso(),
            actor,
            event_type,
            entity_type,
            entity_id,
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
        ),
    )


def _resolve_feeds_set(value: str, feeds_registry_path: Path) -> list[str]:
    if "," in value:
        return [part.strip() for part in value.split(",") if part.strip()]
    if feeds_registry_path.exists():
        registry = load_feeds_registry(feeds_registry_path)
        feed_sets = dict(registry.get("feed_sets") or {})
        if value in feed_sets:
            return [str(feed_id) for feed_id in feed_sets[value]]
    return [value]


def tune_signal(
    ledger_db_path: str | Path,
    signal_id: str,
    set_overrides: dict[str, str],
    created_by: str | None,
    from_version: int | None = None,
    feeds_registry_path: str | Path = "data/feeds.json",
) -> dict:
    db_path = Path(ledger_db_path)
    feeds_path = Path(feeds_registry_path)
    with connect_db(db_path) as conn:
        if from_version is None:
            source = conn.execute(
                "SELECT * FROM signals WHERE signal_id = ? ORDER BY version DESC LIMIT 1",
                (signal_id,),
            ).fetchone()
        else:
            source = conn.execute(
                "SELECT * FROM signals WHERE signal_id = ? AND version = ?",
                (signal_id, int(from_version)),
            ).fetchone()
        if source is None:
            raise KeyError(f"Signal not found: {signal_id}")

        source_version = int(source["version"])
        latest = conn.execute(
            "SELECT MAX(version) AS v FROM signals WHERE signal_id = ?",
            (signal_id,),
        ).fetchone()
        assert latest is not None and latest["v"] is not None
        next_version = int(latest["v"]) + 1

        spec = json.loads(source["spec_json"])
        if str(spec.get("kind") or "") != "rss_keyword_count":
            raise ValueError("signal_tune supports only rss_keyword_count")

        before_spec = json.loads(source["spec_json"])
        patch_applied: dict[str, object] = {}

        if "threshold" in set_overrides:
            spec["threshold"] = int(set_overrides["threshold"])
            patch_applied["threshold"] = spec["threshold"]
        if "window_items" in set_overrides:
            spec["window_items"] = int(set_overrides["window_items"])
            patch_applied["window_items"] = spec["window_items"]
        if "keywords_add" in set_overrides:
            additions = [part.strip() for part in set_overrides["keywords_add"].split(",") if part.strip()]
            current = [str(item) for item in spec.get("keywords") or []]
            for keyword in additions:
                if keyword not in current:
                    current.append(keyword)
            spec["keywords"] = current
            patch_applied["keywords_add"] = additions
        if "keywords_remove" in set_overrides:
            removals = {part.strip() for part in set_overrides["keywords_remove"].split(",") if part.strip()}
            current = [str(item) for item in spec.get("keywords") or []]
            spec["keywords"] = [keyword for keyword in current if keyword not in removals]
            patch_applied["keywords_remove"] = sorted(removals)
        if "feeds_set" in set_overrides:
            feeds = _resolve_feeds_set(set_overrides["feeds_set"], feeds_path)
            spec["feeds"] = feeds
            patch_applied["feeds_set"] = feeds

        status = source["status"]
        if "status" in set_overrides:
            status = set_overrides["status"]
            patch_applied["status"] = status

        conn.execute(
            """
            INSERT INTO signals(
              signal_id, version, name, description, universe_json, spec_json, status, created_at, created_by
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                next_version,
                source["name"],
                source["description"],
                source["universe_json"],
                json.dumps(spec, ensure_ascii=False, sort_keys=True),
                status,
                utc_now_iso(),
                created_by,
            ),
        )
        _event_insert(
            conn,
            actor=created_by,
            event_type="signal.clone_patch",
            entity_type="signal",
            entity_id=f"{signal_id}:{next_version}",
            payload={
                "from_version": source_version,
                "to_version": next_version,
                "patch": patch_applied,
                "before_spec": before_spec,
                "after_spec": spec,
            },
        )
        conn.commit()
        return {
            "signal_id": signal_id,
            "from_version": source_version,
            "to_version": next_version,
            "name": source["name"],
            "before_spec": before_spec,
            "after_spec": spec,
            "status": status,
            "patch": patch_applied,
        }

