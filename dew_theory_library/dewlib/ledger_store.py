from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from uuid import uuid4

from .ledger_db import connect_db, initialize_ledger_db, utc_now_iso
from .ledger_explain import ExplainError, explain_signal
from .ledger_import import resolve_chunk_provenance
from .ledger_models import CONCEPT_STATUSES, LINK_STATUSES, SIGNAL_STATUSES
from .scan_db import ScanDB


class LedgerError(Exception):
    pass


class LedgerNotFoundError(LedgerError):
    pass


class LedgerConflictError(LedgerError):
    pass


class LedgerValidationError(LedgerError):
    pass


def _to_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _parse_json(value: str | None, fallback: object) -> object:
    if value is None:
        return fallback
    return json.loads(value)


def _entity_signal_id(signal_id: str, version: int) -> str:
    return f"{signal_id}:{version}"


def _validate_status(status: str, allowed: set[str], field_name: str) -> None:
    if status not in allowed:
        raise LedgerValidationError(f"Invalid {field_name}: {status}")


def _validate_transition(previous: str, new: str, signal: bool = False) -> None:
    if previous == new:
        return
    if signal:
        allowed = {
            "proposed": {"active", "deprecated"},
            "active": {"deprecated"},
            "deprecated": set(),
        }
    else:
        allowed = {
            "proposed": {"approved", "deprecated"},
            "approved": {"deprecated"},
            "deprecated": set(),
        }
    if new not in allowed.get(previous, set()):
        raise LedgerValidationError(f"Invalid status transition: {previous} -> {new}")


def _merge_values(base: object, patch: object) -> object:
    if isinstance(base, dict) and isinstance(patch, dict):
        merged = dict(base)
        for key, value in patch.items():
            merged[key] = _merge_values(merged[key], value) if key in merged else value
        return merged
    return patch


class LedgerStore:
    def __init__(self, db_path: Path, data_dir: Path) -> None:
        self.db_path = db_path
        self.data_dir = data_dir

    def initialize(self) -> dict:
        return initialize_ledger_db(self.db_path)

    def _connect(self) -> sqlite3.Connection:
        return connect_db(self.db_path)

    def _log_event(
        self,
        conn: sqlite3.Connection,
        actor: str | None,
        event_type: str,
        entity_type: str,
        entity_id: str,
        payload: object,
    ) -> dict:
        event = {
            "event_id": str(uuid4()),
            "ts": utc_now_iso(),
            "actor": actor,
            "event_type": event_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "payload_json": _to_json(payload),
        }
        conn.execute(
            """
            INSERT INTO events(
              event_id, ts, actor, event_type, entity_type, entity_id, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                event["ts"],
                event["actor"],
                event["event_type"],
                event["entity_type"],
                event["entity_id"],
                event["payload_json"],
            ),
        )
        return event

    def _citation_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "citation_id": row["citation_id"],
            "chunk_id": row["chunk_id"],
            "doc_id": row["doc_id"],
            "theorist": row["theorist"],
            "title": row["title"],
            "source_path": row["source_path"],
            "ocr_path": row["ocr_path"],
            "page_start": int(row["page_start"]),
            "page_end": int(row["page_end"]),
            "text_hash": row["text_hash"],
            "quote": row["quote"],
            "created_at": row["created_at"],
            "created_by": row["created_by"],
        }

    def _concept_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "concept_id": row["concept_id"],
            "name": row["name"],
            "description": row["description"],
            "tags": _parse_json(row["tags_json"], []),
            "status": row["status"],
            "created_at": row["created_at"],
            "created_by": row["created_by"],
            "updated_at": row["updated_at"],
        }

    def _signal_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "signal_id": row["signal_id"],
            "version": int(row["version"]),
            "name": row["name"],
            "description": row["description"],
            "universe": _parse_json(row["universe_json"], {}),
            "spec": _parse_json(row["spec_json"], {}),
            "status": row["status"],
            "created_at": row["created_at"],
            "created_by": row["created_by"],
        }

    def _concept_citation_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "concept_id": row["concept_id"],
            "citation_id": row["citation_id"],
            "weight": float(row["weight"]),
            "note": row["note"],
            "status": row["status"],
            "created_at": row["created_at"],
            "created_by": row["created_by"],
        }

    def _concept_signal_from_row(self, row: sqlite3.Row) -> dict:
        return {
            "concept_id": row["concept_id"],
            "signal_id": row["signal_id"],
            "signal_version": int(row["signal_version"]),
            "claim": row["claim"],
            "confidence": float(row["confidence"]) if row["confidence"] is not None else None,
            "status": row["status"],
            "created_at": row["created_at"],
            "created_by": row["created_by"],
        }

    def create_citation_from_chunk(self, chunk_id: str, created_by: str | None = None) -> dict:
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM citations WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchone()
            if existing is not None:
                return self._citation_from_row(existing)

            resolved = resolve_chunk_provenance(self.data_dir, chunk_id)
            if resolved is None:
                raise LedgerNotFoundError(f"Chunk not found: {chunk_id}")

            required = (
                resolved["doc_id"],
                resolved["theorist"],
                resolved["title"],
                resolved["source_path"],
                resolved["ocr_path"],
                resolved["text_hash"],
                resolved["quote"],
            )
            if any(str(value).strip() == "" for value in required):
                raise LedgerValidationError(f"Incomplete chunk provenance for chunk_id={chunk_id}")

            citation_id = str(uuid4())
            created_at = utc_now_iso()
            conn.execute(
                """
                INSERT INTO citations(
                  citation_id, chunk_id, doc_id, theorist, title, source_path, ocr_path,
                  page_start, page_end, text_hash, quote, created_at, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    citation_id,
                    resolved["chunk_id"],
                    resolved["doc_id"],
                    resolved["theorist"],
                    resolved["title"],
                    resolved["source_path"],
                    resolved["ocr_path"],
                    int(resolved["page_start"]),
                    int(resolved["page_end"]),
                    resolved["text_hash"],
                    resolved["quote"],
                    created_at,
                    created_by,
                ),
            )
            row = conn.execute(
                "SELECT * FROM citations WHERE citation_id = ?",
                (citation_id,),
            ).fetchone()
            assert row is not None
            payload = self._citation_from_row(row)
            self._log_event(
                conn,
                actor=created_by,
                event_type="citation.create",
                entity_type="citation",
                entity_id=citation_id,
                payload=payload,
            )
            conn.commit()
            return payload

    def create_concept(
        self,
        name: str,
        description: str | None = None,
        tags: list[str] | None = None,
        status: str = "proposed",
        created_by: str | None = None,
    ) -> dict:
        _validate_status(status, CONCEPT_STATUSES, "concept.status")
        tags_value = sorted(set(tags or []))
        concept_id = str(uuid4())
        created_at = utc_now_iso()
        with self._connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO concepts(
                      concept_id, name, description, tags_json, status, created_at, created_by, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        concept_id,
                        name,
                        description,
                        _to_json(tags_value),
                        status,
                        created_at,
                        created_by,
                        None,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise LedgerConflictError(f"Concept name already exists: {name}") from exc

            row = conn.execute("SELECT * FROM concepts WHERE concept_id = ?", (concept_id,)).fetchone()
            assert row is not None
            payload = self._concept_from_row(row)
            self._log_event(
                conn,
                actor=created_by,
                event_type="concept.create",
                entity_type="concept",
                entity_id=concept_id,
                payload=payload,
            )
            conn.commit()
            return payload

    def list_concepts(self, status: str | None = None, name_contains: str | None = None) -> list[dict]:
        query = "SELECT * FROM concepts WHERE 1=1"
        params: list[object] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if name_contains:
            query += " AND LOWER(name) LIKE ?"
            params.append(f"%{name_contains.lower()}%")
        query += " ORDER BY name ASC, concept_id ASC"

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
            return [self._concept_from_row(row) for row in rows]

    def link_concept_citations(
        self,
        concept_id: str,
        citation_ids: list[str],
        weight: float = 1.0,
        note: str | None = None,
        status: str = "proposed",
        created_by: str | None = None,
    ) -> list[dict]:
        _validate_status(status, LINK_STATUSES, "concept_citation.status")
        if not citation_ids:
            return []

        with self._connect() as conn:
            concept = conn.execute(
                "SELECT concept_id FROM concepts WHERE concept_id = ?",
                (concept_id,),
            ).fetchone()
            if concept is None:
                raise LedgerNotFoundError(f"Concept not found: {concept_id}")

            unique_ids = sorted(set(citation_ids))
            rows: list[dict] = []
            for citation_id in unique_ids:
                citation = conn.execute(
                    "SELECT citation_id FROM citations WHERE citation_id = ?",
                    (citation_id,),
                ).fetchone()
                if citation is None:
                    raise LedgerNotFoundError(f"Citation not found: {citation_id}")

                existing = conn.execute(
                    """
                    SELECT * FROM concept_citations
                    WHERE concept_id = ? AND citation_id = ?
                    """,
                    (concept_id, citation_id),
                ).fetchone()
                if existing is None:
                    created_at = utc_now_iso()
                    conn.execute(
                        """
                        INSERT INTO concept_citations(
                          concept_id, citation_id, weight, note, status, created_at, created_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (concept_id, citation_id, float(weight), note, status, created_at, created_by),
                    )
                    inserted = conn.execute(
                        """
                        SELECT * FROM concept_citations
                        WHERE concept_id = ? AND citation_id = ?
                        """,
                        (concept_id, citation_id),
                    ).fetchone()
                    assert inserted is not None
                    link_payload = self._concept_citation_from_row(inserted)
                    self._log_event(
                        conn,
                        actor=created_by,
                        event_type="concept_citation.link",
                        entity_type="concept_citation",
                        entity_id=f"{concept_id}:{citation_id}",
                        payload=link_payload,
                    )
                    rows.append(link_payload)
                    continue

                rows.append(self._concept_citation_from_row(existing))

            conn.commit()
            return rows

    def create_signal(
        self,
        name: str,
        description: str | None,
        universe: object,
        spec: object,
        status: str = "proposed",
        created_by: str | None = None,
    ) -> dict:
        _validate_status(status, SIGNAL_STATUSES, "signal.status")

        signal_id = str(uuid4())
        version = 1
        created_at = utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO signals(
                  signal_id, version, name, description, universe_json, spec_json,
                  status, created_at, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    version,
                    name,
                    description,
                    _to_json(universe),
                    _to_json(spec),
                    status,
                    created_at,
                    created_by,
                ),
            )
            row = conn.execute(
                "SELECT * FROM signals WHERE signal_id = ? AND version = ?",
                (signal_id, version),
            ).fetchone()
            assert row is not None
            payload = self._signal_from_row(row)
            self._log_event(
                conn,
                actor=created_by,
                event_type="signal.create",
                entity_type="signal",
                entity_id=_entity_signal_id(signal_id, version),
                payload=payload,
            )
            conn.commit()
            return payload

    def get_latest_signal_version(self, conn: sqlite3.Connection, signal_id: str) -> int:
        row = conn.execute(
            "SELECT MAX(version) AS max_version FROM signals WHERE signal_id = ?",
            (signal_id,),
        ).fetchone()
        if row is None or row["max_version"] is None:
            raise LedgerNotFoundError(f"Signal not found: {signal_id}")
        return int(row["max_version"])

    def clone_signal(
        self,
        signal_id: str,
        patch_json: object,
        from_version: int | None = None,
        created_by: str | None = None,
    ) -> dict:
        if not isinstance(patch_json, dict):
            raise LedgerValidationError("patch_json must be an object")

        with self._connect() as conn:
            source_version = from_version if from_version is not None else self.get_latest_signal_version(conn, signal_id)
            source = conn.execute(
                "SELECT * FROM signals WHERE signal_id = ? AND version = ?",
                (signal_id, source_version),
            ).fetchone()
            if source is None:
                raise LedgerNotFoundError(f"Signal version not found: {signal_id}:{source_version}")

            latest_version = self.get_latest_signal_version(conn, signal_id)
            next_version = latest_version + 1

            base_universe = _parse_json(source["universe_json"], {})
            base_spec = _parse_json(source["spec_json"], {})
            name = source["name"]
            description = source["description"]
            status = source["status"]

            if "name" in patch_json:
                name = str(patch_json["name"])
            if "description" in patch_json:
                description = patch_json["description"]
            if "status" in patch_json:
                status = str(patch_json["status"])
            _validate_status(status, SIGNAL_STATUSES, "signal.status")

            if "universe" in patch_json:
                base_universe = _merge_values(base_universe, patch_json["universe"])
            if "spec" in patch_json:
                base_spec = _merge_values(base_spec, patch_json["spec"])

            created_at = utc_now_iso()
            conn.execute(
                """
                INSERT INTO signals(
                  signal_id, version, name, description, universe_json, spec_json,
                  status, created_at, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    next_version,
                    name,
                    description,
                    _to_json(base_universe),
                    _to_json(base_spec),
                    status,
                    created_at,
                    created_by,
                ),
            )
            row = conn.execute(
                "SELECT * FROM signals WHERE signal_id = ? AND version = ?",
                (signal_id, next_version),
            ).fetchone()
            assert row is not None
            payload = self._signal_from_row(row)
            self._log_event(
                conn,
                actor=created_by,
                event_type="signal.clone",
                entity_type="signal",
                entity_id=_entity_signal_id(signal_id, next_version),
                payload={
                    "from_version": source_version,
                    "patch_json": patch_json,
                    "created": payload,
                },
            )
            conn.commit()
            return payload

    def link_concept_signal(
        self,
        concept_id: str,
        signal_id: str,
        signal_version: int | None,
        claim: str,
        confidence: float | None = None,
        status: str = "proposed",
        created_by: str | None = None,
    ) -> dict:
        _validate_status(status, LINK_STATUSES, "concept_signal.status")
        if confidence is not None and (confidence < 0.0 or confidence > 1.0):
            raise LedgerValidationError("confidence must be within [0, 1]")

        with self._connect() as conn:
            concept = conn.execute(
                "SELECT concept_id FROM concepts WHERE concept_id = ?",
                (concept_id,),
            ).fetchone()
            if concept is None:
                raise LedgerNotFoundError(f"Concept not found: {concept_id}")

            version = signal_version if signal_version is not None else self.get_latest_signal_version(conn, signal_id)
            signal = conn.execute(
                "SELECT signal_id, version FROM signals WHERE signal_id = ? AND version = ?",
                (signal_id, version),
            ).fetchone()
            if signal is None:
                raise LedgerNotFoundError(f"Signal version not found: {signal_id}:{version}")

            existing = conn.execute(
                """
                SELECT * FROM concept_signals
                WHERE concept_id = ? AND signal_id = ? AND signal_version = ?
                """,
                (concept_id, signal_id, version),
            ).fetchone()
            if existing is None:
                created_at = utc_now_iso()
                conn.execute(
                    """
                    INSERT INTO concept_signals(
                      concept_id, signal_id, signal_version, claim, confidence, status, created_at, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (concept_id, signal_id, version, claim, confidence, status, created_at, created_by),
                )
                inserted = conn.execute(
                    """
                    SELECT * FROM concept_signals
                    WHERE concept_id = ? AND signal_id = ? AND signal_version = ?
                    """,
                    (concept_id, signal_id, version),
                ).fetchone()
                assert inserted is not None
                payload = self._concept_signal_from_row(inserted)
                self._log_event(
                    conn,
                    actor=created_by,
                    event_type="concept_signal.link",
                    entity_type="concept_signal",
                    entity_id=f"{concept_id}:{signal_id}:{version}",
                    payload=payload,
                )
                conn.commit()
                return payload

            before = self._concept_signal_from_row(existing)
            _validate_transition(before["status"], status, signal=False)
            conn.execute(
                """
                UPDATE concept_signals
                SET claim = ?, confidence = ?, status = ?, created_by = COALESCE(?, created_by)
                WHERE concept_id = ? AND signal_id = ? AND signal_version = ?
                """,
                (claim, confidence, status, created_by, concept_id, signal_id, version),
            )
            row = conn.execute(
                """
                SELECT * FROM concept_signals
                WHERE concept_id = ? AND signal_id = ? AND signal_version = ?
                """,
                (concept_id, signal_id, version),
            ).fetchone()
            assert row is not None
            payload = self._concept_signal_from_row(row)
            self._log_event(
                conn,
                actor=created_by,
                event_type="concept_signal.link",
                entity_type="concept_signal",
                entity_id=f"{concept_id}:{signal_id}:{version}",
                payload={"before": before, "after": payload},
            )
            if before["status"] != payload["status"]:
                self._log_event(
                    conn,
                    actor=created_by,
                    event_type="status.change",
                    entity_type="concept_signal",
                    entity_id=f"{concept_id}:{signal_id}:{version}",
                    payload={"before": {"status": before["status"]}, "after": {"status": payload["status"]}},
                )
            conn.commit()
            return payload

    def update_concept_status(self, concept_id: str, new_status: str, actor: str | None = None) -> dict:
        _validate_status(new_status, CONCEPT_STATUSES, "concept.status")
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM concepts WHERE concept_id = ?", (concept_id,)).fetchone()
            if row is None:
                raise LedgerNotFoundError(f"Concept not found: {concept_id}")
            before = self._concept_from_row(row)
            _validate_transition(before["status"], new_status, signal=False)
            updated_at = utc_now_iso()
            conn.execute(
                "UPDATE concepts SET status = ?, updated_at = ? WHERE concept_id = ?",
                (new_status, updated_at, concept_id),
            )
            after_row = conn.execute("SELECT * FROM concepts WHERE concept_id = ?", (concept_id,)).fetchone()
            assert after_row is not None
            after = self._concept_from_row(after_row)
            self._log_event(
                conn,
                actor=actor,
                event_type="status.change",
                entity_type="concept",
                entity_id=concept_id,
                payload={"before": {"status": before["status"]}, "after": {"status": after["status"]}},
            )
            conn.commit()
            return after

    def update_signal_status(
        self,
        signal_id: str,
        version: int | None,
        new_status: str,
        actor: str | None = None,
    ) -> dict:
        _validate_status(new_status, SIGNAL_STATUSES, "signal.status")
        with self._connect() as conn:
            resolved_version = version if version is not None else self.get_latest_signal_version(conn, signal_id)
            row = conn.execute(
                "SELECT * FROM signals WHERE signal_id = ? AND version = ?",
                (signal_id, resolved_version),
            ).fetchone()
            if row is None:
                raise LedgerNotFoundError(f"Signal not found: {signal_id}:{resolved_version}")
            before = self._signal_from_row(row)
            _validate_transition(before["status"], new_status, signal=True)
            conn.execute(
                "UPDATE signals SET status = ? WHERE signal_id = ? AND version = ?",
                (new_status, signal_id, resolved_version),
            )
            after_row = conn.execute(
                "SELECT * FROM signals WHERE signal_id = ? AND version = ?",
                (signal_id, resolved_version),
            ).fetchone()
            assert after_row is not None
            after = self._signal_from_row(after_row)
            self._log_event(
                conn,
                actor=actor,
                event_type="status.change",
                entity_type="signal",
                entity_id=_entity_signal_id(signal_id, resolved_version),
                payload={"before": {"status": before["status"]}, "after": {"status": after["status"]}},
            )
            conn.commit()
            return after

    def update_concept_citation_status(
        self,
        concept_id: str,
        citation_id: str,
        new_status: str,
        actor: str | None = None,
    ) -> dict:
        _validate_status(new_status, LINK_STATUSES, "concept_citation.status")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM concept_citations
                WHERE concept_id = ? AND citation_id = ?
                """,
                (concept_id, citation_id),
            ).fetchone()
            if row is None:
                raise LedgerNotFoundError(f"Concept citation link not found: {concept_id}:{citation_id}")
            before = self._concept_citation_from_row(row)
            _validate_transition(before["status"], new_status, signal=False)
            conn.execute(
                """
                UPDATE concept_citations
                SET status = ?
                WHERE concept_id = ? AND citation_id = ?
                """,
                (new_status, concept_id, citation_id),
            )
            after_row = conn.execute(
                """
                SELECT * FROM concept_citations
                WHERE concept_id = ? AND citation_id = ?
                """,
                (concept_id, citation_id),
            ).fetchone()
            assert after_row is not None
            after = self._concept_citation_from_row(after_row)
            self._log_event(
                conn,
                actor=actor,
                event_type="status.change",
                entity_type="concept_citation",
                entity_id=f"{concept_id}:{citation_id}",
                payload={"before": {"status": before["status"]}, "after": {"status": after["status"]}},
            )
            conn.commit()
            return after

    def update_concept_signal_status(
        self,
        concept_id: str,
        signal_id: str,
        signal_version: int,
        new_status: str,
        actor: str | None = None,
    ) -> dict:
        _validate_status(new_status, LINK_STATUSES, "concept_signal.status")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM concept_signals
                WHERE concept_id = ? AND signal_id = ? AND signal_version = ?
                """,
                (concept_id, signal_id, signal_version),
            ).fetchone()
            if row is None:
                raise LedgerNotFoundError(
                    f"Concept signal link not found: {concept_id}:{signal_id}:{signal_version}"
                )
            before = self._concept_signal_from_row(row)
            _validate_transition(before["status"], new_status, signal=False)
            conn.execute(
                """
                UPDATE concept_signals
                SET status = ?
                WHERE concept_id = ? AND signal_id = ? AND signal_version = ?
                """,
                (new_status, concept_id, signal_id, signal_version),
            )
            after_row = conn.execute(
                """
                SELECT * FROM concept_signals
                WHERE concept_id = ? AND signal_id = ? AND signal_version = ?
                """,
                (concept_id, signal_id, signal_version),
            ).fetchone()
            assert after_row is not None
            after = self._concept_signal_from_row(after_row)
            self._log_event(
                conn,
                actor=actor,
                event_type="status.change",
                entity_type="concept_signal",
                entity_id=f"{concept_id}:{signal_id}:{signal_version}",
                payload={"before": {"status": before["status"]}, "after": {"status": after["status"]}},
            )
            conn.commit()
            return after

    def explain_signal(
        self,
        signal_id: str,
        version: int | None = None,
        status_filter: str | None = None,
    ) -> dict:
        with self._connect() as conn:
            try:
                return explain_signal(
                    conn,
                    signal_id=signal_id,
                    version=version,
                    status_filter=status_filter,
                )
            except ExplainError as exc:
                raise LedgerNotFoundError(str(exc)) from exc

    def list_events(
        self,
        entity_type: str | None = None,
        entity_id: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        query = "SELECT * FROM events WHERE 1=1"
        params: list[object] = []
        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type)
        if entity_id:
            query += " AND entity_id = ?"
            params.append(entity_id)
        query += " ORDER BY ts DESC, event_id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 500)))

        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
            return [
                {
                    "event_id": row["event_id"],
                    "ts": row["ts"],
                    "actor": row["actor"],
                    "event_type": row["event_type"],
                    "entity_type": row["entity_type"],
                    "entity_id": row["entity_id"],
                    "payload_json": _parse_json(row["payload_json"], {}),
                }
                for row in rows
            ]

    # recommendation helpers are delegated to ScanDB to keep scan/ledger persistence unified.
    def create_recommendation(
        self,
        scan_run_id: str,
        kind: str,
        title: str,
        body: str,
        confidence: float | None = None,
        related_signal_ids: list[dict] | None = None,
        related_observation_ids: list[str] | None = None,
        status: str = "proposed",
        created_by: str | None = None,
        ts: str | None = None,
    ) -> dict:
        db = ScanDB(self.db_path)
        db.initialize()
        return db.create_recommendation(
            scan_run_id=scan_run_id,
            kind=kind,
            title=title,
            body=body,
            confidence=confidence,
            related_signal_ids=related_signal_ids,
            related_observation_ids=related_observation_ids,
            status=status,
            created_by=created_by,
            ts=ts,
        )

    def list_recommendations(
        self,
        scan_run_id: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        db = ScanDB(self.db_path)
        db.initialize()
        return db.list_recommendations(scan_run_id=scan_run_id, status=status, limit=limit)

    def update_recommendation_status(
        self,
        recommendation_id: str,
        status: str,
        actor: str | None = None,
    ) -> dict:
        db = ScanDB(self.db_path)
        db.initialize()
        return db.update_recommendation_status(
            recommendation_id=recommendation_id,
            status=status,
            actor=actor,
        )
