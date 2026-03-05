from __future__ import annotations

from pathlib import Path

import pytest

from dewlib.ledger_store import LedgerStore, LedgerValidationError
from dewlib.util import atomic_write_jsonl


def _write_minimal_corpus(data_dir: Path) -> str:
    chunk_id = "abcd1234abcd1234:1:0"
    (data_dir / "index").mkdir(parents=True, exist_ok=True)
    atomic_write_jsonl(
        data_dir / "index" / "meta.jsonl",
        [
            {
                "chunk_id": chunk_id,
                "doc_id": "abcd1234abcd1234",
                "theorist": "Wiener",
                "title": "Cybernetics",
                "source_path": "Wiener/Cybernetics.pdf",
                "ocr_path": "Wiener/Cybernetics.pdf",
                "page_start": 1,
                "page_end": 1,
                "text_hash": "feedfacefeedface",
                "text": "Cybernetics and communication in the animal and machine.",
            }
        ],
    )
    atomic_write_jsonl(
        data_dir / "chunks.jsonl",
        [
            {
                "chunk_id": chunk_id,
                "doc_id": "abcd1234abcd1234",
                "theorist": "Wiener",
                "title": "Cybernetics",
                "source_path": "Wiener/Cybernetics.pdf",
                "ocr_path": "Wiener/Cybernetics.pdf",
                "page_start": 1,
                "page_end": 1,
                "text_hash": "feedfacefeedface",
                "text": "Cybernetics and communication in the animal and machine.",
            }
        ],
    )
    return chunk_id


def test_ledger_v1_flow_and_status_rules(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    db_path = data_dir / "ledger.sqlite3"
    chunk_id = _write_minimal_corpus(data_dir)
    store = LedgerStore(db_path=db_path, data_dir=data_dir)
    init = store.initialize()
    assert "citations" in init["tables"]
    assert init["schema_version"] == "3"

    first_citation = store.create_citation_from_chunk(chunk_id, created_by="test")
    second_citation = store.create_citation_from_chunk(chunk_id, created_by="test")
    assert first_citation["citation_id"] == second_citation["citation_id"]
    assert first_citation["chunk_id"] == chunk_id
    assert first_citation["doc_id"] == "abcd1234abcd1234"

    concept = store.create_concept(
        name="Feedback Loop",
        description="Signal processing concept.",
        tags=["control", "communication"],
        status="approved",
        created_by="test",
    )
    links = store.link_concept_citations(
        concept_id=concept["concept_id"],
        citation_ids=[first_citation["citation_id"]],
        weight=1.0,
        note="Core citation",
        status="approved",
        created_by="test",
    )
    assert len(links) == 1
    assert links[0]["citation_id"] == first_citation["citation_id"]

    signal_v1 = store.create_signal(
        name="Cybernetic Drift",
        description="Monitors control/communication patterns.",
        universe={"tickers": ["QQQ"]},
        spec={
            "metric": "keyword_share",
            "trigger": ">0.2",
            "sources": ["transcripts"],
            "cadence": "daily",
            "action_template": "watchlist",
        },
        status="active",
        created_by="test",
    )
    assert signal_v1["version"] == 1

    link_claim = store.link_concept_signal(
        concept_id=concept["concept_id"],
        signal_id=signal_v1["signal_id"],
        signal_version=1,
        claim="Rising control discourse can map to this signal.",
        confidence=0.9,
        status="approved",
        created_by="test",
    )
    assert link_claim["signal_version"] == 1

    signal_v2 = store.clone_signal(
        signal_id=signal_v1["signal_id"],
        from_version=1,
        patch_json={"spec": {"trigger": ">0.25"}},
        created_by="test",
    )
    assert signal_v2["version"] == 2
    assert signal_v2["spec"]["trigger"] == ">0.25"

    explain = store.explain_signal(signal_id=signal_v1["signal_id"], version=1)
    assert explain["signal"]["signal_id"] == signal_v1["signal_id"]
    assert explain["concepts"]
    first_concept = explain["concepts"][0]
    assert first_concept["concept"]["name"] == "Feedback Loop"
    assert first_concept["citations"][0]["chunk_id"] == chunk_id
    assert first_concept["citations"][0]["page_start"] == 1

    concept_events = store.list_events(entity_type="concept")
    assert any(event["event_type"] == "concept.create" for event in concept_events)
    signal_events = store.list_events(entity_type="signal")
    assert any(event["event_type"] == "signal.create" for event in signal_events)
    assert any(event["event_type"] == "signal.clone" for event in signal_events)

    transient = store.create_concept(
        name="Deprecated Path",
        description="Transition test",
        status="proposed",
        created_by="test",
    )
    deprecated = store.update_concept_status(
        concept_id=transient["concept_id"],
        new_status="deprecated",
        actor="test",
    )
    assert deprecated["status"] == "deprecated"
    with pytest.raises(LedgerValidationError):
        store.update_concept_status(
            concept_id=transient["concept_id"],
            new_status="approved",
            actor="test",
        )
