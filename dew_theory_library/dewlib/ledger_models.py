from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


CONCEPT_STATUSES = {"proposed", "approved", "deprecated"}
SIGNAL_STATUSES = {"proposed", "active", "deprecated"}
LINK_STATUSES = {"proposed", "approved", "deprecated"}


@dataclass(frozen=True)
class CitationRecord:
    citation_id: str
    chunk_id: str
    doc_id: str
    theorist: str
    title: str
    source_path: str
    ocr_path: str
    page_start: int
    page_end: int
    text_hash: str
    quote: str
    created_at: str
    created_by: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConceptRecord:
    concept_id: str
    name: str
    description: str | None
    tags: list[str]
    status: str
    created_at: str
    created_by: str | None
    updated_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SignalRecord:
    signal_id: str
    version: int
    name: str
    description: str | None
    universe: Any
    spec: Any
    status: str
    created_at: str
    created_by: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConceptCitationLinkRecord:
    concept_id: str
    citation_id: str
    weight: float
    note: str | None
    status: str
    created_at: str
    created_by: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConceptSignalLinkRecord:
    concept_id: str
    signal_id: str
    signal_version: int
    claim: str
    confidence: float | None
    status: str
    created_at: str
    created_by: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EventRecord:
    event_id: str
    ts: str
    actor: str | None
    event_type: str
    entity_type: str
    entity_id: str
    payload: Any

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

