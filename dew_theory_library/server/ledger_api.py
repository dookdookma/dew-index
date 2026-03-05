from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from dewlib.ledger_store import (
    LedgerConflictError,
    LedgerNotFoundError,
    LedgerStore,
    LedgerValidationError,
)


def _data_dir() -> Path:
    return Path(os.getenv("DEW_DATA_DIR", "data"))


def _db_path() -> Path:
    configured = os.getenv("DEW_LEDGER_DB_PATH")
    if configured:
        return Path(configured)
    return _data_dir() / "ledger.sqlite3"


@lru_cache(maxsize=1)
def _store() -> LedgerStore:
    store = LedgerStore(db_path=_db_path(), data_dir=_data_dir())
    store.initialize()
    return store


class CitationFromChunkRequest(BaseModel):
    chunk_id: str = Field(..., min_length=1)
    created_by: str | None = None


class CreateConceptRequest(BaseModel):
    name: str = Field(..., min_length=1)
    description: str | None = None
    tags: list[str] | None = None
    status: str = "proposed"
    created_by: str | None = None


class LinkConceptCitationsRequest(BaseModel):
    citation_ids: list[str] = Field(..., min_length=1)
    weight: float = 1.0
    note: str | None = None
    status: str = "proposed"
    created_by: str | None = None


class CreateSignalRequest(BaseModel):
    name: str = Field(..., min_length=1)
    description: str | None = None
    universe: Any
    spec: Any
    status: str = "proposed"
    created_by: str | None = None


class CloneSignalRequest(BaseModel):
    from_version: int | None = None
    patch_json: dict[str, Any]
    created_by: str | None = None


class LinkConceptSignalRequest(BaseModel):
    signal_id: str = Field(..., min_length=1)
    signal_version: int | None = None
    claim: str = Field(..., min_length=1)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    status: str = "proposed"
    created_by: str | None = None


router = APIRouter(prefix="/ledger", tags=["ledger"])


def _raise_http(exc: Exception) -> None:
    if isinstance(exc, LedgerNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, LedgerValidationError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, LedgerConflictError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/citations/from_chunk")
def create_citation_from_chunk(request: CitationFromChunkRequest) -> dict:
    try:
        return _store().create_citation_from_chunk(
            chunk_id=request.chunk_id,
            created_by=request.created_by,
        )
    except Exception as exc:
        _raise_http(exc)


@router.post("/concepts")
def create_concept(request: CreateConceptRequest) -> dict:
    try:
        return _store().create_concept(
            name=request.name,
            description=request.description,
            tags=request.tags,
            status=request.status,
            created_by=request.created_by,
        )
    except Exception as exc:
        _raise_http(exc)


@router.get("/concepts")
def list_concepts(
    status: str | None = Query(None),
    name_contains: str | None = Query(None),
) -> list[dict]:
    try:
        return _store().list_concepts(status=status, name_contains=name_contains)
    except Exception as exc:
        _raise_http(exc)


@router.post("/concepts/{concept_id}/citations")
def link_concept_citations(concept_id: str, request: LinkConceptCitationsRequest) -> list[dict]:
    try:
        return _store().link_concept_citations(
            concept_id=concept_id,
            citation_ids=request.citation_ids,
            weight=request.weight,
            note=request.note,
            status=request.status,
            created_by=request.created_by,
        )
    except Exception as exc:
        _raise_http(exc)


@router.post("/signals")
def create_signal(request: CreateSignalRequest) -> dict:
    try:
        return _store().create_signal(
            name=request.name,
            description=request.description,
            universe=request.universe,
            spec=request.spec,
            status=request.status,
            created_by=request.created_by,
        )
    except Exception as exc:
        _raise_http(exc)


@router.post("/signals/{signal_id}/clone")
def clone_signal(signal_id: str, request: CloneSignalRequest) -> dict:
    try:
        return _store().clone_signal(
            signal_id=signal_id,
            from_version=request.from_version,
            patch_json=request.patch_json,
            created_by=request.created_by,
        )
    except Exception as exc:
        _raise_http(exc)


@router.post("/concepts/{concept_id}/signals")
def link_concept_signal(concept_id: str, request: LinkConceptSignalRequest) -> dict:
    try:
        return _store().link_concept_signal(
            concept_id=concept_id,
            signal_id=request.signal_id,
            signal_version=request.signal_version,
            claim=request.claim,
            confidence=request.confidence,
            status=request.status,
            created_by=request.created_by,
        )
    except Exception as exc:
        _raise_http(exc)


@router.get("/explain/signal/{signal_id}")
def explain_signal_endpoint(
    signal_id: str,
    version: int | None = Query(None),
    status_filter: str | None = Query("approved+active"),
) -> dict:
    try:
        return _store().explain_signal(
            signal_id=signal_id,
            version=version,
            status_filter=status_filter,
        )
    except Exception as exc:
        _raise_http(exc)


@router.get("/events")
def list_events(
    entity_type: str | None = Query(None),
    entity_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    try:
        return _store().list_events(
            entity_type=entity_type,
            entity_id=entity_id,
            limit=limit,
        )
    except Exception as exc:
        _raise_http(exc)


app = FastAPI(title="DEW Evidence Ledger API", version="1.0")
app.include_router(router)

