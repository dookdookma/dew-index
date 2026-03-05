from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from dewlib.ledger_db import connect_db
from dewlib.recommend_review import list_queue as list_recommendation_queue
from dewlib.recommend_review import set_status as set_recommendation_status
from dewlib.scan_db import ScanDB
from dewlib.scan_runtime import run_scan


def _default_ledger_db_path() -> Path:
    data_dir = Path(os.getenv("DEW_DATA_DIR", "data"))
    configured = os.getenv("DEW_LEDGER_DB_PATH")
    return Path(configured) if configured else (data_dir / "ledger.sqlite3")


class RunScanRequest(BaseModel):
    ledger_db_path: str | None = None
    feeds: list[Any] = Field(..., min_length=1)
    options: dict[str, Any] | None = None
    created_by: str | None = "scanner"


class CreateRecommendationRequest(BaseModel):
    ledger_db_path: str | None = None
    scan_run_id: str
    kind: str
    title: str
    body: str
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    related_signal_ids_json: list[dict[str, Any]] | None = None
    related_observation_ids_json: list[str] | None = None
    status: str = "proposed"
    created_by: str | None = "scanner"


class UpdateRecommendationStatusRequest(BaseModel):
    status: str
    actor: str
    note: str | None = None


router = APIRouter(prefix="/scan", tags=["scan"])


@router.post("/run")
def run_scan_endpoint(request: RunScanRequest) -> dict:
    db_path = Path(request.ledger_db_path) if request.ledger_db_path else _default_ledger_db_path()
    try:
        result = run_scan(
            ledger_db_path=db_path,
            feed_sources=request.feeds,
            run_options=request.options or {},
            created_by=request.created_by,
        )
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs")
def list_scan_runs(
    ledger_db_path: str | None = Query(None),
    limit: int = Query(20, ge=1, le=500),
) -> list[dict]:
    db_path = Path(ledger_db_path) if ledger_db_path else _default_ledger_db_path()
    db = ScanDB(db_path=db_path)
    db.initialize()
    return db.list_scan_runs(limit=limit)


@router.get("/runs/{scan_run_id}")
def get_scan_run(scan_run_id: str, ledger_db_path: str | None = Query(None)) -> dict:
    db_path = Path(ledger_db_path) if ledger_db_path else _default_ledger_db_path()
    db = ScanDB(db_path=db_path)
    db.initialize()
    payload = db.get_scan_run(scan_run_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"scan_run not found: {scan_run_id}")
    return payload


@router.get("/runs/{scan_run_id}/report")
def get_scan_report(scan_run_id: str, ledger_db_path: str | None = Query(None)) -> dict:
    db_path = Path(ledger_db_path) if ledger_db_path else _default_ledger_db_path()
    db = ScanDB(db_path=db_path)
    db.initialize()
    payload = db.get_scan_run(scan_run_id)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"scan_run not found: {scan_run_id}")

    run = payload["scan_run"]
    report_path = run.get("report_path")
    if not report_path:
        return {"scan_run_id": scan_run_id, "report_path": None, "markdown": None}

    report_file = Path(report_path)
    if not report_file.exists():
        return {"scan_run_id": scan_run_id, "report_path": report_path, "markdown": None}
    return {
        "scan_run_id": scan_run_id,
        "report_path": report_path,
        "markdown": report_file.read_text(encoding="utf-8"),
    }


@router.post("/recommendations")
def create_recommendation(request: CreateRecommendationRequest) -> dict:
    db_path = Path(request.ledger_db_path) if request.ledger_db_path else _default_ledger_db_path()
    db = ScanDB(db_path=db_path)
    db.initialize()
    try:
        return db.create_recommendation(
            scan_run_id=request.scan_run_id,
            kind=request.kind,
            title=request.title,
            body=request.body,
            confidence=request.confidence,
            related_signal_ids=request.related_signal_ids_json,
            related_observation_ids=request.related_observation_ids_json,
            status=request.status,
            created_by=request.created_by,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/{scan_run_id}/recommendations")
def list_run_recommendations(
    scan_run_id: str,
    ledger_db_path: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> list[dict]:
    db_path = Path(ledger_db_path) if ledger_db_path else _default_ledger_db_path()
    db = ScanDB(db_path=db_path)
    db.initialize()
    return db.list_recommendations(scan_run_id=scan_run_id, status=status, limit=limit)


@router.get("/recommendations/queue")
def list_queue(
    ledger_db_path: str | None = Query(None),
    status: str = Query("proposed"),
    limit: int = Query(50, ge=1, le=500),
    since_ts: str | None = Query(None),
) -> dict:
    db_path = Path(ledger_db_path) if ledger_db_path else _default_ledger_db_path()
    try:
        with connect_db(db_path) as conn:
            return list_recommendation_queue(conn, status=status, limit=limit, since_ts=since_ts)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/recommendations/{recommendation_id}/status")
def update_recommendation_status(
    recommendation_id: str,
    request: UpdateRecommendationStatusRequest,
    ledger_db_path: str | None = Query(None),
) -> dict:
    db_path = Path(ledger_db_path) if ledger_db_path else _default_ledger_db_path()
    try:
        with connect_db(db_path) as conn:
            payload = set_recommendation_status(
                conn=conn,
                recommendation_id=recommendation_id,
                new_status=request.status,
                actor=request.actor,
                note=request.note,
            )
            conn.commit()
            return payload
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


app = FastAPI(title="DEW Scanner API", version="1.0")
app.include_router(router)
