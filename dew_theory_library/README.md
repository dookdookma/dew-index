# DEW Theory Library v1

Local ETL + hybrid retrieval service for a PDF theory library with citation-safe provenance.

## What it does

- Ingests PDFs from `./library` (or `--library-root` override).
- Computes stable `doc_id` from original source PDF bytes (`sha256[:16]`).
- Optional OCR with OCRmyPDF into `data/ocr/`.
- Extracts page-bounded text via PyMuPDF.
- Chunks within page boundaries only.
- Builds BM25 + FAISS hybrid index.
- Serves FastAPI endpoints for grounded retrieval.
- Produces health report for OCR/extraction quality.

## System dependencies (OCR optional)

For OCR stage (`make ocr`):
- `ocrmypdf`
- `tesseract`

If OCRmyPDF is not installed, OCR stage no-ops with warning and exits 0.

## Install

```bash
python -m venv .venv
. .venv/Scripts/activate  # PowerShell: .venv\Scripts\Activate.ps1
pip install -e .[dev]
```

Optional sentence-transformers backend:

```bash
pip install -e .[st]
```

## Library layout

```text
library/
  McLuhan/
    understanding_media.pdf
  Debord/
    spectacle.pdf
```

## Pipeline commands

```bash
make manifest
make ocr
make extract
make chunk
make index
make health
make all
```

Force rebuild for stages that support it:

```bash
make all FORCE=1
```

## Trust + audit commands

```bash
make triage
make validate
make canon
make audit
```

## Serve API

```bash
make serve
```

Runs on `http://127.0.0.1:8787`.

### Search

```bash
curl -X POST http://127.0.0.1:8787/search \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"medium is the message\", \"theorist\":\"McLuhan\", \"top_k\":8}"
```

### Chunk

```bash
curl http://127.0.0.1:8787/chunk/<chunk_id>
```

### Doc metadata

```bash
curl http://127.0.0.1:8787/doc/<doc_id>
```

### Doc pages

```bash
curl "http://127.0.0.1:8787/doc/<doc_id>/pages?start=1&end=3"
```

## Evidence Ledger (Associations Layer)

Initialize ledger DB:

```bash
make ledger-init
```

Run a local smoke flow (citation -> concept -> signal -> explain):

```bash
make ledger-smoke
```

Serve ledger API on `http://127.0.0.1:8788`:

```bash
make ledger-serve
```

### Ledger API examples

Create concept:

```bash
curl -X POST http://127.0.0.1:8788/ledger/concepts \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"speed logistics\",\"description\":\"Operationalized Virilio node\",\"tags\":[\"virilio\",\"dromology\"],\"status\":\"proposed\",\"created_by\":\"analyst\"}"
```

Create citation from an existing chunk:

```bash
curl -X POST http://127.0.0.1:8788/ledger/citations/from_chunk \
  -H "Content-Type: application/json" \
  -d "{\"chunk_id\":\"<doc_id>:<page>:<k>\",\"created_by\":\"analyst\"}"
```

Explain signal:

```bash
curl "http://127.0.0.1:8788/ledger/explain/signal/<signal_id>?version=1"
```

`/ledger/explain/signal/<signal_id>` returns the audited rationale chain:
signal definition -> linked concept claims -> grounded citations with page provenance.

## Scanner v1 (Observations Only)

Scanner v1 reads external inputs (RSS/Atom), evaluates active ledger signals, logs observations,
and writes evidence-backed scan reports (Markdown + JSON sidecar). It does not execute trades.

Seed feed registry and signal pack:

```bash
make seed-feeds
make seed-signal-pack
```

Run a scan from CLI:

```bash
python scripts/scan_run.py \
  --ledger-db data/ledger.sqlite3 \
  --feeds-registry data/feeds.json \
  --feed-set core \
  --cadence morning \
  --out-dir out/scans \
  --created-by scanner
```

Serve scan API on `http://127.0.0.1:8789`:

```bash
make scan-serve
```

Example API run:

```bash
curl -X POST http://127.0.0.1:8789/scan/run \
  -H "Content-Type: application/json" \
  -d "{\"feeds\":[\"file:///tmp/sample_feed.xml\"],\"options\":{\"max_items\":200,\"cadence\":\"ad_hoc\"},\"created_by\":\"scanner\"}"
```

Create recommendation (paper mode):

```bash
curl -X POST http://127.0.0.1:8789/scan/recommendations \
  -H "Content-Type: application/json" \
  -d "{\"scan_run_id\":\"<scan_run_id>\",\"kind\":\"watch\",\"title\":\"Paper watch\",\"body\":\"Manual note.\",\"status\":\"proposed\",\"created_by\":\"analyst\"}"
```

List run recommendations:

```bash
curl "http://127.0.0.1:8789/scan/runs/<scan_run_id>/recommendations"
```

Global recommendation review queue:

```bash
curl "http://127.0.0.1:8789/scan/recommendations/queue?status=proposed&limit=50"
```

Update recommendation status (paper-mode review action):

```bash
curl -X POST http://127.0.0.1:8789/scan/recommendations/<recommendation_id>/status \
  -H "Content-Type: application/json" \
  -d "{\"status\":\"accepted\",\"actor\":\"operator\",\"note\":\"Reviewed in morning loop\"}"
```

Generate a daily digest (aggregation only):

```bash
python scripts/scan_digest.py \
  --ledger-db data/ledger.sqlite3 \
  --date 2026-03-05 \
  --tz America/New_York \
  --cadences morning,midday,close \
  --out-dir out/digests
```

Tune an `rss_keyword_count` signal by clone+patch (creates new version):

```bash
python scripts/signal_tune.py \
  --ledger-db data/ledger.sqlite3 \
  --signal-id <signal_id> \
  --set threshold=4 \
  --set window_items=200 \
  --set keywords_add=export\ controls \
  --created-by operator
```

Every scan writes feed-health diagnostics:
- `out/scans/<run_basename>_feeds_health.json` with per-feed fetch/parse stats.
- Scan JSON sidecar includes `feeds_total`, `feeds_ok`, `feeds_failed`, `items_total`, and `feeds_health_path`.

Why `explain signal` matters:
- Every trigger is tied back to concept claims and source citations.
- Scan outputs remain auditable and provenance-safe before any future execution layer.

## Stable citation guarantee

`doc_id` is derived from the original source PDF bytes, not OCR output, so citations remain stable across re-OCR.

## Tests (offline)

```bash
pytest -q
```
