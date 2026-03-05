PYTHON ?= .venv/bin/python3
LIBRARY_ROOT ?= library
DATA_DIR ?= data
FORCE ?= 0

FORCE_FLAG := $(if $(filter 1,$(FORCE)),--force,)
FORCE_OCR_FLAG := $(if $(filter 1,$(FORCE)),--force-ocr,)

.PHONY: manifest ocr extract chunk index health triage validate canon audit ledger-init ledger-smoke ledger-serve ledger-test seed-feeds seed-signal-pack scan-run scan-serve scan-test reco-queue scan-digest signal-tune serve all

manifest:
	$(PYTHON) scripts/build_manifest.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR)

ocr:
	$(PYTHON) scripts/ocr_batch.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR) $(FORCE_OCR_FLAG)

extract:
	$(PYTHON) scripts/extract_pages.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR) $(FORCE_FLAG)

chunk:
	$(PYTHON) scripts/chunk_pages.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR) $(FORCE_FLAG)

index:
	$(PYTHON) scripts/build_index.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR) --embedder hash $(FORCE_FLAG)

health:
	$(PYTHON) scripts/library_health.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR)

triage:
	$(PYTHON) scripts/triage_corpus.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR)

validate:
	$(PYTHON) scripts/validate_retrieval.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR)

canon:
	$(PYTHON) scripts/canon_registry.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR)

audit: triage validate canon

ledger-init:
	$(PYTHON) scripts/ledger_init.py --data-dir $(DATA_DIR)

ledger-smoke: ledger-init
	$(PYTHON) scripts/ledger_smoke.py --data-dir $(DATA_DIR)

ledger-serve:
	$(PYTHON) scripts/ledger_serve.py --data-dir $(DATA_DIR) --port 8788

ledger-test:
	$(PYTHON) -m pytest -q tests/test_ledger_v1.py

seed-feeds:
	$(PYTHON) scripts/seed_feeds.py --feeds-registry $(DATA_DIR)/feeds.json

seed-signal-pack:
	$(PYTHON) scripts/seed_signal_pack.py --ledger-db $(DATA_DIR)/ledger.sqlite3 --signal-pack $(DATA_DIR)/signal_pack_v1.json

scan-run:
	$(PYTHON) scripts/scan_run.py --ledger-db $(DATA_DIR)/ledger.sqlite3 --feeds-registry $(DATA_DIR)/feeds.json --feed-set core --cadence ad_hoc --out-dir out/scans --created-by scanner

scan-serve:
	$(PYTHON) scripts/scan_serve.py --data-dir $(DATA_DIR) --port 8789

scan-test:
	$(PYTHON) -m pytest -q tests/test_scanner_v1.py

reco-queue:
	$(PYTHON) scripts/reco_queue.py --ledger-db $(DATA_DIR)/ledger.sqlite3 --status proposed --limit 50

scan-digest:
	$(PYTHON) scripts/scan_digest.py --ledger-db $(DATA_DIR)/ledger.sqlite3 --tz America/New_York --cadences morning,midday,close --out-dir out/digests

signal-tune:
	@echo "Example:"
	@echo "./scripts/signal_tune.py --ledger-db $(DATA_DIR)/ledger.sqlite3 --signal-id <signal_id> --set threshold=4 --set window_items=200 --created-by operator"

serve:
	$(PYTHON) scripts/serve.py --library-root $(LIBRARY_ROOT) --data-dir $(DATA_DIR) --port 8787

all: manifest ocr extract chunk index health
