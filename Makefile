PYTHON ?= python
DATA_DIR ?= data

.PHONY: manifest ocr extract chunk index health serve all

manifest:
	$(PYTHON) scripts/build_manifest.py --data-dir $(DATA_DIR)

ocr:
	$(PYTHON) scripts/ocr_batch.py --data-dir $(DATA_DIR)

extract:
	$(PYTHON) scripts/extract_pages.py --data-dir $(DATA_DIR)

chunk:
	$(PYTHON) scripts/chunk_pages.py --data-dir $(DATA_DIR)

index:
	$(PYTHON) scripts/build_index.py --data-dir $(DATA_DIR) --backend hash

health:
	$(PYTHON) scripts/library_health.py --data-dir $(DATA_DIR)

serve:
	$(PYTHON) scripts/serve.py --data-dir $(DATA_DIR) --port 8787

all:
	$(PYTHON) scripts/build_all.py --data-dir $(DATA_DIR)
