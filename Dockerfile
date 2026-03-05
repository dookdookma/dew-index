FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY dewlib ./dewlib
COPY server ./server
COPY scripts ./scripts
COPY data ./data

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

EXPOSE 8080
CMD ["sh", "-c", "set -e; mkdir -p /data/data; cp -rn /app/data/* /data/data/ || true; [ -f /data/data/chunks.jsonl ] || python scripts/chunk_pages.py --data-dir /data/data --manifest-path /data/data/manifest.jsonl --force; [ -f /data/data/index/bm25_tokens.json ] || python scripts/build_index.py --data-dir /data/data; uvicorn server.api:app --host 0.0.0.0 --port ${PORT:-8080}"]







