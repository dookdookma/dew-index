FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY dewlib ./dewlib
COPY server ./server
COPY data ./data

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

EXPOSE 8080
CMD ["sh", "-c", "mkdir -p /data/data && cp -rn /app/data/* /data/data/ || true && uvicorn server.api:app --host 0.0.0.0 --port ${PORT:-8080}"]


