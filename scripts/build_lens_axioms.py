#!/usr/bin/env python3
from __future__ import annotations
import json, os, datetime as dt
from urllib import request
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
OUT = BASE / "cache" / "lens_axioms.json"
URL = os.getenv("DEW_LIBRARY_URL", "https://dew-index-production.up.railway.app").rstrip("/")

THEORISTS = [
  "McLuhan","Flusser","Illich","Virilio","Debord","Baudrillard",
  "Deleuze","Galloway","Thacker","Kittler","Castells","Sontag","Lacan","Girard","Wiener"
]

def post_json(url, payload):
  data = json.dumps(payload).encode("utf-8")
  req = request.Request(url, data=data, headers={"Content-Type":"application/json"})
  with request.urlopen(req, timeout=12) as r:
    return json.loads(r.read().decode("utf-8", errors="ignore"))

def main():
  axioms = {}
  for t in THEORISTS:
    q = f"{t} core essence structural bottleneck"
    try:
      hits = post_json(f"{URL}/search", {"query": q, "theorist": t, "top_k": 3})
    except Exception:
      hits = []
    rows = []
    for h in (hits or [])[:3]:
      rows.append({
        "theorist": h.get("theorist", t),
        "title": h.get("title", ""),
        "doc_id": h.get("doc_id", ""),
        "page_start": h.get("page_start", 0),
        "page_end": h.get("page_end", 0),
        "chunk_id": h.get("chunk_id", ""),
        "axiom": (h.get("excerpt") or "")[:280]
      })
    axioms[t] = rows
  OUT.write_text(json.dumps({"updatedAt": dt.datetime.utcnow().isoformat()+"Z", "axioms": axioms}, indent=2), encoding="utf-8")

if __name__ == "__main__":
  main()
