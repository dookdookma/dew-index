from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .util import atomic_write_json


STATIC_GENERATED_AT = "2026-03-05T00:00:00Z"

DEFAULT_FEEDS_REGISTRY: dict[str, Any] = {
    "generated_at": STATIC_GENERATED_AT,
    "feeds": [
        {"id": "reuters-world", "url": "https://www.reuters.com/world/rss", "tags": ["news", "world"], "enabled": True},
        {"id": "ft-world", "url": "https://www.ft.com/world?format=rss", "tags": ["news", "world"], "enabled": True},
        {"id": "ap-world", "url": "https://apnews.com/hub/world-news?output=1", "tags": ["news", "world"], "enabled": True},
        {"id": "wsj-tech", "url": "https://feeds.a.dj.com/rss/RSSWSJD.xml", "tags": ["news", "tech"], "enabled": True},
        {"id": "semiengineering", "url": "https://semiengineering.com/feed/", "tags": ["semis", "manufacturing"], "enabled": True},
        {"id": "iea-news", "url": "https://www.iea.org/newsroom/news.rss", "tags": ["energy", "grid"], "enabled": True},
        {"id": "ferc-news", "url": "https://www.ferc.gov/rss", "tags": ["energy", "policy"], "enabled": True},
        {"id": "eia-today", "url": "https://www.eia.gov/todayinenergy/rss.php", "tags": ["energy", "macro"], "enabled": True},
        {"id": "oecd-policy", "url": "https://www.oecd.org/newsroom/rss.xml", "tags": ["policy", "macro"], "enabled": True},
        {"id": "economist-science", "url": "https://www.economist.com/science-and-technology/rss.xml", "tags": ["science", "tech"], "enabled": True},
        {"id": "guardian-tech", "url": "https://www.theguardian.com/uk/technology/rss", "tags": ["news", "tech"], "enabled": True},
        {"id": "local-fixture", "url": "file:///tmp/dew_scan_feed.xml", "tags": ["local", "test"], "enabled": False},
    ],
    "feed_sets": {
        "core": ["reuters-world", "ft-world", "ap-world", "wsj-tech", "semiengineering"],
        "electric_stack": ["reuters-world", "ft-world", "wsj-tech", "semiengineering"],
        "culture_sensors": ["guardian-tech", "economist-science", "oecd-policy"],
        "markets_macro": ["reuters-world", "ft-world", "eia-today", "iea-news", "oecd-policy"],
    },
}

DEFAULT_SIGNAL_PACK_V1: dict[str, Any] = {
    "pack_id": "signal_pack_v1",
    "generated_at": STATIC_GENERATED_AT,
    "concepts": [
        {"name": "dromology", "description": "Virilio: speed as politics", "tags": ["virilio", "speed"], "status": "approved"},
        {"name": "logistics of perception", "description": "Virilio: mediated war and perception pipelines", "tags": ["virilio", "perception"], "status": "approved"},
        {"name": "medium environment", "description": "McLuhan: medium/message and environmental effects", "tags": ["mcluhan", "media"], "status": "approved"},
        {"name": "spectacle dynamics", "description": "Debord: image mediation and separation effects", "tags": ["debord", "spectacle"], "status": "approved"},
        {"name": "simulation drift", "description": "Baudrillard: simulation/hyperreality drift", "tags": ["baudrillard", "simulation"], "status": "approved"},
        {"name": "inscription systems", "description": "Kittler: media determine recording/processing conditions", "tags": ["kittler", "media"], "status": "approved"},
        {"name": "space of flows", "description": "Castells: network society and flow architectures", "tags": ["castells", "network"], "status": "approved"},
        {"name": "counterproductivity", "description": "Illich: institutional overreach and counterproductivity", "tags": ["illich", "institution"], "status": "approved"},
        {"name": "feedback control", "description": "Wiener: control/communication feedback loops", "tags": ["wiener", "feedback"], "status": "approved"},
        {"name": "protocol governance", "description": "Galloway: protocol as control architecture", "tags": ["galloway", "protocol"], "status": "approved"},
        {"name": "mimetic contagion", "description": "Girard: mimesis and contagion dynamics", "tags": ["girard", "mimesis"], "status": "proposed"},
        {"name": "symbolic stress", "description": "Lacan: symbolic/imaginary tensions in narratives", "tags": ["lacan", "symbolic"], "status": "proposed"},
    ],
    "signals": [
        {
            "name": "Export controls escalation",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["electric_stack"], "keywords": ["export controls", "entity list", "licensing restrictions"], "window_items": 200, "threshold": 3, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "dromology", "claim": "Escalating export controls indicate speed/constraint contests in technological logistics.", "confidence": 0.72, "status": "approved"}],
        },
        {
            "name": "Sanctions ratchet pressure",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["core"], "keywords": ["sanctions", "secondary sanctions", "asset freeze"], "window_items": 200, "threshold": 3, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "protocol governance", "claim": "Sanctions sequences reflect protocolized control over global networks.", "confidence": 0.68, "status": "approved"}],
        },
        {
            "name": "Critical minerals bottlenecks",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["markets_macro"], "keywords": ["critical minerals", "rare earth", "lithium supply"], "window_items": 200, "threshold": 3, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "space of flows", "claim": "Minerals chokepoints signal structural pressure in flow infrastructures.", "confidence": 0.71, "status": "approved"}],
        },
        {
            "name": "Shipping chokepoint stress",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["core"], "keywords": ["shipping chokepoint", "canal disruption", "maritime rerouting"], "window_items": 200, "threshold": 2, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "logistics of perception", "claim": "Narratives around chokepoints reshape perceived strategic urgency.", "confidence": 0.67, "status": "approved"}],
        },
        {
            "name": "Semiconductor equipment controls",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["electric_stack"], "keywords": ["semiconductor equipment", "lithography restrictions", "chip tool export"], "window_items": 200, "threshold": 3, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "inscription systems", "claim": "Control over fabrication tools shapes inscription-system sovereignty.", "confidence": 0.74, "status": "approved"}],
        },
        {
            "name": "Datacenter buildout acceleration",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["electric_stack"], "keywords": ["datacenter expansion", "hyperscale buildout", "server campus"], "window_items": 200, "threshold": 3, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "medium environment", "claim": "Buildout cadence reflects infrastructural medium effects, not only product narratives.", "confidence": 0.69, "status": "approved"}],
        },
        {
            "name": "HBM and advanced packaging surge",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["electric_stack"], "keywords": ["HBM", "advanced packaging", "chiplet packaging"], "window_items": 200, "threshold": 3, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "feedback control", "claim": "Packaging bottlenecks can create recursive capacity feedback across compute supply chains.", "confidence": 0.73, "status": "approved"}],
        },
        {
            "name": "Foundry capacity constraints",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["electric_stack"], "keywords": ["foundry capacity", "wafer shortage", "fab utilization"], "window_items": 200, "threshold": 3, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "counterproductivity", "claim": "Capacity races can produce institutional counterproductivity and fragility.", "confidence": 0.64, "status": "approved"}],
        },
        {
            "name": "Transformer shortage stress",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["markets_macro"], "keywords": ["transformer shortage", "substation delays", "grid equipment backlog"], "window_items": 200, "threshold": 2, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "space of flows", "claim": "Grid component constraints reveal bottlenecks in socio-technical flow space.", "confidence": 0.7, "status": "approved"}],
        },
        {
            "name": "HVDC buildout momentum",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["markets_macro"], "keywords": ["HVDC", "transmission corridor", "grid modernization"], "window_items": 200, "threshold": 2, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "protocol governance", "claim": "Transmission coordination reflects protocol-level governance of energy networks.", "confidence": 0.66, "status": "approved"}],
        },
        {
            "name": "Interconnect queue congestion",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["markets_macro"], "keywords": ["interconnect queue", "grid connection delays", "queue reform"], "window_items": 200, "threshold": 2, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [{"concept": "feedback control", "claim": "Queue congestion creates control-loop lag between intention and infrastructure realization.", "confidence": 0.71, "status": "approved"}],
        },
        {
            "name": "AI safety legitimacy wave",
            "status": "active",
            "universe": {"scope": "DEW", "note": "paper mode"},
            "spec": {"kind": "rss_keyword_count", "feeds": ["culture_sensors"], "keywords": ["AI safety", "alignment regulation", "model governance"], "window_items": 200, "threshold": 3, "match_fields": ["title", "summary"], "case_sensitive": False},
            "concept_links": [
                {"concept": "spectacle dynamics", "claim": "Safety discourse can function as legitimacy spectacle under acceleration pressure.", "confidence": 0.61, "status": "approved"},
                {"concept": "simulation drift", "claim": "Narrative layers may outrun material system constraints.", "confidence": 0.6, "status": "approved"},
            ],
        },
    ],
}


def load_feeds_file(path: Path) -> list[str]:
    feeds: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        feeds.append(line)
    return feeds


def load_json_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_feeds_registry(path: Path) -> dict[str, Any]:
    return load_json_config(path)


def _feed_map(feeds: list[dict]) -> dict[str, dict]:
    mapping: dict[str, dict] = {}
    for row in feeds:
        feed_id = str(row.get("id") or "")
        if not feed_id:
            continue
        mapping[feed_id] = row
    return mapping


def merge_feeds_registry(existing: dict[str, Any], default: dict[str, Any]) -> dict[str, Any]:
    current = dict(existing)
    current.setdefault("generated_at", STATIC_GENERATED_AT)
    current.setdefault("feeds", [])
    current.setdefault("feed_sets", {})

    existing_map = _feed_map(list(current.get("feeds") or []))
    for row in list(default.get("feeds") or []):
        feed_id = row["id"]
        if feed_id not in existing_map:
            existing_map[feed_id] = {
                "id": row["id"],
                "url": row["url"],
                "tags": list(row.get("tags") or []),
                "enabled": bool(row.get("enabled", True)),
            }
            continue
        target = existing_map[feed_id]
        target.setdefault("url", row["url"])
        target.setdefault("tags", list(row.get("tags") or []))
        target.setdefault("enabled", bool(row.get("enabled", True)))

    merged_feeds = [existing_map[key] for key in sorted(existing_map)]
    merged_sets: dict[str, list[str]] = {}
    existing_sets = dict(current.get("feed_sets") or {})
    default_sets = dict(default.get("feed_sets") or {})
    all_set_names = sorted(set(existing_sets).union(default_sets))
    for set_name in all_set_names:
        existing_ids = [str(item) for item in (existing_sets.get(set_name) or [])]
        seen = set(existing_ids)
        for default_id in (default_sets.get(set_name) or []):
            if default_id not in seen:
                existing_ids.append(default_id)
                seen.add(default_id)
        merged_sets[set_name] = existing_ids

    return {
        "generated_at": current.get("generated_at", STATIC_GENERATED_AT),
        "feeds": merged_feeds,
        "feed_sets": merged_sets,
    }


def seed_feeds_registry(path: Path, default_registry: dict[str, Any] | None = None) -> dict[str, Any]:
    default = default_registry or DEFAULT_FEEDS_REGISTRY
    if path.exists():
        existing = load_feeds_registry(path)
        merged = merge_feeds_registry(existing, default)
    else:
        merged = merge_feeds_registry({}, default)
    atomic_write_json(path, merged)
    return merged


def resolve_feed_set_to_sources(registry: dict[str, Any], feed_set_name: str) -> list[dict]:
    feed_sets = dict(registry.get("feed_sets") or {})
    if feed_set_name not in feed_sets:
        raise KeyError(f"feed_set not found: {feed_set_name}")
    feed_ids = [str(feed_id) for feed_id in feed_sets[feed_set_name]]
    feeds_map = _feed_map(list(registry.get("feeds") or []))

    sources: list[dict] = []
    for feed_id in feed_ids:
        row = feeds_map.get(feed_id)
        if row is None:
            continue
        if not bool(row.get("enabled", True)):
            continue
        sources.append({"id": feed_id, "url": str(row.get("url") or "")})
    return sources


def ensure_signal_pack_file(path: Path, default_pack: dict[str, Any] | None = None) -> dict[str, Any]:
    default = default_pack or DEFAULT_SIGNAL_PACK_V1
    if path.exists():
        payload = load_json_config(path)
        changed = False
        for key, value in default.items():
            if key not in payload:
                payload[key] = value
                changed = True
        if changed:
            atomic_write_json(path, payload)
        return payload
    atomic_write_json(path, default)
    return default

