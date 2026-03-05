from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen
import xml.etree.ElementTree as ET

from .feed_health import new_feed_health_entry


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _text_from_child(node: ET.Element, names: list[str]) -> str | None:
    targets = {name.lower() for name in names}
    for child in list(node):
        if _local_name(child.tag).lower() in targets:
            text = (child.text or "").strip()
            if text:
                return text
    return None


def _atom_link(entry: ET.Element) -> str | None:
    for child in list(entry):
        if _local_name(child.tag).lower() != "link":
            continue
        rel = (child.attrib.get("rel") or "").strip().lower()
        href = (child.attrib.get("href") or "").strip()
        if href and (not rel or rel == "alternate"):
            return href
    for child in list(entry):
        if _local_name(child.tag).lower() == "link":
            href = (child.attrib.get("href") or "").strip()
            if href:
                return href
    return None


def _stable_item_id(given_id: str | None, title: str | None, link: str | None, published: str | None) -> str:
    if given_id and given_id.strip():
        return given_id.strip()
    payload = f"{title or ''}|{link or ''}|{published or ''}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _parse_rss(root: ET.Element, feed_id: str) -> list[dict]:
    items: list[dict] = []
    for node in root.iter():
        if _local_name(node.tag).lower() != "item":
            continue
        title = _text_from_child(node, ["title"])
        link = _text_from_child(node, ["link"])
        summary = _text_from_child(node, ["description", "summary"])
        published = _text_from_child(node, ["pubDate", "published", "updated"])
        guid = _text_from_child(node, ["guid", "id"])
        item_id = _stable_item_id(guid, title, link, published)
        items.append(
            {
                "item_id": item_id,
                "title": title or "",
                "link": link,
                "summary": summary,
                "published": published,
                "source": feed_id,
            }
        )
    return items


def _parse_atom(root: ET.Element, feed_id: str) -> list[dict]:
    items: list[dict] = []
    for node in root.iter():
        if _local_name(node.tag).lower() != "entry":
            continue
        title = _text_from_child(node, ["title"])
        summary = _text_from_child(node, ["summary", "content"])
        published = _text_from_child(node, ["updated", "published"])
        entry_id = _text_from_child(node, ["id", "guid"])
        link = _atom_link(node)
        item_id = _stable_item_id(entry_id, title, link, published)
        items.append(
            {
                "item_id": item_id,
                "title": title or "",
                "link": link,
                "summary": summary,
                "published": published,
                "source": feed_id,
            }
        )
    return items


def parse_feed_xml(raw_xml: bytes, feed_id: str) -> list[dict]:
    root = ET.fromstring(raw_xml)
    root_name = _local_name(root.tag).lower()
    if root_name in {"rss", "rdf", "rdf:rdf"}:
        return _parse_rss(root, feed_id=feed_id)
    if root_name == "feed":
        return _parse_atom(root, feed_id=feed_id)

    rss_items = _parse_rss(root, feed_id=feed_id)
    atom_items = _parse_atom(root, feed_id=feed_id)
    return rss_items or atom_items


def normalize_feed_sources(feed_sources: list[Any]) -> list[dict]:
    normalized: list[dict] = []
    for index, source in enumerate(feed_sources, start=1):
        if isinstance(source, str):
            source_id = f"feed_{index}"
            normalized.append({"id": source_id, "url": source})
            continue
        if isinstance(source, dict):
            url = str(source.get("url") or source.get("path") or "").strip()
            if not url:
                raise ValueError(f"Feed source missing url/path at position {index}")
            source_id = str(source.get("id") or f"feed_{index}")
            normalized.append({"id": source_id, "url": url})
            continue
        raise ValueError(f"Unsupported feed source at position {index}: {source!r}")
    return normalized


def _read_feed_bytes(url_or_path: str, timeout: float = 20.0) -> tuple[bytes, int | None]:
    parsed = urlparse(url_or_path)
    if parsed.scheme in {"http", "https", "file"}:
        with urlopen(url_or_path, timeout=timeout) as response:  # nosec B310
            status = getattr(response, "status", None)
            return response.read(), int(status) if status is not None else None
    path = Path(url_or_path)
    return path.read_bytes(), None


def load_feed_items(
    feed_sources: list[Any],
    max_items: int | None = None,
    timeout: float = 20.0,
) -> dict:
    sources = normalize_feed_sources(feed_sources)
    items: list[dict] = []
    source_summaries: list[dict] = []
    feed_health: list[dict] = []
    for source in sources:
        entry = new_feed_health_entry(feed_id=source["id"], url=source["url"])
        started = time.perf_counter()
        parsed_items: list[dict] = []
        try:
            raw, status = _read_feed_bytes(source["url"], timeout=timeout)
            entry["fetch_ok"] = True
            entry["http_status"] = status
            entry["bytes"] = len(raw)
        except Exception as exc:
            entry["error"] = f"fetch_error: {exc}"
            raw = b""

        if entry["fetch_ok"]:
            try:
                parsed_items = parse_feed_xml(raw, feed_id=source["id"])
                entry["parse_ok"] = True
            except Exception as exc:
                entry["error"] = f"parse_error: {exc}"
                parsed_items = []

        entry["item_count"] = len(parsed_items)
        entry["elapsed_ms"] = int((time.perf_counter() - started) * 1000)

        source_summaries.append(
            {
                "id": source["id"],
                "url": source["url"],
                "item_count": len(parsed_items),
            }
        )
        items.extend(parsed_items)
        feed_health.append(entry)

    if max_items is not None:
        items = items[: max(0, int(max_items))]
    return {"sources": source_summaries, "items": items, "feed_health": feed_health}
