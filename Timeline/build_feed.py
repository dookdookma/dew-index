#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import feedparser
import requests
from dateutil import parser as dateparser
from xml.etree import ElementTree as ET

DEFAULT_TIMEOUT = 20
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; MixedSocialFeed/1.0; +https://example.com/feed)"
)


@dataclass
class FeedItem:
    guid: str
    title: str
    link: str
    description: str
    pub_date: datetime
    source_platform: str
    source_name: str
    author: str | None = None
    raw_source_url: str | None = None


class FetchError(RuntimeError):
    pass


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def fetch_xml(url: str, timeout: int = DEFAULT_TIMEOUT) -> bytes:
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "application/atom+xml, application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    if resp.status_code >= 400:
        raise FetchError(f"{url} returned HTTP {resp.status_code}")
    return resp.content


def parse_datetime(entry: Any) -> datetime:
    candidates = [
        getattr(entry, "published", None),
        getattr(entry, "updated", None),
        getattr(entry, "created", None),
    ]
    for candidate in candidates:
        if candidate:
            try:
                dt = dateparser.parse(candidate)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
    if getattr(entry, "published_parsed", None):
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(text: str) -> str:
    text = TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return WS_RE.sub(" ", text).strip()


def trim_text(text: str, limit: int) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def source_label_from_url(url: str, platform: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if platform == "reddit":
        if len(parts) >= 2 and parts[0] == "r":
            return f"r/{parts[1]}"
    if platform == "x":
        if parts:
            return f"@{parts[0]}"
    return parsed.netloc


def build_reddit_item(entry: Any, source_url: str) -> FeedItem:
    source_name = source_label_from_url(source_url, "reddit")
    title = f"[Reddit] {source_name} — {getattr(entry, 'title', 'Untitled')}"
    link = getattr(entry, "link", "")
    author = getattr(entry, "author", None)

    summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
    description = summary
    if not description:
        description = getattr(entry, "title", "")

    guid = getattr(entry, "id", None) or getattr(entry, "guid", None) or link
    return FeedItem(
        guid=str(guid),
        title=title,
        link=link,
        description=description,
        pub_date=parse_datetime(entry),
        source_platform="reddit",
        source_name=source_name,
        author=author,
        raw_source_url=source_url,
    )


def build_nitter_item(entry: Any, source_url: str, include_full_text: bool = True) -> FeedItem:
    source_name = source_label_from_url(source_url, "x")
    link = getattr(entry, "link", "")
    author = getattr(entry, "author", None) or source_name

    summary_html = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
    plain = strip_html(summary_html)
    title_text = trim_text(plain or getattr(entry, "title", "Untitled"), 180)
    title = f"[X] {source_name} — {title_text}"
    description = summary_html if include_full_text and summary_html else html.escape(plain or title_text)

    guid = getattr(entry, "id", None) or getattr(entry, "guid", None) or link
    return FeedItem(
        guid=str(guid),
        title=title,
        link=link,
        description=description,
        pub_date=parse_datetime(entry),
        source_platform="x",
        source_name=source_name,
        author=author,
        raw_source_url=source_url,
    )


def parse_feed(url: str, platform: str, include_full_text: bool) -> list[FeedItem]:
    xml_bytes = fetch_xml(url)
    parsed = feedparser.parse(xml_bytes)
    if parsed.bozo and not parsed.entries:
        raise FetchError(f"Failed to parse feed: {url}")

    items: list[FeedItem] = []
    for entry in parsed.entries:
        try:
            if platform == "reddit":
                items.append(build_reddit_item(entry, url))
            elif platform == "x":
                items.append(build_nitter_item(entry, url, include_full_text=include_full_text))
            else:
                raise ValueError(f"Unsupported platform: {platform}")
        except Exception as exc:
            print(f"Skipping malformed item from {url}: {exc}", file=sys.stderr)
    return items


def dedupe_items(items: Iterable[FeedItem]) -> list[FeedItem]:
    seen: dict[str, FeedItem] = {}
    for item in items:
        key = item.link or item.guid
        existing = seen.get(key)
        if existing is None or item.pub_date > existing.pub_date:
            seen[key] = item
    return list(seen.values())


def rfc2822(dt: datetime) -> str:
    return format_datetime(dt.astimezone(timezone.utc))


def build_rss(items: list[FeedItem], feed_meta: dict[str, Any]) -> bytes:
    rss = ET.Element("rss", version="2.0")
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = feed_meta["title"]
    ET.SubElement(channel, "link").text = feed_meta["site_url"]
    ET.SubElement(channel, "description").text = feed_meta["description"]
    ET.SubElement(channel, "language").text = feed_meta.get("language", "en-us")
    ET.SubElement(channel, "lastBuildDate").text = rfc2822(datetime.now(timezone.utc))
    ET.SubElement(channel, "generator").text = "MixedSocialFeed/1.0"
    if feed_meta.get("feed_url"):
        atom_ns = "http://www.w3.org/2005/Atom"
        rss.set("xmlns:atom", atom_ns)
        ET.SubElement(
            channel,
            f"{{{atom_ns}}}link",
            attrib={
                "href": feed_meta["feed_url"],
                "rel": "self",
                "type": "application/rss+xml",
            },
        )

    for item in items:
        item_el = ET.SubElement(channel, "item")
        ET.SubElement(item_el, "title").text = item.title
        ET.SubElement(item_el, "link").text = item.link
        ET.SubElement(item_el, "guid", isPermaLink="false").text = item.guid
        ET.SubElement(item_el, "pubDate").text = rfc2822(item.pub_date)
        if item.author:
            ET.SubElement(item_el, "author").text = item.author
        desc = ET.SubElement(item_el, "description")
        desc.text = item.description
        ET.SubElement(item_el, "category").text = item.source_platform
        ET.SubElement(item_el, "source", url=item.raw_source_url or "").text = item.source_name

    xml_bytes = ET.tostring(rss, encoding="utf-8", xml_declaration=True)
    return xml_bytes


def run(config_path: Path, output_path: Path) -> int:
    cfg = load_config(config_path)
    feed_meta = cfg["feed"]
    include_full_text = bool(feed_meta.get("include_full_text", True))

    collected: list[FeedItem] = []
    failures: list[str] = []

    for url in cfg.get("reddit", {}).get("feeds", []):
        try:
            collected.extend(parse_feed(url, platform="reddit", include_full_text=include_full_text))
        except Exception as exc:
            failures.append(f"Reddit feed failed: {url} :: {exc}")

    for url in cfg.get("nitter", {}).get("feeds", []):
        try:
            collected.extend(parse_feed(url, platform="x", include_full_text=include_full_text))
        except Exception as exc:
            failures.append(f"Nitter feed failed: {url} :: {exc}")

    items = dedupe_items(collected)
    items.sort(key=lambda x: x.pub_date, reverse=True)
    items = items[: int(feed_meta.get("max_items", 50))]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(build_rss(items, feed_meta))

    if failures:
        print("Completed with source errors:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)

    print(f"Wrote {len(items)} items to {output_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a merged RSS feed from Reddit and Nitter sources.")
    parser.add_argument(
        "--config",
        default="config/sources.json",
        help="Path to sources.json configuration file",
    )
    parser.add_argument(
        "--output",
        default="dist/feed.xml",
        help="Path to output feed XML file",
    )
    args = parser.parse_args()
    return run(Path(args.config), Path(args.output))


if __name__ == "__main__":
    raise SystemExit(main())
