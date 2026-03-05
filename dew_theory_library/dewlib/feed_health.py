from __future__ import annotations


def new_feed_health_entry(feed_id: str, url: str) -> dict:
    return {
        "id": feed_id,
        "url": url,
        "fetch_ok": False,
        "http_status": None,
        "bytes": 0,
        "parse_ok": False,
        "item_count": 0,
        "error": None,
        "elapsed_ms": 0,
    }


def summarize_feed_health(entries: list[dict]) -> dict:
    feeds_total = len(entries)
    feeds_ok = sum(1 for row in entries if bool(row.get("fetch_ok")) and bool(row.get("parse_ok")))
    feeds_failed = feeds_total - feeds_ok
    items_total = sum(int(row.get("item_count") or 0) for row in entries)
    return {
        "feeds_total": feeds_total,
        "feeds_ok": feeds_ok,
        "feeds_failed": feeds_failed,
        "items_total": items_total,
    }


def failed_feed_entries(entries: list[dict]) -> list[dict]:
    failed = [row for row in entries if not (bool(row.get("fetch_ok")) and bool(row.get("parse_ok")))]
    return sorted(failed, key=lambda row: (str(row.get("id", "")), str(row.get("url", ""))))

