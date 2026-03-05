from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .feed_health import summarize_feed_health
from .ledger_store import LedgerNotFoundError, LedgerStore
from .scan_db import ScanDB
from .scan_inputs import load_feed_items, normalize_feed_sources
from .scan_recommend import generate_recommendations
from .scan_report import build_scan_markdown
from .util import atomic_write_json, atomic_write_text, ensure_dir


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _report_stamp(ts_iso: str) -> str:
    dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def _evaluate_rss_keyword_count(
    signal: dict,
    items: list[dict],
    source_summaries: list[dict],
    feed_sets_map: dict[str, list[str]] | None = None,
) -> tuple[dict, bool, dict]:
    spec = dict(signal.get("spec") or {})
    feeds_filter = spec.get("feeds", ["all"])
    keywords = list(spec.get("keywords") or [])
    window_items = int(spec.get("window_items", 50))
    threshold = int(spec.get("threshold", 3))
    match_fields = list(spec.get("match_fields") or ["title", "summary"])
    case_sensitive = bool(spec.get("case_sensitive", False))

    if not isinstance(feeds_filter, list):
        feeds_filter = ["all"]
    feeds_filter = [str(value) for value in feeds_filter]
    expanded_filters: list[str] = []
    for value in feeds_filter:
        if feed_sets_map and value in feed_sets_map:
            expanded_filters.extend([str(feed_id) for feed_id in feed_sets_map[value]])
        else:
            expanded_filters.append(value)
    feeds_filter = expanded_filters
    use_all_feeds = "all" in {value.lower() for value in feeds_filter}
    allowed_feeds = set(feeds_filter)

    filtered_items = items
    if not use_all_feeds:
        filtered_items = [item for item in items if item.get("source") in allowed_feeds]
    window = filtered_items[: max(0, window_items)]

    keyword_hits = {keyword: 0 for keyword in keywords}
    matched_items: list[dict] = []
    matched_item_ids: list[str] = []
    match_count = 0

    normalized_keywords = [keyword if case_sensitive else keyword.lower() for keyword in keywords]
    for item in window:
        fields_text = []
        for field in match_fields:
            value = item.get(field)
            if isinstance(value, str):
                fields_text.append(value)
        haystack = " ".join(fields_text)
        haystack_cmp = haystack if case_sensitive else haystack.lower()

        item_matched = False
        for original_keyword, normalized_keyword in zip(keywords, normalized_keywords):
            if normalized_keyword and normalized_keyword in haystack_cmp:
                keyword_hits[original_keyword] = keyword_hits.get(original_keyword, 0) + 1
                item_matched = True
        if item_matched:
            match_count += 1
            matched_item_ids.append(item["item_id"])
            matched_items.append(
                {
                    "item_id": item["item_id"],
                    "title": item.get("title", ""),
                    "link": item.get("link"),
                    "published": item.get("published"),
                    "source": item.get("source"),
                }
            )

    metric = {
        "kind": "rss_keyword_count",
        "match_count": match_count,
        "threshold": threshold,
        "matched_items": matched_item_ids[:20],
        "keyword_hits": keyword_hits,
        "window_items": window_items,
    }
    triggered = match_count >= threshold if keywords else False
    feeds_used_ids = sorted({item.get("source", "") for item in window})
    feeds_used = [source for source in source_summaries if source["id"] in feeds_used_ids]
    context = {
        "feed_sources_used": feeds_used,
        "item_ids": matched_item_ids[:20],
        "matched_items": matched_items[:20],
        "window_considered": len(window),
        "feeds_filter": sorted(allowed_feeds) if not use_all_feeds else ["all"],
    }
    return metric, triggered, context


def _evaluate_signal(
    signal: dict,
    items: list[dict],
    source_summaries: list[dict],
    feed_sets_map: dict[str, list[str]] | None = None,
) -> tuple[dict, bool, dict]:
    spec = dict(signal.get("spec") or {})
    kind = str(spec.get("kind") or "")
    if kind == "rss_keyword_count":
        return _evaluate_rss_keyword_count(
            signal=signal,
            items=items,
            source_summaries=source_summaries,
            feed_sets_map=feed_sets_map,
        )

    metric = {
        "kind": kind,
        "error": "unsupported_kind",
        "match_count": 0,
        "threshold": 1,
        "matched_items": [],
        "keyword_hits": {},
    }
    context = {"feed_sources_used": source_summaries, "item_ids": [], "matched_items": []}
    return metric, False, context


def run_scan(
    ledger_db_path: str | Path,
    feed_sources: list,
    run_options: dict | None = None,
    created_by: str | None = None,
) -> dict:
    options = dict(run_options or {})
    db_path = Path(ledger_db_path)
    scan_db = ScanDB(db_path=db_path)
    scan_db.initialize()

    normalized_sources = normalize_feed_sources(feed_sources)
    max_items = options.get("max_items")
    loaded = load_feed_items(
        feed_sources=normalized_sources,
        max_items=int(max_items) if max_items is not None else None,
        timeout=float(options.get("timeout_seconds", 20.0)),
    )
    items = loaded["items"]
    source_summaries = loaded["sources"]
    feed_health_entries = loaded.get("feed_health") or []
    feed_health_summary = summarize_feed_health(feed_health_entries)

    ts = _utc_now_iso()
    cadence = str(options.get("cadence") or "ad_hoc")
    feed_set = options.get("feed_set")
    feed_sets_map = options.get("feed_sets_map")
    if feed_sets_map is not None and not isinstance(feed_sets_map, dict):
        raise ValueError("run_options.feed_sets_map must be an object mapping feed_set -> [feed_ids]")
    scan_run = scan_db.create_scan_run(
        inputs={
            "feeds": normalized_sources,
            "options": options,
            "item_count": len(items),
            "feeds_health_summary": feed_health_summary,
        },
        notes=options.get("notes"),
        created_by=created_by,
        ts=ts,
    )

    pinned_versions = options.get("pinned_versions")
    if pinned_versions is not None and not isinstance(pinned_versions, dict):
        raise ValueError("run_options.pinned_versions must be an object mapping signal_id -> version")
    active_signals = scan_db.load_active_signals_latest(pinned_versions=pinned_versions)
    store = LedgerStore(db_path=db_path, data_dir=db_path.parent)

    evaluations: list[dict] = []
    for signal in active_signals:
        metric, triggered, context = _evaluate_signal(
            signal,
            items=items,
            source_summaries=source_summaries,
            feed_sets_map=feed_sets_map,
        )
        observation = scan_db.create_observation(
            scan_run_id=scan_run["scan_run_id"],
            signal_id=signal["signal_id"],
            signal_version=signal["version"],
            metric=metric,
            triggered=triggered,
            context=context,
            created_by=created_by,
            ts=ts,
        )

        explain_payload = {"signal": {"signal_id": signal["signal_id"], "version": signal["version"]}, "concepts": []}
        if triggered:
            try:
                explain_payload = store.explain_signal(
                    signal_id=signal["signal_id"],
                    version=signal["version"],
                    status_filter=options.get("status_filter", "approved+active"),
                )
            except LedgerNotFoundError:
                explain_payload = {
                    "signal": {"signal_id": signal["signal_id"], "version": signal["version"]},
                    "concepts": [],
                }

        evaluations.append(
            {
                "signal": {
                    "signal_id": signal["signal_id"],
                    "version": signal["version"],
                    "name": signal["name"],
                    "status": signal["status"],
                },
                "metric": metric,
                "triggered": triggered,
                "context": context,
                "observation_id": observation["observation_id"],
                "explain": explain_payload,
            }
        )

    triggered_rows = [row for row in evaluations if row["triggered"]]
    generated_recommendations = generate_recommendations(triggered_rows)
    stored_recommendations: list[dict] = []
    for recommendation in generated_recommendations:
        stored = scan_db.create_recommendation(
            scan_run_id=scan_run["scan_run_id"],
            ts=ts,
            kind=recommendation["kind"],
            title=recommendation["title"],
            body=recommendation["body"],
            confidence=recommendation.get("confidence"),
            related_signal_ids=recommendation.get("related_signal_ids"),
            related_observation_ids=recommendation.get("related_observation_ids"),
            status=recommendation.get("status", "proposed"),
            created_by=created_by,
        )
        stored_recommendations.append(stored)

    out_dir = Path(options.get("out_dir", "out/scans"))
    ensure_dir(out_dir)
    stamp = _report_stamp(ts)
    report_file = out_dir / f"{stamp}_{cadence}_scan.md"
    report_content = build_scan_markdown(
        run_ts=ts,
        source_summaries=source_summaries,
        total_items=len(items),
        evaluations=evaluations,
        recommendations=stored_recommendations,
        cadence=cadence,
        feed_set=str(feed_set) if feed_set is not None else None,
    )
    atomic_write_text(report_file, report_content)
    sidecar_file = report_file.with_suffix(".json")
    feeds_health_file = report_file.with_name(f"{report_file.stem}_feeds_health.json")

    try:
        report_path = report_file.relative_to(Path.cwd()).as_posix()
    except ValueError:
        report_path = report_file.as_posix()
    scan_db.update_scan_run_report_path(scan_run_id=scan_run["scan_run_id"], report_path=report_path)

    triggered_payload = [
        {
            "signal_id": row["signal"]["signal_id"],
            "version": row["signal"]["version"],
            "name": row["signal"]["name"],
            "metric": row["metric"],
            "explain": row["explain"],
        }
        for row in triggered_rows
    ]
    recommendations_payload = [
        {
            "recommendation_id": row["recommendation_id"],
            "kind": row["kind"],
            "title": row["title"],
            "body": row["body"],
            "confidence": row["confidence"],
            "related_signal_ids_json": row["related_signal_ids_json"],
            "related_observation_ids_json": row["related_observation_ids_json"],
            "status": row["status"],
        }
        for row in stored_recommendations
    ]
    feeds_health_payload = {
        "scan_run_id": scan_run["scan_run_id"],
        "ts": ts,
        "summary": feed_health_summary,
        "feeds": feed_health_entries,
    }
    atomic_write_json(feeds_health_file, feeds_health_payload)
    try:
        feeds_health_path = feeds_health_file.relative_to(Path.cwd()).as_posix()
    except ValueError:
        feeds_health_path = feeds_health_file.as_posix()
    sidecar_payload = {
        "scan_run_id": scan_run["scan_run_id"],
        "ts": ts,
        "cadence": cadence,
        "feed_set": feed_set,
        "report_path": report_path,
        "signals_evaluated": len(evaluations),
        "signals_triggered": len(triggered_rows),
        "triggered": triggered_payload,
        "recommendations": recommendations_payload,
        "feeds_health_path": feeds_health_path,
        **feed_health_summary,
    }
    atomic_write_json(sidecar_file, sidecar_payload)

    try:
        sidecar_path = sidecar_file.relative_to(Path.cwd()).as_posix()
    except ValueError:
        sidecar_path = sidecar_file.as_posix()

    return {
        "scan_run_id": scan_run["scan_run_id"],
        "ts": ts,
        "report_path": report_path,
        "report_json_path": sidecar_path,
        "cadence": cadence,
        "feed_set": feed_set,
        "signals_evaluated": len(evaluations),
        "signals_triggered": len(triggered_rows),
        "triggered": triggered_payload,
        "recommendations": recommendations_payload,
        "feeds_health_path": feeds_health_path,
        **feed_health_summary,
    }
