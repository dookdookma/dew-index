from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .feed_health import failed_feed_entries, summarize_feed_health
from .ledger_db import connect_db
from .util import atomic_write_json, atomic_write_text, ensure_dir


def _parse_iso_utc(ts: str) -> datetime:
    value = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if value.tzinfo is None:
        return value.replace(tzinfo=ZoneInfo("UTC"))
    return value


def _today_local_iso(tz_name: str) -> str:
    return datetime.now(ZoneInfo(tz_name)).date().isoformat()


def _resolve_path(path_value: str | None) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path
    return Path.cwd() / path


def _load_json(path: Path | None) -> dict | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _run_from_row(row) -> dict:
    inputs = json.loads(row["inputs_json"]) if row["inputs_json"] else {}
    options = dict(inputs.get("options") or {})
    return {
        "scan_run_id": row["scan_run_id"],
        "ts": row["ts"],
        "inputs_json": inputs,
        "cadence": str(options.get("cadence") or "ad_hoc"),
        "feed_set": options.get("feed_set"),
        "report_path": row["report_path"],
    }


def _local_date(ts: str, tz_name: str) -> str:
    return _parse_iso_utc(ts).astimezone(ZoneInfo(tz_name)).date().isoformat()


def _load_triggered_for_run(conn, scan_run_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
          o.scan_run_id,
          o.observation_id,
          o.signal_id,
          o.signal_version,
          o.metric_json,
          s.name AS signal_name
        FROM observations o
        LEFT JOIN signals s ON s.signal_id = o.signal_id AND s.version = o.signal_version
        WHERE o.scan_run_id = ? AND o.triggered = 1
        ORDER BY o.signal_id ASC, o.signal_version ASC, o.observation_id ASC
        """,
        (scan_run_id,),
    ).fetchall()
    payload: list[dict] = []
    for row in rows:
        metric = json.loads(row["metric_json"]) if row["metric_json"] else {}
        payload.append(
            {
                "scan_run_id": row["scan_run_id"],
                "observation_id": row["observation_id"],
                "signal_id": row["signal_id"],
                "signal_version": int(row["signal_version"]),
                "signal_name": row["signal_name"] or row["signal_id"],
                "metric": metric,
            }
        )
    return payload


def _load_recommendations_for_run(conn, scan_run_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
          recommendation_id, scan_run_id, ts, kind, title, confidence, status
        FROM recommendations
        WHERE scan_run_id = ?
        ORDER BY ts ASC, recommendation_id ASC
        """,
        (scan_run_id,),
    ).fetchall()
    payload: list[dict] = []
    for row in rows:
        payload.append(
            {
                "recommendation_id": row["recommendation_id"],
                "scan_run_id": row["scan_run_id"],
                "ts": row["ts"],
                "kind": row["kind"],
                "title": row["title"],
                "confidence": float(row["confidence"]) if row["confidence"] is not None else None,
                "status": row["status"],
            }
        )
    return payload


def _load_feed_health_for_run(run: dict) -> tuple[dict, list[dict], str | None]:
    inputs = dict(run.get("inputs_json") or {})
    input_summary = dict(inputs.get("feeds_health_summary") or {})
    report_path = _resolve_path(run.get("report_path"))
    sidecar = _load_json(report_path.with_suffix(".json") if report_path else None)

    feeds_health_path_value: str | None = None
    if sidecar and sidecar.get("feeds_health_path"):
        feeds_health_path_value = str(sidecar.get("feeds_health_path"))
        feeds_health_path = _resolve_path(feeds_health_path_value)
    elif report_path is not None:
        feeds_health_path = report_path.with_name(f"{report_path.stem}_feeds_health.json")
        feeds_health_path_value = feeds_health_path.as_posix()
    else:
        feeds_health_path = None

    feeds_payload = _load_json(feeds_health_path)
    if feeds_payload:
        entries = list(feeds_payload.get("feeds") or [])
        summary = dict(feeds_payload.get("summary") or summarize_feed_health(entries))
        failures = failed_feed_entries(entries)
    else:
        entries = []
        summary = dict(input_summary or summarize_feed_health(entries))
        failures = []

    return summary, failures, feeds_health_path_value


def collect_daily_digest(
    ledger_db_path: str | Path,
    date: str | None = None,
    tz_name: str = "America/New_York",
    cadences: list[str] | None = None,
) -> dict:
    target_date = date or _today_local_iso(tz_name)
    cadence_order = list(cadences or ["morning", "midday", "close"])
    cadence_order_map = {name: idx for idx, name in enumerate(cadence_order)}
    cadence_set = set(cadence_order)

    with connect_db(Path(ledger_db_path)) as conn:
        runs = [_run_from_row(row) for row in conn.execute("SELECT * FROM scan_runs").fetchall()]
        filtered_runs = [
            run
            for run in runs
            if _local_date(run["ts"], tz_name) == target_date and run["cadence"] in cadence_set
        ]
        filtered_runs.sort(
            key=lambda run: (
                cadence_order_map.get(run["cadence"], len(cadence_order_map)),
                run["ts"],
                run["scan_run_id"],
            )
        )

        triggered_by_cadence: dict[str, list[dict]] = {cadence: [] for cadence in cadence_order}
        accepted: list[dict] = []
        proposed: list[dict] = []
        rejected_or_superseded: list[dict] = []
        feed_failures: list[dict] = []

        runs_payload: list[dict] = []
        for run in filtered_runs:
            run_triggered = _load_triggered_for_run(conn, run["scan_run_id"])
            run_recommendations = _load_recommendations_for_run(conn, run["scan_run_id"])
            feed_summary, failures, feeds_health_path = _load_feed_health_for_run(run)

            for row in run_triggered:
                triggered_by_cadence.setdefault(run["cadence"], []).append(
                    {
                        "scan_run_id": run["scan_run_id"],
                        "ts": run["ts"],
                        "signal_id": row["signal_id"],
                        "signal_version": row["signal_version"],
                        "signal_name": row["signal_name"],
                        "match_count": int((row["metric"] or {}).get("match_count") or 0),
                        "threshold": int((row["metric"] or {}).get("threshold") or 0),
                    }
                )

            for recommendation in run_recommendations:
                row = {**recommendation, "run_ts": run["ts"], "cadence": run["cadence"]}
                status = recommendation["status"]
                if status == "accepted":
                    accepted.append(row)
                elif status == "proposed":
                    proposed.append(row)
                else:
                    rejected_or_superseded.append(row)

            for failure in failures:
                feed_failures.append(
                    {
                        "scan_run_id": run["scan_run_id"],
                        "cadence": run["cadence"],
                        "run_ts": run["ts"],
                        "id": failure.get("id"),
                        "url": failure.get("url"),
                        "error": failure.get("error"),
                    }
                )

            runs_payload.append(
                {
                    "scan_run_id": run["scan_run_id"],
                    "ts": run["ts"],
                    "cadence": run["cadence"],
                    "feed_set": run.get("feed_set"),
                    "report_path": run.get("report_path"),
                    "feeds_health_path": feeds_health_path,
                    "feeds_health_summary": feed_summary,
                }
            )

    for cadence in list(triggered_by_cadence):
        triggered_by_cadence[cadence].sort(
            key=lambda row: (row["ts"], row["signal_name"].lower(), row["signal_id"], row["signal_version"])
        )

    accepted.sort(key=lambda row: (row["run_ts"], row["ts"], row["recommendation_id"]))
    proposed.sort(key=lambda row: (row["run_ts"], row["ts"], row["recommendation_id"]))
    rejected_or_superseded.sort(key=lambda row: (row["run_ts"], row["ts"], row["recommendation_id"]))
    feed_failures.sort(key=lambda row: (row["run_ts"], str(row.get("id") or ""), str(row.get("url") or "")))

    return {
        "date": target_date,
        "tz": tz_name,
        "cadences": cadence_order,
        "runs": runs_payload,
        "triggered_by_cadence": triggered_by_cadence,
        "recommendations": {
            "accepted": accepted,
            "proposed": proposed,
            "rejected_superseded": rejected_or_superseded,
        },
        "feed_health": {
            "total_failed_feeds": len(feed_failures),
            "failures": feed_failures,
        },
    }


def build_digest_markdown(payload: dict) -> str:
    lines: list[str] = []
    lines.append(f"# DEW Digest — {payload['date']}")
    lines.append("")
    lines.append("## Runs Included")
    runs = payload.get("runs") or []
    if not runs:
        lines.append("- None")
    else:
        for run in runs:
            lines.append(
                f"- `{run['cadence']}` | {run['ts']} | "
                f"report: `{run.get('report_path') or '-'}` | "
                f"feed_set: `{run.get('feed_set') or 'custom'}`"
            )
    lines.append("")

    lines.append("## Triggered Signals")
    triggered_by_cadence = dict(payload.get("triggered_by_cadence") or {})
    for cadence in payload.get("cadences") or []:
        lines.append(f"### {cadence}")
        rows = list(triggered_by_cadence.get(cadence) or [])
        if not rows:
            lines.append("- none")
            continue
        for row in rows:
            lines.append(
                f"- {row['signal_name']} (`{row['signal_id']}` v{row['signal_version']}) "
                f"match_count={row['match_count']} threshold={row['threshold']} "
                f"(run `{row['scan_run_id']}`)"
            )
    lines.append("")

    recs = dict(payload.get("recommendations") or {})
    lines.append("## Recommendations")
    lines.append("### Accepted")
    accepted = list(recs.get("accepted") or [])
    if not accepted:
        lines.append("- none")
    else:
        for row in accepted:
            confidence = row.get("confidence")
            conf_text = "n/a" if confidence is None else f"{float(confidence):.2f}"
            lines.append(
                f"- {row['title']} [{row['kind']}] conf={conf_text} "
                f"(run `{row['scan_run_id']}`)"
            )

    lines.append("### Proposed")
    proposed = list(recs.get("proposed") or [])
    if not proposed:
        lines.append("- none")
    else:
        for row in proposed:
            confidence = row.get("confidence")
            conf_text = "n/a" if confidence is None else f"{float(confidence):.2f}"
            lines.append(
                f"- {row['title']} [{row['kind']}] conf={conf_text} "
                f"(run `{row['scan_run_id']}`)"
            )

    lines.append("### Rejected/Superseded")
    other = list(recs.get("rejected_superseded") or [])
    if not other:
        lines.append("- none")
    else:
        for row in other:
            lines.append(f"- {row['title']} [{row['status']}] (run `{row['scan_run_id']}`)")
    lines.append("")

    feed_health = dict(payload.get("feed_health") or {})
    lines.append("## Feed Health Summary")
    lines.append(f"- Total failed feeds: **{int(feed_health.get('total_failed_feeds') or 0)}**")
    failures = list(feed_health.get("failures") or [])
    if not failures:
        lines.append("- No feed failures detected.")
    else:
        for row in failures:
            lines.append(
                f"- run `{row['scan_run_id']}` ({row['cadence']}) | "
                f"`{row.get('id') or '-'}` | `{row.get('url') or '-'}` | "
                f"{row.get('error') or 'unknown error'}"
            )
    return "\n".join(lines) + "\n"


def generate_daily_digest(
    ledger_db_path: str | Path,
    out_dir: str | Path = "out/digests",
    date: str | None = None,
    tz_name: str = "America/New_York",
    cadences: list[str] | None = None,
) -> dict:
    payload = collect_daily_digest(
        ledger_db_path=ledger_db_path,
        date=date,
        tz_name=tz_name,
        cadences=cadences,
    )
    out_path = Path(out_dir)
    ensure_dir(out_path)
    stem = payload["date"].replace("-", "")
    markdown_path = out_path / f"{stem}_digest.md"
    json_path = out_path / f"{stem}_digest.json"

    markdown = build_digest_markdown(payload)
    atomic_write_text(markdown_path, markdown)
    atomic_write_json(json_path, payload)

    return {
        "date": payload["date"],
        "tz": payload["tz"],
        "run_count": len(payload.get("runs") or []),
        "markdown_path": markdown_path.as_posix(),
        "json_path": json_path.as_posix(),
        "payload": payload,
    }
