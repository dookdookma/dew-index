from __future__ import annotations


def _confidence_for_kind(kind: str, ratio: float) -> float:
    capped = min(max(ratio, 0.0), 6.0)
    if kind == "watch":
        return round(min(0.95, 0.55 + 0.06 * capped), 3)
    if kind == "paper_alloc":
        return round(min(0.99, 0.62 + 0.06 * capped), 3)
    return 0.5


def _top_items_text(matched_items: list[dict], limit: int = 3) -> str:
    if not matched_items:
        return "- none"
    lines: list[str] = []
    for item in matched_items[:limit]:
        title = item.get("title") or "(untitled)"
        link = item.get("link")
        if link:
            lines.append(f"- {title} ({link})")
        else:
            lines.append(f"- {title}")
    return "\n".join(lines)


def generate_recommendations(triggered_results: list[dict]) -> list[dict]:
    recommendations: list[dict] = []
    rows = sorted(
        triggered_results,
        key=lambda row: (row["signal"]["name"].lower(), row["signal"]["signal_id"], row["observation_id"]),
    )
    for row in rows:
        signal = row["signal"]
        metric = row["metric"]
        context = row["context"]
        threshold = max(1, int(metric.get("threshold") or 1))
        match_count = int(metric.get("match_count") or 0)
        ratio = match_count / threshold

        kinds: list[str] = []
        if match_count >= threshold * 2:
            kinds.append("watch")
        if match_count >= threshold * 3:
            kinds.append("paper_alloc")

        for kind in kinds:
            title = (
                f"[{kind}] {signal['name']} v{signal['version']} "
                f"({match_count}/{threshold})"
            )
            body = (
                f"Signal `{signal['name']}` (v{signal['version']}) recorded "
                f"`match_count={match_count}` against `threshold={threshold}`.\n\n"
                "Top matched items:\n"
                f"{_top_items_text(context.get('matched_items', []), limit=3)}\n\n"
                "Evidence: See explain section."
            )
            recommendations.append(
                {
                    "kind": kind,
                    "title": title,
                    "body": body,
                    "confidence": _confidence_for_kind(kind, ratio),
                    "related_signal_ids": [
                        {"signal_id": signal["signal_id"], "version": signal["version"]}
                    ],
                    "related_observation_ids": [row["observation_id"]],
                    "status": "proposed",
                }
            )

    return recommendations

