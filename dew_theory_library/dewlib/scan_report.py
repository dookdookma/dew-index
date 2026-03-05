from __future__ import annotations


def _short_quote(text: str, limit: int = 240) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[:limit].rstrip() + "..."


def build_scan_markdown(
    run_ts: str,
    source_summaries: list[dict],
    total_items: int,
    evaluations: list[dict],
    recommendations: list[dict] | None = None,
    cadence: str = "ad_hoc",
    feed_set: str | None = None,
) -> str:
    lines: list[str] = []
    lines.append(f"# DEW Scan — {run_ts}")
    lines.append("")
    lines.append("## Metadata")
    lines.append(f"- Cadence: **{cadence}**")
    lines.append(f"- Feed set: **{feed_set or 'custom'}**")
    lines.append(f"- Item count: **{total_items}**")
    lines.append("")
    lines.append("## Inputs")
    for source in source_summaries:
        lines.append(f"- `{source['id']}`: `{source['url']}` (items={source['item_count']})")
    lines.append(f"- Total normalized items: **{total_items}**")
    lines.append("")

    triggered = [row for row in evaluations if row["triggered"]]
    non_triggered = [row for row in evaluations if not row["triggered"]]

    lines.append("## Triggered Signals")
    if not triggered:
        lines.append("- None")
    else:
        for row in triggered:
            signal = row["signal"]
            metric = row["metric"]
            keyword_hits = metric.get("keyword_hits", {})
            top_keywords = sorted(keyword_hits.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
            matched_items = row["context"].get("matched_items", [])
            explain = row.get("explain", {"concepts": []})

            lines.append(
                f"### {signal['name']} (`{signal['signal_id']}` v{signal['version']})"
            )
            lines.append(
                f"- Metric: match_count={metric.get('match_count', 0)} "
                f"threshold={metric.get('threshold', 0)}"
            )
            if top_keywords:
                lines.append(
                    "- Top keywords: "
                    + ", ".join([f"`{keyword}`={hits}" for keyword, hits in top_keywords])
                )
            else:
                lines.append("- Top keywords: none")

            lines.append("- Top matched items:")
            if not matched_items:
                lines.append("  - none")
            else:
                for item in matched_items[:10]:
                    title = item.get("title") or "(untitled)"
                    link = item.get("link")
                    if link:
                        lines.append(f"  - {title} — {link}")
                    else:
                        lines.append(f"  - {title}")

            lines.append("- Explanation:")
            concepts = explain.get("concepts", [])
            if not concepts:
                lines.append("  - No linked concepts/citations.")
            else:
                for concept_bundle in concepts:
                    concept = concept_bundle["concept"]
                    claim = concept_bundle["claim"]
                    confidence = claim.get("confidence")
                    conf_text = "n/a" if confidence is None else f"{float(confidence):.2f}"
                    lines.append(
                        f"  - Concept: {concept['name']} ({concept['status']}) | "
                        f"Claim: {claim['claim']} | confidence={conf_text}"
                    )
                    citations = concept_bundle.get("citations", [])
                    if not citations:
                        lines.append("    - Citations: none")
                    else:
                        for citation in citations[:8]:
                            lines.append(
                                "    - "
                                f"{citation['theorist']} | {citation['title']} | "
                                f"pp.{citation['page_start']}-{citation['page_end']} | "
                                f"chunk `{citation['chunk_id']}` | "
                                f"\"{_short_quote(citation.get('quote', ''))}\""
                            )
            lines.append("")

    lines.append("## Recommendations")
    recs = recommendations or []
    if not recs:
        lines.append("- None")
    else:
        for rec in recs:
            confidence = rec.get("confidence")
            conf_text = "n/a" if confidence is None else f"{float(confidence):.2f}"
            related = rec.get("related_signal_ids_json") or rec.get("related_signal_ids") or []
            related_text = ", ".join(
                [f"{row.get('signal_id')} v{row.get('version')}" for row in related]
            ) or "-"
            lines.append(f"- **{rec.get('kind', 'note')}**: {rec.get('title', '')}")
            lines.append(f"  - confidence: {conf_text}")
            lines.append(f"  - related signals: {related_text}")
            body = str(rec.get("body") or "").strip()
            if body:
                lines.append(f"  - {body}")
            else:
                lines.append("  - (no body)")
    lines.append("")

    lines.append("## Non-Triggered Signals")
    if not non_triggered:
        lines.append("- None")
    else:
        lines.append("| Signal | Version | match_count | threshold |")
        lines.append("| --- | ---: | ---: | ---: |")
        for row in non_triggered:
            signal = row["signal"]
            metric = row["metric"]
            lines.append(
                f"| {signal['name']} | {signal['version']} | "
                f"{metric.get('match_count', 0)} | {metric.get('threshold', 0)} |"
            )
    lines.append("")
    lines.append("## Notes")
    lines.append("- No execution performed; recommendations are non-binding.")
    return "\n".join(lines) + "\n"
