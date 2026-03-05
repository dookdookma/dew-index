from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from .search import SearchService
from .triage import (
    TIER_1_UNUSABLE_ZERO_TEXT,
    TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE,
)
from .util import atomic_write_json, atomic_write_text, read_json, tokenize

DEW_THEORISTS = [
    "McLuhan",
    "Flusser",
    "Illich",
    "Virilio",
    "Debord",
    "Baudrillard",
    "Deleuze",
    "Galloway",
    "Thacker",
    "Kittler",
    "Castells",
    "Sontag",
    "Lacan",
    "Girard",
    "Wiener",
]

DEFAULT_QUERY_REGISTRY: list[dict] = [
    {
        "query_id": "mcluhan_exact_medium_message",
        "query_text": "the medium is the message",
        "theorist": "McLuhan",
        "query_type": "exact",
        "expected_terms": ["medium", "message"],
        "notes": "Canonical phrase retrieval.",
    },
    {
        "query_id": "mcluhan_conceptual_extensions",
        "query_text": "media as extensions of man and sensory ratio",
        "theorist": "McLuhan",
        "query_type": "conceptual",
        "expected_terms": ["extensions", "media", "sensory"],
        "notes": "Conceptual vocabulary from McLuhan corpus.",
    },
    {
        "query_id": "flusser_exact_technical_images",
        "query_text": "technical images apparatus program",
        "theorist": "Flusser",
        "query_type": "exact",
        "expected_terms": ["technical", "images", "apparatus", "program"],
        "notes": "Core Flusser lexical cluster.",
    },
    {
        "query_id": "flusser_conceptual_photography",
        "query_text": "philosophy of photography gesture and code",
        "theorist": "Flusser",
        "query_type": "conceptual",
        "expected_terms": ["photography", "gesture", "code"],
        "notes": "Photography and code framing.",
    },
    {
        "query_id": "illich_exact_tools_conviviality",
        "query_text": "tools for conviviality",
        "theorist": "Illich",
        "query_type": "exact",
        "expected_terms": ["tools", "conviviality"],
        "notes": "Exact title phrase.",
    },
    {
        "query_id": "illich_conceptual_deschooling",
        "query_text": "deschooling society institutional monopoly",
        "theorist": "Illich",
        "query_type": "conceptual",
        "expected_terms": ["deschooling", "institutional", "monopoly"],
        "notes": "Education critique query.",
    },
    {
        "query_id": "virilio_exact_dromology",
        "query_text": "dromology speed and politics",
        "theorist": "Virilio",
        "query_type": "exact",
        "expected_terms": ["dromology", "speed", "politics"],
        "notes": "Exact Virilio terminology.",
    },
    {
        "query_id": "virilio_conceptual_accident",
        "query_text": "integral accident war perception logistics",
        "theorist": "Virilio",
        "query_type": "conceptual",
        "expected_terms": ["accident", "war", "perception"],
        "notes": "Accident/perception conceptual retrieval.",
    },
    {
        "query_id": "debord_exact_society_spectacle",
        "query_text": "society of the spectacle",
        "theorist": "Debord",
        "query_type": "exact",
        "expected_terms": ["society", "spectacle"],
        "notes": "Exact title phrase.",
    },
    {
        "query_id": "debord_conceptual_separation",
        "query_text": "spectacle separation commodity image",
        "theorist": "Debord",
        "query_type": "conceptual",
        "expected_terms": ["spectacle", "commodity", "image"],
        "notes": "Commodity-image critique retrieval.",
    },
    {
        "query_id": "baudrillard_exact_simulacra",
        "query_text": "simulacra and simulation",
        "theorist": "Baudrillard",
        "query_type": "exact",
        "expected_terms": ["simulacra", "simulation"],
        "notes": "Exact title phrase.",
    },
    {
        "query_id": "baudrillard_conceptual_hyperreality",
        "query_text": "hyperreality sign value symbolic exchange",
        "theorist": "Baudrillard",
        "query_type": "conceptual",
        "expected_terms": ["hyperreality", "sign", "value"],
        "notes": "Sign-value and hyperreality vocabulary.",
    },
    {
        "query_id": "deleuze_exact_postscript_control",
        "query_text": "postscript on the societies of control",
        "theorist": "Deleuze",
        "query_type": "exact",
        "expected_terms": ["societies", "control"],
        "notes": "Exact essay phrase.",
    },
    {
        "query_id": "deleuze_conceptual_rhizome",
        "query_text": "rhizome assemblage deterritorialization",
        "theorist": "Deleuze",
        "query_type": "conceptual",
        "expected_terms": ["rhizome", "assemblage", "deterritorialization"],
        "notes": "Conceptual Deleuze terms.",
    },
    {
        "query_id": "galloway_exact_protocol",
        "query_text": "protocol how control exists after decentralization",
        "theorist": "Galloway",
        "query_type": "exact",
        "expected_terms": ["protocol", "control", "decentralization"],
        "notes": "Exact key title phrase.",
    },
    {
        "query_id": "galloway_conceptual_interface",
        "query_text": "interface effect network protocol power",
        "theorist": "Galloway",
        "query_type": "conceptual",
        "expected_terms": ["interface", "network", "protocol"],
        "notes": "Interface and protocol conceptual query.",
    },
    {
        "query_id": "thacker_exact_biomedia",
        "query_text": "biomedia network culture life science",
        "theorist": "Thacker",
        "query_type": "exact",
        "expected_terms": ["biomedia", "network", "life"],
        "notes": "Exact lexical set from Thacker texts.",
    },
    {
        "query_id": "thacker_conceptual_horror",
        "query_text": "horror of philosophy world without us",
        "theorist": "Thacker",
        "query_type": "conceptual",
        "expected_terms": ["horror", "philosophy", "world"],
        "notes": "Horror-philosophy conceptual query.",
    },
    {
        "query_id": "kittler_exact_gramophone",
        "query_text": "gramophone film typewriter",
        "theorist": "Kittler",
        "query_type": "exact",
        "expected_terms": ["gramophone", "film", "typewriter"],
        "notes": "Exact title phrase.",
    },
    {
        "query_id": "kittler_conceptual_media_determine",
        "query_text": "media determine our situation discourse networks",
        "theorist": "Kittler",
        "query_type": "conceptual",
        "expected_terms": ["media", "determine", "networks"],
        "notes": "Media determinism query.",
    },
    {
        "query_id": "castells_exact_network_society",
        "query_text": "rise of the network society",
        "theorist": "Castells",
        "query_type": "exact",
        "expected_terms": ["network", "society"],
        "notes": "Exact title phrase.",
    },
    {
        "query_id": "castells_conceptual_space_flows",
        "query_text": "space of flows informational capitalism",
        "theorist": "Castells",
        "query_type": "conceptual",
        "expected_terms": ["flows", "informational", "capitalism"],
        "notes": "Informational-capitalism conceptual query.",
    },
    {
        "query_id": "sontag_exact_against_interpretation",
        "query_text": "against interpretation",
        "theorist": "Sontag",
        "query_type": "exact",
        "expected_terms": ["against", "interpretation"],
        "notes": "Exact title phrase.",
    },
    {
        "query_id": "sontag_conceptual_pain_of_others",
        "query_text": "regarding the pain of others photography war",
        "theorist": "Sontag",
        "query_type": "conceptual",
        "expected_terms": ["pain", "others", "photography"],
        "notes": "Sontag on imaging and suffering.",
    },
    {
        "query_id": "lacan_exact_mirror_stage",
        "query_text": "the mirror stage",
        "theorist": "Lacan",
        "query_type": "exact",
        "expected_terms": ["mirror", "stage"],
        "notes": "Exact concept phrase.",
    },
    {
        "query_id": "lacan_conceptual_symbolic_imaginary_real",
        "query_text": "symbolic imaginary real desire",
        "theorist": "Lacan",
        "query_type": "conceptual",
        "expected_terms": ["symbolic", "imaginary", "real"],
        "notes": "Core Lacanian triad.",
    },
    {
        "query_id": "girard_exact_mimetic_desire",
        "query_text": "mimetic desire",
        "theorist": "Girard",
        "query_type": "exact",
        "expected_terms": ["mimetic", "desire"],
        "notes": "Exact conceptual phrase.",
    },
    {
        "query_id": "girard_conceptual_scapegoat",
        "query_text": "scapegoat violence and the sacred",
        "theorist": "Girard",
        "query_type": "conceptual",
        "expected_terms": ["scapegoat", "violence", "sacred"],
        "notes": "Scapegoat mechanism query.",
    },
    {
        "query_id": "wiener_exact_cybernetics",
        "query_text": "cybernetics or control and communication",
        "theorist": "Wiener",
        "query_type": "exact",
        "expected_terms": ["cybernetics", "control", "communication"],
        "notes": "Exact Wiener framing.",
    },
    {
        "query_id": "wiener_conceptual_feedback",
        "query_text": "feedback loop information entropy machine",
        "theorist": "Wiener",
        "query_type": "conceptual",
        "expected_terms": ["feedback", "information", "entropy"],
        "notes": "Core cybernetic concepts.",
    },
    {
        "query_id": "cross_spectacle_simulation_media",
        "query_text": "spectacle simulation and media environment",
        "query_type": "cross_theorist",
        "expected_terms": ["spectacle", "simulation", "media"],
        "notes": "Debord x Baudrillard x McLuhan surface.",
    },
    {
        "query_id": "cross_speed_control_network",
        "query_text": "speed control protocol network power",
        "query_type": "cross_theorist",
        "expected_terms": ["speed", "control", "network"],
        "notes": "Virilio x Deleuze x Galloway bridge query.",
    },
    {
        "query_id": "cross_mimesis_symbolic_media",
        "query_text": "mimesis symbolic order media apparatus",
        "query_type": "cross_theorist",
        "expected_terms": ["mimesis", "symbolic", "apparatus"],
        "notes": "Girard x Lacan x Flusser bridge query.",
    },
    {
        "query_id": "cross_network_society_cybernetics",
        "query_text": "network society cybernetics communication control",
        "query_type": "cross_theorist",
        "expected_terms": ["network", "society", "cybernetics"],
        "notes": "Castells x Wiener bridge query.",
    },
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clamp(value: int, lo: int = 0, hi: int = 100) -> int:
    return max(lo, min(value, hi))


def _normalize_registry(query_registry: list[dict] | None) -> list[dict]:
    rows = query_registry if query_registry is not None else DEFAULT_QUERY_REGISTRY
    normalized: list[dict] = []
    for row in rows:
        normalized.append(
            {
                "query_id": row["query_id"],
                "query_text": row["query_text"],
                "theorist": row.get("theorist"),
                "query_type": row.get("query_type", "conceptual"),
                "expected_terms": list(row.get("expected_terms") or []),
                "notes": row.get("notes", ""),
            }
        )
    return normalized


def _triage_by_doc(data_dir: Path, triage_report: dict | None) -> dict[str, dict]:
    if triage_report is not None:
        return {row["doc_id"]: row for row in triage_report.get("docs", [])}
    triage_path = data_dir / "ocr_triage.json"
    if not triage_path.exists():
        return {}
    payload = read_json(triage_path)
    return {row["doc_id"]: row for row in payload.get("docs", [])}


def _expected_term_hits_top3(expected_terms: list[str], results: list[dict]) -> int:
    if not expected_terms:
        return 0
    top3_text = " ".join(row.get("excerpt", "") for row in results[:3])
    token_set = set(tokenize(top3_text))
    hits = 0
    for term in expected_terms:
        term_tokens = tokenize(term)
        if term_tokens and all(token in token_set for token in term_tokens):
            hits += 1
    return hits


def _evaluate_query(
    service: SearchService,
    query: dict,
    triage_map: dict[str, dict],
    top_k: int,
) -> dict:
    theorist = query.get("theorist")
    results = service.search(
        query=query["query_text"],
        theorist=theorist,
        top_k=top_k,
    )
    expected_terms = list(query.get("expected_terms") or [])
    expected_term_hits = _expected_term_hits_top3(expected_terms, results)
    expected_term_points = (
        int(round(20.0 * (expected_term_hits / len(expected_terms)))) if expected_terms else 0
    )

    theorist_match = None
    if theorist:
        theorist_match = bool(results) and results[0]["theorist"] == theorist

    doc_counter = Counter(row["doc_id"] for row in results)
    dominant_doc_id = ""
    doc_dominance_ratio = 0.0
    if doc_counter and results:
        dominant_doc_id, dominant_count = sorted(
            doc_counter.items(),
            key=lambda item: (-item[1], item[0]),
        )[0]
        doc_dominance_ratio = dominant_count / len(results)

    tier_1_intrusion = any(
        triage_map.get(row["doc_id"], {}).get("triage_class") == TIER_1_UNUSABLE_ZERO_TEXT
        for row in results
    )
    mixed_content_domination = False
    mixed_content_doc_id = ""
    if results:
        for doc_id, count in sorted(doc_counter.items(), key=lambda item: item[0]):
            tier = triage_map.get(doc_id, {}).get("triage_class")
            if (
                tier == TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE
                and (count / len(results)) > 0.5
            ):
                mixed_content_domination = True
                mixed_content_doc_id = doc_id
                break

    score = 50
    if theorist is not None and theorist_match:
        score += 20
    score += expected_term_points
    if tier_1_intrusion:
        score -= 15
    if doc_dominance_ratio > 0.6:
        score -= 10
    if mixed_content_domination:
        score -= 5
    retrieval_quality_score = _clamp(score)

    diagnostics: list[str] = []
    if not results:
        diagnostics.append("no_results")
    if theorist is not None and theorist_match is False:
        diagnostics.append("top_result_theorist_mismatch")
    if expected_terms and expected_term_hits == 0:
        diagnostics.append("expected_terms_missing_top3")
    if tier_1_intrusion:
        diagnostics.append("tier_1_intrusion")
    if doc_dominance_ratio > 0.6:
        diagnostics.append(f"doc_dominance_ratio={doc_dominance_ratio:.2f}")
    if mixed_content_domination:
        diagnostics.append(f"mixed_content_domination={mixed_content_doc_id}")

    result_rows: list[dict] = []
    for rank, row in enumerate(results, start=1):
        triage_row = triage_map.get(row["doc_id"], {})
        result_rows.append(
            {
                "rank": rank,
                "fused_score": float(row["score"]),
                "chunk_id": row["chunk_id"],
                "doc_id": row["doc_id"],
                "theorist": row["theorist"],
                "title": row["title"],
                "page_start": int(row["page_start"]),
                "page_end": int(row["page_end"]),
                "excerpt": row.get("excerpt", ""),
                "triage_class": triage_row.get("triage_class"),
                "quality_score": triage_row.get("quality_score"),
            }
        )

    return {
        "query_id": query["query_id"],
        "query_text": query["query_text"],
        "theorist": theorist,
        "query_type": query["query_type"],
        "expected_terms": expected_terms,
        "notes": query.get("notes", ""),
        "metrics": {
            "theorist_match": theorist_match,
            "expected_term_hits_top3": expected_term_hits,
            "expected_term_count": len(expected_terms),
            "doc_dominance_ratio": doc_dominance_ratio,
            "dominant_doc_id": dominant_doc_id,
            "mixed_content_domination": mixed_content_domination,
            "mixed_content_doc_id": mixed_content_doc_id,
            "tier_1_intrusion": tier_1_intrusion,
            "retrieval_quality_score": retrieval_quality_score,
            "diagnostics": diagnostics,
        },
        "results": result_rows,
    }


def _per_theorist_summary(per_query: list[dict]) -> list[dict]:
    rows_by_theorist: dict[str, list[dict]] = defaultdict(list)
    for row in per_query:
        theorist = row.get("theorist")
        if theorist:
            rows_by_theorist[theorist].append(row)

    summary_rows: list[dict] = []
    for theorist in DEW_THEORISTS:
        rows = rows_by_theorist.get(theorist, [])
        scores = [row["metrics"]["retrieval_quality_score"] for row in rows]
        avg_score = float(mean(scores)) if scores else 0.0
        worst = sorted(
            [
                {
                    "query_id": row["query_id"],
                    "score": row["metrics"]["retrieval_quality_score"],
                    "diagnostics": row["metrics"]["diagnostics"],
                }
                for row in rows
                if row["metrics"]["retrieval_quality_score"] < 40
            ],
            key=lambda item: (item["score"], item["query_id"]),
        )
        best = sorted(
            [
                {
                    "query_id": row["query_id"],
                    "score": row["metrics"]["retrieval_quality_score"],
                }
                for row in rows
            ],
            key=lambda item: (-item["score"], item["query_id"]),
        )[:3]
        summary_rows.append(
            {
                "theorist": theorist,
                "query_count": len(rows),
                "average_retrieval_quality_score": avg_score,
                "worst_queries": worst,
                "best_queries": best,
            }
        )
    return summary_rows


def _doc_behavior_summary(per_query: list[dict], top_k: int) -> dict:
    total_queries = len(per_query)
    total_results = sum(len(row["results"]) for row in per_query)

    appearances: Counter[str] = Counter()
    top1_appearances: Counter[str] = Counter()
    query_hits: Counter[str] = Counter()
    score_accumulator: dict[str, list[int]] = defaultdict(list)
    doc_meta: dict[str, dict] = {}

    for query_row in per_query:
        score = int(query_row["metrics"]["retrieval_quality_score"])
        seen_in_query: set[str] = set()
        for result in query_row["results"]:
            doc_id = result["doc_id"]
            appearances[doc_id] += 1
            score_accumulator[doc_id].append(score)
            doc_meta.setdefault(
                doc_id,
                {
                    "doc_id": doc_id,
                    "theorist": result.get("theorist", ""),
                    "title": result.get("title", ""),
                    "triage_class": result.get("triage_class"),
                    "quality_score": result.get("quality_score"),
                },
            )
            seen_in_query.add(doc_id)
        for doc_id in seen_in_query:
            query_hits[doc_id] += 1
        if query_row["results"]:
            top1_appearances[query_row["results"][0]["doc_id"]] += 1

    rows: list[dict] = []
    for doc_id, count in sorted(appearances.items(), key=lambda item: (-item[1], item[0])):
        avg_query_score = float(mean(score_accumulator[doc_id])) if score_accumulator[doc_id] else 0.0
        appearance_ratio = (count / total_results) if total_results else 0.0
        rows.append(
            {
                **doc_meta.get(doc_id, {"doc_id": doc_id}),
                "result_appearances": count,
                "query_appearances": query_hits[doc_id],
                "top1_appearances": top1_appearances[doc_id],
                "appearance_ratio": appearance_ratio,
                "avg_query_score_when_present": avg_query_score,
            }
        )

    over_dominant_docs = [
        row
        for row in rows
        if (
            row["appearance_ratio"] >= 0.12
            or row["top1_appearances"] >= max(2, total_queries // 5)
        )
    ]
    remediation_priorities = [
        row
        for row in rows
        if row["result_appearances"] >= max(3, top_k // 2)
        and row["avg_query_score_when_present"] < 50.0
    ]
    remediation_priorities = sorted(
        remediation_priorities,
        key=lambda row: (
            row["avg_query_score_when_present"],
            -row["result_appearances"],
            row["doc_id"],
        ),
    )
    return {
        "total_queries": total_queries,
        "total_results": total_results,
        "docs": rows,
        "over_dominant_docs": over_dominant_docs,
        "remediation_priorities": remediation_priorities,
    }


def build_retrieval_validation_report(
    data_dir: Path,
    query_registry: list[dict] | None = None,
    triage_report: dict | None = None,
    top_k: int = 8,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
) -> dict:
    registry = _normalize_registry(query_registry)
    triage_map = _triage_by_doc(data_dir, triage_report)
    service = SearchService(data_dir=data_dir)

    per_query = [
        _evaluate_query(service=service, query=query, triage_map=triage_map, top_k=top_k)
        for query in registry
    ]
    overall_scores = [row["metrics"]["retrieval_quality_score"] for row in per_query]
    overall_average = float(mean(overall_scores)) if overall_scores else 0.0

    per_theorist = _per_theorist_summary(per_query)
    doc_behavior = _doc_behavior_summary(per_query, top_k=top_k)

    report = {
        "generated_at": _utc_now_iso(),
        "heuristic_note": (
            "Retrieval quality is a heuristic score, not a benchmark metric. "
            "Scores are deterministic for a fixed corpus/index/query registry."
        ),
        "top_k": top_k,
        "query_count": len(per_query),
        "overall": {
            "average_retrieval_quality_score": overall_average,
            "min_score": min(overall_scores) if overall_scores else 0,
            "max_score": max(overall_scores) if overall_scores else 0,
            "weak_query_count": sum(1 for row in per_query if row["metrics"]["retrieval_quality_score"] < 40),
        },
        "per_theorist": per_theorist,
        "per_query": per_query,
        "doc_behavior": doc_behavior,
    }

    if output_json_path is not None:
        atomic_write_json(output_json_path, report)
    if output_md_path is not None:
        atomic_write_text(output_md_path, render_retrieval_validation_markdown(report))
    return report


def render_retrieval_validation_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append("# DEW Retrieval Validation")
    lines.append("")
    lines.append(f"- Generated at: `{report['generated_at']}`")
    lines.append(f"- Query count: **{report['query_count']}**")
    lines.append(f"- Top K: **{report['top_k']}**")
    lines.append(
        f"- Overall average retrieval score: **{report['overall']['average_retrieval_quality_score']:.2f}**"
    )
    lines.append(f"- Weak queries (score < 40): **{report['overall']['weak_query_count']}**")
    lines.append("")
    lines.append(f"> {report['heuristic_note']}")
    lines.append("")
    lines.append("## Per-Theorist Scores")
    lines.append("| Theorist | Queries | Avg Score | Worst (<40) |")
    lines.append("| --- | ---: | ---: | ---: |")
    for row in report["per_theorist"]:
        lines.append(
            f"| {row['theorist']} | {row['query_count']} | "
            f"{row['average_retrieval_quality_score']:.2f} | {len(row['worst_queries'])} |"
        )
    lines.append("")
    lines.append("## Worst Queries")
    weak = sorted(
        [
            row
            for row in report["per_query"]
            if row["metrics"]["retrieval_quality_score"] < 40
        ],
        key=lambda row: (row["metrics"]["retrieval_quality_score"], row["query_id"]),
    )
    if not weak:
        lines.append("- None")
    else:
        for row in weak[:30]:
            diag = ", ".join(row["metrics"]["diagnostics"]) if row["metrics"]["diagnostics"] else "-"
            lines.append(
                f"- `{row['query_id']}` score={row['metrics']['retrieval_quality_score']} "
                f"| type={row['query_type']} | diagnostics={diag}"
            )
    lines.append("")
    lines.append("## Over-Dominant Documents")
    over = report["doc_behavior"]["over_dominant_docs"]
    if not over:
        lines.append("- None")
    else:
        for row in over[:20]:
            lines.append(
                f"- `{row['doc_id']}` | {row.get('theorist', '')} | "
                f"appearances={row['result_appearances']} | ratio={row['appearance_ratio']:.2f} | "
                f"tier={row.get('triage_class')} | mixed={row.get('triage_class') == TIER_3_USABLE_MIXED_CONTENT_HIGH_BLANK_RATE}"
            )
    lines.append("")
    lines.append("## Top Remediation Priorities")
    priorities = report["doc_behavior"]["remediation_priorities"]
    if not priorities:
        lines.append("- None")
    else:
        for row in priorities[:20]:
            lines.append(
                f"- `{row['doc_id']}` | {row.get('theorist', '')} | "
                f"avg_query_score_when_present={row['avg_query_score_when_present']:.2f} | "
                f"appearances={row['result_appearances']} | tier={row.get('triage_class')}"
            )
    lines.append("")
    lines.append(
        "- Tier 3 mixed-content docs are treated as usable, but they may dominate top-k lists in some queries."
    )
    return "\n".join(lines) + "\n"

