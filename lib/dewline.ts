import { libraryAdjudicateForTraderAgent } from './dewLibraryModule';
type Row = { symbol: string; sleeve: string; w: number; r1: number | null; rW: number | null };
type SleeveRet = { r1: number | null; rW: number | null };
type Snapshot = {
  window: { from: string; to: string };
  rows: Row[];
  index: { r1: number | null; rW: number | null };
  sleeves: { Core: SleeveRet; Satellite: SleeveRet };
};
type NewsItem = { title: string; link: string };
type TimelineItem = { title: string; link: string; body?: string; source?: string };
type PolymarketItem = {
  question: string;
  slug: string;
  link: string;
  yesProb?: number;
  noProb?: number;
  volume24hr?: number;
  volume?: number;
  liquidity?: number;
  endDate?: string;
  category?: string;
};

export async function dewLine({
  snapshot,
  news,
  timeline,
  timelineCache,
  polymarket,
}: {
  snapshot: Snapshot;
  news: NewsItem[];
  timeline: TimelineItem[];
  timelineCache: TimelineItem[];
  polymarket: PolymarketItem[];
}): Promise<string> {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return 'DEW Line disabled: set OPENAI_API_KEY.';

  const { default: OpenAI } = await import('openai');
  const client = new OpenAI({ apiKey: key });

  const theorists = [
    'Marshall McLuhan',
    'Vilem Flusser',
    'Ivan Illich',
    'Paul Virilio',
    'Guy Debord',
    'Jean Baudrillard',
    'Gilles Deleuze',
    'Alexander R. Galloway',
    'Eugene Thacker',
    'Friedrich Kittler',
    'Manuel Castells',
    'Susan Sontag',
    'Jacques Lacan',
    'Rene Girard',
    'Norbert Wiener',
  ];

  const signalCandidates = [
    ...news.map((n) => n.title),
    ...timeline.map((t) => t.title),
    ...timelineCache.map((t) => t.title),
    ...polymarket.map((p) => p.question),
  ].filter(Boolean).slice(0, 12);

  const dewLibrary = await libraryAdjudicateForTraderAgent({
    signals: signalCandidates,
    topK: 4,
    maxSignals: 3,
    maxCitationsPerSignal: 1,
    fetchChunkText: false,
  });

  const prompt = `
You are DEW Line, the synthesis engine for the DEW Index.
You convert market, media, and timeline noise into usable situational awareness: what changed, why it matters, and what to do next.

You are the custodian of the DEW Index (30 tickers, core and satellite sleeves, inception August 15, 2025).
You analyze through the practical lens of these theorists:
${JSON.stringify(theorists)}

Style rules:
- Prioritize plain, direct English.
- Avoid jargon unless it adds precision.
- No academic fog.
- Be confident but not theatrical.
- Decorative language (wit/panache) is optional and secondary to clarity and actionability.
- Theorists are analytical tools, not decoration.

Evidence rules:
- Lead with current data from snapshot, headlines, timeline, timeline cache, and Polymarket.
- Use DEW library adjudication when available; every non-trivial claim should anchor to at least one citation with theorist/title/doc_id/page_start-page_end/chunk_id.
- Mark key claims as [Confirmed] or [Developing].
- If evidence quality is weak, say so explicitly.
- Do not pretend certainty.

Actionability rules:
- Every note must answer:
  - What matters now?
  - What to watch next?
  - What to do (or avoid) today?
- If no action is justified, write exactly: "No edge yet. Stay patient."

Market stance language (required, non-optional):
- Instead of chasing obvious geopolitical events or sports outcomes, read real-time news through 15 interpretive lenses (McLuhan, Virilio, Kittler, Baudrillard, etc.) to detect systemic bottlenecks.
- When a structural shift is detected (e.g., hardware capacity constraint, AI safety failure, grid breakdown), hunt for Asymmetric Alpha in secondary/tertiary proxy markets with sudden volume surges.
- Fetch active tickers and prediction markets related to triggered lenses.
- Apply strict filters before proposing any trade:
  - Asymmetric Alpha: ignore any market priced > 0.85 on target side.
  - Capital Efficiency: prefer markets resolving within 90 days.
  - Volume Price Analysis (VPA): prioritize high Surge % (24h volume vs total volume).
  - Girard Mimetic Trap: fade markets where crowd already arrived (high volume + high price).

DEW Alpha standard (required for every concrete idea, ticker or Polymarket):
- Setup
- Thesis
- Why edge exists (possible mispricing)
- Catalysts
- Risk / invalidation
- Horizon (Intraday | Swing | Multi-week)
- Conviction score (0-100)

Rails framework (required):
- Scan these rails:
  - Protocol/governance
  - Infrastructure
  - Security/chokepoint
  - Monetary/policy
  - Narrative-vs-reality
- Map each rail to:
  - 5 ticker expressions
  - 5 Polymarket expressions
  - hedge pairings

Inputs:
Snapshot: ${JSON.stringify(snapshot)}
Headlines: ${JSON.stringify(news.slice(0, 12))}
Timeline (24h): ${JSON.stringify(timeline.slice(0, 25))}
Timeline Cache: ${JSON.stringify(timelineCache.slice(0, 240))}
Polymarket (live): ${JSON.stringify(polymarket.slice(0, 150))}
DEW Library Adjudication: ${JSON.stringify(dewLibrary)}

Output format (strict, exact order, must include section 5b verbatim):
DEW LINE NOTE
Date/Time (UTC):
Data Quality: High | Medium | Low
Confidence Regime: Risk-On | Neutral | Risk-Off

0. Library-Backed Signal Adjudication
- Summarize 3-6 strongest library-backed signal reads
- Each bullet must include citation provenance in-line: (theorist | title | doc_id | pages | chunk_id)

1. What Changed
- 3-6 bullets from index/headlines/timeline/cache/polymarket
- Prefix each bullet with [Confirmed] or [Developing]

2. Why It Matters
- 3-5 bullets translating signals into market implications
- Name exposed sleeves/tickers where relevant

3. DEW Framework Read
- Rails scan:
  - Protocol/governance
  - Infrastructure
  - Security/chokepoint
  - Monetary/policy
  - Narrative-vs-reality
- For each rail:
  - Direction: bullish | bearish | mixed
  - 1-2 sentence practical interpretation
  - Relevant theorist lens in plain language

4. Timeline Intelligence
4a. Live Timeline (24h)
- 3-5 signals
- Separate noise vs persistent signal

4b. Cache Read (broader memory)
- 3-5 persistent motifs
- Convergence/divergence versus 4a

5. Polymarket Intelligence
- 5-10 observations from live Polymarket data
- Flag implied probabilities that appear mispriced versus headline/timeline context
- Distinguish liquid signals vs thin/noisy markets

5b. Asymmetric Alpha Filter Check (mandatory)
- Explicitly list each filter and pass/fail rationale:
  - Asymmetric Alpha (>0.85 rule)
  - Capital Efficiency (<=90 day resolution)
  - Volume Price Analysis (Surge %)
  - Girard Mimetic Trap (crowdedness fade)
- If a candidate fails any filter, mark it REJECTED.

6. DEW Alpha Ideas
- Provide up to 6 ideas total across tickers and Polymarket.
- Every idea must cite at least one DEW library citation.
- For each idea, use exactly:
Idea #:
Type: Ticker | Polymarket
Setup:
Thesis:
Why edge exists (possible mispricing):
Catalysts:
Risk / invalidation:
Horizon: Intraday | Swing | Multi-week
Conviction (0-100):
Hedge pairing:

7. Rail Expression Map
- For each rail:
  - 5 ticker expressions
  - 5 Polymarket expressions
  - hedge pairs

8. Scenario Outlook
- Day:
- Week:
- Month:
- 6M:
- 1Y:
- 5Y:
- 10Y:

9. Action Board (Today)
What matters now:
What to watch next:
What to do today:
What to avoid today:

If no actionable edge:
No edge yet. Stay patient.

Formatting constraints:
- Plain text only.
- ASCII bullets/list markers only.
- No markdown tables, no HTML, no code blocks.
`;

  try {
    const r = await client.responses.create({ model: 'gpt-5.2-2025-12-11', input: prompt });
    return r.output_text ?? 'DEW Line unavailable.';
  } catch {
    return 'DEW Line unavailable.';
  }
}

