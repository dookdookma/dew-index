export type DewLibCitation = {
  theorist: string;
  title: string;
  doc_id: string;
  page_start: number;
  page_end: number;
  chunk_id: string;
  quote: string;
  score?: number;
};

export type DewLibSignalRead = {
  signal: string;
  citations: DewLibCitation[];
};

export type DewLibAdjudication = {
  enabled: boolean;
  source: string;
  reads: DewLibSignalRead[];
  errors?: string[];
};

const DEW_LIB_URL = process.env.DEW_LIBRARY_URL || 'http://127.0.0.1:8787';

function cleanQuote(text: string, max = 220): string {
  const oneLine = (text || '').replace(/\s+/g, ' ').trim();
  return oneLine.length > max ? `${oneLine.slice(0, max - 3)}...` : oneLine;
}

async function getChunkQuote(chunkId: string): Promise<string> {
  try {
    const r = await fetch(`${DEW_LIB_URL}/chunk/${encodeURIComponent(chunkId)}`, { cache: 'no-store' });
    if (!r.ok) return '';
    const j = await r.json();
    return cleanQuote(j?.text || j?.chunk_text || j?.excerpt || '');
  } catch {
    return '';
  }
}

export async function libraryAdjudicateForTraderAgent({
  signals,
  topK = 8,
  maxSignals = 8,
  maxCitationsPerSignal = 3,
}: {
  signals: string[];
  topK?: number;
  maxSignals?: number;
  maxCitationsPerSignal?: number;
}): Promise<DewLibAdjudication> {
  const trimmed = signals.map((s) => (s || '').trim()).filter(Boolean).slice(0, maxSignals);
  if (!trimmed.length) {
    return { enabled: false, source: DEW_LIB_URL, reads: [], errors: ['No signals provided'] };
  }

  const reads: DewLibSignalRead[] = [];
  const errors: string[] = [];

  for (const signal of trimmed) {
    try {
      const sr = await fetch(`${DEW_LIB_URL}/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: signal, top_k: topK }),
        cache: 'no-store',
      });
      if (!sr.ok) {
        errors.push(`search failed for signal: ${signal}`);
        continue;
      }
      const hits = (await sr.json()) as any[];
      const seen = new Set<string>();
      const picked = [] as DewLibCitation[];

      for (const h of hits || []) {
        const theorist = String(h?.theorist || '').trim();
        const chunkId = String(h?.chunk_id || '').trim();
        if (!theorist || !chunkId || seen.has(theorist)) continue;
        seen.add(theorist);

        const quote = (await getChunkQuote(chunkId)) || cleanQuote(String(h?.excerpt || ''));
        picked.push({
          theorist,
          title: String(h?.title || ''),
          doc_id: String(h?.doc_id || ''),
          page_start: Number(h?.page_start || 0),
          page_end: Number(h?.page_end || 0),
          chunk_id: chunkId,
          quote,
          score: Number(h?.score || 0),
        });

        if (picked.length >= maxCitationsPerSignal) break;
      }

      reads.push({ signal, citations: picked });
    } catch {
      errors.push(`exception for signal: ${signal}`);
    }
  }

  return {
    enabled: reads.some((r) => r.citations.length > 0),
    source: DEW_LIB_URL,
    reads,
    errors: errors.length ? errors : undefined,
  };
}
