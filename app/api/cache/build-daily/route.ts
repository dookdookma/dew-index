import { NextRequest, NextResponse } from 'next/server';
import { getRedis } from '@/lib/kv';
import { libraryAdjudicateForTraderAgent } from '@/lib/dewLibraryModule';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

async function fetchJson(url: string, timeoutMs = 12000): Promise<any> {
  const c = new AbortController();
  const t = setTimeout(() => c.abort(), timeoutMs);
  try {
    const r = await fetch(url, { cache: 'no-store', signal: c.signal });
    if (!r.ok) return null;
    return await r.json();
  } catch {
    return null;
  } finally { clearTimeout(t); }
}

export async function GET(req: NextRequest) {
  try {
  const key = process.env.DEW_CACHE_BUILD_KEY || '';
  const provided = req.nextUrl.searchParams.get('key') || '';
  const isCron = req.headers.get('x-vercel-cron') === '1';
  if (!isCron && key && provided !== key) {
    return NextResponse.json({ error: 'unauthorized' }, { status: 401 });
  }

  const origin = req.nextUrl.origin;
  const now = new Date();
  const date = now.toISOString().slice(0, 10);

  const timelineUrl = `${origin}/api/timeline?hours=24&limit=1500&include_x_cache=1&x_cache_only=1&disable_x_api=1&x_cache_limit=1200`;
  const newsUrl = `${origin}/api/news`;
  const gmailUrl = `${origin}/api/gmail/feed`;
  const polyUrl = `${origin}/api/polymarket-debug?limit=150`;

  const [timeline, news, gmail, poly] = await Promise.all([
    fetchJson(timelineUrl),
    fetchJson(newsUrl),
    fetchJson(gmailUrl),
    fetchJson(polyUrl),
  ]);

  const headlines = [
    ...Object.values((news?.newsByCategory || {})).flat().map((x: any) => ({ title: x?.title || '', link: x?.link || '', source: 'news', ts: x?.pub || x?.ts || null })),
    ...Object.values((gmail?.newsByCategory || {})).flat().map((x: any) => ({ title: x?.title || '', link: x?.link || '', source: 'commentary', ts: x?.pub || x?.ts || null })),
  ].filter((x: any) => x.title && x.link).slice(0, 400);

  const timelineItems = Array.isArray(timeline?.items) ? timeline.items : [];
  const timelineCache = Array.isArray(timeline?.xCache) ? timeline.xCache : [];

  const signalCandidates = [
    ...headlines.map((x: any) => x.title),
    ...timelineItems.map((x: any) => x.title),
    ...timelineCache.map((x: any) => x.title),
  ].filter(Boolean).slice(0, 24);

  const adjudication = await libraryAdjudicateForTraderAgent({
    signals: signalCandidates,
    topK: 3,
    maxSignals: 6,
    maxCitationsPerSignal: 1,
    fetchChunkText: false,
  });

  const signals = (adjudication.reads || []).map((r: any) => ({
    id: `${date}:${(r.signal || '').slice(0, 40)}`,
    lens: r.citations?.[0]?.theorist || '',
    state: (r.citations && r.citations.length) ? 'confirmed_structural' : 'insufficient_evidence',
    confidence: (r.citations && r.citations.length) ? 0.62 : 0.2,
    summary: r.signal,
    citations: (r.citations || []).map((c: any) => (
      { theorist: c.theorist, title: c.title, doc_id: c.doc_id, page_start: c.page_start, page_end: c.page_end, chunk_id: c.chunk_id }
    ))
  }));

  const contradictionsDetected = signals.filter((s: any) => s.state !== 'confirmed_structural').length;
  const polySummary = poly?.summary || {};

  const daily = {
    date,
    generatedAt: now.toISOString(),
    inputs: {
      headlines,
      commentary: headlines.filter((x: any) => x.source === 'commentary'),
      timeline: timelineItems,
      timelineCache,
    },
    signals,
    markets: {
      candidatesTotal: Number(polySummary.retainedCount || 0),
      passAllTotal: Number(polySummary.passAllCount || 0),
      rejections: polySummary.filterCounts || {}
    },
    adjudicationMeta: {
      enabled: adjudication.enabled,
      errors: adjudication.errors || [],
      contradictionsDetected,
      xFetchAttempted: false,
      xFetchBlocked: true,
    }
  };

  const redis = getRedis();
  await redis.set(`cache:daily:${date}`, daily, { ex: 60 * 60 * 24 * 40 });
  await redis.set('cache:daily:latest', daily, { ex: 60 * 60 * 24 * 40 });
  await redis.set('cache:state', {
    lastDaily: date,
    lastBuiltAt: now.toISOString(),
  }, { ex: 60 * 60 * 24 * 365 });

  return NextResponse.json({
    ok: true,
    date,
    counts: {
      headlines: headlines.length,
      timeline: timelineItems.length,
      timelineCache: timelineCache.length,
      signals: signals.length,
      passAll: Number(polySummary.passAllCount || 0),
    }
  });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e?.message || String(e) }, { status: 500 });
  }
}


