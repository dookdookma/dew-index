import { NextRequest, NextResponse } from 'next/server';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
dayjs.extend(utc);

import { buildUniverse, fetchDailyBars, fetchSnapshots, type Bar, fetchLatest, type LatestMap } from '@/lib/alpaca';
import { SLEEVE_MAP } from 'data/mappings';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

type DewNews = { title: string; link: string; ts?: number; pub?: string };
type TimelineNews = { title: string; link: string; body?: string; ts?: number; pub?: string; source?: string };
type PolymarketIdea = {
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
const ALLOWED_CATS = new Set(['Tech','Business','Politics','International']);
const DEW_TIMELINE_LIMIT = Math.max(12, Math.min(240, Number(process.env.DEW_TIMELINE_LIMIT ?? '96')));
const DEW_X_CACHE_LIMIT = Math.max(24, Math.min(1200, Number(process.env.DEW_X_CACHE_LIMIT ?? '320')));
const DEW_POLYMARKET_LIMIT = Math.max(10, Math.min(240, Number(process.env.DEW_POLYMARKET_LIMIT ?? '150')));
const DEW_FETCH_TIMEOUT_NEWS_MS = Math.max(1500, Math.min(30000, Number(process.env.DEW_FETCH_TIMEOUT_NEWS_MS ?? '7000')));
const DEW_FETCH_TIMEOUT_GMAIL_MS = Math.max(1500, Math.min(30000, Number(process.env.DEW_FETCH_TIMEOUT_GMAIL_MS ?? '7000')));
const DEW_FETCH_TIMEOUT_TIMELINE_MS = Math.max(2000, Math.min(45000, Number(process.env.DEW_FETCH_TIMEOUT_TIMELINE_MS ?? '15000')));
const DEW_FETCH_TIMEOUT_POLYMARKET_MS = Math.max(2000, Math.min(30000, Number(process.env.DEW_FETCH_TIMEOUT_POLYMARKET_MS ?? '9000')));
const DEW_POLY_ASYM_MAX = Math.max(0.5, Math.min(0.99, Number(process.env.DEW_POLY_ASYM_MAX ?? '0.85')));
const DEW_POLY_HORIZON_DAYS = Math.max(1, Math.min(3650, Number(process.env.DEW_POLY_HORIZON_DAYS ?? '90')));
const DEW_POLY_SURGE_MIN = Math.max(0, Math.min(1, Number(process.env.DEW_POLY_SURGE_MIN ?? '0.10')));
const DEW_POLY_CROWD_PRICE = Math.max(0.5, Math.min(0.99, Number(process.env.DEW_POLY_CROWD_PRICE ?? '0.85')));
const DEW_POLY_CROWD_SURGE = Math.max(0, Math.min(1, Number(process.env.DEW_POLY_CROWD_SURGE ?? '0.25')));

function pickHeadlines(
  newsByCategory: Record<string, DewNews[] | undefined> | null | undefined
): { title: string; link: string }[] {
  if (!newsByCategory) return [];
  const flat: DewNews[] = [];
  for (const [cat, items] of Object.entries(newsByCategory)) {
    if (!ALLOWED_CATS.has(cat)) continue;
    if (!Array.isArray(items)) continue;
    for (const it of items) {
      if (it?.title && it?.link) flat.push(it);
    }
  }
  // newest first if ts exists; cap 12
  return flat.sort((a, b) => (b.ts ?? 0) - (a.ts ?? 0))
             .slice(0, 12)
             .map(({ title, link }) => ({ title, link }));
}

function pickTimeline(
  items: TimelineNews[] | null | undefined,
  maxItems: number
): { title: string; link: string; body?: string; source?: string }[] {
  if (!Array.isArray(items)) return [];
  return items
    .filter((it) => !!it?.title && !!it?.link)
    .sort((a, b) => (b?.ts ?? 0) - (a?.ts ?? 0))
    .slice(0, maxItems)
    .map(({ title, link, body, source }) => ({ title, link, body, source }));
}

function toNum(v: unknown): number | undefined {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

function daysTo(endDate?: string): number | null {
  if (!endDate) return null;
  const t = Date.parse(endDate);
  if (!Number.isFinite(t)) return null;
  return (t - Date.now()) / (1000 * 60 * 60 * 24);
}

function parseJsonArrayStrings(v: unknown): string[] {
  if (Array.isArray(v)) return v.map((x) => String(x));
  if (typeof v !== 'string') return [];
  try {
    const arr = JSON.parse(v);
    return Array.isArray(arr) ? arr.map((x) => String(x)) : [];
  } catch {
    return [];
  }
}

async function fetchJsonWithTimeout<T>(url: string, timeoutMs: number): Promise<T | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { cache: 'no-store', signal: controller.signal });
    if (!res.ok) return null;
    return await res.json() as T;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchPolymarketIdeas(limit: number, timeoutMs: number): Promise<PolymarketIdea[]> {
  const url = `https://gamma-api.polymarket.com/markets?limit=${Math.max(10, Math.min(500, limit * 3))}&active=true&closed=false`;
  const raw = await fetchJsonWithTimeout<unknown>(url, timeoutMs);
  if (!raw) return [];
  const arr = Array.isArray(raw) ? raw : [];
  const out: PolymarketIdea[] = [];
  for (const row of arr) {
    const r = row && typeof row === 'object' ? (row as Record<string, unknown>) : {};
    const question = typeof r.question === 'string' ? r.question.trim() : '';
    const slug = typeof r.slug === 'string' ? r.slug.trim() : '';
    if (!question || !slug) continue;
    const outcomes = parseJsonArrayStrings(r.outcomes).map((x) => x.toLowerCase());
    const prices = parseJsonArrayStrings(r.outcomePrices).map((x) => Number(x));
    const yesIdx = outcomes.indexOf('yes');
    const noIdx = outcomes.indexOf('no');
    const yesProb = yesIdx >= 0 && Number.isFinite(prices[yesIdx]) ? prices[yesIdx] : undefined;
    const noProb = noIdx >= 0 && Number.isFinite(prices[noIdx]) ? prices[noIdx] : undefined;
    out.push({
      question,
      slug,
      link: `https://polymarket.com/event/${slug}`,
      yesProb,
      noProb,
      volume24hr: toNum(r.volume24hr),
      volume: toNum(r.volumeNum ?? r.volume),
      liquidity: toNum(r.liquidityNum ?? r.liquidity),
      endDate: typeof r.endDate === 'string' ? r.endDate : undefined,
      category: typeof r.category === 'string' ? r.category : undefined,
    });
  }
  // Strict inclusion: only markets passing all four filters are eligible for DEW analysis
  const eligible = out.filter((m) => {
    const maxSide = Math.max(m.yesProb ?? 0, m.noProb ?? 0);
    const d = daysTo(m.endDate);
    const v24 = m.volume24hr ?? 0;
    const vTot = m.volume ?? 0;
    const surgePct = vTot > 0 ? v24 / vTot : 0;

    const asymmetricAlpha = maxSide <= DEW_POLY_ASYM_MAX;
    const capitalEfficiency = d !== null && d <= DEW_POLY_HORIZON_DAYS;
    const vpaSurge = surgePct >= DEW_POLY_SURGE_MIN;
    const girardMimeticTrap = !(maxSide >= DEW_POLY_CROWD_PRICE && surgePct >= DEW_POLY_CROWD_SURGE);

    return asymmetricAlpha && capitalEfficiency && vpaSurge && girardMimeticTrap;
  });

  return eligible
    .sort((a, b) => (b.volume24hr ?? b.volume ?? 0) - (a.volume24hr ?? a.volume ?? 0))
    .slice(0, limit);
}

type Period = '1D'|'1W'|'1M'|'1Q'|'1Y';

function windowFrom(period: Period) {
  const to = dayjs().utc().format('YYYY-MM-DD');
  let from = dayjs().utc();
  if (period === '1D') from = from.subtract(7, 'day'); // ensure ≥2 bars exist
  if (period === '1W') from = from.subtract(7, 'day');
  if (period === '1M') from = from.subtract(1, 'month');
  if (period === '1Q') from = from.subtract(3, 'month');
  if (period === '1Y') from = from.subtract(1, 'year');
  return { from: from.format('YYYY-MM-DD'), to };
}

function windowReturn(bars: Bar[], fromISO: string, toISO: string) {
  const f = dayjs(fromISO).utc();
  const T = dayjs(toISO).utc().endOf('day');
  const xs = (bars || []).filter(b => {
    const d = dayjs(b.t).utc();
    return (d.isSame(f,'day') || d.isAfter(f,'day')) && (d.isSame(T,'day') || d.isBefore(T,'day'));
  });
  if (xs.length < 2) return null;
  const a = xs[0].c, b = xs[xs.length - 1].c;
  return a > 0 ? b / a - 1 : null;
}

const bestISO = (...candidates: (string|undefined|null)[]) => {
  let best: string|undefined;
  let bestMs = -Infinity;
  for (const t of candidates) {
    const ms = Date.parse(String(t ?? ''));
    if (Number.isFinite(ms) && ms > bestMs) { bestMs = ms; best = t as string; }
  }
  return best ? new Date(best).toISOString() : undefined;
};


type Diagnostics = {
  requestedSymbols: number;
  withBars: number;
  withoutBars: string[];
  barsError: string | null;
  weightsSum: number;
  snapshots: boolean;
};

export async function GET(req: NextRequest) {
  const url = new URL(req.url);
  const period = (url.searchParams.get('period') as Period) || '1W';
  const fromQ = url.searchParams.get('from');
  const toQ = url.searchParams.get('to');
  const dew = url.searchParams.get('dew') === '1';
  const universeParam = (url.searchParams.get('universe') || 'auto') as Parameters<typeof buildUniverse>[1];

  const win = (fromQ && toQ) ? { from: fromQ, to: toQ } : windowFrom(period);

  // universe
  let symbols: string[] = [];
  let weights: Record<string, number> = {};
  let source = '';
  try {
    const uni = await buildUniverse(30, universeParam);
    symbols = uni.symbols.map(s => s.toUpperCase());
    weights = uni.weights;
    source = uni.source;
  } catch (e: unknown) {
    return NextResponse.json(
      { error: 'universe build failed', message: e instanceof Error ? e.message : String(e) },
      { status: 500 }
    );
  }

  const diagnostics: Diagnostics = {
    requestedSymbols: symbols.length,
    withBars: 0,
    withoutBars: [] as string[],
    barsError: null as string | null,
    weightsSum: Number(Object.values(weights).reduce((a, b) => a + (b || 0), 0).toFixed(6)),
    snapshots: true,
  };

  // bars for window (pad start; lib adds +1 day to end)
  const startISO = dayjs(win.from).utc().subtract(3, 'day').toISOString();
  const endISO = dayjs(win.to).utc().toISOString();

  let barsMap: Record<string, Bar[]> = {};
  try {
    barsMap = await fetchDailyBars(symbols, startISO, endISO);
  } catch (e: unknown) {
    diagnostics.barsError = e instanceof Error ? e.message : String(e);
  }

  // snapshots for Day
  let snaps: Record<string, {
    prevClose: number|null; minuteClose: number|null; dailyClose: number|null; lastPrice: number|null;
    prevTS?: string|null; minuteTS?: string|null; dailyTS?: string|null; tradeTS?: string|null;
  }> = {};
  try {
    snaps = await fetchSnapshots(symbols);
  } catch {
    diagnostics.snapshots = false;
  }

let latest: LatestMap = {};
  try { latest = await fetchLatest(symbols); } catch {}

  // prefer minute bar; else latest trade; else daily close; else last two daily bars
const dayReturn = (sym: string, bars: Bar[]) => {
  const s = snaps[sym];
  const ok = (x: number | null | undefined) => typeof x === 'number' && Number.isFinite(x) && x > 0;
  if (s && ok(s.prevClose)) {
    if (ok(s.minuteClose)) return s.minuteClose! / s.prevClose! - 1;
    if (ok(s.lastPrice))   return s.lastPrice!   / s.prevClose! - 1;   // ← allow after-hours
    if (ok(s.dailyClose))  return s.dailyClose!  / s.prevClose! - 1;
  }
  // fallback: last two bars
  const clean = (bars || []).filter(b => Number.isFinite(b.c));
  if (clean.length >= 2) {
    const a = clean[clean.length - 2].c, b = clean[clean.length - 1].c;
    if (a > 0) return b / a - 1;
  }
  return null;
};


  // rows
  const rows = symbols.map(sym => {
    const bars = barsMap[sym] || [];
    if (!bars.length) diagnostics.withoutBars.push(sym);

    const snap = snaps[sym];
    const lat  = latest[sym];
    const lastBarTS = bars.length ? bars[bars.length - 1].t : undefined;

    const asOf = bestISO(
      snap?.minuteTS,          // most precise during RTH
      snap?.tradeTS,           // snapshot trade
      lat?.tradeT,             // freshest trade
      lat?.quoteT,             // fallback if no trade
      snap?.dailyTS,           // end-of-day
      lastBarTS,               // last window bar
      snap?.prevTS,            // previous close
    );

    const r1 = dayReturn(sym, bars);
    const rW = windowReturn(bars, win.from, win.to);
    const w = weights[sym] ?? 0;
    const sleeve = SLEEVE_MAP[sym] ?? '-';
    return { symbol: sym, sleeve, w, r1, rW, asOf };
  });
  diagnostics.withBars = rows.length - diagnostics.withoutBars.length;

  // weight fallback
  if (diagnostics.weightsSum < 1e-9) {
    const per = rows.length ? 1 / rows.length : 0;
    rows.forEach(r => r.w = per);
    diagnostics.weightsSum = 1;
  }

  const aggAll = (key: 'r1' | 'rW') => {
    const valid = rows.filter(r => r[key] != null && r.w > 0);
    if (!valid.length) return 0;
    let s = 0, w = 0;
    for (const r of valid) { s += (r[key] as number) * r.w; w += r.w; }
    return w > 0 ? s / w : 0;
  };
  const aggSleeve = (key: 'r1' | 'rW', s: 'Core' | 'Satellite') => {
    const valid = rows.filter(r => r.sleeve === s && r[key] != null && r.w > 0);
    if (!valid.length) return 0;
    let num = 0, den = 0;
    for (const r of valid) { num += (r[key] as number) * r.w; den += r.w; }
    return den > 0 ? num / den : 0;
  };

  const snapshot = {
    window: win,
    rows,
    index: { r1: aggAll('r1'), rW: aggAll('rW') },
    sleeves: {
      Core: { r1: aggSleeve('r1', 'Core'), rW: aggSleeve('rW', 'Core') },
      Satellite: { r1: aggSleeve('r1', 'Satellite'), rW: aggSleeve('rW', 'Satellite') },
    },
  };

// --- gather server-side headlines for DEW Line ---
  let dewNews: { title: string; link: string }[] = [];
  let dewTimeline: { title: string; link: string; body?: string; source?: string }[] = [];
  let dewXCache: { title: string; link: string; body?: string; source?: string }[] = [];
  let dewPolymarket: PolymarketIdea[] = [];
  const ingestStatus = {
    news: false,
    gmail: false,
    timeline: false,
    polymarket: false,
  };
  try {
    const nUrl = new URL('/api/news', req.url);
    const gUrl = new URL('/api/gmail/feed', req.url);
    const tUrl = new URL('/api/timeline', req.url);
    tUrl.searchParams.set('hours', '24');
    tUrl.searchParams.set('limit', '1000');
    tUrl.searchParams.set('x_max', '500');
    tUrl.searchParams.set('include_x_cache', '1');
    tUrl.searchParams.set('x_cache_limit', String(Math.max(400, DEW_X_CACHE_LIMIT)));

    const [nRes, gRes, tRes, pRes] = await Promise.allSettled([
      fetchJsonWithTimeout<{ newsByCategory?: Record<string, DewNews[]> }>(nUrl.toString(), DEW_FETCH_TIMEOUT_NEWS_MS),
      fetchJsonWithTimeout<{ newsByCategory?: Record<string, DewNews[]> }>(gUrl.toString(), DEW_FETCH_TIMEOUT_GMAIL_MS),
      fetchJsonWithTimeout<{ items?: TimelineNews[]; xCache?: TimelineNews[] }>(tUrl.toString(), DEW_FETCH_TIMEOUT_TIMELINE_MS),
      fetchPolymarketIdeas(DEW_POLYMARKET_LIMIT, DEW_FETCH_TIMEOUT_POLYMARKET_MS),
    ]);

    const nb = (nRes.status === 'fulfilled' && nRes.value?.newsByCategory)
      ? (nRes.value.newsByCategory as Record<string, DewNews[]>) : {};
    const gb = (gRes.status === 'fulfilled' && gRes.value?.newsByCategory)
      ? (gRes.value.newsByCategory as Record<string, DewNews[]>) : {};
    ingestStatus.news = Object.keys(nb).length > 0;
    ingestStatus.gmail = Object.keys(gb).length > 0;

    // merge only allowed categories
    const merged: Record<string, DewNews[]> = {};
    for (const src of [nb, gb]) {
      for (const [k, arr] of Object.entries(src)) {
        if (!ALLOWED_CATS.has(k)) continue;
        (merged[k] ||= []).push(...(Array.isArray(arr) ? arr : []));
      }
    }
    dewNews = pickHeadlines(merged);
    const tItems = (tRes.status === 'fulfilled' && Array.isArray(tRes.value?.items))
      ? (tRes.value.items as TimelineNews[])
      : [];
    const tCache = (tRes.status === 'fulfilled' && Array.isArray(tRes.value?.xCache))
      ? (tRes.value.xCache as TimelineNews[])
      : [];
    ingestStatus.timeline = tItems.length > 0 || tCache.length > 0;
    dewPolymarket = (pRes.status === 'fulfilled' && Array.isArray(pRes.value))
      ? (pRes.value as PolymarketIdea[])
      : [];
    ingestStatus.polymarket = dewPolymarket.length > 0;
    dewTimeline = pickTimeline(tItems, DEW_TIMELINE_LIMIT);
    dewXCache = pickTimeline(tCache, DEW_X_CACHE_LIMIT);
  } catch {
    dewNews = [];
    dewTimeline = [];
    dewXCache = [];
    dewPolymarket = [];
  }


  // optional DEW text
  let dewText = '';
  if (dew) {
    try {
      const key = (process.env.OPENAI_API_KEY || '').trim();
      if (!key) dewText = 'DEW Line disabled: set OPENAI_API_KEY.';
      else {
        const { dewLine } = await import('@/lib/dewline');
        dewText = await dewLine({
          snapshot,
          news: dewNews,
          timeline: dewTimeline,
          timelineCache: dewXCache,
          polymarket: dewPolymarket,
        });
      }
    } catch (e: unknown) {
      dewText = `Error: ${e instanceof Error ? e.message : String(e)}`;
    }
  }

  return NextResponse.json({
    source,
    diagnostics,
    snapshot,
    newsByCategory: {},
    dew: dewText,
    ingest: {
      status: ingestStatus,
      counts: {
        headlines: dewNews.length,
        timeline: dewTimeline.length,
        timelineCache: dewXCache.length,
        polymarket: dewPolymarket.length,
      },
    },
  });
}

