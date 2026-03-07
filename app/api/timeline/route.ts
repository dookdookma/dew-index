import { NextResponse } from 'next/server';
import fs from 'node:fs/promises';
import path from 'node:path';
import { load } from 'cheerio';
import sanitizeHtml from 'sanitize-html';
import { getRedis } from '@/lib/kv';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

type TimelineConfig = {
  feed?: { max_items?: number };
  reddit?: { feeds?: string[] };
  nitter?: { feeds?: string[]; accounts?: string[] };
  x?: { accounts?: string[] };
};

type TimelineItem = {
  title: string;
  link: string;
  body: string;
  bodyHtml?: string;
  ts: number;
  pub?: string;
  source: string;
};
type XUsageStatus = 'good' | 'warn' | 'bad' | 'exhausted';
type XUsageMeta = {
  projectCap: number;
  projectUsage: number;
  creditsLeft: number;
  creditsLeftRatio: number;
  capResetDay?: number;
  status: XUsageStatus;
};
type XBillingMeta = {
  startUsd: number;
  remainingUsd: number;
  remainingRatio: number;
  status: XUsageStatus;
  asOfTs: number;
};
type XBillingDebug = {
  enabledRaw: string;
  enabledParsed: boolean;
  startUsdRaw: string;
  startUsdParsed: number | null;
  remainingUsdRaw: string;
  remainingUsdParsed: number | null;
  baselineRemainingUsdRaw: string;
  baselineRemainingUsdParsed: number | null;
  usdPerPostRaw: string;
  usdPerPostParsed: number | null;
  resetDayRaw: string;
  resetDayParsed: number | null;
  capResetDayFromUsage?: number;
  resetApplied?: boolean;
  reasons: string[];
};
type ProviderKind = 'source' | 'xcancel' | 'nitter' | 'rsshub' | 'xapi';
type Candidate = { url: string; provider: ProviderKind };

const SOURCES_JSON = path.join(process.cwd(), 'Timeline', 'sources.json');
const USER_AGENT =
  'Mozilla/5.0 (compatible; DewIndexTimeline/1.0; +https://dew-index.vercel.app)';
const DEFAULT_NITTER_MIRRORS = [
  'nitter.poast.org',
  'nitter.tiekoetter.com',
  'nitter.catsarch.com',
  'nitter.space',
];
const DEFAULT_XCANCEL_HOSTS = ['rss.xcancel.com'];
const FETCH_TIMEOUT_MS = 10000;
const X_API_BASE = 'https://api.x.com/2';
const X_API_MAX_PAGES = Math.max(1, Math.min(10, Number(process.env.TIMELINE_X_API_MAX_PAGES ?? '3')));
const X_API_CACHE_TTL_HOURS = Math.max(24, Math.min(24 * 14, Number(process.env.TIMELINE_X_API_CACHE_TTL_HOURS ?? '48')));
const X_API_MIN_POLL_SECONDS = Math.max(0, Math.min(3600, Number(process.env.TIMELINE_X_API_MIN_POLL_SECONDS ?? '120')));
const X_STATUS_BASE = (process.env.TIMELINE_X_STATUS_BASE || 'https://nitter.poast.org').trim();
const X_API_EXCLUDE_REPLIES = process.env.TIMELINE_X_EXCLUDE_REPLIES !== '0';
const X_BILLING_ESTIMATE_ENABLED = /^(1|true|yes|on)$/i.test((process.env.X_BILLING_ESTIMATE_ENABLED || '').trim());
const X_BILLING_USD_PER_POST = Math.max(0, Number(String(process.env.X_BILLING_USD_PER_POST ?? '0.005').replace(/[$,\s]/g, '')));
const X_BILLING_BASELINE_REMAINING_USD = Number(String(process.env.X_BILLING_BASELINE_REMAINING_USD ?? '').replace(/[$,\s]/g, ''));

function asText(html: string): string {
  if (!html) return '';
  const $ = load(`<div>${html}</div>`);
  const text = $('div').text().replace(/\s+/g, ' ').trim();
  return text;
}

function sanitizeTimelineHtml(html: string): string {
  return sanitizeHtml(html || '', {
    allowedTags: [
      'p', 'br', 'strong', 'b', 'em', 'i', 'u', 'a', 'ul', 'ol', 'li', 'blockquote', 'code', 'pre', 'span',
    ],
    allowedAttributes: {
      a: ['href', 'target', 'rel'],
      span: ['class'],
    },
    allowedSchemes: ['http', 'https', 'mailto'],
    transformTags: {
      a: sanitizeHtml.simpleTransform('a', { rel: 'noreferrer nofollow', target: '_blank' }),
    },
  }).trim();
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function textToTimelineHtml(text: string): string {
  const escaped = escapeHtml(text || '');
  const linked = escaped.replace(
    /(https?:\/\/[^\s<]+)/g,
    '<a href="$1" target="_blank" rel="noreferrer nofollow">$1</a>'
  );
  return sanitizeTimelineHtml(linked.replace(/\r\n|\r|\n/g, '<br/>'));
}

function normalizeFeedBodyHtml(raw: string, sourceUrl: string): string {
  const sourceHost = (() => {
    try {
      return new URL(sourceUrl).hostname.toLowerCase();
    } catch {
      return '';
    }
  })();
  let candidate = raw || '';
  if (!candidate.trim()) return '';

  // Some feeds (notably Reddit) HTML-encode their description payload.
  if (candidate.includes('&lt;') || candidate.includes('&#')) {
    const $dec = load(`<div>${candidate}</div>`);
    candidate = $dec('div').text();
  }

  if (sourceHost.includes('reddit.com')) {
    const $r = load(candidate);
    const md = $r('.md').first();
    if (md.length) {
      candidate = md.html() || md.text() || '';
    }
    // Remove common Reddit RSS footer wrapper.
    candidate = candidate
      .replace(/&#32;/gi, ' ')
      .replace(/\s+/g, ' ')
      .replace(/<br\s*\/?>/gi, '<br/>')
      .replace(/submitted by[\s\S]*$/i, '')
      .trim();
    // If no rich body survived and it's just wrapper fragments, drop it.
    if (/^(&#32;|\s|<br\s*\/?>|<span[^>]*>[\s\S]*<\/span>|<a[^>]*>[\s\S]*<\/a>)*$/i.test(candidate)) {
      candidate = '';
    }
    candidate = candidate
      .replace(/<!--\s*SC_OFF\s*-->/gi, '')
      .replace(/<!--\s*SC_ON\s*-->/gi, '')
      .trim();
  }

  return sanitizeTimelineHtml(candidate);
}

function sourceLabelFromUrl(sourceUrl: string): string {
  try {
    const u = new URL(sourceUrl);
    const parts = u.pathname.split('/').filter(Boolean);
    if (u.hostname.includes('reddit.com')) {
      if (parts[0] === 'r' && parts[1]) return `r/${parts[1]}`;
    }
    if (u.hostname.includes('nitter') && parts[0]) return `@${parts[0]}`;
    return u.hostname;
  } catch {
    return 'feed';
  }
}

function nitterUserFromUrl(sourceUrl: string): string | null {
  try {
    const u = new URL(sourceUrl);
    if (!u.hostname.includes('nitter')) return null;
    const user = u.pathname.split('/').filter(Boolean)[0] || '';
    return user || null;
  } catch {
    return null;
  }
}

function xUserFromSource(source: string): string | null {
  const trimmed = source.trim();
  if (!trimmed) return null;
  if (trimmed.startsWith('@')) return trimmed.slice(1).trim() || null;
  if (/^[A-Za-z0-9_]{1,15}$/.test(trimmed)) return trimmed;
  try {
    const u = new URL(trimmed);
    const host = u.hostname.toLowerCase();
    if (
      host.includes('nitter') ||
      host.includes('twitter.com') ||
      host === 'x.com' ||
      host.endsWith('.x.com') ||
      host.includes('xcancel.com')
    ) {
      const first = u.pathname.split('/').filter(Boolean)[0] || '';
      return first || null;
    }
  } catch {}
  return null;
}

function nitterCandidates(sourceUrl: string): Candidate[] {
  const user = nitterUserFromUrl(sourceUrl) || xUserFromSource(sourceUrl);
  if (!user) return [{ url: sourceUrl, provider: 'source' }];
  const customMirrors = (process.env.TIMELINE_NITTER_HOSTS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const customXcancel = (process.env.TIMELINE_XCANCEL_HOSTS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const customRsshub = (process.env.TIMELINE_RSSHUB_HOSTS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const orderRaw = (process.env.TIMELINE_PROVIDER_ORDER || 'xcancel,nitter,rsshub,source')
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const order = orderRaw.filter(
    (p): p is ProviderKind => p === 'source' || p === 'xcancel' || p === 'nitter' || p === 'rsshub' || p === 'xapi'
  );
  const mirrors = [...customMirrors, ...DEFAULT_NITTER_MIRRORS];
  const xcancelHosts = [...customXcancel, ...DEFAULT_XCANCEL_HOSTS];
  const byProvider: Record<ProviderKind, Candidate[]> = {
    xapi: [],
    source: [{ url: sourceUrl, provider: 'source' }],
    xcancel: xcancelHosts.map((host) => ({ url: `https://${host}/${user}/rss`, provider: 'xcancel' })),
    nitter: mirrors.map((host) => ({ url: `https://${host}/${user}/rss`, provider: 'nitter' })),
    rsshub: customRsshub.flatMap((host) => [
      { url: `https://${host}/twitter/user/${user}`, provider: 'rsshub' as const },
      { url: `https://${host}/x/user/${user}`, provider: 'rsshub' as const },
    ]),
  };
  const orderedProviders = order.length ? order : (['xapi', 'xcancel', 'nitter', 'rsshub', 'source'] as ProviderKind[]);
  const out = orderedProviders.flatMap((p) => byProvider[p] || []);
  const seen = new Set<string>();
  const deduped: Candidate[] = [];
  for (const c of out) {
    if (seen.has(c.url)) continue;
    seen.add(c.url);
    deduped.push(c);
  }
  return deduped;
}

function accountFromSource(sourceUrl: string): string | null {
  return nitterUserFromUrl(sourceUrl) || xUserFromSource(sourceUrl);
}

async function getPreferredProvider(account: string): Promise<ProviderKind | null> {
  try {
    const redis = getRedis();
    const v = await redis.get<string>(`timeline:provider:${account.toLowerCase()}`);
    if (v === 'source' || v === 'xcancel' || v === 'nitter' || v === 'rsshub' || v === 'xapi') return v;
    return null;
  } catch {
    return null;
  }
}

async function setPreferredProvider(account: string, provider: ProviderKind): Promise<void> {
  try {
    const redis = getRedis();
    await redis.set(`timeline:provider:${account.toLowerCase()}`, provider, { ex: 60 * 60 * 24 * 14 });
  } catch {
    // KV is optional for timeline ingestion.
  }
}

async function fetchFeedText(url: string): Promise<string> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      headers: {
        'User-Agent': USER_AGENT,
        Accept:
          'application/atom+xml, application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8',
      },
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.text();
  } finally {
    clearTimeout(t);
  }
}

function prioritizeCandidates(candidates: Candidate[], preferred: ProviderKind | null): Candidate[] {
  if (!preferred) return candidates;
  const preferredItems = candidates.filter((c) => c.provider === preferred);
  const rest = candidates.filter((c) => c.provider !== preferred);
  return [...preferredItems, ...rest];
}

function summarizeFailures(failures: string[]): string {
  if (failures.length <= 5) return failures.join(' | ');
  return [...failures.slice(0, 5), `... +${failures.length - 5} more`].join(' | ');
}

function maxSnowflake(a: string, b: string): string {
  try {
    return BigInt(a) >= BigInt(b) ? a : b;
  } catch {
    if (a.length !== b.length) return a.length > b.length ? a : b;
    return a >= b ? a : b;
  }
}

function xModeKey(): 'posts' | 'all' {
  return X_API_EXCLUDE_REPLIES ? 'posts' : 'all';
}

function getXBearerToken(): string | null {
  const raw = (process.env.X_BEARER_TOKEN || '').trim();
  if (!raw) return null;
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

async function getSinceId(account: string): Promise<string | null> {
  try {
    const redis = getRedis();
    const v = await redis.get<string>(`timeline:xapi:since:v2:${xModeKey()}:${account.toLowerCase()}`);
    return typeof v === 'string' && v.trim() ? v.trim() : null;
  } catch {
    return null;
  }
}

async function setSinceId(account: string, sinceId: string): Promise<void> {
  try {
    const redis = getRedis();
    await redis.set(`timeline:xapi:since:v2:${xModeKey()}:${account.toLowerCase()}`, sinceId, { ex: 60 * 60 * 24 * 45 });
  } catch {
    // Optional cache only.
  }
}

async function getLastPolledMs(account: string): Promise<number | null> {
  try {
    const redis = getRedis();
    const v = await redis.get<number | string>(`timeline:xapi:lastpoll:${account.toLowerCase()}`);
    const n = typeof v === 'number' ? v : Number(v);
    return Number.isFinite(n) && n > 0 ? n : null;
  } catch {
    return null;
  }
}

async function setLastPolledMs(account: string, tsMs: number): Promise<void> {
  try {
    const redis = getRedis();
    await redis.set(`timeline:xapi:lastpoll:${account.toLowerCase()}`, tsMs, {
      ex: Math.max(60, X_API_MIN_POLL_SECONDS * 4),
    });
  } catch {
    // Optional cache only.
  }
}

function cacheKeyXApiItems(account: string): string {
  const mode = X_API_EXCLUDE_REPLIES ? 'posts' : 'all';
  return `timeline:xapi:items:v2:${mode}:${account.toLowerCase()}`;
}

function normalizeCachedItem(v: unknown): TimelineItem | null {
  const rec = (v && typeof v === 'object') ? (v as Record<string, unknown>) : null;
  if (!rec) return null;
  const title = typeof rec.title === 'string' ? rec.title : '';
  const link = typeof rec.link === 'string' ? rec.link : '';
  const body = typeof rec.body === 'string' ? rec.body : '';
  const bodyHtml = typeof rec.bodyHtml === 'string' ? rec.bodyHtml : undefined;
  const ts = typeof rec.ts === 'number' ? rec.ts : Number(rec.ts);
  const pub = typeof rec.pub === 'string' ? rec.pub : undefined;
  const source = typeof rec.source === 'string' ? rec.source : '';
  if (!title || !link || !body || !source || !Number.isFinite(ts) || ts <= 0) return null;
  return { title, link, body, bodyHtml, ts, pub, source };
}

function parseTweetIdFromLink(link: string): string | null {
  const m = link.match(/\/status\/(\d+)/);
  return m?.[1] || null;
}

function buildXStatusLink(account: string, tweetId: string): string {
  const base = X_STATUS_BASE.replace(/\/+$/, '');
  return `${base}/${account}/status/${tweetId}`;
}

function maxTweetIdInItems(items: TimelineItem[]): string | null {
  let out: string | null = null;
  for (const item of items) {
    const id = parseTweetIdFromLink(item.link);
    if (!id) continue;
    out = out ? maxSnowflake(out, id) : id;
  }
  return out;
}

function dedupeTimelineItems(items: TimelineItem[]): TimelineItem[] {
  const byKey = new Map<string, TimelineItem>();
  for (const item of items) {
    const key = item.link || `${item.title}-${item.ts}`;
    const prev = byKey.get(key);
    if (!prev || item.ts > prev.ts) byKey.set(key, item);
  }
  return [...byKey.values()];
}

async function getCachedXApiItems(account: string): Promise<TimelineItem[]> {
  try {
    const redis = getRedis();
    const raw = await redis.get<unknown>(cacheKeyXApiItems(account));
    const arr = Array.isArray(raw) ? raw : [];
    const out: TimelineItem[] = [];
    for (const row of arr) {
      const item = normalizeCachedItem(row);
      if (item) out.push(item);
    }
    return out;
  } catch {
    return [];
  }
}

async function setCachedXApiItems(account: string, items: TimelineItem[]): Promise<void> {
  try {
    const redis = getRedis();
    await redis.set(cacheKeyXApiItems(account), items, { ex: 60 * 60 * X_API_CACHE_TTL_HOURS });
  } catch {
    // Optional cache only.
  }
}

async function fetchXApiJson(url: string, bearer: string): Promise<unknown> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${bearer}` },
      cache: 'no-store',
      signal: controller.signal,
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status}${txt ? `: ${txt.slice(0, 180)}` : ''}`);
    }
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

function buildXApiSearchQuery(account: string): string {
  return X_API_EXCLUDE_REPLIES ? `from:${account} -is:reply` : `from:${account}`;
}

type XApiFetchResult = {
  items: TimelineItem[];
  fetchedFromApiCount: number;
};

async function fetchXApiPostsWithMetrics(account: string, cutoffTs: number, bearer: string): Promise<XApiFetchResult> {
  const now = Date.now();
  const cacheFloorTs = now - (X_API_CACHE_TTL_HOURS * 60 * 60 * 1000);
  const cachedAll = (await getCachedXApiItems(account)).filter((x) => x.ts >= cacheFloorTs);
  const cachedWindow = cachedAll.filter((x) => x.ts >= cutoffTs);
  const cachedSinceId = maxTweetIdInItems(cachedAll);
  const storedSinceId = await getSinceId(account);
  const sinceId =
    cachedSinceId && storedSinceId
      ? maxSnowflake(cachedSinceId, storedSinceId)
      : (cachedSinceId || storedSinceId);
  const lastPolledMs = await getLastPolledMs(account);
  const pollCooldownMs = X_API_MIN_POLL_SECONDS * 1000;
  const withinCooldown = !!lastPolledMs && pollCooldownMs > 0 && (now - lastPolledMs) < pollCooldownMs;
  if (withinCooldown && cachedWindow.length) return { items: cachedWindow, fetchedFromApiCount: 0 };

  const startTime = new Date(cutoffTs).toISOString();
  const fresh: TimelineItem[] = [];
  let nextToken: string | undefined;
  let newestId: string | null = null;

  try {
    for (let page = 0; page < X_API_MAX_PAGES; page += 1) {
      const u = new URL(`${X_API_BASE}/tweets/search/recent`);
      u.searchParams.set('query', buildXApiSearchQuery(account));
      u.searchParams.set('max_results', '100');
      u.searchParams.set('tweet.fields', 'created_at');
      if (sinceId) u.searchParams.set('since_id', sinceId);
      else u.searchParams.set('start_time', startTime);
      if (nextToken) u.searchParams.set('next_token', nextToken);

      const jRaw = await fetchXApiJson(u.toString(), bearer);
      const j = (jRaw && typeof jRaw === 'object') ? (jRaw as Record<string, unknown>) : {};
      const data = Array.isArray(j.data) ? j.data : [];
      for (const row of data) {
        const rec = row && typeof row === 'object' ? (row as Record<string, unknown>) : {};
        const id = typeof rec.id === 'string' ? rec.id : '';
        const text = typeof rec.text === 'string' ? rec.text : '';
        const createdAt = typeof rec.created_at === 'string' ? rec.created_at : '';
        const ts = Date.parse(createdAt);
        if (!id || !text || !Number.isFinite(ts)) continue;
        newestId = newestId ? maxSnowflake(newestId, id) : id;
        fresh.push({
          title: text.slice(0, 160),
          link: buildXStatusLink(account, id),
          body: text,
          bodyHtml: textToTimelineHtml(text),
          ts,
          pub: createdAt,
          source: `@${account}`,
        });
      }
      const meta = (j.meta && typeof j.meta === 'object') ? (j.meta as Record<string, unknown>) : {};
      const nt = typeof meta.next_token === 'string' ? meta.next_token : '';
      if (!nt) break;
      nextToken = nt;
    }
  } catch {
    if (cachedWindow.length) return { items: cachedWindow, fetchedFromApiCount: 0 };
    throw new Error(`xapi ${account} unavailable`);
  }

  const merged = dedupeTimelineItems([...fresh, ...cachedAll])
    .filter((x) => x.ts >= cacheFloorTs)
    .sort((a, b) => b.ts - a.ts)
    .slice(0, 5000);

  const mergedSinceId = maxTweetIdInItems(merged);
  const nextSinceId =
    newestId && mergedSinceId ? maxSnowflake(newestId, mergedSinceId)
    : (newestId || mergedSinceId || sinceId);

  await Promise.all([
    setCachedXApiItems(account, merged),
    setLastPolledMs(account, now),
    nextSinceId ? setSinceId(account, nextSinceId) : Promise.resolve(),
  ]);

  return {
    items: merged.filter((x) => x.ts >= cutoffTs),
    fetchedFromApiCount: fresh.length,
  };
}

function toFiniteNumber(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const cleaned = v.replace(/[$,\s]/g, '');
    if (!cleaned) return null;
    const n = Number(cleaned);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function deriveXUsageStatus(creditsLeftRatio: number, creditsLeft: number): XUsageStatus {
  if (!Number.isFinite(creditsLeft) || creditsLeft <= 0) return 'exhausted';
  if (creditsLeftRatio > 2 / 3) return 'good';
  if (creditsLeftRatio > 1 / 3) return 'warn';
  return 'bad';
}

function getBillingMetaFromEnv(): XBillingMeta | null {
  const startRaw = toFiniteNumber(process.env.X_BILLING_START_USD);
  const remainingRaw = toFiniteNumber(process.env.X_BILLING_REMAINING_USD);
  if (startRaw == null || remainingRaw == null || startRaw <= 0) return null;
  const remainingUsd = Math.max(0, remainingRaw);
  const remainingRatio = remainingUsd / startRaw;
  return {
    startUsd: startRaw,
    remainingUsd,
    remainingRatio,
    status: deriveXUsageStatus(remainingRatio, remainingUsd),
    asOfTs: Date.now(),
  };
}

function getBillingDebug(capResetDayFromUsage?: number, resetApplied?: boolean): XBillingDebug {
  const enabledRaw = String(process.env.X_BILLING_ESTIMATE_ENABLED ?? '');
  const startUsdRaw = String(process.env.X_BILLING_START_USD ?? '');
  const remainingUsdRaw = String(process.env.X_BILLING_REMAINING_USD ?? '');
  const baselineRemainingUsdRaw = String(process.env.X_BILLING_BASELINE_REMAINING_USD ?? '');
  const usdPerPostRaw = String(process.env.X_BILLING_USD_PER_POST ?? '');
  const resetDayRaw = String(process.env.X_BILLING_RESET_DAY ?? '');

  const enabledParsed = /^(1|true|yes|on)$/i.test(enabledRaw.trim());
  const startUsdParsed = toFiniteNumber(startUsdRaw);
  const remainingUsdParsed = toFiniteNumber(remainingUsdRaw);
  const baselineRemainingUsdParsed = toFiniteNumber(baselineRemainingUsdRaw);
  const usdPerPostParsed = toFiniteNumber(usdPerPostRaw || '0.005');
  const resetDayParsed = toFiniteNumber(resetDayRaw);

  const reasons: string[] = [];
  if (!enabledParsed) reasons.push('X_BILLING_ESTIMATE_ENABLED not truthy');
  if (startUsdParsed == null || startUsdParsed <= 0) reasons.push('X_BILLING_START_USD invalid');
  if (usdPerPostParsed == null || usdPerPostParsed <= 0) reasons.push('X_BILLING_USD_PER_POST invalid');
  if (remainingUsdRaw.trim()) reasons.push('X_BILLING_REMAINING_USD set (manual override active)');

  return {
    enabledRaw,
    enabledParsed,
    startUsdRaw,
    startUsdParsed,
    remainingUsdRaw,
    remainingUsdParsed,
    baselineRemainingUsdRaw,
    baselineRemainingUsdParsed,
    usdPerPostRaw,
    usdPerPostParsed,
    resetDayRaw,
    resetDayParsed,
    capResetDayFromUsage,
    resetApplied,
    reasons,
  };
}

function billingCycleId(nowMs: number, resetDay: number): string {
  const d = new Date(nowMs);
  let y = d.getUTCFullYear();
  let m = d.getUTCMonth() + 1;
  if (d.getUTCDate() < resetDay) {
    m -= 1;
    if (m === 0) {
      m = 12;
      y -= 1;
    }
  }
  return `${y}-${String(m).padStart(2, '0')}-d${resetDay}`;
}

function billingResetDay(capResetDay?: number): number {
  const resetEnv = toFiniteNumber(process.env.X_BILLING_RESET_DAY);
  const resetRaw = resetEnv ?? capResetDay ?? 1;
  return Math.max(1, Math.min(31, Math.round(resetRaw)));
}

async function resetEstimatedBillingCounter(capResetDay?: number): Promise<boolean> {
  try {
    const resetDay = billingResetDay(capResetDay);
    const cycle = billingCycleId(Date.now(), resetDay);
    const key = `timeline:xbilling:consumed_mills:${cycle}`;
    const redis = getRedis();
    await redis.del(key);
    return true;
  } catch {
    return false;
  }
}

async function getEstimatedBillingMeta(accessedPostCount: number, capResetDay?: number): Promise<XBillingMeta | null> {
  if (!X_BILLING_ESTIMATE_ENABLED) return null;
  const startUsd = toFiniteNumber(process.env.X_BILLING_START_USD);
  if (startUsd == null || startUsd <= 0) return null;
  if (!Number.isFinite(X_BILLING_USD_PER_POST) || X_BILLING_USD_PER_POST <= 0) return null;

  const resetDay = billingResetDay(capResetDay);
  const now = Date.now();
  const cycle = billingCycleId(now, resetDay);
  const key = `timeline:xbilling:consumed_mills:${cycle}`;
  const deltaMills = Math.max(0, Math.round(accessedPostCount * X_BILLING_USD_PER_POST * 1000));
  const startMills = Math.round(startUsd * 1000);
  const baselineRemaining = Number.isFinite(X_BILLING_BASELINE_REMAINING_USD)
    ? Math.max(0, Math.min(startUsd, X_BILLING_BASELINE_REMAINING_USD))
    : startUsd;
  const baselineConsumedMills = Math.max(0, startMills - Math.round(baselineRemaining * 1000));

  try {
    const redis = getRedis();
    const raw = await redis.get<number | string | null>(key);
    const hasStored = raw !== null && raw !== undefined && raw !== '';
    let consumedMills: number;
    if (hasStored) {
      const n = typeof raw === 'number' ? raw : Number(raw);
      consumedMills = Number.isFinite(n) && n > 0 ? n : 0;
    } else {
      consumedMills = baselineConsumedMills;
      await redis.set(key, consumedMills, { ex: 60 * 60 * 24 * 90 });
    }
    if (deltaMills > 0) {
      consumedMills = await redis.incrby(key, deltaMills);
      await redis.expire(key, 60 * 60 * 24 * 90);
    }
    const remainingMills = Math.max(0, startMills - consumedMills);
    const remainingUsd = remainingMills / 1000;
    const remainingRatio = startMills > 0 ? (remainingMills / startMills) : 0;
    return {
      startUsd,
      remainingUsd,
      remainingRatio,
      status: deriveXUsageStatus(remainingRatio, remainingUsd),
      asOfTs: now,
    };
  } catch {
    // Return a non-null estimate even if KV is unavailable.
    const consumedMills = baselineConsumedMills + deltaMills;
    const remainingMills = Math.max(0, startMills - consumedMills);
    const remainingUsd = remainingMills / 1000;
    const remainingRatio = startMills > 0 ? (remainingMills / startMills) : 0;
    return {
      startUsd,
      remainingUsd,
      remainingRatio,
      status: deriveXUsageStatus(remainingRatio, remainingUsd),
      asOfTs: Date.now(),
    };
  }
}

async function fetchXApiUsage(bearer: string): Promise<XUsageMeta | null> {
  const url = `${X_API_BASE}/usage/tweets`;
  const jRaw = await fetchXApiJson(url, bearer);
  const j = (jRaw && typeof jRaw === 'object') ? (jRaw as Record<string, unknown>) : {};
  const data = (j.data && typeof j.data === 'object') ? (j.data as Record<string, unknown>) : {};

  const projectCap = toFiniteNumber(data.project_cap);
  const projectUsage = toFiniteNumber(data.project_usage);
  if (projectCap == null || projectUsage == null || projectCap <= 0) return null;

  const creditsLeft = Math.max(0, projectCap - projectUsage);
  const creditsLeftRatio = creditsLeft / projectCap;
  const capResetDay = toFiniteNumber(data.cap_reset_day) ?? undefined;
  return {
    projectCap,
    projectUsage,
    creditsLeft,
    creditsLeftRatio,
    capResetDay,
    status: deriveXUsageStatus(creditsLeftRatio, creditsLeft),
  };
}

function readRssItems(xml: string, sourceUrl: string): TimelineItem[] {
  const $ = load(xml, { xmlMode: true });
  const source = sourceLabelFromUrl(sourceUrl);
  const isRedditSource = source.startsWith('r/');
  const out: TimelineItem[] = [];
  $('item').each((_, el) => {
    const title = $(el).find('title').first().text().trim();
    const link = $(el).find('link').first().text().trim();
    const pubRaw =
      $(el).find('pubDate').first().text().trim() ||
      $(el).find('dc\\:date').first().text().trim();
    const bodyRaw =
      $(el).find('description').first().text().trim() ||
      $(el).find('content\\:encoded').first().text().trim();
    if (!title || !link) return;
    const ts = Date.parse(pubRaw || '') || Date.now();
    const sanitizedBodyHtml = normalizeFeedBodyHtml(bodyRaw, sourceUrl);
    const bodyText = sanitizedBodyHtml
      ? asText(sanitizedBodyHtml)
      : (isRedditSource ? '' : asText(bodyRaw));
    out.push({
      title,
      link,
      body: bodyText,
      bodyHtml: sanitizedBodyHtml || undefined,
      ts,
      pub: pubRaw || undefined,
      source,
    });
  });
  return out;
}

function readAtomItems(xml: string, sourceUrl: string): TimelineItem[] {
  const $ = load(xml, { xmlMode: true });
  const source = sourceLabelFromUrl(sourceUrl);
  const isRedditSource = source.startsWith('r/');
  const out: TimelineItem[] = [];
  $('entry').each((_, el) => {
    const title = $(el).find('title').first().text().trim();
    const link =
      $(el).find('link[rel="alternate"]').attr('href') ||
      $(el).find('link').first().attr('href') ||
      '';
    const pubRaw =
      $(el).find('published').first().text().trim() ||
      $(el).find('updated').first().text().trim();
    const contentNode = $(el).find('content').first();
    const summaryNode = $(el).find('summary').first();
    const bodyRaw =
      contentNode.html()?.trim() ||
      contentNode.text().trim() ||
      summaryNode.html()?.trim() ||
      summaryNode.text().trim();
    if (!title || !link) return;
    const ts = Date.parse(pubRaw || '') || Date.now();
    const sanitizedBodyHtml = normalizeFeedBodyHtml(bodyRaw || '', sourceUrl);
    const bodyText = sanitizedBodyHtml
      ? asText(sanitizedBodyHtml)
      : (isRedditSource ? '' : asText(bodyRaw || ''));
    out.push({
      title,
      link,
      body: bodyText,
      bodyHtml: sanitizedBodyHtml || undefined,
      ts,
      pub: pubRaw || undefined,
      source,
    });
  });
  return out;
}

function dedupe(items: TimelineItem[]): TimelineItem[] {
  const byKey = new Map<string, TimelineItem>();
  for (const item of items) {
    const key = item.link || `${item.title}-${item.ts}`;
    const prev = byKey.get(key);
    if (!prev || item.ts > prev.ts) byKey.set(key, item);
  }
  return [...byKey.values()];
}

function capXItemsPerRefresh(items: TimelineItem[], maxXItems: number): TimelineItem[] {
  if (!Number.isFinite(maxXItems) || maxXItems < 0) return items;
  let xCount = 0;
  const out: TimelineItem[] = [];
  for (const item of items) {
    const isX = String(item.source || '').startsWith('@');
    if (isX) {
      if (xCount >= maxXItems) continue;
      xCount += 1;
    }
    out.push(item);
  }
  return out;
}

export async function GET(req: Request) {
  try {
    const params = new URL(req.url).searchParams;
    const debugBilling = params.get('debug_billing') === '1';
    const requestedBillingReset = params.get('reset_billing') === '1';
    const resetKeyParam = params.get('reset_key') || '';
    const includeXCache = params.get('include_x_cache') === '1';
    const disableXApi = params.get('disable_x_api') === '1';
    const xCacheOnly = params.get('x_cache_only') === '1';
    const raw = await fs.readFile(SOURCES_JSON, 'utf8');
    const cfg = JSON.parse(raw) as TimelineConfig;
    const limitRaw = Number(params.get('limit') ?? '0');
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 5000) : 0;
    const hoursRaw = Number(params.get('hours') ?? '24');
    const hours = Number.isFinite(hoursRaw) && hoursRaw > 0 ? Math.min(hoursRaw, 24 * 14) : 24;
    const cutoffTs = Date.now() - Math.round(hours * 60 * 60 * 1000);
    const redditFeeds = (Array.isArray(cfg.reddit?.feeds) ? cfg.reddit!.feeds! : []).filter(Boolean);
    const nitterFeeds = (Array.isArray(cfg.nitter?.feeds) ? cfg.nitter!.feeds! : []).filter(Boolean);
    const nitterAccounts = (Array.isArray(cfg.nitter?.accounts) ? cfg.nitter!.accounts! : [])
      .map((x) => x.trim())
      .filter(Boolean);
    const xAccounts = (Array.isArray(cfg.x?.accounts) ? cfg.x!.accounts! : [])
      .map((x) => x.trim())
      .filter(Boolean);
    const xSourcesRaw = [...nitterFeeds, ...nitterAccounts, ...xAccounts];
    const seenX = new Set<string>();
    const xSources: string[] = [];
    for (const source of xSourcesRaw) {
      const account = accountFromSource(source);
      const key = account ? `acct:${account.toLowerCase()}` : `src:${source}`;
      if (seenX.has(key)) continue;
      seenX.add(key);
      xSources.push(source);
    }
    const entries = [
      ...redditFeeds.map((source) => ({ source, channel: 'reddit' as const })),
      ...(xCacheOnly ? [] : xSources.map((source) => ({ source, channel: 'x' as const }))),
    ];

    const bearer = getXBearerToken();
    const usagePromise =
      bearer && process.env.TIMELINE_USE_X_API !== '0' && !disableXApi
        ? fetchXApiUsage(bearer).catch(() => null)
        : Promise.resolve(null);

    const results = await Promise.allSettled(
      entries.map(async (entry) => {
        const account = accountFromSource(entry.source);
        const useXApi = entry.channel === 'x' && !!account && !!bearer && process.env.TIMELINE_USE_X_API !== '0' && !disableXApi;
        const preferred = entry.channel === 'x' && account ? await getPreferredProvider(account) : null;
        const candidates = entry.channel === 'x'
          ? prioritizeCandidates([
            ...(useXApi ? [{ url: `xapi:${account}`, provider: 'xapi' as const }] : []),
            ...nitterCandidates(entry.source),
          ], preferred)
          : [{ url: entry.source, provider: 'source' as const }];
        const failures: string[] = [];
        let sawXApiNoRecent = false;
        let xFetchedFromApiCount = 0;
        for (const candidate of candidates) {
          try {
            let items: TimelineItem[] = [];
            if (candidate.provider === 'xapi') {
              if (!account || !bearer) throw new Error('xapi unavailable');
              const xres = await fetchXApiPostsWithMetrics(account, cutoffTs, bearer);
              items = xres.items;
              xFetchedFromApiCount = xres.fetchedFromApiCount;
            } else {
              const xml = await fetchFeedText(candidate.url);
              items = readRssItems(xml, entry.source);
              if (!items.length) items = readAtomItems(xml, entry.source);
            }
            const inWindow = items.filter((x) => (x.ts || 0) >= cutoffTs);
            const requireRecent = entry.channel === 'x' && useXApi;
            if (requireRecent) {
              if (inWindow.length) {
                if (entry.channel === 'x' && account) await setPreferredProvider(account, candidate.provider);
                return { items: inWindow, channel: entry.channel, xFetchedFromApiCount };
              }
              if (candidate.provider === 'xapi') sawXApiNoRecent = true;
              failures.push(`${candidate.url} -> no_recent_items`);
              continue;
            }
            if (items.length) {
              if (entry.channel === 'x' && account) await setPreferredProvider(account, candidate.provider);
              return { items, channel: entry.channel, xFetchedFromApiCount };
            }
            failures.push(`${candidate.url} -> no_items`);
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            failures.push(`${candidate.url} -> ${msg}`);
          }
        }
        if (sawXApiNoRecent) {
          return { items: [], channel: entry.channel, xFetchedFromApiCount };
        }
        throw new Error(summarizeFailures(failures) || `${entry.source} -> unavailable`);
      })
    );

    const merged: TimelineItem[] = [];
    const errors: string[] = [];
    let okSources = 0;
    let failedSources = 0;
    let nitterOk = 0;
    let nitterFailed = 0;
    let xFetchedFromApiTotal = 0;
    for (let i = 0; i < results.length; i += 1) {
      const r = results[i];
      if (r.status === 'fulfilled') {
        merged.push(...r.value.items);
        okSources += 1;
        if (r.value.channel === 'x') nitterOk += 1;
        xFetchedFromApiTotal += typeof r.value.xFetchedFromApiCount === 'number' ? r.value.xFetchedFromApiCount : 0;
      } else {
        errors.push(String(r.reason));
        failedSources += 1;
        const src = entries[i]?.channel;
        if (src === 'x') nitterFailed += 1;
      }
    }

    const deduped = dedupe(merged);
    const windowed = deduped.filter((x) => (x.ts || 0) >= cutoffTs);
    const sorted = windowed.sort((a, b) => b.ts - a.ts);
    const maxXRaw = Number(params.get('x_max') ?? process.env.TIMELINE_X_MAX_ITEMS_PER_REFRESH ?? '50');
    const maxXPerRefresh = Number.isFinite(maxXRaw) ? Math.max(0, Math.min(5000, Math.round(maxXRaw))) : 50;
    const xCapped = capXItemsPerRefresh(sorted, maxXPerRefresh);
    const items = limit > 0 ? xCapped.slice(0, limit) : xCapped;
    const nitterCount = xCapped.filter((x) => String(x.source || '').startsWith('@')).length;
    const redditCount = xCapped.filter((x) => String(x.source || '').startsWith('r/')).length;
    const xUsage = await usagePromise;
    const expectedResetKey = (process.env.X_BILLING_RESET_KEY || '').trim();
    const canResetBilling = requestedBillingReset && !!expectedResetKey && resetKeyParam === expectedResetKey;
    const billingResetApplied = canResetBilling ? await resetEstimatedBillingCounter(xUsage?.capResetDay) : false;
    const xBilling = getBillingMetaFromEnv() || await getEstimatedBillingMeta(xFetchedFromApiTotal, xUsage?.capResetDay);
    const xBillingDebug = debugBilling ? getBillingDebug(xUsage?.capResetDay, billingResetApplied) : null;
    let xCache: TimelineItem[] | undefined;
    if (includeXCache) {
      const now = Date.now();
      const cacheFloorTs = now - (X_API_CACHE_TTL_HOURS * 60 * 60 * 1000);
      const xCacheLimitRaw = Number(params.get('x_cache_limit') ?? process.env.TIMELINE_X_CACHE_ANALYSIS_LIMIT ?? '800');
      const xCacheLimit = Number.isFinite(xCacheLimitRaw) ? Math.max(0, Math.min(5000, Math.round(xCacheLimitRaw))) : 800;
      const xAccountsUniq = [...new Set(xSources.map((s) => (accountFromSource(s) || '').toLowerCase()).filter(Boolean))];
      const cachedByAccount = await Promise.all(xAccountsUniq.map((acct) => getCachedXApiItems(acct)));
      const mergedCache = dedupeTimelineItems(cachedByAccount.flat())
        .filter((x) => (x.ts || 0) >= cacheFloorTs)
        .sort((a, b) => b.ts - a.ts);
      xCache = xCacheLimit > 0 ? mergedCache.slice(0, xCacheLimit) : mergedCache;
    }
    return NextResponse.json({
      items,
      updated: Date.now(),
      errors,
      ...(xCache ? { xCache } : {}),
      meta: {
        hours,
        total: xCapped.length,
        nitter: nitterCount,
        reddit: redditCount,
        limitedTo: limit > 0 ? limit : xCapped.length,
        errorCount: errors.length,
        sourcesTotal: entries.length,
        sourcesOk: okSources,
        sourcesFailed: failedSources,
        nitterSources: xSources.length,
        nitterOk,
        nitterFailed,
        xBilling,
        ...(xBillingDebug ? { xBillingDebug } : {}),
        xUsage,
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'timeline_error';
    return NextResponse.json(
      { items: [], updated: Date.now(), error: message },
      { status: 200 }
    );
  }
}
