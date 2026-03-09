// lib/alpaca.ts
// Up-to-minute “Day” inputs + MARKET-VALUE weights.

import { WATCH_WEIGHTS } from '@/data/watchlistWeights';

const DATA_BASE  = 'https://data.alpaca.markets/v2';
const TRADE_BASE = 'https://paper-api.alpaca.markets/v2';

const HDRS = () => ({
  'APCA-API-KEY-ID': process.env.APCA_API_KEY_ID ?? '',
  'APCA-API-SECRET-KEY': process.env.APCA_API_SECRET_KEY ?? '',
});

// ---------- Types ----------
export type UniverseMode = 'auto'|'positions'|'positions+watchlist'|'watchlist_only';

export type Bar = { t:string; c:number };
export type BarsMap = Record<string, Bar[]>;

export type Snapshot = {
  prevClose: number|null; minuteClose: number|null; dailyClose: number|null; lastPrice: number|null;
  prevTS?: string|null;   minuteTS?: string|null;   dailyTS?: string|null;   tradeTS?: string|null;
};
export type SnapshotsMap = Record<string, Snapshot>;

export type LatestMap = Record<string, { tradeT?: string|null; quoteT?: string|null }>;
export type Position = { symbol:string; qty:number; mv:number };
export type AccountValues = {
  portfolioValue: number;
  cash: number;
  equity?: number;
  buyingPower?: number;
};

export async function getAlpacaAccountValues(): Promise<AccountValues> {
  const r = await fetch(`${TRADE_BASE}/account`, { headers: HDRS(), cache:'no-store' });
  if (!r.ok) throw new Error(`account ${r.status}`);
  const j = await r.json() as Record<string, unknown>;
  const n = (v: unknown) => { const x = Number(v ?? 0); return Number.isFinite(x) ? x : 0; };
  return {
    portfolioValue: n(j.portfolio_value),
    cash: n(j.cash),
    equity: n(j.equity),
    buyingPower: n(j.buying_power),
  };
}

const isRec = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object';

// ---------- Positions (market-value weights) ----------
export async function getAlpacaPositions(): Promise<Position[]> {
  const r = await fetch(`${TRADE_BASE}/positions`, { headers: HDRS(), cache:'no-store' });
  if (!r.ok) throw new Error(`positions ${r.status}`);
  const arr = (await r.json()) as Array<Record<string, unknown>>;
  return arr.map(p => ({
    symbol: String(p.symbol ?? '').toUpperCase(),
    qty: Number(p.qty ?? 0),
    mv: Math.abs(Number(p.market_value ?? 0)),
  }));
}

// ---------- Watchlists ----------
export async function getWatchlistSymbols(): Promise<string[]> {
  const namePref = (process.env.WATCHLIST_NAME ?? '').toLowerCase();
  const base = `${TRADE_BASE}/watchlists`;
  try {
    const res = await fetch(base, { headers: HDRS(), cache:'no-store' });
    if (!res.ok) throw new Error(`watchlists ${res.status}`);
    const raw = await res.json();
    const lists = Array.isArray(raw) ? raw : [];

    const selected = namePref
      ? lists.filter((w) => isRec(w) && String(w.name ?? '').toLowerCase() === namePref)
      : lists;
    const ids = (selected.length ? selected : lists)
      .map((w) => (isRec(w) ? w.id : undefined))
      .filter((v): v is string => typeof v === 'string' && v.length > 0);

    const symbols = new Set<string>();
    for (const id of ids) {
      try {
        const r = await fetch(`${base}/${id}`, { headers: HDRS(), cache:'no-store' });
        if (!r.ok) continue;
        const j = await r.json();
        const assets = isRec(j) && Array.isArray(j.assets) ? j.assets : [];
        assets.forEach((a: unknown) => {
          const sym = isRec(a) ? String(a.symbol ?? '').toUpperCase() : '';
          if (sym) symbols.add(sym);
        });
      } catch {}
    }
    return Array.from(symbols);
  } catch {
    return Object.keys(WATCH_WEIGHTS); // fallback
  }
}

// ---------- Latest trade/quote timestamps (freshest ASOF) ----------
export async function fetchLatest(symbols: string[]): Promise<LatestMap> {
  const out: LatestMap = {};
  for (let i = 0; i < symbols.length; i += 25) {
    const chunk = symbols.slice(i, i + 25).join(',');

    // latest trades
    {
      const u = new URL(`${DATA_BASE}/stocks/trades/latest`);
      u.searchParams.set('symbols', chunk);
      u.searchParams.set('feed', 'iex');
      const r = await fetch(u, { headers: HDRS(), cache: 'no-store' });
      if (r.ok) {
        const j = await r.json();
        const map = j?.trades || {};
        for (const sym of Object.keys(map)) {
          (out[sym] ||= {}).tradeT = map[sym]?.t ?? null;
        }
      }
    }
    // latest quotes (backup if no trade)
    {
      const u = new URL(`${DATA_BASE}/stocks/quotes/latest`);
      u.searchParams.set('symbols', chunk);
      u.searchParams.set('feed', 'iex');
      const r = await fetch(u, { headers: HDRS(), cache: 'no-store' });
      if (r.ok) {
        const j = await r.json();
        const map = j?.quotes || {};
        for (const sym of Object.keys(map)) {
          (out[sym] ||= {}).quoteT = map[sym]?.t ?? null;
        }
      }
    }
  }
  return out;
}

// ---------- Daily Bars (window calc) ----------
/** Adjusted daily bars. NOTE: end is exclusive → add +1 day. */
export async function fetchDailyBars(symbols:string[], startISO:string, endISO:string): Promise<BarsMap> {
  const out: BarsMap = {};
  for (let i=0;i<symbols.length;i+=25) {
    const chunk = symbols.slice(i,i+25).join(',');
    let pageToken: string | undefined = undefined;
    let guard = 0;
    do {
      const u = new URL(`${DATA_BASE}/stocks/bars`);
      u.searchParams.set('symbols', chunk);
      u.searchParams.set('timeframe','1Day');
      u.searchParams.set('adjustment','all');
      u.searchParams.set('start', startISO);
      u.searchParams.set('end', new Date(new Date(endISO).getTime()+86400000).toISOString());
      u.searchParams.set('feed','iex');
      u.searchParams.set('limit','1000');
      if (pageToken) u.searchParams.set('page_token', pageToken);

      const r = await fetch(u, { headers: HDRS(), cache:'no-store' });
      if (!r.ok) throw new Error(`bars ${r.status}`);
      const j = await r.json();

      const add = (sym:string, arr: unknown[]) => {
        if (!arr || !arr.length) return;
        const mapped = arr.map((b) => {
          const rec = isRec(b) ? b : {};
          return {
          t: String(rec.t ?? rec.T ?? ''),
          c: Number(rec.c ?? rec.C ?? NaN),
          };
        }).filter(x => Number.isFinite(x.c));
        if (!mapped.length) return;
        (out[sym] ||= []).push(...mapped);
      };

      if (j?.bars && !Array.isArray(j.bars)) {
        for (const sym of Object.keys(j.bars)) add(sym, j.bars[sym]);
      } else if (Array.isArray(j?.bars)) {
        const tmp: Record<string, unknown[]> = {};
        for (const b of j.bars) {
          const rec = isRec(b) ? b : {};
          const s = String(rec.S ?? rec.Symbol ?? rec.symbol ?? '').toUpperCase();
          if (!s) continue;
          (tmp[s] ||= []).push(b);
        }
        for (const sym of Object.keys(tmp)) add(sym, tmp[sym]);
      }

      const nextRaw = typeof j?.next_page_token === 'string' ? j.next_page_token : null;
      pageToken = nextRaw && nextRaw.trim() ? nextRaw.trim() : undefined;
      guard += 1;
      if (guard > 50) break; // safety valve
    } while (pageToken);
  }

  // ensure chronological order + dedupe per ticker
  for (const sym of Object.keys(out)) {
    out[sym].sort((a,b)=> new Date(a.t).getTime() - new Date(b.t).getTime());
    const dedup: Bar[] = [];
    const seen = new Set<string>();
    for (const bar of out[sym]) {
      const key = bar.t;
      if (seen.has(key)) continue;
      seen.add(key);
      dedup.push(bar);
    }
    out[sym] = dedup;
  }
  return out;
}

// ---------- Snapshots (intraday “Day”) ----------
export async function fetchSnapshots(symbols: string[]): Promise<SnapshotsMap> {
  const out: SnapshotsMap = {};
  for (let i = 0; i < symbols.length; i += 25) {
    const chunk = symbols.slice(i, i + 25).join(',');
    const u = new URL(`${DATA_BASE}/stocks/snapshots`);
    u.searchParams.set('symbols', chunk);
    u.searchParams.set('feed', 'iex');
    const r = await fetch(u, { headers: HDRS(), cache: 'no-store' });
    if (!r.ok) throw new Error(`snapshots ${r.status}`);
    const j = await r.json();
    const map = j?.snapshots || {};
    for (const sym of Object.keys(map)) {
      const s = map[sym] || {};
      const prev = Number(s?.prev_daily_bar?.c ?? NaN);
      const mb   = Number(s?.minute_bar?.c     ?? NaN);
      const db   = Number(s?.daily_bar?.c      ?? NaN);
      const lt   = Number(s?.latest_trade?.p   ?? NaN);
      out[sym] = {
        prevClose:  Number.isFinite(prev) ? prev : null,
        minuteClose:Number.isFinite(mb)   ? mb   : null,
        dailyClose: Number.isFinite(db)   ? db   : null,
        lastPrice:  Number.isFinite(lt)   ? lt   : null,
        prevTS:  s?.prev_daily_bar?.t ?? null,
        minuteTS:s?.minute_bar?.t     ?? null,
        dailyTS: s?.daily_bar?.t      ?? null,
        tradeTS: s?.latest_trade?.t   ?? null,
      };
    }
  }
  return out;
}

// ---------- Universe builder (MV weights + watchlist fill) ----------
export async function buildUniverse(
  target = 30,
  mode: UniverseMode = 'auto',
): Promise<{ symbols:string[]; weights:Record<string,number>; source: UniverseMode | 'positions+watchlist(fallback)' | 'watchlist_only(fallback)' }> {
  // positions
  let pos: Position[] = [];
  try { pos = await getAlpacaPositions(); } catch {}
  const posSyms = pos.map(p => p.symbol);

  const mvSum = pos.reduce((s,p)=> s + (p.mv || Math.abs(p.qty)), 0);
  const posWeights: Record<string, number> = {};
  if (mvSum > 0) for (const p of pos) posWeights[p.symbol] = (p.mv || Math.abs(p.qty)) / mvSum;

  // watchlist
  let wlSyms: string[] = [];
  try { wlSyms = await getWatchlistSymbols(); } catch { wlSyms = Object.keys(WATCH_WEIGHTS); }

  const applyWatchWeights = (symbols:string[], leftover:number) => {
    const w: Record<string, number> = {};
    const slice = Object.fromEntries(symbols.map(s=>[s, WATCH_WEIGHTS[s] ?? 0]));
    const sum = Object.values(slice).reduce((a,b)=>a+b,0);
    if (sum > 0) for (const s of symbols) w[s] = leftover * (slice[s] / sum);
    else {
      const per = symbols.length ? leftover / symbols.length : 0;
      for (const s of symbols) w[s] = per;
    }
    return w;
  };

  const renorm = (m:Record<string,number>)=>{
    const tot = Object.values(m).reduce((a,b)=>a+b,0) || 1;
    const out: Record<string,number> = {};
    for (const k of Object.keys(m)) out[k] = m[k]/tot;
    return out;
  };

  // modes
  if (mode === 'positions') {
    if (!posSyms.length) {
      const pick = wlSyms.slice(0, target);
      const w = applyWatchWeights(pick, 1);
      return { symbols: pick, weights: renorm(w), source: 'watchlist_only(fallback)' };
    }
    return { symbols: posSyms, weights: renorm(posWeights), source: 'positions' };
  }

  if (mode === 'watchlist_only') {
    const pick = wlSyms.slice(0, target);
    const w = applyWatchWeights(pick, 1);
    return { symbols: pick, weights: renorm(w), source: 'watchlist_only' };
  }

  if (mode === 'positions+watchlist') {
    if (!posSyms.length) {
      const pick = wlSyms.slice(0, target);
      const w = applyWatchWeights(pick, 1);
      return { symbols: pick, weights: renorm(w), source: 'watchlist_only(fallback)' };
    }
    const need = Math.max(0, target - posSyms.length);
    const extras = wlSyms.filter(s=>!posWeights[s]).slice(0, need);
    const w: Record<string, number> = { ...posWeights };
    const used = Object.values(posWeights).reduce((a,b)=>a+b,0);
    const leftover = Math.max(0, 1 - used);
    Object.assign(w, applyWatchWeights(extras, leftover));
    return { symbols: [...posSyms, ...extras], weights: renorm(w), source: 'positions+watchlist' };
  }

  // auto
  if (posSyms.length >= target) return { symbols: posSyms, weights: renorm(posWeights), source: 'positions' };
  if (!posSyms.length) {
    const pick = wlSyms.slice(0, target);
    const w = applyWatchWeights(pick, 1);
    return { symbols: pick, weights: renorm(w), source: 'watchlist_only' };
  }
  const need = Math.max(0, target - posSyms.length);
  const extras = wlSyms.filter(s=>!posWeights[s]).slice(0, need);
  const w: Record<string, number> = { ...posWeights };
  const used = Object.values(posWeights).reduce((a,b)=>a+b,0);
  const leftover = Math.max(0, 1 - used);
  Object.assign(w, applyWatchWeights(extras, leftover));
  return { symbols: [...posSyms, ...extras], weights: renorm(w), source: 'positions+watchlist' };
}

