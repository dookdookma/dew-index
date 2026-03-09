// app/page.tsx
'use client';

import Image from 'next/image';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';

/* ===================== types ===================== */
type Period = '1D' | '1W' | '1M' | '1Q' | '1Y' | 'Custom';
type Universe = 'auto' | 'positions' | 'positions+watchlist' | 'watchlist_only';

type Row = { symbol: string; sleeve?: string; w: number; r1: number | null; rW: number | null; asOf?: string };
type Snapshot = {
  window: { from: string; to: string };
  rows: Row[];
  account?: { portfolioValue: number; cash: number; equity?: number; buyingPower?: number };
  index: { r1: number; rW: number };
  sleeves: { Core: { r1: number; rW: number }; Satellite: { r1: number; rW: number } };
};
type ComposeResp = {
  source?: string;
  diagnostics?: { requestedSymbols: number; withBars: number; withoutBars?: string[]; barsError?: string | null };
  snapshot?: Snapshot;
  dew?: string;
};

type SortDir = 'asc' | 'desc';
type SortKey = 'symbol' | 'r1' | 'rW' | 'w' | 'sleeve' | 'age';
type Headline = { title: string; link: string; ts: number; pub?: string };
type TimelineItem = {
  title: string;
  link: string;
  body: string;
  bodyHtml?: string;
  ts: number;
  pub?: string;
  source?: string;
};
type TimelineMeta = {
  hours: number;
  total: number;
  nitter: number;
  reddit: number;
  limitedTo: number;
  errorCount?: number;
  sourcesTotal?: number;
  sourcesOk?: number;
  sourcesFailed?: number;
  nitterSources?: number;
  nitterOk?: number;
  nitterFailed?: number;
  xUsage?: {
    projectCap: number;
    projectUsage: number;
    creditsLeft: number;
    creditsLeftRatio: number;
    capResetDay?: number;
    status: 'good' | 'warn' | 'bad' | 'exhausted';
  };
  xBilling?: {
    startUsd: number;
    remainingUsd: number;
    remainingRatio: number;
    status: 'good' | 'warn' | 'bad' | 'exhausted';
    asOfTs: number;
  };
};

/* ===================== helpers ===================== */
const LINE_COLORS: Record<string, string> = {
  A:'#0039A6', C:'#0039A6', E:'#0039A6',
  B:'#FF6319', D:'#FF6319', F:'#FF6319', M:'#FF6319',
  G:'#6CBE45',
  J:'#996633', Z:'#996633',
  L:'#A7A9AC',
  N:'#FCCC0A', Q:'#FCCC0A', R:'#FCCC0A', W:'#FCCC0A',
  '1':'#EE352E','2':'#EE352E','3':'#EE352E',
  '4':'#00933C','5':'#00933C','6':'#00933C',
  '7':'#B933AD',
  S:'#808183', SIR:'#2850AD',
  SYSTEM:'#222'
};
const lineColor = (r:string)=> LINE_COLORS[(r||'').toUpperCase().trim()] || '#222';

const NEWS_ORDER = [
  'DEW Line Theorists','Commentary','Tech','Business','Politics','International','NYC Metro',
] as const;
type CanonCat = typeof NEWS_ORDER[number];

const NEWS_LIMIT: Record<CanonCat, number> = {
  'DEW Line Theorists': 15, 'Commentary': 10, 'Tech': 5, 'Business': 5,
  'Politics': 5, 'International': 5, 'NYC Metro': 5,
};

const makeBuckets = () => {
  const o = {} as Record<CanonCat, Headline[]>;
  NEWS_ORDER.forEach(c => { o[c] = []; });
  return o;
};
const fmtPct = (x: number | null | undefined) => (x == null ? '-' : `${(x * 100).toFixed(2)}%`);
const colorFor = (x: number | null | undefined) =>
  x == null ? 'var(--muted)' : x > 0 ? 'var(--pos)' : x < 0 ? 'var(--neg)' : 'var(--muted)';
const fmtUsd = (x?: number | null) => (x == null ? '-' : `$${Number(x).toLocaleString(undefined, { maximumFractionDigits: 0 })}`);
const ageLabel = (iso?: string) => {
  if (!iso) return '-';
  const ms = Date.now() - Date.parse(iso);
  if (!Number.isFinite(ms) || ms < 0) return '-';
  const m = Math.floor(ms / 60000);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  return `${h}h`;
};
const ageFromTs = (ts?: number) => {
  if (!ts || !Number.isFinite(ts) || ts <= 0) return '-';
  const ms = Date.now() - ts;
  if (!Number.isFinite(ms) || ms < 0) return '-';
  const m = Math.floor(ms / 60000);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h`;
  const d = Math.floor(h / 24);
  return `${d}d`;
};
const absTsLabel = (ts?: number) =>
  ts && Number.isFinite(ts) && ts > 0 ? new Date(ts).toLocaleString() : 'Unknown time';
const isRec = (x:unknown): x is Record<string, unknown> => !!x && typeof x === 'object';
type NycCellVars = React.CSSProperties & Record<'--nycCellH' | '--nycWideH', string>;

/* ===================== ASP / WX / MTA hooks ===================== */
type ASPView = 'today'|'tomorrow'|'7day';
type ASPItem = { date:string; status:string; notes?:string };
type ASPState = { items: ASPItem[]; updated?: number; loading:boolean; error?:string };

const ASP_URL = '/api/asp';
const SUBWAY_URL = '/api/mta';
const WEATHER_URL = '/api/weather';

// a) ASP
function useASP(view: ASPView, refreshKey?: number): ASPState {
  const [st, setSt] = React.useState<ASPState>({ items: [], loading: true });

  React.useEffect(() => {
    let alive = true;
    (async () => {
      setSt(s => ({ ...s, loading: true, error: undefined }));
      try {
        const u = new URL(ASP_URL, window.location.origin);
        u.searchParams.set('view', view);
        u.searchParams.set('t', String(refreshKey ?? Date.now())); // cache-bust
        const r = await fetch(u.toString(), { cache: 'no-store' });
        const j = await r.json().catch(() => ({} as unknown));

        let items: ASPItem[] = [];
        let updated: number | undefined = undefined;

        if (isRec(j) || Array.isArray(j)) {
          const obj = isRec(j) ? j : {};
          const arr = Array.isArray(obj.items) ? obj.items : (Array.isArray(j) ? j : []);
          if (Array.isArray(arr)) {
            items = arr.map((x: unknown) => {
              const o = isRec(x) ? x : {};
              return {
                date: String(o.date ?? o.dateTxt ?? '') || '',
                status: String(o.status ?? o.aspStatus ?? '') || '',
                notes: typeof o.notes === 'string' ? o.notes : undefined,
              };
            });
          }
          const updatedVal = obj.updated;
          const uRaw = typeof updatedVal === 'number'
            ? updatedVal
            : (typeof updatedVal === 'string' ? Date.parse(updatedVal) : undefined);
          if (typeof uRaw === 'number' && Number.isFinite(uRaw)) updated = uRaw;
        }

        if (alive) setSt({ items, updated, loading: false });
      } catch (e: unknown) {
        const message = e instanceof Error ? e.message : 'error';
        if (alive) setSt({ items: [], updated: undefined, loading: false, error: message });
      }
    })();
    return () => {
      alive = false;
    };
  }, [view, refreshKey]);

  return st;
}

type SubwayAlert = { line:string; text:string; ts?:number; status?:string; why?:string };
function useSubway(refreshKey?: number): { alerts: SubwayAlert[]; updated?: number; loading: boolean } {
  const [st,setSt] = React.useState<{alerts:SubwayAlert[];updated?:number;loading:boolean}>({ alerts:[], loading:true });
  React.useEffect(() => {
    let alive = true;
    (async () => {
      try{
        const r = await fetch(`${SUBWAY_URL}?t=${refreshKey ?? Date.now()}`, { cache:'no-store' });
        const j = await r.json().catch(() => ({} as unknown));
        let alerts: SubwayAlert[] = [];
        let updated: number|undefined = undefined;
        if (isRec(j)) {
          const arr = Array.isArray(j.lines) ? j.lines : (Array.isArray(j.alerts) ? j.alerts : []);
          alerts = arr.map((a: unknown) => {
            const o = isRec(a) ? a : {};
            return {
              line: String(o.route ?? o.line ?? ''),
              text: String(o.text ?? o.why ?? o.status ?? ''),
              ts: typeof o.ts === 'number' ? o.ts : undefined,
              status: typeof o.status === 'string' ? o.status : undefined,
              why: typeof o.why === 'string' ? o.why : undefined,
            };
          }).filter((x: { line?: string }) => !!x.line);
          const u = j.updated;
          updated = typeof u === 'number' ? u : (typeof u === 'string' ? Date.parse(u) : undefined);
        }
        if (alive) setSt({ alerts, updated, loading:false });
      }catch{
        if (alive) setSt({ alerts:[], updated:undefined, loading:false });
      }
    })();
    return ()=>{ alive=false; };
  }, [refreshKey]);                    // <-- add refreshKey here
  return st;
}


type WeatherNow = { temp:number; desc:string; hi?:number; lo?:number; pop?:number; updated?:number };
function useWeather(refreshKey?: number): { now?: WeatherNow; loading:boolean } {
  const [st,setSt] = React.useState<{now?:WeatherNow;loading:boolean}>({ loading:true });
  React.useEffect(() => {
    let alive = true;
    (async () => {
      try{
        const r = await fetch(`${WEATHER_URL}?t=${refreshKey ?? Date.now()}`, { cache:'no-store' });
        const j = await r.json().catch(() => ({} as unknown));
        let now: WeatherNow|undefined = undefined;
        if (isRec(j)) {
          const main = isRec(j.now) ? j.now : j;
          const t  = Number(main.temp);
          const hi = Number(main.hi);
          const lo = Number(main.lo);
          const p  = Number(main.pop ?? main.precipProb ?? main.precipitation_probability);
          const d  = String(main.desc ?? '');
          const updatedVal = main.updated;
          const u  = typeof updatedVal === 'number'
                      ? updatedVal
                      : (typeof updatedVal === 'string' ? Date.parse(updatedVal) : undefined);
          if (Number.isFinite(t)) {
  now = {
    temp: t,
    desc: d,
    hi: Number.isFinite(hi) ? hi : undefined,
    lo: Number.isFinite(lo) ? lo : undefined,
    pop: Number.isFinite(p)  ? p  : undefined,
    updated: u
  };
}
        }
         if (alive) setSt({ now, loading:false });
      }catch{
        if (alive) setSt({ now:undefined, loading:false });
      }
    })();
    return ()=>{ alive=false; };
  }, [refreshKey]);                    // <-- add refreshKey here
  return st;
}

/* ===================== news loader ===================== */
function normalizeCat(raw: string): CanonCat | null {
  const s = (raw||'').toLowerCase();
  if (s.includes('theorist')) return 'DEW Line Theorists';
  if (s === 'gmail' || s.includes('commentary')) return 'Commentary';
  if (s.startsWith('tech')) return 'Tech';
  if (s.startsWith('business') || s==='markets') return 'Business';
  if (s.startsWith('politic')) return 'Politics';
  if (s.startsWith('international') || s==='world') return 'International';
  if (s.includes('nyc') || s.includes('metro') || s.includes('new york')) return 'NYC Metro';
  return null;
}
function useNewsByCategory(refreshKey?: number) {
  const [byCat, setByCat] = React.useState<Record<CanonCat, Headline[]>>(makeBuckets);
  React.useEffect(() => {
    let alive = true;
    (async () => {
      const [newsRes, gmailRes] = await Promise.allSettled([
        fetch(`/api/news?t=${refreshKey ?? Date.now()}`,  { cache: 'no-store' }).then(r => r.ok ? r.json() : null),
        fetch(`/api/gmail/feed?t=${refreshKey ?? Date.now()}`, { cache: 'no-store' }).then(r => r.ok ? r.json() : null),
      ]);
      const buckets = makeBuckets();
      const add = (obj: unknown) => {
        if (!isRec(obj)) return;
        const map = (obj.newsByCategory as unknown);
        if (!isRec(map)) return;
        for (const [raw, arr] of Object.entries(map)) {
          const cat = normalizeCat(raw);
          if (!cat) continue;
          const list = Array.isArray(arr) ? arr as Headline[] : [];
          buckets[cat].push(...list);
        }
      };
      if (newsRes.status==='fulfilled' && newsRes.value) add(newsRes.value);
      if (gmailRes.status==='fulfilled' && gmailRes.value) add(gmailRes.value);
      for (const cat of NEWS_ORDER) {
        buckets[cat].sort((a,b)=>(b.ts??0)-(a.ts??0));
        buckets[cat] = buckets[cat].slice(0, NEWS_LIMIT[cat]);
      }
       if (alive) setByCat(buckets);
    })();
    return ()=>{ alive=false; };
  }, [refreshKey]);                    // <-- add refreshKey here
  return byCat;
}

function useTimeline(refreshKey?: number) {
  const [items, setItems] = React.useState<TimelineItem[]>([]);
  const [meta, setMeta] = React.useState<TimelineMeta | null>(null);
  const [loading, setLoading] = React.useState<boolean>(true);

  React.useEffect(() => {
    let alive = true;
    (async () => {
      setLoading(true);
      try {
        const r = await fetch(`/api/timeline?hours=24&t=${refreshKey ?? Date.now()}`, { cache: 'no-store' });
        const j = await r.json().catch(() => ({} as unknown));
        const obj = isRec(j) ? j : {};
        const arr = Array.isArray(obj.items) ? obj.items : [];
        const m = isRec(obj.meta) ? obj.meta : {};
        const next = arr
          .map((x: unknown) => {
            const o = isRec(x) ? x : {};
            return {
              title: String(o.title ?? ''),
              link: String(o.link ?? ''),
              body: String(o.body ?? ''),
              bodyHtml: typeof o.bodyHtml === 'string' ? o.bodyHtml : undefined,
              ts: Number(o.ts ?? 0),
              pub: typeof o.pub === 'string' ? o.pub : undefined,
              source: typeof o.source === 'string' ? o.source : undefined,
            };
          })
          .filter((x: TimelineItem) => !!x.title && !!x.link)
          .sort((a: TimelineItem, b: TimelineItem) => (b.ts ?? 0) - (a.ts ?? 0));
        if (alive) {
          setItems(next);
          setMeta({
            hours: typeof m.hours === 'number' ? m.hours : 24,
            total: typeof m.total === 'number' ? m.total : next.length,
            nitter: typeof m.nitter === 'number' ? m.nitter : next.filter((x) => String(x.source || '').startsWith('@')).length,
            reddit: typeof m.reddit === 'number' ? m.reddit : next.filter((x) => String(x.source || '').startsWith('r/')).length,
            limitedTo: typeof m.limitedTo === 'number' ? m.limitedTo : next.length,
            errorCount: typeof m.errorCount === 'number' ? m.errorCount : undefined,
            sourcesTotal: typeof m.sourcesTotal === 'number' ? m.sourcesTotal : undefined,
            sourcesOk: typeof m.sourcesOk === 'number' ? m.sourcesOk : undefined,
            sourcesFailed: typeof m.sourcesFailed === 'number' ? m.sourcesFailed : undefined,
            nitterSources: typeof m.nitterSources === 'number' ? m.nitterSources : undefined,
            nitterOk: typeof m.nitterOk === 'number' ? m.nitterOk : undefined,
            nitterFailed: typeof m.nitterFailed === 'number' ? m.nitterFailed : undefined,
            xUsage: isRec(m.xUsage)
              ? {
                projectCap: Number(m.xUsage.projectCap ?? 0),
                projectUsage: Number(m.xUsage.projectUsage ?? 0),
                creditsLeft: Number(m.xUsage.creditsLeft ?? 0),
                creditsLeftRatio: Number(m.xUsage.creditsLeftRatio ?? 0),
                capResetDay: typeof m.xUsage.capResetDay === 'number' ? m.xUsage.capResetDay : undefined,
                status:
                  m.xUsage.status === 'good' ||
                  m.xUsage.status === 'warn' ||
                  m.xUsage.status === 'bad' ||
                  m.xUsage.status === 'exhausted'
                    ? m.xUsage.status
                    : 'bad',
              }
              : undefined,
            xBilling: isRec(m.xBilling)
              ? {
                startUsd: Number(m.xBilling.startUsd ?? 0),
                remainingUsd: Number(m.xBilling.remainingUsd ?? 0),
                remainingRatio: Number(m.xBilling.remainingRatio ?? 0),
                asOfTs: Number(m.xBilling.asOfTs ?? 0),
                status:
                  m.xBilling.status === 'good' ||
                  m.xBilling.status === 'warn' ||
                  m.xBilling.status === 'bad' ||
                  m.xBilling.status === 'exhausted'
                    ? m.xBilling.status
                    : 'bad',
              }
              : undefined,
          });
        }
      } catch {
        if (alive) {
          setItems([]);
          setMeta(null);
        }
      } finally {
        if (alive) setLoading(false);
      }
    })();
    return () => {
      alive = false;
    };
  }, [refreshKey]);

  return { items, loading, meta };
}

/* ===================== small UI bits ===================== */
const GUTTER = 6;
function Th({label,onSort,active,dir}:{label:string;onSort:()=>void;active:boolean;dir:SortDir}) {
  const ariaSort: 'none'|'ascending'|'descending'|'other' = active ? (dir==='asc'?'ascending':'descending') : 'none';
  return (
    <th onClick={onSort} aria-sort={ariaSort} style={{ padding:'6px 8px', textAlign:'left', cursor:'pointer', userSelect:'none' }}>
      {label} {active ? (dir === 'asc' ? '^' : 'v') : ''}
    </th>
  );
}
function StatCard({
  title, day, win, hideWindow
}: { title: string; day: number; win: number; hideWindow?: boolean }) {
  const colorHard = (x: number | null | undefined) =>
    x == null ? '#777' : x > 0 ? '#137333' : x < 0 ? '#a50e0e' : '#777';

  return (
    <div className="card">
      <div className="cardTitle">{title}</div>

      <div className="cardRow">
        {!hideWindow && (
          <div>
            <div className="sub">Window</div>
            <div className="val" style={{ color: colorHard(win) }}>{fmtPct(win)}</div>
          </div>
        )}
        <div>
          <div className="sub">Day</div>
          <div className="val" style={{ color: colorHard(day) }}>{fmtPct(day)}</div>
        </div>
      </div>

      <style jsx>{`
        .card {
          border: 1px solid var(--border);
          border-radius: 6px;
          padding: 8px 10px;
          min-width: 140px;
          background: var(--panel-bg);
        }
        .cardTitle { font-size: 12px; color: var(--muted); }
        .cardRow { display: flex; gap: 12px; margin-top: 4px; align-items: baseline; }
        .sub { font-size: 11px; color: var(--muted2); }
        .val { font-weight: 600; }
      `}</style>
    </div>
  );
}


/* ===================== News Panel ===================== */
function NewsPanel() {
  // refresh keys
  const [newsRefreshKey, setNewsRefreshKey] = React.useState(0);
  const [timelineRefreshKey, setTimelineRefreshKey] = React.useState(0);

  // use hooks with refresh keys
  const [aspView, setAspView] = React.useState<ASPView>('today');
  const asp = useASP(aspView, newsRefreshKey);
  const aspPrimary = Array.isArray(asp.items) && asp.items.length ? asp.items[0] : null;
  const { now } = useWeather(newsRefreshKey);
  const subway = useSubway(newsRefreshKey);
  const timeline = useTimeline(timelineRefreshKey);
  const byCat = useNewsByCategory(newsRefreshKey);
  const allEmpty = NEWS_ORDER.every(cat => (byCat[cat]?.length ?? 0) === 0);
  const [expandedTimeline, setExpandedTimeline] = React.useState<Record<string, boolean>>({});
  const [overflowedTimeline, setOverflowedTimeline] = React.useState<Record<string, boolean>>({});

  const toggleTimeline = React.useCallback((key: string) => {
    setExpandedTimeline(prev => ({ ...prev, [key]: !prev[key] }));
  }, []);

  React.useEffect(() => {
    const measure = () => {
      const next: Record<string, boolean> = {};
      const cards = document.querySelectorAll<HTMLElement>('article[data-tl-key]');
      cards.forEach((card) => {
        const k = card.dataset.tlKey;
        if (!k) return;
        const titleEl = card.querySelector<HTMLElement>('.timelineItemHead a');
        const bodyEl = card.querySelector<HTMLElement>('.timelineBody');
        const titleOverflow = !!titleEl && (
          titleEl.scrollWidth > titleEl.clientWidth + 1 ||
          titleEl.scrollHeight > titleEl.clientHeight + 1
        );
        const bodyOverflow = !!bodyEl && (
          bodyEl.scrollWidth > bodyEl.clientWidth + 1 ||
          bodyEl.scrollHeight > bodyEl.clientHeight + 1
        );
        next[k] = titleOverflow || bodyOverflow;
      });
      setOverflowedTimeline((prev) => {
        const pKeys = Object.keys(prev);
        const nKeys = Object.keys(next);
        if (pKeys.length === nKeys.length && nKeys.every((k) => prev[k] === next[k])) return prev;
        return next;
      });
    };

    const raf = window.requestAnimationFrame(measure);
    const timer = window.setTimeout(measure, 120);
    window.addEventListener('resize', measure);
    return () => {
      window.cancelAnimationFrame(raf);
      window.clearTimeout(timer);
      window.removeEventListener('resize', measure);
    };
  }, [timeline.items, expandedTimeline]);

  const nycCellVars: NycCellVars = { '--nycCellH': '68px', '--nycWideH': '80px' };
  const statusClass = (ok?: number, total?: number) => {
    const o = typeof ok === 'number' ? ok : 0;
    const t = typeof total === 'number' ? total : 0;
    if (t <= 0 || o <= 0) return 'isBad';
    if (o >= t) return 'isGood';
    return 'isWarn';
  };
  const errorClass = (count?: number) => {
    const c = typeof count === 'number' ? count : 0;
    if (c <= 0) return 'isGood';
    if (c <= 2) return 'isWarn';
    return 'isBad';
  };
  const nitterClass = (meta: TimelineMeta | null) => {
    if (!meta) return 'isBad';
    if ((meta.nitter ?? 0) <= 0) return 'isBad';
    return statusClass(meta.nitterOk, meta.nitterSources);
  };
  const billingClass = (meta: TimelineMeta | null) => {
    const s = meta?.xBilling?.status;
    if (s === 'good') return 'isGood';
    if (s === 'warn') return 'isWarn';
    if (s === 'bad') return 'isBad';
    if (s === 'exhausted') return 'isExhausted';
    return 'isBad';
  };
  const apiUsageClass = (meta: TimelineMeta | null) => {
    const s = meta?.xUsage?.status;
    if (s === 'good') return 'isGood';
    if (s === 'warn') return 'isWarn';
    if (s === 'bad') return 'isBad';
    if (s === 'exhausted') return 'isExhausted';
    return 'isBad';
  };

  return (
    <div className="panel panel--news">
      <div className="panelHeader" style={{ display:'flex', alignItems:'center', gap:8 }}>
  <h2 style={{ margin: 0 }}>News</h2>
  <button
    onClick={() => setNewsRefreshKey(Date.now())}
    style={{ marginLeft:'auto', padding:'4px 10px' }}
    aria-label="Refresh news headlines and NYC widgets"
    title="Refresh News"
  >
    Refresh
  </button>
</div>

      <div
        className="panelBody"
        style={nycCellVars}
      >
        {/* NYC widgets */}
        <div className="nycGrid">
          {/* Weather (UL) */}
          <div className="nycCell nycCell--wx">
            <h4 className="nycTitle">Weather</h4>
            <div className="nycCellBody">
              {now ? (
                <div style={{fontSize:13, lineHeight:1.35}}>
                  <div>{now.desc || 'Current conditions'}</div>
                  <div>Now {Math.round(now.temp)} F</div>
                  {(Number.isFinite(now.hi as number) || Number.isFinite(now.lo as number)) && (
                    <div>H {Math.round(now.hi as number)} / L {Math.round(now.lo as number)}</div>
                  )}
                  {'pop' in now && Number.isFinite(now.pop as number) && (
                    <div>Precipitation {Math.round(now.pop as number)}%</div>
                  )}
                </div>
              ) : (
                <div style={{color:'var(--muted2)'}}>No weather.</div>
              )}
            </div>
          </div> {/* <-- ADDED closing div for Weather .nycCell */}

          {/* ASP (UR) */}
          <div className="nycCell">
            <div className="aspHead">
              <h4 className="nycTitle">NYC ASP</h4>
              <div className="aspBtns">
                <button className={'segBtn'+(aspView==='today'?' on':'')} onClick={()=>setAspView('today')}>Today</button>
                <button className={'segBtn'+(aspView==='tomorrow'?' on':'')} onClick={()=>setAspView('tomorrow')}>Tmrw</button>
                <button className={'segBtn'+(aspView==='7day'?' on':'')} onClick={()=>setAspView('7day')}>Week</button>
              </div>
            </div>
            <div className="nycCellBody">
              {asp.loading ? (
                <div style={{color:'var(--muted2)'}}>Loading...</div>
              ) : asp.error ? (
                <div style={{color:'var(--muted2)'}}>Error: {asp.error}</div>
              ) : aspView !== '7day' ? (
  aspPrimary ? (
    <div style={{fontSize:13, lineHeight:1.45}}>
      <div>{aspPrimary.date}: <b>{aspPrimary.status || '-'}</b></div>
      {aspPrimary.notes ? (
        <div style={{color:'var(--muted2)'}}>{aspPrimary.notes}</div>
      ) : null}
    </div>
  ) : (
    <div style={{color:'var(--muted2)'}}>No ASP.</div>
  )
)
 : /* 7-day list (no table) */
              (Array.isArray(asp.items) && asp.items.length) ? (
  <div className="aspList" style={{display:'grid', gap:6}}>
    {asp.items.slice(0,7).map((d,i)=>(
      <div key={i} style={{fontSize:13, lineHeight:1.45}}>
        <div>{d.date}: <b>{d.status || '-'}</b></div>
        {d.notes ? <div style={{color:'var(--muted2)'}}>{d.notes}</div> : null}
      </div>
    ))}
  </div>
) : (
  <div style={{color:'var(--muted2)'}}>No ASP.</div>
)

              }
            </div>
          </div>

          {/* Subway (bottom, full width) */}
          <div className="nycCell nycCell--wide">
            <h4 className="nycTitle">Subway Service Alerts</h4>
            <div className="nycCellBody">
              {subway.loading ? (
                <div style={{color:'var(--muted2)'}}>Loading...</div>
              ) : (subway.alerts.length === 0) ? (
                <div style={{color:'var(--muted2)'}}>All good</div>
              ) : (
                <ul style={{listStyle:'none', padding:0, margin:0}}>
                  {subway.alerts.map((ln, i) => (
                    <li key={i} style={{margin:'4px 0', lineHeight:1.35}}>
                      <span style={{fontWeight:600, color: lineColor(ln.line)}}>{ln.line}</span>
                      {' - '}<span>{ln.status || ln.text}</span>
                      {ln.why ? <span style={{color:'var(--muted2)'}}> - {ln.why}</span> : null}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </div>

        <div className="timelineWrap">
          <div className="timelineHead">
            <h4 className="timelineTitle">Timeline</h4>
            {timeline.meta ? (
              <div className="healthDots" aria-label="Timeline feed health">
                <span
                  className={`healthDot ${statusClass(timeline.meta.sourcesOk, timeline.meta.sourcesTotal)}`}
                  title={`Sources OK: ${timeline.meta.sourcesOk ?? 0}/${timeline.meta.sourcesTotal ?? 0}`}
                />
                <span className="healthSep">|</span>
                <span
                  className={`healthDot ${nitterClass(timeline.meta)}`}
                  title={`X/Nitter posts: ${timeline.meta.nitter ?? 0} | sources OK: ${timeline.meta.nitterOk ?? 0}/${timeline.meta.nitterSources ?? 0}`}
                />
                <span className="healthSep">|</span>
                <span
                  className={`healthDot ${errorClass(timeline.meta.errorCount)}`}
                  title={`Feed errors: ${timeline.meta.errorCount ?? 0}`}
                />
                <span className="healthSep">|</span>
                <span
                  className={`healthDot ${billingClass(timeline.meta)}`}
                  title={
                    timeline.meta.xBilling
                      ? `X billing remaining: $${(timeline.meta.xBilling.remainingUsd ?? 0).toFixed(2)}/$${(timeline.meta.xBilling.startUsd ?? 0).toFixed(2)} (${Math.round((timeline.meta.xBilling.remainingRatio ?? 0) * 100)}%)`
                      : 'X billing unavailable'
                  }
                />
                <span className="healthSep">|</span>
                <span
                  className={`healthDot ${apiUsageClass(timeline.meta)}`}
                  title={
                    timeline.meta.xUsage
                      ? `X API credits left: ${timeline.meta.xUsage?.creditsLeft ?? 0}/${timeline.meta.xUsage?.projectCap ?? 0} (${Math.round((timeline.meta.xUsage?.creditsLeftRatio ?? 0) * 100)}%)`
                      : 'X API usage unavailable'
                  }
                />
              </div>
            ) : null}
            <button
              type="button"
              className="timelineRefreshBtn"
              onClick={() => setTimelineRefreshKey(Date.now())}
              aria-label="Refresh timeline only"
              title="Refresh Timeline"
            >
              Refresh
            </button>
          </div>
          {timeline.loading ? (
            <div style={{ color:'var(--muted2)' }}>Loading...</div>
          ) : timeline.items.length === 0 ? (
            <div style={{ color:'var(--muted2)' }}>No timeline posts.</div>
          ) : (
            <div className="timelineList" aria-label="Timeline feed">
              {timeline.items.map((it, idx) => {
                const key = `${it.link}-${idx}`;
                const expanded = !!expandedTimeline[key];
                const isXPost = String(it.source || '').startsWith('@');
                const body = isXPost
                  ? (it.body || '').trim()
                  : (it.body || '').trim();
                const hasHtml = !!(it.bodyHtml && it.bodyHtml.trim());
                const bodyForMeasure = (it.body || '').trim();
                const needsExpand =
                  expanded ||
                  !!overflowedTimeline[key] ||
                  it.title.length > 140 ||
                  bodyForMeasure.length > 220 ||
                  ((bodyForMeasure.match(/\n/g)?.length ?? 0) > 1);
                const rel = ageFromTs(it.ts);
                const abs = absTsLabel(it.ts);
                return (
                  <article key={key} data-tl-key={key} className={expanded ? 'timelineItem isExpanded' : 'timelineItem'}>
                    <div className="timelineItemHead">
                      {isXPost ? (
                        <a href={it.link} target="_blank" rel="noreferrer" className="timelineHeadMeta">
                          {`${it.source || '@unknown'} | ${expanded ? abs : `${rel} ago`}`}
                        </a>
                      ) : (
                        <a href={it.link} target="_blank" rel="noreferrer">{it.title}</a>
                      )}
                    </div>
                    <div className={expanded ? 'timelineBody isExpanded' : 'timelineBody'}>
                      {hasHtml ? (
                        <div
                          className="timelineBodyHtml"
                          dangerouslySetInnerHTML={{ __html: it.bodyHtml || '' }}
                        />
                      ) : (
                        body
                      )}
                    </div>
                    <div className="timelineMeta">
                      {isXPost ? null : (
                        <span>{expanded ? `${it.source || 'feed'} | ${abs}` : `${it.source || 'feed'} | ${rel} ago`}</span>
                      )}
                      {needsExpand ? (
                        <button
                          type="button"
                          className="timelineBtn"
                          onClick={() => toggleTimeline(key)}
                          aria-expanded={expanded}
                        >
                          {expanded ? 'Collapse' : 'Expand'}
                        </button>
                      ) : null}
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </div>

        <hr className="sep" />

        {/* Headlines */}
        {allEmpty ? (
          <div style={{ color:'var(--muted2)' }}>No headlines.</div>
        ) : (
          NEWS_ORDER.map(cat => {
            const items = byCat[cat] || [];
            if (!items.length) return null;
            return (
              <div key={cat} style={{ marginBottom: 14 }}>
                <h4 style={{ margin:'8px 0 6px' }}>{cat}</h4>
                <ol style={{ margin:0, paddingLeft:18 }}>
                  {items.map((it, i) => (
                    <li key={i} style={{ marginBottom: 6, lineHeight: 1.25 }}>
                      <a href={it.link} target="_blank" rel="noreferrer">{it.title}</a>
                      {it.pub ? <span style={{ color:'var(--muted2)', marginLeft:8, fontSize:12 }}>- {it.pub}</span> : null}
                    </li>
                  ))}
                </ol>
              </div>
            );
          })
        )}
      </div>

      <style jsx>{`
        /* News panel chrome */
        .panel--news{
          display:flex;
          flex-direction:column;
          height:100%;
          border:1px solid var(--border);
          border-radius:6px;
          background:var(--panel-bg);
        }
        .panel--news .panelHeader{
          padding:8px 10px;
          border-bottom:1px solid var(--border);
          background:var(--header-bg);
        }
        .panel--news .panelBody{
          flex:1 1 auto;
          min-height:0;
          overflow:auto;
          padding:8px;
        }
        /* NYC header widgets boxes */
        .panel--news .nycCell{
          border:1px solid var(--border);
          border-radius:6px;
          background:var(--panel-bg);
          padding:8px;
        }
      `}</style>

      <style jsx>{`
        .nycGrid{
   display:grid;
   grid-template-columns: max-content 1fr; /* Weather fits its content, ASP fills remaining */
   gap: 8px;
   margin-bottom: 10px;
 }
 .nycCell{
   border:1px solid var(--border);
   border-radius:6px;
   background:var(--panel-bg);
   padding:8px;
   display:flex;
   flex-direction:column;
   min-width:0; /* allow ASP to shrink/grow smoothly */
 }
/* keep Weather column sized to its content */
.nycCell--wx{ justify-self:start;
padding-right: calc(8px + var(--wx-pad, 5px));.) }

        .nycCell--wide{ grid-column: 1 / span 2; }
        .nycTitle{ margin:0; }
        .nycCell--wx .nycTitle{ margin-bottom:6px; }
        .aspHead{
          display:flex;
          align-items:center;
          justify-content:space-between;
          gap:8px;
          margin:0 0 6px;
        }
        .aspBtns{ display:flex; gap:6px; margin:0; }
        .segBtn{ font-size:12px; padding:3px 8px; border:1px solid var(--border); border-radius:4px; background:var(--panel-bg); cursor:pointer; white-space:nowrap; line-height:1.1; }
        .segBtn.on{ background:#e8f0fe; border-color:#c6dafc; }

        /* constant-height scrollable bodies */
        .nycCellBody{
          flex: 1 1 auto;
          min-height: 0;
          height: var(--nycCellH, 50px);
          overflow: auto;
          padding-right: 2px;
          font-size: 13px;
        }
        .nycCell.nycCell--wide .nycCellBody{
          height: var(--nycWideH, 80px);
        }

        .aspTableWrap{ overflow:auto; }
        .aspTable{ width:100%; border-collapse:collapse; font-size:13px; }
        .aspTable td{ border-top:1px solid var(--border); padding:4px 6px; vertical-align:top; }
        .sep{ border:none; border-top:1px solid var(--border-subtle); margin:10px 0; }
        .timelineWrap{
          border:1px solid var(--border);
          border-radius:6px;
          padding:8px;
          background:var(--panel-bg);
          margin-bottom:10px;
        }
        .timelineHead{
          display:flex;
          align-items:center;
          justify-content:flex-start;
          gap:10px;
          margin:0 0 6px;
        }
        .timelineTitle{ margin:0; }
        .timelineRefreshBtn{
          margin-left:auto;
          font-size:12px;
          padding:2px 8px;
          border:1px solid var(--border);
          border-radius:4px;
          background:var(--panel-bg);
          cursor:pointer;
        }
        .healthDots{
          display:flex;
          align-items:center;
          gap:6px;
        }
        .healthSep{
          color:var(--muted2);
          font-size:12px;
          line-height:1;
          user-select:none;
        }
        .healthDot{
          width:10px;
          height:10px;
          border-radius:50%;
          border:1px solid rgba(0,0,0,0.2);
          display:inline-block;
        }
        .healthDot.isGood{ background:#1a7f37; }
        .healthDot.isWarn{ background:#b08800; }
        .healthDot.isBad{ background:#d1242f; }
        .healthDot.isExhausted{ background:#111; }
        .timelineList{
          --timelineRowH: 88px;
          display:grid;
          gap:8px;
          max-height:calc((var(--timelineRowH) * 5) + (8px * 4));
          overflow:auto;
          padding-right:2px;
        }
        .timelineItem{
          height:var(--timelineRowH);
          box-sizing:border-box;
          border:1px solid var(--border);
          border-radius:6px;
          padding:8px;
          background:var(--panel-bg);
          overflow:hidden;
          display:flex;
          flex-direction:column;
          min-width:0;
        }
        .timelineItem.isExpanded{
          height:auto;
          overflow:visible;
        }
        .timelineItemHead a{
          font-weight:600;
          text-decoration:none;
          display:-webkit-box;
          -webkit-box-orient:vertical;
          -webkit-line-clamp:2;
          overflow:hidden;
          overflow-wrap:anywhere;
          word-break:break-word;
          min-width:0;
        }
        .timelineItemHead a.timelineHeadMeta{
          display:block;
          -webkit-line-clamp:unset;
          overflow:visible;
          white-space:nowrap;
          text-overflow:ellipsis;
          overflow:hidden;
        }
        .timelineItem.isExpanded .timelineItemHead a{
          display:block;
          -webkit-line-clamp:unset;
          overflow:visible;
        }
        .timelineItemHead a:hover{ text-decoration:underline; }
        .timelineBody{
          margin-top:4px;
          color:var(--muted);
          line-height:1.35;
          display:-webkit-box;
          -webkit-box-orient:vertical;
          -webkit-line-clamp:2;
          overflow:hidden;
          overflow-wrap:anywhere;
          word-break:break-word;
          min-width:0;
        }
        .timelineBody.isExpanded{
          display:block;
          -webkit-line-clamp:unset;
          overflow:visible;
          white-space:pre-wrap;
        }
        .timelineBodyHtml :global(p){ margin:0 0 6px; }
        .timelineBodyHtml :global(p:last-child){ margin-bottom:0; }
        .timelineBodyHtml :global(a){ color:inherit; text-decoration:underline; }
        .timelineBodyHtml :global(ul), .timelineBodyHtml :global(ol){ margin:0 0 6px 18px; padding:0; }
        .timelineBodyHtml :global(li){ margin:0 0 2px; }
        .timelineBodyHtml :global(*){ max-width:100%; }
        .timelineBodyHtml :global(a), .timelineBodyHtml :global(span), .timelineBodyHtml :global(code){
          overflow-wrap:anywhere;
          word-break:break-word;
        }
        .timelineMeta{
          margin-top:auto;
          padding-top:6px;
          display:flex;
          align-items:center;
          justify-content:space-between;
          color:var(--muted2);
          font-size:12px;
          gap:8px;
        }
        .timelineBtn{
          font-size:12px;
          padding:2px 8px;
          border:1px solid var(--border);
          border-radius:4px;
          background:var(--panel-bg);
          cursor:pointer;
        }
        @media (max-width: 920px){
          .aspBtns{ gap:4px; }
          .segBtn{ font-size:11px; padding:2px 6px; }
          .nycCell.nycCell--wide .nycCellBody{ height: 180px; }
        }
      `}</style>
    </div>
  );
}

/* ===================== page ===================== */
export default function Home() {
  // controls
  const [period, setPeriod] = useState<Period>('1W');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [universe, setUniverse] = useState<Universe>('auto');

  // data
  const [data, setData] = useState<ComposeResp | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // sorting
  const [sortKey, setSortKey] = useState<SortKey>('rW');
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  // layout widths + dragging
  const [wNews, setWNews] = useState(33);
  const [wIndex, setWIndex] = useState(34);
  const [wDew, setWDew] = useState(33);
  const dragRef = useRef<{ col: 1 | 2; startX: number; startW: number; nextW: number } | null>(null);

  // DEW line
  const [dewText, setDewText] = useState('');
  const [dewRunning, setDewRunning] = useState(false);
  const [dewImg, setDewImg] = useState('/dew/01.jpg');
  const [copyState, setCopyState] = useState<'idle' | 'copied' | 'error'>('idle');

  // mobile/desktop mode + tabs
  type Tab = 'news'|'index'|'dew';
  const [tab, setTab] = useState<Tab>('news');
  const [isMobile, setIsMobile] = useState<boolean | null>(null); // null until mounted
  const [viewMode, setViewMode] = useState<'auto'|'mobile'|'desktop'>(() => {
    if (typeof window !== 'undefined') {
      const m = new URLSearchParams(window.location.search).get('mode');
      if (m === 'mobile' || m === 'desktop') return m;
    }
    return 'auto';
  });

  useEffect(() => {
    const decide = () => {
      if (viewMode === 'mobile') { setIsMobile(true); return; }
      if (viewMode === 'desktop') { setIsMobile(false); return; }
      const mm = window.matchMedia('(max-width: 920px)');
      setIsMobile(mm.matches);
    };
    decide();
    const mm = window.matchMedia('(max-width: 920px)');
    const onMM = (e: MediaQueryListEvent) => { if (viewMode === 'auto') setIsMobile(e.matches); };
    mm.addEventListener('change', onMM);
    window.addEventListener('resize', decide);
    return () => { mm.removeEventListener('change', onMM); window.removeEventListener('resize', decide); };
  }, [viewMode]);

  // compose fetch
  const buildQuery = useCallback((withDew: boolean) => {
    const u = new URL('/api/compose', typeof window === 'undefined' ? 'http://localhost' : window.location.origin);
    u.searchParams.set('universe', universe);
    if (period === 'Custom' && from && to) { u.searchParams.set('from', from); u.searchParams.set('to', to); }
    else { u.searchParams.set('period', period === 'Custom' ? '1W' : period); }
    if (withDew) u.searchParams.set('dew', '1');
    return u.toString();
  }, [universe, period, from, to]);

  const load = useCallback(async (withDew: boolean) => {
    setLoading(true); setErr(null);
    try {
      const url = buildQuery(withDew);
      const r = await fetch(url, { cache:'no-store' });
      if (!r.ok) throw new Error(`${r.status}`);
      const j: ComposeResp = await r.json();
      setData(j);
      if (withDew) setDewText(j.dew || '');
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : String(e));
    }
    finally { setLoading(false); }
  }, [buildQuery]);

  const runDew = useCallback(async () => { setDewRunning(true); await load(true); setDewRunning(false); }, [load]);
  const copyDew = useCallback(async () => {
    const text = (dewText || '').trim();
    if (!text) return;
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const el = document.createElement('textarea');
        el.value = text;
        el.setAttribute('readonly', '');
        el.style.position = 'fixed';
        el.style.left = '-9999px';
        document.body.appendChild(el);
        el.select();
        document.execCommand('copy');
        document.body.removeChild(el);
      }
      setCopyState('copied');
      window.setTimeout(() => setCopyState('idle'), 1500);
    } catch {
      setCopyState('error');
      window.setTimeout(() => setCopyState('idle'), 1500);
    }
  }, [dewText]);
  useEffect(() => { void load(false); }, [load]);
  useEffect(() => { setCopyState('idle'); }, [dewText]);

  // drag gutters
  const onMouseMove = useCallback((e: MouseEvent) => {
    const st = dragRef.current; if (!st) return;
    const dx = e.clientX - st.startX;
    const total = wNews + wIndex + wDew;
    if (st.col === 1) {
      const nw = Math.max(20, Math.min(60, st.startW + (dx / window.innerWidth) * 100));
      const ni = Math.max(20, Math.min(60, st.nextW - (dx / window.innerWidth) * 100));
      const rest = Math.max(10, total - nw - ni);
      setWNews(nw); setWIndex(ni); setWDew(rest);
    } else {
      const ni = Math.max(20, Math.min(60, st.startW + (dx / window.innerWidth) * 100));
      const nd = Math.max(20, Math.min(60, st.nextW - (dx / window.innerWidth) * 100));
      const rest = Math.max(10, total - wNews - ni);
      setWIndex(ni); setWDew(nd || rest);
    }
  }, [wNews, wIndex, wDew]);
  const onMouseUp = useCallback(() => {
    dragRef.current = null;
    window.removeEventListener('mousemove', onMouseMove);
    window.removeEventListener('mouseup', onMouseUp);
  }, [onMouseMove]);
  const startDrag = (col: 1 | 2) => (e: React.MouseEvent) => {
    e.preventDefault();
    dragRef.current = { col, startX: e.clientX, startW: col === 1 ? wNews : wIndex, nextW: col === 1 ? wIndex : wDew };
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
  };

  // rows
  const rowsSorted = useMemo(() => {
  const rows: Row[] = Array.isArray(data?.snapshot?.rows) ? data!.snapshot!.rows.filter((r) => (r.w ?? 0) > 0) : [];

  // active key: in 1D we force 'r1' regardless of header state
  const activeKey: SortKey = period === '1D' ? 'r1' : sortKey; 
  const dirMul = sortDir === 'asc' ? 1 : -1;

  const val = (r: Row, k: SortKey): number | string => {
    switch (k) {
      case 'symbol':  return r.symbol?.toUpperCase() ?? '';
      case 'r1':      return r.r1 ?? Number.NEGATIVE_INFINITY;
      case 'rW':      return r.rW ?? Number.NEGATIVE_INFINITY;
      case 'w':       return r.w ?? 0;
      case 'sleeve':  return (r.sleeve ?? '-').toUpperCase();
      case 'age': {
        const ms = Date.parse(r.asOf ?? '');
        return Number.isFinite(ms) ? ms : Number.NEGATIVE_INFINITY;
      }
    }
  };

  return [...rows].sort((a, b) => {
    const A = val(a, activeKey);
    const B = val(b, activeKey);

    if (typeof A === 'string' || typeof B === 'string') {
      return dirMul * String(A).localeCompare(String(B), 'en', { numeric: true, sensitivity: 'base' });
    }
    if (A < B) return -1 * dirMul;
    if (A > B) return  1 * dirMul;
    return 0;
  });
}, [data, period, sortKey, sortDir]);

  const toggleSort = (k: SortKey) => {
  // block rW only when the column doesn't exist (1D)
  if (period === '1D' && k === 'rW') return;

  const nextDir: SortDir =
    sortKey === k ? (sortDir === 'asc' ? 'desc' : 'asc') : 'desc';

  setSortKey(k);
  setSortDir(nextDir);
};

  /* ===================== RENDER ===================== */
  if (isMobile === null) return null; // wait until decided

  return isMobile ? (
    /* ------------ MOBILE (tabs-only) ------------ */
    <main style={{display:'flex', flexDirection:'column', gap:8, padding:12, minHeight:'100vh', boxSizing:'border-box', background:'var(--bg)', color:'var(--text)'}}>
      {/* mode switch */}
      <div style={{display:'flex', alignItems:'center', gap:8}}>
        <h2 style={{margin:'0 8px 0 0'}}>The DEW Index</h2>
        <span style={{fontSize:12, color:'var(--muted2)'}}>View:</span>
        <select value={viewMode} onChange={e=>setViewMode(e.target.value as 'auto' | 'mobile' | 'desktop')}>
          <option value="auto">Auto</option>
          <option value="mobile">Mobile</option>
          <option value="desktop">Desktop</option>
        </select>
      </div>

      {/* tabs */}
      <div style={{display:'flex', gap:6, marginTop:4}}>
        {(['news','index','dew'] as const).map(k=>(
          <button
            key={k}
            onClick={()=>setTab(k)}
            style={{
              padding:'6px 10px', border:'1px solid var(--border)', borderRadius:6,
              background: tab===k ? 'var(--header-bg)' : 'var(--panel-bg)', fontWeight: tab===k ? 700 : 500
            }}
          >
            {k==='news' ? 'News' : k==='index' ? 'Index' : 'DEW Line'}
          </button>
        ))}
      </div>

      {/* active single panel */}
      <section className="panel" style={{flex:'1 1 auto', minHeight:0}}>
        <div className="panelHeader">
          <h3 style={{margin:0}}>{tab==='news' ? '' : tab==='index' ? 'DEW Index' : 'DEW Line'}</h3>
        </div>
        <div className="panelBody">
          {tab === 'news' && <NewsPanel />}

         {tab === 'index' && (
  <>
    {/* controls */}
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center', marginBottom: 8 }}>
      <label className="lbl">Universe</label>
      <select value={universe} onChange={(e) => setUniverse(e.target.value as Universe)}>
        <option value="auto">auto</option><option value="positions">positions</option>
        <option value="positions+watchlist">positions+watchlist</option><option value="watchlist_only">watchlist</option>
      </select>
      <label className="lbl">Window</label>
      <select value={period} onChange={(e) => setPeriod(e.target.value as Period)}>
        <option value="1D">Day</option><option value="1W">Week</option><option value="1M">Month</option>
        <option value="1Q">Quarter</option><option value="1Y">Year</option><option value="Custom">Custom</option>
      </select>
      {period === 'Custom' && (
        <>
          <input type="date" value={from} onChange={(e) => setFrom(e.target.value)} />
          <input type="date" value={to} onChange={(e) => setTo(e.target.value)} />
        </>
      )}
      <button onClick={() => load(false)} disabled={loading} style={{ padding: '4px 10px' }}>
        {loading ? 'Loading...' : 'Apply'}
      </button>
    </div>

    {/* stat cards */}
    <div style={{ display: 'flex', gap: 12, marginBottom: 8, flexWrap: 'wrap' }}>
      <StatCard title="Index"     day={data?.snapshot?.index?.r1 ?? 0}           win={data?.snapshot?.index?.rW ?? 0}           hideWindow={period==='1D'} />
      <StatCard title="Core"      day={data?.snapshot?.sleeves?.Core?.r1 ?? 0}   win={data?.snapshot?.sleeves?.Core?.rW ?? 0}   hideWindow={period==='1D'} />
      <StatCard title="Satellite" day={data?.snapshot?.sleeves?.Satellite?.r1 ?? 0} win={data?.snapshot?.sleeves?.Satellite?.rW ?? 0} hideWindow={period==='1D'} />
    </div>

    {/* sortable table (mobile) */}
<div className="mTableBox">
  {/* non-scrolling header */}
  <div className="mHead">
  <button onClick={() => toggleSort('symbol')} className={sortKey==='symbol' ? 'isActive' : ''}>
    Ticker {sortKey==='symbol' ? (sortDir==='asc' ? '^' : 'v') : ''}
  </button>

  {period !== '1D' && (
    <button onClick={() => toggleSort('rW')} className={sortKey==='rW' ? 'isActive' : ''}>
      Window {sortKey==='rW' ? (sortDir==='asc' ? '^' : 'v') : ''}
    </button>
  )}

  <button onClick={() => toggleSort('r1')} className={sortKey==='r1' ? 'isActive' : ''}>
    Day {sortKey==='r1' ? (sortDir==='asc' ? '^' : 'v') : ''}
  </button>

  <button onClick={() => toggleSort('age')} className={sortKey==='age' ? 'isActive' : ''}>
    Age {sortKey==='age' ? (sortDir==='asc' ? '^' : 'v') : ''}
  </button>

  <button onClick={() => toggleSort('sleeve')} className={sortKey==='sleeve' ? 'isActive' : ''}>
    Sleeve {sortKey==='sleeve' ? (sortDir==='asc' ? '^' : 'v') : ''}
  </button>
</div>

  {/* scrolling body */}
  <div className="mBody">
    <table className="mTable">
      <tbody>
        {rowsSorted.length === 0 ? (
          <tr><td colSpan={period==='1D' ? 4 : 5} className="empty">No data.</td></tr>
        ) : rowsSorted.map((r) => (
          <tr key={r.symbol}>
            <td>{r.symbol}</td>

            {period !== '1D' && (
              <td className="num">
                {r.rW == null ? '-' : (
                  <span style={{ color: r.rW > 0 ? '#137333' : r.rW < 0 ? '#a50e0e' : '#777', fontWeight: 600 }}>
                    {(r.rW * 100).toFixed(2)}%
                  </span>
                )}
              </td>
            )}

            <td className="num">
              {r.r1 == null ? '-' : (
                <span style={{ color: r.r1 > 0 ? '#137333' : r.r1 < 0 ? '#a50e0e' : '#777', fontWeight: 600 }}>
                  {(r.r1 * 100).toFixed(2)}%
                </span>
              )}
            </td>

            <td>{ageLabel(r.asOf)}</td>
            <td className="sleeve">{r.sleeve ?? '-'}</td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
</div>

<style jsx>{`
  .mTableBox{
    /* fixed-height box with internal scroll; header is separate so no bleed */
    max-height: 60vh;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--panel-bg);
    display: flex;
    flex-direction: column;
    overflow: hidden;         /* clip body under header edge */
  }
  .mHead{
    display: grid;
    grid-template-columns: ${period==='1D' ? '1fr 1fr 1fr 1fr' : '1fr 1fr 1fr 1fr 1fr'};
    gap: 0;
    background: var(--thead-bg);
    color: var(--thead-fg);
    border-bottom: 1px solid var(--border);
  }
  .mHead > button{
    text-align: left;
    padding: 6px 8px;
    background: transparent;
    border: none;
    border-right: 1px solid var(--border);
    font: inherit;
    cursor: pointer;
    user-select: none;
  }
  .mHead > button:last-child{ border-right: 0; }
  .mHead > button.isActive{ font-weight: 700; }

  .mBody{
    flex: 1 1 auto;
    min-height: 0;
    overflow: auto;          /* only rows scroll */
    background: var(--panel-bg);
  }
  .mTable{
    width: 100%;
    border-collapse: separate; /* plays nice with rounded container */
    border-spacing: 0;
    font-size: 14px;
  }
  .mTable td{
    padding: 6px 8px;
    border-top: 1px solid var(--row);
    background: var(--panel-bg);
  }
  .mTable tr:first-child td{ border-top: 0; }
  .mTable td.num{ text-align: left; }
  .mTable td.empty{ color: var(--muted2); padding: 12px; }
  .mTable td.sleeve{ text-align:right; }
  .mHead > button:last-child{ text-align:center; }
`}</style>

  </>
)}

          {tab === 'dew' && (
            <>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
                <button onClick={runDew} disabled={dewRunning} style={{ padding: '4px 10px' }}>
                  {dewRunning ? 'Running...' : '1-800-DEW-LINE'}
                </button>
                <button
                  onClick={copyDew}
                  disabled={!dewText.trim()}
                  style={{ padding: '4px 10px' }}
                  title="Copy full DEW Line output"
                >
                  {copyState === 'copied' ? 'Copied' : copyState === 'error' ? 'Copy failed' : 'Copy'}
                </button>
              </div>
              <div style={{ border: '1px solid var(--border)', borderRadius: 6, padding: 6, background:'var(--panel-bg)' }}>
                {dewText && !dewText.startsWith('DEW Line disabled')
                  ? <div style={{ whiteSpace: 'pre-wrap', minHeight: 320 }}>{dewText}</div>
                  : (
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 12, color: 'var(--muted2)', marginBottom: 6 }}>{dewText || 'Click the image to change'}</div>
                      <Image
                        src={dewImg}
                        alt="DEW placeholder"
                        width={1200}
                        height={900}
                        sizes="100vw"
                        style={{ maxWidth: '100%', height: 'auto', cursor: 'pointer', borderRadius: 4 }}
                        onClick={() => {
                          const n = Math.floor(Math.random() * 55) + 1;
                          const s = n.toString().padStart(2, '0');
                          const next = `/dew/${s}.jpg`;
                          setDewImg(next === dewImg ? '/dew/01.jpg' : next);
                        }}
                      />
                    </div>
                  )
                }
              </div>
            </>
          )}
        </div>
      </section>

    </main>
  ) : (
    /* ------------ DESKTOP (3 columns) ------------ */
    <main className="layout">
      <section style={{ width: `${wNews}%`, minWidth: 260 }}>
        <NewsPanel />
      </section>

      <div className="gutter" onMouseDown={startDrag(1)} />

      <section className="panel" style={{ width: `${wIndex}%` }}>
        <div className="panelHeader" style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center' }}>
          <h2 style={{ margin: 0 }}>DEW Index</h2>
          <label className="lbl">Universe</label>
          <select value={universe} onChange={(e) => setUniverse(e.target.value as Universe)}>
            <option value="auto">auto</option><option value="positions">positions</option>
            <option value="positions+watchlist">positions+watchlist</option><option value="watchlist_only">watchlist</option>
          </select>
          <label className="lbl">Window</label>
          <select value={period} onChange={(e) => setPeriod(e.target.value as Period)}>
            <option value="1D">Day</option><option value="1W">Week</option><option value="1M">Month</option>
            <option value="1Q">Quarter</option><option value="1Y">Year</option><option value="Custom">Custom</option>
          </select>
          {period === 'Custom' && (<><input type="date" value={from} onChange={(e) => setFrom(e.target.value)} />
            <input type="date" value={to} onChange={(e) => setTo(e.target.value)} /></>)}
          <button onClick={()=>load(false)} disabled={loading} style={{ padding: '4px 10px' }}>{loading ? 'Loading...' : 'Apply'}</button>
          <div style={{ fontSize: 12, color: 'var(--muted2)', marginLeft: 'auto' }}>
            {data?.source ? `Source: ${data.source}` : ''}
            {data?.diagnostics ? ` - Coverage ${data.diagnostics.withBars}/${data.diagnostics.requestedSymbols}` : ''}
            {data?.diagnostics?.barsError ? ` - Error: ${data.diagnostics.barsError}` : ''}
          </div>
        </div>

        <div className="panelBody">
          <div style={{ display: 'flex', gap: 16, marginBottom: 8, flexWrap: 'wrap' }}>
            <StatCard title="Index"     day={data?.snapshot?.index?.r1 ?? 0}                     win={data?.snapshot?.index?.rW ?? 0}                     hideWindow={period==='1D'} />
            <StatCard title="Core"      day={data?.snapshot?.sleeves?.Core?.r1 ?? 0}             win={data?.snapshot?.sleeves?.Core?.rW ?? 0}             hideWindow={period==='1D'} />
            <StatCard title="Satellite" day={data?.snapshot?.sleeves?.Satellite?.r1 ?? 0}        win={data?.snapshot?.sleeves?.Satellite?.rW ?? 0}        hideWindow={period==='1D'} />
          </div>

          <div style={{ display: 'flex', gap: 16, marginBottom: 8, flexWrap: 'wrap', fontSize: 12, color: 'var(--muted2)' }}>
            <div><strong>Portfolio:</strong> {fmtUsd(data?.snapshot?.account?.portfolioValue ?? null)}</div>
            <div><strong>Cash:</strong> {fmtUsd(data?.snapshot?.account?.cash ?? null)}</div>
          </div>

          <div style={{ maxHeight: '100%', overflow: 'auto', border: '1px solid var(--border)', borderRadius: 6 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
              <thead>
  <tr className="tblHead">
    <Th label="Ticker"  onSort={() => toggleSort('symbol')}  active={sortKey==='symbol'}  dir={sortDir} />
    {period !== '1D' && (
      <Th label="Window" onSort={() => toggleSort('rW')}      active={sortKey==='rW'}      dir={sortDir} />
    )}
    <Th label="Day"     onSort={() => toggleSort('r1')}      active={sortKey==='r1'}      dir={sortDir} />
    <Th label="Age"     onSort={() => toggleSort('age')}     active={sortKey==='age'}     dir={sortDir} />
<Th label="Weight"  onSort={() => toggleSort('w')}       active={sortKey==='w'}       dir={sortDir} />    
<Th label="Sleeve"  onSort={() => toggleSort('sleeve')}  active={sortKey==='sleeve'}  dir={sortDir} />
    
  </tr>
</thead>

<tbody>
  {rowsSorted.length === 0 ? (
    <tr>
      <td colSpan={period === '1D' ? 6 : 7} style={{ padding: 12, color: 'var(--muted2)' }}>
        No data.
      </td>
    </tr>
  ) : (
    rowsSorted.map((r) => (
      <tr key={r.symbol} style={{ borderTop: '1px solid var(--row)' }}>
        <td style={{ padding: '6px 8px' }}>{r.symbol}</td>
        {period !== '1D' && (
          <td style={{ padding: '6px 8px', color: colorFor(r.rW), fontWeight: 600 }}>{fmtPct(r.rW)}</td>
        )}
        <td style={{ padding: '6px 8px', color: colorFor(r.r1), fontWeight: 600 }}>{fmtPct(r.r1)}</td>
        <td style={{ padding: '6px 8px' }}>{ageLabel(r.asOf)}</td>
       
        <td style={{ padding: '6px 8px', textAlign: 'right' }}>{(r.w * 100).toFixed(2)}</td>
         <td style={{ padding: '6px 8px' }}>{r.sleeve ?? '-'}</td>
      </tr>
    ))
  )}
</tbody>

            </table>
          </div>

          {err ? <div style={{ color: 'var(--neg)', marginTop: 8 }}>Error: {err}</div> : null}
        </div>
      </section>

      <div className="gutter" onMouseDown={startDrag(2)} />

      <section className="panel" style={{ width: `${wDew}%` }}>
        <div className="panelHeader" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <h2 style={{ margin: 0 }}>DEW Line</h2>
          <button onClick={runDew} disabled={dewRunning} style={{ padding: '4px 10px' }}>
            {dewRunning ? 'Running...' : '1-800-DEW-LINE'}
          </button>
          <button
            onClick={copyDew}
            disabled={!dewText.trim()}
            style={{ padding: '4px 10px' }}
            title="Copy full DEW Line output"
          >
            {copyState === 'copied' ? 'Copied' : copyState === 'error' ? 'Copy failed' : 'Copy'}
          </button>
        </div>
        <div className="panelBody">
          {dewText && !dewText.startsWith('DEW Line disabled') ? (
            <div style={{ whiteSpace: 'pre-wrap', border: '1px solid var(--border)', borderRadius: 6, padding: 10, minHeight: 320, background: 'var(--panel-bg)' }}>
              {dewText}
            </div>
          ) : (
            <div style={{ border: '1px solid var(--border)', borderRadius: 6, padding: 6, textAlign: 'center', background: 'var(--panel-bg)' }}>
              <div style={{ fontSize: 12, color: 'var(--muted2)', marginBottom: 6 }}>{dewText || 'Click the image to change'}</div>
              <Image
                src={dewImg}
                alt="DEW placeholder"
                width={1200}
                height={900}
                sizes="100vw"
                style={{ maxWidth: '100%', height: 'auto', cursor: 'pointer', borderRadius: 4 }}
                onClick={() => {
                  const n = Math.floor(Math.random() * 55) + 1;
                  const s = n.toString().padStart(2, '0');
                  const next = `/dew/${s}.jpg`;
                  setDewImg(next === dewImg ? '/dew/01.jpg' : next);
                }}
              />
            </div>
          )}
        </div>
      </section>
<style jsx>{`
  .layout{
    display:flex;
    flex-direction:row;
    flex-wrap:nowrap;           /* never wrap into multiple rows */
    gap: ${GUTTER}px;
    padding:12px;
    box-sizing:border-box;
    height:100vh;               /* so children can stretch */
    align-items:stretch;        /* bottoms line up */
    background:var(--bg);
    color:var(--text);
  }
  .layout > section{
    display:flex;
    flex-direction:column;
    min-width:260px;
    min-height:0;               /* allows internal scrolling */
  }
  .panel{
    display:flex;
    flex-direction:column;
    min-height:0;
    border:1px solid var(--border);
    border-radius:6px;
    background:var(--panel-bg);
  }
  .panelHeader{ padding:8px 10px; border-bottom:1px solid var(--border); background:var(--header-bg); }
  .panelBody{ flex:1 1 auto; min-height:0; overflow:auto; padding:8px; }
  .gutter{ width:${GUTTER}px; flex:0 0 ${GUTTER}px; cursor:col-resize; }
`}</style>

<style jsx global>{`
  :root{
    --bg: #ffffff;
    --text: #111111;
    --panel-bg: #ffffff;
    --border: #e1e1e1;
    --header-bg: #f7f7f7;
    --thead-bg: #e0e0e0;
    --thead-fg: #111111;
    --row: #eeeeee;

    /* critical for colors in the table */
    --muted: #444;
    --muted2: #777;
    --pos: #137333;   /* GREEN */
    --neg: #a50e0e;   /* RED */
  }
  @media (prefers-color-scheme: dark) {
    :root{
      --bg: #0f1115;
      --text: #e6e6e6;
      --panel-bg: #131722;
      --border: #2a2f3a;
      --header-bg: #171c2a;
      --thead-bg: #222a3a;
      --thead-fg: #f2f2f2;
      --row: #1f2533;

      --muted: #cfcfcf;
      --muted2: #a8a8a8;
      --pos: #44c38b; /* GREEN */
      --neg: #ff5c5c; /* RED */
    }
  }
`}</style>

    </main>
  );
}


