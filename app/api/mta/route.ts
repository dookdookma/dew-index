// app/api/mta/route.ts

import { NextResponse } from 'next/server';
import * as cheerio from 'cheerio';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const revalidate = 0;

type Line = { route:string; status:string; why:string };
type DiagBlock = { url: string | null; ok?: boolean; code?: number; lines?: number; entities?: number };
type MtaDiag = {
  t: number;
  statusJson: DiagBlock;
  alerts?: DiagBlock;
  scrape?: DiagBlock;
  statusJsonErr?: string;
  alertsErr?: string;
  scrapeErr?: string;
};
const isRec = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object';
const asString = (v: unknown) => (typeof v === 'string' ? v : '');
const errMsg = (e: unknown) => (e instanceof Error ? e.message : String(e));

function fromStatusJSON(j: unknown): Line[] {
  const root = isRec(j) ? j : {};
  const subway = isRec(root.subway) ? root.subway : {};
  const linesRaw = Array.isArray(root.lines) ? root.lines : (Array.isArray(subway.lines) ? subway.lines : []);
  const out: Line[] = [];
  for (const raw of linesRaw) {
    const L = isRec(raw) ? raw : {};
    const route = String(L.name ?? L.route ?? L.id ?? '').toUpperCase().trim();
    const status = String(L.status ?? L.state ?? '').trim();
    const why = String(L.text ?? L.summary ?? L.reason ?? '').replace(/\s+/g,' ').trim();
    if (!route) continue;
    if (!status || /^good/i.test(status)) continue;
    out.push({ route, status, why });
  }
  const rank = (s:string)=>['No service','Major delays','Delays','Reduced','Modified','Detour','Stop moved','Advisory','Extra'].indexOf(s);
  const best: Record<string,Line> = {};
  for (const x of out) {
    const p = best[x.route];
    if (!p || rank(x.status) < rank(p.status)) best[x.route] = x;
  }
  return Object.values(best).sort((a,b)=>a.route.localeCompare(b.route));
}

export async function GET() {
  const diag: MtaDiag = { t: Date.now(), statusJson: { url: null } };

  // 1) STATUS JSON (no key)
  const statusUrl = process.env.MTA_STATUS_URL?.trim();
  if (statusUrl) {
    try {
      const r = await fetch(statusUrl, { cache:'no-store' });
      diag.statusJson = { url: statusUrl, ok: r.ok, code: r.status };
      if (r.ok) {
        const j = await r.json();
        const lines = fromStatusJSON(j);
        diag.statusJson.lines = lines.length;
        if (lines.length) return NextResponse.json({ updated: Date.now(), source:'status', lines, diag });
      }
    } catch (e: unknown) {
      diag.statusJsonErr = errMsg(e);
    }
  }

  // 2) GTFS alerts without key (reworked)
try {
  const url = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json';
  const r = await fetch(url, { cache:'no-store', headers:{ 'user-agent':'dew-index/1.0' } });
  diag.alerts = { url, ok: r.ok, code: r.status };
  if (r.ok) {
    const j = await r.json();
    const root = isRec(j) ? j : {};
    const ents = Array.isArray(root.entity) ? root.entity : [];
    diag.alerts.entities = ents.length;

    const toLabel = (effect:string) => {
      const E = (effect||'').toUpperCase();
      if (E.includes('NO_SERVICE')) return 'No service';
      if (E.includes('SIGNIFICANT_DELAYS')) return 'Major delays';
      if (E.includes('DELAY')) return 'Delays';
      if (E.includes('REDUCED')) return 'Reduced';
      if (E.includes('MODIFIED')) return 'Modified';
      if (E.includes('DETOUR')) return 'Detour';
      if (E.includes('STOP_MOVED')) return 'Stop moved';
      if (E.includes('ADDITIONAL_SERVICE') || E.includes('NO_EFFECT')) return 'Info';
      return 'Advisory';
    };

    const txt = (x: unknown) => {
      const obj = isRec(x) ? x : {};
      const list = Array.isArray(obj.translation) ? obj.translation : [];
      const first = list.length > 0 && isRec(list[0]) ? list[0] : {};
      return asString(first.text).trim();
    };
    const problemHint = (s:string) => /delay|detour|reduc|modif|suspend|no\s+service|stopp?ed|skipp|rerout|signal|congestion|police|track|work|weather/i.test(s);

    type Line = { route:string; status:string; why:string };
    const pick: Record<string, Line> = {};
    const rank = (s:string)=>['No service','Major delays','Delays','Reduced','Modified','Detour','Stop moved','Advisory','Info'].indexOf(s);

    for (const e of ents) {
      const ent = isRec(e) ? e : {};
      const a = isRec(ent.alert) ? ent.alert : null;
      if (!a) continue;
      const status = toLabel(asString(a.effect));
      if (status === 'Info') continue; // ignore pure informational

      const why = (txt(a.header_text) || txt(a.description_text) || '').replace(/\s+/g,' ').trim();
      if (status === 'Advisory' && !problemHint(why)) continue; // keep only problem-like advisories

      const informed = Array.isArray(a.informed_entity) ? a.informed_entity : [];
      const routes = informed
        .map((ie: unknown) => {
          const rec = isRec(ie) ? ie : {};
          const trip = isRec(rec.trip) ? rec.trip : {};
          return rec.route_id ?? trip.route_id;
        })
        .filter((s): s is string | number => typeof s === 'string' || typeof s === 'number')
        .map((s) => String(s).toUpperCase());

      const targets = routes.length ? routes : ['SYSTEM'];
      for (const route of targets) {
        const cur = { route, status, why };
        const prev = pick[route];
        if (!prev || rank(status) < rank(prev.status)) pick[route] = cur;
      }
    }

    const lines = Object.values(pick).sort((a,b)=> a.route.localeCompare(b.route));
    diag.alerts.lines = lines.length;
    if (lines.length) return NextResponse.json({ updated: Date.now(), source:'alerts', lines, diag });
  }
} catch (e: unknown) { diag.alertsErr = errMsg(e); }

  // 3) Scrape fallback
  try {
    const url = 'https://new.mta.info/';
    const r = await fetch(url, { headers:{'user-agent':'dew-index/1.0'}, cache:'no-store' });
    diag.scrape = { url, ok: r.ok, code: r.status };
    if (r.ok) {
      const html = await r.text();
      const $ = cheerio.load(html);
      const lines: Line[] = [];
      $('[data-service-status] .service-status__line').each((_, el) => {
        const route = $(el).find('.service-status__badge').text().trim().toUpperCase();
        const status = $(el).find('.service-status__status').text().trim();
        const why = $(el).find('.service-status__description').text().replace(/\s+/g,' ').trim();
        if (route && status && !/^good/i.test(status)) lines.push({ route, status, why });
      });
      diag.scrape.lines = lines.length;
      if (lines.length) return NextResponse.json({ updated: Date.now(), source:'scrape', lines, diag });
    }
  } catch (e: unknown) {
    diag.scrapeErr = errMsg(e);
  }

  return NextResponse.json({ updated: Date.now(), source:'none', lines: [], error:'unavailable', diag }, { status: 200 });
}
