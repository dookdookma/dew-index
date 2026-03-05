// app/api/asp/route.ts
import { NextRequest, NextResponse } from 'next/server';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';
import customParseFormat from 'dayjs/plugin/customParseFormat';

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(customParseFormat);

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

type ASPView = 'today'|'tomorrow'|'7day';
type ASPItem = { date: string; status: string; notes?: string };
type StatusValue = 'Suspended' | 'In Effect' | 'Not In Effect' | 'Unknown';
type ExternalStatus = { status: StatusValue; notes?: string };
const isRec = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object';

type SuspensionMap = Map<string, { notes?: string }>;
let suspensionCache: { expires: number; data: SuspensionMap } | null = null;

const TZ = 'America/New_York';
const DEFAULT_NOTES = 'Street cleaning + meter rules remain in effect unless NYC issues an emergency suspension.';
const NYCASP_ENDPOINTS: Record<'today'|'tomorrow'|'7day', string | undefined> = {
  today: process.env.ASP_TODAY_URL,
  tomorrow: process.env.ASP_TOMORROW_URL,
  '7day': process.env.ASP_7DAY_URL,
};
const NYC311_CALENDAR_URL = process.env.NYC_311_CALENDAR_URL || 'https://api.nyc.gov/public/api/GetCalendar';
const NYC311_API_KEY = (process.env.NYC_311_API_KEY || process.env.NYC_311_SUBSCRIPTION_KEY || '').trim();

function viewOffsets(view: ASPView): number[] {
  return view === '7day'
    ? Array.from({ length: 7 }, (_, i) => i)
    : [view === 'tomorrow' ? 1 : 0];
}

function yearsToFetch(): number[] {
  const now = dayjs().tz(TZ);
  const year = now.year();
  return Array.from(new Set([year - 1, year, year + 1])).filter(y => y >= 2020);
}

async function fetchIcs(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

function unfoldIcs(text: string) {
  return text.replace(/\r/g, '').replace(/\n[ \t]/g, '');
}

function decodeIcsText(input: string) {
  return input
    .replace(/\\n/g, ' ')
    .replace(/\\,/g, ',')
    .replace(/\\;/g, ';')
    .replace(/\s+/g, ' ')
    .trim();
}

function addSuspension(block: string, map: SuspensionMap) {
  const startMatch = block.match(/DTSTART[^:]*:(\d{8})/);
  if (!startMatch) return;
  const endMatch = block.match(/DTEND[^:]*:(\d{8})/);
  const descMatch = block.match(/DESCRIPTION:(.*)/);
  const rawNotes = descMatch ? decodeIcsText(descMatch[1]) : undefined;

  const start = dayjs.tz(startMatch[1], 'YYYYMMDD', TZ);
  const end = endMatch ? dayjs.tz(endMatch[1], 'YYYYMMDD', TZ) : start.add(1, 'day');
  if (!start.isValid() || !end.isValid()) return;

  for (let cursor = start; cursor.isBefore(end); cursor = cursor.add(1, 'day')) {
    map.set(cursor.format('YYYY-MM-DD'), { notes: rawNotes });
  }
}

async function loadSuspensions(): Promise<SuspensionMap> {
  if (suspensionCache && suspensionCache.expires > Date.now()) return suspensionCache.data;

  const map: SuspensionMap = new Map();
  for (const year of yearsToFetch()) {
    const url = `https://www.nyc.gov/html/dot/downloads/misc/${year}-alternate-side.ics`;
    const ics = await fetchIcs(url);
    if (!ics) continue;
    const unfolded = unfoldIcs(ics);
    const blocks = unfolded.split('BEGIN:VEVENT').slice(1);
    for (const block of blocks) addSuspension(block.split('END:VEVENT')[0], map);
  }

  suspensionCache = { data: map, expires: Date.now() + 6 * 60 * 60 * 1000 };
  return map;
}

const normalizeStatus = (input?: string | null): StatusValue | null => {
  if (!input) return null;
  const v = input.toLowerCase();
  if (v.includes('not in effect') || v.includes('suspend')) return 'Suspended';
  if (v.includes('in effect')) return 'In Effect';
  return null;
};

async function fetchOfficialCalendar(view: ASPView): Promise<Map<string, ExternalStatus> | null> {
  const offsets = viewOffsets(view);
  const base = dayjs().tz(TZ).startOf('day');
  const minOffset = Math.min(...offsets);
  const maxOffset = Math.max(...offsets);
  const start = base.add(minOffset, 'day');
  const end = base.add(maxOffset, 'day');

  try {
    const url = new URL(NYC311_CALENDAR_URL);
    url.searchParams.set('fromdate', start.format('YYYYMMDD'));
    url.searchParams.set('todate', end.format('YYYYMMDD'));
    if (NYC311_API_KEY) {
      url.searchParams.set('subscription-key', NYC311_API_KEY);
    }

    const headers: Record<string, string> = { Accept: 'application/json' };
    if (NYC311_API_KEY) headers['Ocp-Apim-Subscription-Key'] = NYC311_API_KEY;

    const res = await fetch(url.toString(), { headers, cache: 'no-store' });
    if (!res.ok) return null;
    const body = await res.json();
    const days = Array.isArray(body?.days) ? body.days : [];
    const map = new Map<string, ExternalStatus>();

    for (const day of days) {
      const tidRaw = day?.today_id;
      const tid = typeof tidRaw === 'string' ? tidRaw : (typeof tidRaw === 'number' ? String(tidRaw) : '');
      if (!tid) continue;
      const parsed = dayjs.tz(tid, 'YYYYMMDD', TZ);
      if (!parsed.isValid()) continue;
      const iso = parsed.format('YYYY-MM-DD');
      const items = Array.isArray(day?.items) ? day.items : [];
      const aspItem = items.find((item: unknown) => {
        const rec = isRec(item) ? item : {};
        return String(rec.type ?? '').toLowerCase().includes('alternate side parking');
      });
      if (!aspItem) continue;

      const aspRec = isRec(aspItem) ? aspItem : {};
      const status = normalizeStatus(typeof aspRec.status === 'string' ? aspRec.status : '') || 'Unknown';
      const exceptionName = typeof aspRec.exceptionName === 'string' ? aspRec.exceptionName.trim() : '';
      const details = typeof aspRec.details === 'string' ? aspRec.details.trim() : '';
      const notes = [exceptionName, details].filter(Boolean).join(' - ') || undefined;
      map.set(iso, { status, notes });
    }

    return map.size ? map : null;
  } catch {
    return null;
  }
}

type NycaspResponse = { status?: string; notes?: string }[];

async function fetchNycasp(view: ASPView): Promise<NycaspResponse | null> {
  const url = NYCASP_ENDPOINTS[view];
  if (!url) return null;
  try {
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) return null;
    const body = await res.json().catch(() => null);
    if (!body) return null;
    const arr = Array.isArray(body?.items) ? body.items : (Array.isArray(body) ? body : []);
    return arr.map((x: unknown) => {
      const rec = isRec(x) ? x : {};
      return {
        status: typeof rec.status === 'string'
          ? rec.status
          : (typeof rec.aspStatus === 'string' ? rec.aspStatus : (typeof rec.aspRules === 'string' ? rec.aspRules : undefined)),
        notes: typeof rec.notes === 'string' ? rec.notes : undefined,
      };
    });
  } catch {
    return null;
  }
}

type ManualOverride = { status: StatusValue; notes?: string; from?: dayjs.Dayjs; to?: dayjs.Dayjs };

function loadManualOverride(): ManualOverride | null {
  const raw = (process.env.ASP_MANUAL_OVERRIDE || '').trim();
  if (!raw) return null;
  let parsed: unknown = raw;
  try {
    parsed = JSON.parse(raw);
  } catch {
    parsed = { status: raw };
  }
  const rec = isRec(parsed) ? parsed : {};
  const statusRaw = rec.status;
  const status = normalizeStatus(typeof statusRaw === 'string' ? statusRaw : null)
    || (statusRaw ? String(statusRaw).trim() as StatusValue : null);
  if (!status || status === 'Unknown') return null;
  const from = rec.from ? dayjs(String(rec.from)).tz(TZ) : undefined;
  const to = rec.to ? dayjs(String(rec.to)).tz(TZ) : undefined;
  return {
    status,
    notes: typeof rec.notes === 'string' ? rec.notes : undefined,
    from,
    to,
  };
}

type OverrideSource =
  | { kind: '311'; data: Map<string, ExternalStatus> }
  | { kind: 'nycasp'; data: NycaspResponse }
  | { kind: 'manual'; data: ManualOverride }
  | null;

function determineOverrideForDay(
  source: OverrideSource,
  cursor: dayjs.Dayjs,
  index: number,
): ExternalStatus | null {
  if (!source) return null;
  if (source.kind === '311') {
    const iso = cursor.format('YYYY-MM-DD');
    return source.data.get(iso) || null;
  }
  if (source.kind === 'nycasp') {
    const row = source.data[index];
    if (!row) return null;
    const status = normalizeStatus(row.status) || (row.status ? (row.status as StatusValue) : null);
    if (!status || status === 'Unknown') return null;
    return { status, notes: row.notes };
  }
  if (source.kind === 'manual') {
    const { from, to } = source.data;
    const afterFrom = !from || !cursor.isBefore(from.startOf('day'));
    const beforeTo = !to || !cursor.isAfter(to.endOf('day'));
    if (afterFrom && beforeTo) {
      return { status: source.data.status, notes: source.data.notes || 'Manual override' };
    }
  }
  return null;
}

function buildItems(
  view: ASPView,
  suspensions: SuspensionMap,
  override: OverrideSource,
): ASPItem[] {
  const base = dayjs().tz(TZ).startOf('day');
  const offsets = viewOffsets(view);

  return offsets.map((offset, idx) => {
    const cursor = base.add(offset, 'day');
    const iso = cursor.format('YYYY-MM-DD');
    const pretty = cursor.format('ddd, MMM D');
    const weekday = cursor.format('dddd');
    const suspension = suspensions.get(iso);

    let item: ASPItem;
    if (suspension) {
      item = {
        date: pretty,
        status: 'Suspended',
        notes: suspension.notes || 'Scheduled holiday suspension.',
      };
    } else if (weekday === 'Sunday') {
      item = {
        date: pretty,
        status: 'Not In Effect',
        notes: 'ASP rules are always suspended on Sundays; meters are also waived.',
      };
    } else {
      item = {
        date: pretty,
        status: 'In Effect',
        notes: DEFAULT_NOTES,
      };
    }

    const overrideStatus = determineOverrideForDay(override, cursor, idx);
    if (overrideStatus && overrideStatus.status) {
      item = {
        date: pretty,
        status: overrideStatus.status,
        notes: overrideStatus.notes || item.notes,
      };
    }
    return item;
  });
}

export async function GET(req: NextRequest) {
  try {
    const urlObj = new URL(req.url);
    const view = (urlObj.searchParams.get('view') || 'today') as 'today'|'tomorrow'|'7day';
    const suspensions = await loadSuspensions();

    let override: OverrideSource = null;
    const cal311 = await fetchOfficialCalendar(view);
    if (cal311) override = { kind: '311', data: cal311 };
    else {
      const nycasp = await fetchNycasp(view);
      if (nycasp) override = { kind: 'nycasp', data: nycasp };
      else {
        const manual = loadManualOverride();
        if (manual) override = { kind: 'manual', data: manual };
      }
    }

    const items = buildItems(view, suspensions, override);
    return NextResponse.json({ items, updated: Date.now(), source: override?.kind ?? 'schedule' });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'asp_error';
    return NextResponse.json({ items: [], updated: Date.now(), error: message }, { status: 200 });
  }
}
