// app/api/weather/route.ts

import { NextRequest, NextResponse } from 'next/server';

export const runtime = 'edge';
export const dynamic = 'force-dynamic';

// Simple weather-code -> description map (trim as you like)
const WMO: Record<number, string> = {
  0: 'Clear',
  1: 'Mainly clear',
  2: 'Partly cloudy',
  3: 'Overcast',
  45: 'Fog',
  48: 'Depositing rime fog',
  51: 'Drizzle (light)',
  53: 'Drizzle (moderate)',
  55: 'Drizzle (dense)',
  61: 'Rain (light)',
  63: 'Rain (moderate)',
  65: 'Rain (heavy)',
  66: 'Freezing rain (light)',
  67: 'Freezing rain (heavy)',
  71: 'Snow (light)',
  73: 'Snow (moderate)',
  75: 'Snow (heavy)',
  77: 'Snow grains',
  80: 'Rain showers (slight)',
  81: 'Rain showers (moderate)',
  82: 'Rain showers (violent)',
  85: 'Snow showers (slight)',
  86: 'Snow showers (heavy)',
  95: 'Thunderstorm',
  96: 'Thunderstorm w/ hail (slight)',
  99: 'Thunderstorm w/ hail (heavy)',
};

export async function GET(req: NextRequest) {
  try {
    const url = new URL(req.url);

    // Allow optional overrides; default to Midtown Manhattan
    const lat = Number(url.searchParams.get('lat') ?? '40.758');
    const lon = Number(url.searchParams.get('lon') ?? '-73.9855');
    const label = url.searchParams.get('label') ?? 'NYC';

    // We’ll use:
    // - current: temperature_2m, weather_code
    // - hourly: precipitation_probability (to get “current” PoP)
    // - daily: temperature_2m_max/min (for hi/lo)
    const q = new URL('https://api.open-meteo.com/v1/forecast');
    q.searchParams.set('latitude', String(lat));
    q.searchParams.set('longitude', String(lon));
    q.searchParams.set('temperature_unit', 'fahrenheit');
    q.searchParams.set('timezone', 'auto');
    q.searchParams.set('current', 'temperature_2m,weather_code');
    q.searchParams.set('hourly', 'precipitation_probability,temperature_2m');
    q.searchParams.set('daily', 'temperature_2m_max,temperature_2m_min');

    const r = await fetch(q.toString(), { cache: 'no-store' });
    if (!r.ok) {
      return NextResponse.json({ ok: false, error: `open-meteo ${r.status}` }, { status: r.status });
    }
    const j = await r.json();

    // Current temp (already Fahrenheit due to temperature_unit=fahrenheit)
    const currTempF: number | undefined = j?.current?.temperature_2m;
    const wcode: number | undefined = j?.current?.weather_code;
    const desc = typeof wcode === 'number' && WMO[wcode] ? WMO[wcode] : '';

    // Daily hi/lo (F because of the unit param)
    const hiF: number | undefined = Array.isArray(j?.daily?.temperature_2m_max) ? j.daily.temperature_2m_max[0] : undefined;
    const loF: number | undefined = Array.isArray(j?.daily?.temperature_2m_min) ? j.daily.temperature_2m_min[0] : undefined;

    // "Current" precip probability: choose the hourly PoP closest to current time index
    let pop: number | undefined = undefined;
    if (Array.isArray(j?.hourly?.time) && Array.isArray(j?.hourly?.precipitation_probability)) {
      const times: string[] = j.hourly.time;
      const pops: (number | null)[] = j.hourly.precipitation_probability;

      // Find index matching current.hour (exact string match best effort)
      const currISO: string | undefined = j?.current?.time;
      let idx = -1;
      if (currISO) idx = times.indexOf(currISO);

      // Fallback: nearest by time difference if exact match not found
      if (idx === -1 && currISO) {
        const now = Date.parse(currISO);
        let best = Number.POSITIVE_INFINITY;
        for (let i = 0; i < times.length; i++) {
          const dt = Math.abs(Date.parse(times[i]) - now);
          if (dt < best) { best = dt; idx = i; }
        }
      }

      if (idx >= 0 && idx < pops.length) {
        const v = pops[idx];
        if (typeof v === 'number' && Number.isFinite(v)) pop = v;
      }
    }

    // Build response (keep top-level fields and nested "now" for compatibility)
    const payload = {
      ok: true,
      label,
      nowF: typeof currTempF === 'number' ? currTempF : undefined,
      hiF: typeof hiF === 'number' ? hiF : undefined,
      loF: typeof loF === 'number' ? loF : undefined,
      pop: typeof pop === 'number' ? pop : undefined,
      desc,
      now: {
        temp: typeof currTempF === 'number' ? currTempF : undefined,
        hi: typeof hiF === 'number' ? hiF : undefined,
        lo: typeof loF === 'number' ? loF : undefined,
        pop: typeof pop === 'number' ? pop : undefined,
        desc,
        updated: Date.now(),
      },
      updated: Date.now(),
    };

    return NextResponse.json(payload, { headers: { 'Cache-Control': 'no-store' } });
  } catch (e: unknown) {
    const detail = e instanceof Error ? e.message : String(e);
    return NextResponse.json(
      { ok: false, error: 'weather_failed', detail },
      { status: 500 }
    );
  }
}
