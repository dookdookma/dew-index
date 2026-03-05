// app/api/debug-apca/route.ts
import { NextResponse } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  const key = process.env.APCA_API_KEY_ID;
  const sec = process.env.APCA_API_SECRET_KEY;
  const envOk = Boolean(key && sec);

  let probe: unknown = null;
  let error: string | null = null;
  let status: number | null = null;

  if (envOk) {
    try {
      const u = new URL('https://data.alpaca.markets/v2/stocks/bars');
      u.searchParams.set('symbols', 'SMH,VRT');
      u.searchParams.set('timeframe', '1Day');
      u.searchParams.set('limit', '1');
      u.searchParams.set('feed', 'iex');

      const r = await fetch(u.toString(), {
        headers: {
          'APCA-API-KEY-ID': key as string,
          'APCA-API-SECRET-KEY': sec as string,
        },
        cache: 'no-store',
      });
      status = r.status;
      const txt = await r.text();
      try { probe = JSON.parse(txt); } catch { probe = txt; }
    } catch (e: unknown) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  return NextResponse.json({
    envOk,
    status,
    error,
    sample: probe,
  });
}
