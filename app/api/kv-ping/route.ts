// app/api/kv-ping/route.ts
import { NextResponse } from 'next/server';
import { getRedis, kvDiag } from '@/lib/kv';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const diag = kvDiag();
    const up = getRedis();
    if (!up) {
      return NextResponse.json({ ok:false, reason:'no_redis', diag }, { status:500 });
    }
    const key = 'dew:test:' + Date.now();
    await up.set(key, 'ok', { ex: 60 });
    const val = await up.get<string>(key);
    return NextResponse.json({ ok:true, diag, roundtrip: val === 'ok' });
  } catch (e: unknown) {
    const reason = e instanceof Error ? e.message : 'err';
    return NextResponse.json({ ok:false, reason }, { status:500 });
  }
}
