//app/api/kv-who/route.ts
import { NextResponse } from 'next/server';
import { getRedis } from '@/lib/kv';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  const r = getRedis();
  if (!r) return NextResponse.json({ ok:false, reason:'no_client' }, { status:500 });
  try {
    const key = 'kv:probe';
    await r.set(key, { t: Date.now() }, { ex: 30 });
    const val = await r.get(key);
    return NextResponse.json({ ok:true, wrote:!!val });
  } catch (e: unknown) {
    const err = e instanceof Error ? e.message : 'fail';
    return NextResponse.json({ ok:false, err }, { status:500 });
  }
}
