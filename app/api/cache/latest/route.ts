import { NextResponse } from 'next/server';
import { getRedis } from '@/lib/kv';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const redis = getRedis();
    const latest = await redis.get<any>('cache:daily:latest');
    if (!latest) return NextResponse.json({ ok: false, error: 'no daily cache' }, { status: 404 });
    return NextResponse.json({ ok: true, data: latest });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e?.message || 'cache read failed' }, { status: 500 });
  }
}
