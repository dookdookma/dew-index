import { NextRequest, NextResponse } from 'next/server';
import { getRedis } from '@/lib/kv';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

async function checkLibrary(baseUrl: string): Promise<{ ok: boolean; status?: number; error?: string }> {
  const c = new AbortController();
  const t = setTimeout(() => c.abort(), 8000);
  try {
    const r = await fetch(`${baseUrl.replace(/\/$/, '')}/openapi.json`, { cache: 'no-store', signal: c.signal });
    return { ok: r.ok, status: r.status };
  } catch (e: any) {
    return { ok: false, error: e?.message || 'fetch failed' };
  } finally {
    clearTimeout(t);
  }
}

export async function GET(req: NextRequest) {
  try {
    const redis = getRedis();
    const latest = await redis.get<any>('cache:daily:latest');
    const state = await redis.get<any>('cache:state');

    const origin = req.nextUrl.origin;
    const pm = await fetch(`${origin}/api/polymarket-debug?limit=150`, { cache: 'no-store' })
      .then((r) => (r.ok ? r.json() : null))
      .catch(() => null);

    const libBase = process.env.DEW_LIBRARY_URL || 'https://dew-index-production.up.railway.app';
    const lib = await checkLibrary(libBase);

    return NextResponse.json({
      ok: true,
      generatedAt: new Date().toISOString(),
      library: {
        base: libBase,
        ...lib,
      },
      cache: {
        hasLatest: !!latest,
        lastDaily: state?.lastDaily || null,
        lastBuiltAt: state?.lastBuiltAt || null,
        counts: latest ? {
          headlines: latest?.inputs?.headlines?.length ?? 0,
          commentary: latest?.inputs?.commentary?.length ?? 0,
          timeline: latest?.inputs?.timeline?.length ?? 0,
          timelineCache: latest?.inputs?.timelineCache?.length ?? 0,
          signals: latest?.signals?.length ?? 0,
          passAll: latest?.markets?.passAllTotal ?? 0,
        } : null,
      },
      polymarket: pm?.summary || null,
    });
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e?.message || 'ops health failed' }, { status: 500 });
  }
}
