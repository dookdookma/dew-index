import { NextResponse } from 'next/server';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  const dir = path.join(process.cwd(), 'cache', 'daily');
  try {
    const files = (await readdir(dir)).filter((f) => f.endsWith('.json')).sort();
    if (!files.length) return NextResponse.json({ ok: false, error: 'no daily cache' }, { status: 404 });
    const latest = files[files.length - 1];
    const raw = await readFile(path.join(dir, latest), 'utf-8');
    return NextResponse.json({ ok: true, latest, data: JSON.parse(raw) });
  } catch {
    return NextResponse.json({ ok: false, error: 'cache read failed' }, { status: 500 });
  }
}
