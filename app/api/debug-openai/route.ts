import { NextResponse } from 'next/server';
export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  const raw = process.env.OPENAI_API_KEY ?? '';
  const trimmed = raw.trim();
  return NextResponse.json({
    keyOk: !!trimmed,
    len: trimmed.length,          // do not log the key itself
    hasWhitespace: raw.length !== trimmed.length
  });
}
