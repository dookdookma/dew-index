// app/api/gmail/callback/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { google } from 'googleapis';
import { getOAuth2, getOriginFromReq } from '@/lib/gmail';
import { getRedis } from '@/lib/kv'; // your working Upstash helper

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

type GmailTokens = {
  access_token?: string; refresh_token?: string; id_token?: string;
  scope?: string; token_type?: string; expiry_date?: number;
};
const isRec = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object';

export async function GET(req: NextRequest) {
  const url = new URL(req.url);
  const code = url.searchParams.get('code');
  if (!code) return NextResponse.json({ error:'callback_failed', message:'missing_code' }, { status:400 });

  try {
    // Build OAuth client with the SAME redirect for this host
    const origin = getOriginFromReq(req.url, req.headers);
    const o = getOAuth2(origin);

    // Exchange (fails with invalid_grant if code reused OR redirect mismatch)
    const { tokens } = await o.getToken(code);
    o.setCredentials(tokens);

    // Identify user
    const oauth2 = google.oauth2({ version: 'v2', auth: o });
    const me = await oauth2.userinfo.get();
    const email = String(me.data.email || '').toLowerCase();
    if (!email) return NextResponse.json({ error:'callback_failed', message:'no_email' }, { status:400 });

    // Normalize
    const norm = (x: string | null | undefined) => (x ?? undefined);
    const clean: GmailTokens = {
      access_token:  norm(tokens.access_token),
      refresh_token: norm(tokens.refresh_token),
      id_token:      norm(tokens.id_token),
      scope:         tokens.scope ?? undefined,
      token_type:    tokens.token_type ?? undefined,
      expiry_date:   typeof tokens.expiry_date === 'number' ? tokens.expiry_date : undefined,
    };

    // Store (you verified Upstash already)
    const r = getRedis();
    if (!r) return NextResponse.json({ error:'callback_failed', message:'kv_null' }, { status:500 });
    await r.set(`gmail:tokens:${email}`, clean);

    const res = NextResponse.redirect(new URL('/', req.url));
    res.cookies.set('gmail_user', email, {
  httpOnly: true,
  secure: true,          // required on Vercel
  sameSite: 'lax',
  path: '/',
  maxAge: 60 * 60 * 24 * 180, // 180 days
});
    return res;
  } catch (e: unknown) {
    const err = isRec(e) ? e : {};
    const message = typeof err.message === 'string' ? err.message : 'invalid_grant';
    const response = isRec(err.response) ? err.response : {};
    const detail = response.data ?? null;
    return NextResponse.json({ error:'callback_failed', message, detail }, { status:500 });
  }
}
