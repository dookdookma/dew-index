// app/api/gmail/message/[id]/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { cookies } from 'next/headers';
import { google } from 'googleapis';
import { getOAuth2 } from '@/lib/gmail';
import { loadTokens, clearTokens } from '@/lib/gmailStore';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
const isRec = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object';

type GmailHeader = { name?: string | null; value?: string | null };
type GmailPart = {
  mimeType?: string;
  body?: { data?: string };
  parts?: GmailPart[];
  headers?: GmailHeader[];
};
type GmailApiError = {
  code?: number;
  message?: string;
  response?: { data?: { error?: { message?: string } | string } };
};

/* ---------- helpers (added/rewritten) ---------- */
const b64uToUtf8 = (s: string) =>
  Buffer.from(s.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');

// very small quoted-printable decoder (good enough for newsletters)
function decodeQP(s: string) {
  // soft line breaks
  s = s.replace(/=\r?\n/g, '');
  // hex escapes
  return s.replace(/=([A-Fa-f0-9]{2})/g, (_, h) => String.fromCharCode(parseInt(h, 16)));
}

const wrapPlain = (txt: string) =>
  `<pre style="white-space:pre-wrap;margin:0;font:inherit">${txt
    .replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]!))}</pre>`;

function getHeader(headers: GmailHeader[] | undefined, name: string): string {
  return headers?.find(h => (h.name || '').toLowerCase() === name.toLowerCase())?.value || '';
}

/** recursively find first part matching mime */
function findPartByMime(p: GmailPart | undefined, mime: string): GmailPart | undefined {
  if (!p) return undefined;
  if (p.mimeType === mime && p.body?.data) return p;
  if (Array.isArray(p.parts)) {
    for (const c of p.parts) {
      const hit = findPartByMime(c, mime);
      if (hit) return hit;
    }
  }
  return undefined;
}

/** decode a part's body.data (base64url) + handle quoted-printable remnants */
function decodePart(part: GmailPart): string | null {
  const data = part?.body?.data;
  if (!data) return null;
  let out = b64uToUtf8(data);
  const enc = getHeader(part.headers, 'Content-Transfer-Encoding').toLowerCase();
  // many providers leave qp artifacts even when Gmail already base64-wrapped it
  if (enc.includes('quoted-printable') || /=\r?\n|=[A-Fa-f0-9]{2}/.test(out)) out = decodeQP(out);
  return out;
}

/* ---------- HTML extraction (drop-in signature) ---------- */
function extractHtml(payload: GmailPart | undefined): string | null {
  if (!payload) return null;

  // 1) prefer text/html anywhere in the tree
  const htmlPart = findPartByMime(payload, 'text/html');
  if (htmlPart) {
    const html = decodePart(htmlPart);
    if (html != null) return html;
  }

  // 2) fallback to text/plain (wrap for readability)
  const plainPart = findPartByMime(payload, 'text/plain');
  if (plainPart) {
    const txt = decodePart(plainPart) ?? '';
    return wrapPlain(txt);
  }

  // 3) rare: single body on root
  if (payload.body?.data) {
    const body = b64uToUtf8(payload.body.data);
    return wrapPlain(body);
  }

  return null;
}

/* ---------- route (unchanged except signature to satisfy Next 15) ---------- */
export async function GET(
  _req: NextRequest,
  ctx: { params: Promise<{ id: string }> } // Next 15 expects params as a Promise
) {
  const { id } = await ctx.params;

  const cookieStore = await cookies();
const email = cookieStore.get('gmail_user')?.value || '';


  if (!email) {
    const o = getOAuth2();
    const authUrl = o.generateAuthUrl({
      access_type: 'offline',
      prompt: 'consent',
      scope: ['https://www.googleapis.com/auth/gmail.readonly', 'openid', 'email'],
    });
    return NextResponse.json({ ok: false, reason: 'no_gmail_user', authUrl }, { status: 401 });
  }

  const tokens = await loadTokens(email);
  if (!tokens) {
    const o = getOAuth2();
    const authUrl = o.generateAuthUrl({
      access_type: 'offline',
      prompt: 'consent',
      scope: ['https://www.googleapis.com/auth/gmail.readonly', 'openid', 'email'],
    });
    return NextResponse.json({ ok: false, reason: 'no_tokens', authUrl }, { status: 401 });
  }

  const o = getOAuth2();
  o.setCredentials(tokens);

  try {
    const gmail = google.gmail({ version: 'v1', auth: o });
    const m = await gmail.users.messages.get({ userId: 'me', id, format: 'full' });

    const payload = (m.data.payload || {}) as GmailPart;
    const headers = Array.isArray(payload.headers) ? payload.headers : [];
    const subject = getHeader(headers, 'Subject') || '(no subject)';
    const from = getHeader(headers, 'From') || 'Gmail';
    const dateStr = getHeader(headers, 'Date');
    const date = Date.parse(dateStr) || Number(m.data.internalDate) || Date.now();
    const html = extractHtml(payload) || '<em>(no content)</em>';

    return NextResponse.json({
      ok: true,
      id,
      subject,
      from,
      date,
      html,
    });
  
} catch (e: unknown) {
  const err = (isRec(e) ? e : {}) as GmailApiError;
  const respError = err.response?.data?.error;
  const responseMessage =
    typeof respError === 'string'
      ? respError
      : (isRec(respError) && typeof respError.message === 'string' ? respError.message : '');
  const msg = String(responseMessage || err.message || 'gmail_error').toLowerCase();

  if (msg.includes('invalid_grant') || err.code === 401) {
    // Only clear tokens for true invalid_grant. (Keeps users signed in on transient 401s.)
    if (msg.includes('invalid_grant')) {
      await clearTokens(email);
    }
    const authUrl = getOAuth2().generateAuthUrl({
      access_type: 'offline',
      prompt: 'consent',
      scope: ['https://www.googleapis.com/auth/gmail.readonly', 'openid', 'email'],
    });
    return NextResponse.json({ ok: false, reason: 'invalid_grant', authUrl }, { status: 401 });
  }

  return NextResponse.json({ ok: false, reason: msg }, { status: 500 });
}
}
