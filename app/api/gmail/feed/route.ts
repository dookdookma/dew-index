// app/api/gmail/feed/route.ts
import { NextRequest, NextResponse } from 'next/server';
import { google, gmail_v1 } from 'googleapis';
import { getOAuth2 } from '@/lib/gmail';
import { loadTokens } from '@/lib/gmailStore';
import { ALLOW_SENDERS } from '@/data/gmailAllowList';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
const isRec = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object';

type NewsItem = { title: string; link: string; ts: number; pub?: string };

// Build allow-list set (lowercased, trimmed, non-empty)
const ALLOW = new Set(
  (ALLOW_SENDERS || [])
    .map(s => String(s || '').trim().toLowerCase())
    .filter(Boolean)
);

// Pull email addr from `From:` header (`Name <addr>` → `addr`), lowercase
function parseEmailAddress(raw: string | undefined | null): string {
  if (!raw) return '';
  const m = String(raw).match(/<([^>]+)>/);
  const addr = (m ? m[1] : raw).replace(/^"|"$|^'|'$/g, '').trim().toLowerCase();
  return addr;
}

// Header helper
function getHeader(hdrs: gmail_v1.Schema$MessagePartHeader[] | undefined, name: string): string {
  if (!hdrs) return '';
  const h = hdrs.find(x => (x.name || '').toLowerCase() === name.toLowerCase());
  return (h?.value ?? '').trim();
}

// Gmail query like: (from:"a@x.com" OR from:"b@y.com")
function buildFromQuery(): string {
  const parts = [...ALLOW].map(a => `from:"${a.replace(/"/g, '\\"')}"`);
  if (!parts.length) return ''; // if empty, return nothing
  return `(${parts.join(' OR ')})`;
}

export async function GET(req: NextRequest) {
  // Cookie must contain the authed Gmail user (set during OAuth callback)
  const emailCookie = req.cookies.get('gmail_user')?.value?.toLowerCase();
  if (!emailCookie) {
    return NextResponse.json({ connected: false, reason: 'no_cookie', newsByCategory: {} });
  }

  const tokens = await loadTokens(emailCookie);
  if (!tokens?.access_token && !tokens?.refresh_token) {
    return NextResponse.json({ connected: false, reason: 'not_connected', newsByCategory: {} });
  }

  const oAuth2 = getOAuth2();
  oAuth2.setCredentials(tokens);
  const gmail = google.gmail({ version: 'v1', auth: oAuth2 });

  try {
    // 1) List message IDs from allowed senders
    const q = buildFromQuery();
    if (!q) {
      // No allow-list configured: return empty Gmail bucket
      return NextResponse.json({ connected: true, newsByCategory: { Gmail: [] } });
    }

    const wanted = 30;       // fetch more than 10 so we can safely filter/sort
    const pageSize = 100;    // reasonable page size
    const ids: string[] = [];
    let pageToken: string | undefined = undefined;
    let guard = 0;

    while (ids.length < wanted && guard < 10) {
      const listParams: gmail_v1.Params$Resource$Users$Messages$List = {
        userId: 'me',
        q,
        maxResults: pageSize,
        pageToken,
        includeSpamTrash: false,
      };

      const { data }: { data: gmail_v1.Schema$ListMessagesResponse } =
        await gmail.users.messages.list(listParams);

      const msgs = (data.messages ?? []) as gmail_v1.Schema$Message[];
      ids.push(...msgs.map(m => m.id!).filter(Boolean));
      pageToken = data.nextPageToken || undefined;
      guard++;
      if (!pageToken || msgs.length === 0) break;
    }

    if (ids.length === 0) {
      return NextResponse.json({ connected: true, newsByCategory: { Gmail: [] } });
    }

    // 2) Fetch metadata for each; double-check allow-list; build items
    const getOne = async (id: string): Promise<NewsItem | null> => {
      const params: gmail_v1.Params$Resource$Users$Messages$Get = {
        userId: 'me',
        id,
        format: 'metadata',
        metadataHeaders: ['Subject', 'From', 'Date'],
      };
      const { data }: { data: gmail_v1.Schema$Message } = await gmail.users.messages.get(params);
      const hdrs = data.payload?.headers || [];
      const subject = getHeader(hdrs, 'Subject') || '(no subject)';
      const fromRaw = getHeader(hdrs, 'From');
      const fromEmail = parseEmailAddress(fromRaw);
      if (!ALLOW.has(fromEmail)) return null; // enforce exact allow-list

      const ts =
        Number(data.internalDate ?? 0) ||
        Date.parse(getHeader(hdrs, 'Date')) ||
        0;

      return {
        title: subject,
        link: `/mail/${encodeURIComponent(id)}`,
        ts,
        pub: fromEmail,
      };
    };

    const meta = await Promise.all(ids.slice(0, 200).map(getOne));
    const items = meta
      .filter((x): x is NewsItem => !!x)
      .sort((a, b) => (b.ts ?? 0) - (a.ts ?? 0))
      .slice(0, 10); // newest 10

    return NextResponse.json({
      connected: true,
      newsByCategory: { Gmail: items }, // UI maps to “Commentary”
    });
  } catch (e: unknown) {
  const err = isRec(e) ? e : {};
  const response = isRec(err.response) ? err.response : {};
  const data = isRec(response.data) ? response.data : {};
  const msg = String(data.error ?? err.message ?? '').toLowerCase();
  if (msg.includes('invalid_grant')) {
    // do NOT clear cookie here; just tell client to re-auth
    return NextResponse.json({ connected: false, reason: 'invalid_grant', newsByCategory: {} }, { status: 401 });
  }
  return NextResponse.json({ connected: false, reason: 'fetch_error', message: msg, newsByCategory: {} }, { status: 500 });
}
}
