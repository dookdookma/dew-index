// lib/gmail.ts
import { google } from 'googleapis';

export function getOriginFromReq(reqUrl?: string, hdrs?: Headers) {
  if (reqUrl) {
    const u = new URL(reqUrl);
    return `${u.protocol}//${u.host}`;
  }
  const proto = hdrs?.get('x-forwarded-proto') || 'https';
  const host  = hdrs?.get('x-forwarded-host') || hdrs?.get('host');
  return host ? `${proto}://${host}` : (process.env.NEXT_PUBLIC_BASE_URL || process.env.VERCEL_URL && `https://${process.env.VERCEL_URL}` || 'http://localhost:3000');
}

export function getOAuth2(origin?: string) {
  const clientId = process.env.GOOGLE_CLIENT_ID!;
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET!;
  // If you prefer a fixed env, keep GOOGLE_REDIRECT_URI as fallback.
  const computed = origin ? new URL('/api/gmail/callback', origin).toString() : (process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3000/api/gmail/callback');
  return new google.auth.OAuth2(clientId, clientSecret, computed);
}
