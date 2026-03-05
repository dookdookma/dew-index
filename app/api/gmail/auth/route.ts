import { NextRequest, NextResponse } from 'next/server';
import { getOAuth2, getOriginFromReq } from '@/lib/gmail';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(req: NextRequest) {
  const origin = getOriginFromReq(req.url, req.headers);
  const o = getOAuth2(origin);
  const url = o.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    scope: [
      'https://www.googleapis.com/auth/gmail.readonly',
      'openid',
      'email',
    ],
    state: 'v=1', // avoid adding anything to callback URL manually
  });
  return NextResponse.redirect(url);
}
