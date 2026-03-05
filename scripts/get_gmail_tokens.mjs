import http from 'node:http';
import open from 'node:child_process';
import { google } from 'googleapis';

const {
  GOOGLE_CLIENT_ID,
  GOOGLE_CLIENT_SECRET,
} = process.env;

if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) {
  console.error('Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in your shell or .env.local');
  process.exit(1);
}

const REDIRECT = 'http://localhost:8787/oauth2callback';
const oauth2 = new google.auth.OAuth2(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, REDIRECT);

const url = oauth2.generateAuthUrl({
  access_type: 'offline',        // get refresh_token
  prompt: 'consent',             // force refresh_token on repeat runs
  scope: [
    'https://www.googleapis.com/auth/gmail.readonly',
    'openid',
    'email',
  ],
});

console.log('\nOpen this URL in a browser to authorize:\n\n', url, '\n');

try { open.exec(`start "" "${url}"`); } catch {}

const server = http.createServer(async (req, res) => {
  if (!req.url.startsWith('/oauth2callback')) { res.writeHead(404).end(); return; }
  const u = new URL(req.url, 'http://localhost:8787');
  const code = u.searchParams.get('code');
  if (!code) { res.writeHead(400).end('Missing code'); return; }

  try {
    const { tokens } = await oauth2.getToken(code);
    oauth2.setCredentials(tokens);

    const oauth = google.oauth2({ version: 'v2', auth: oauth2 });
    const me = await oauth.userinfo.get();
    const email = String(me.data.email || '').toLowerCase();

    const payload = {
      email,
      tokens: {
        access_token: tokens.access_token,
        refresh_token: tokens.refresh_token,
        scope: tokens.scope,
        token_type: tokens.token_type,
        expiry_date: tokens.expiry_date,
        id_token: tokens.id_token,
      },
    };

    const json = JSON.stringify(payload.tokens);
    console.log('\nGMAIL_BACKEND_EMAIL:', email);
    console.log('\nGMAIL_TOKEN_JSON:', json, '\n');

    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK. You can close this tab. Check the terminal for GMAIL_TOKEN_JSON.');
  } catch (e) {
    console.error(e);
    res.writeHead(500).end('Token exchange failed');
  } finally {
    setTimeout(() => server.close(), 500);
  }
});

server.listen(8787, () => console.log('Listening on http://localhost:8787/oauth2callback'));
