// app/mail/[id]/page.tsx
'use client';

import React, { use, useEffect, useMemo, useState } from 'react';

type Msg = {
  ok: boolean;
  id: string;
  subject: string;
  from: string;
  date: number;
  html: string;     // server should prefer text/html part; fallback handled server-side
  reason?: string;
  authUrl?: string;
};
const isRec = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object';

export default function MailView({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);

  const [msg, setMsg] = useState<Msg | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);

  useEffect(() => {
    let alive = true;
    const ac = new AbortController();
    (async () => {
      setLoading(true); setErr(null);
      try {
        const r = await fetch(`/api/gmail/message/${encodeURIComponent(id)}`, {
          cache: 'no-store',
          signal: ac.signal,
        });
        const j = await r.json() as unknown;
        const o = isRec(j) ? j : {};
        if (!alive) return;

        if (!r.ok || !o.ok) {
          if (o.reason === 'invalid_grant' && typeof o.authUrl === 'string') {
            window.location.href = o.authUrl;
            return;
          }
          throw new Error(typeof o.reason === 'string' ? o.reason : `status_${r.status}`);
        }
        setMsg(o as Msg);
      } catch (e: unknown) {
        if (!alive || (e instanceof Error && e.name === 'AbortError')) return;
        setErr(e instanceof Error ? e.message : 'error');
      } finally {
        if (alive) setLoading(false);
      }
    })();
    return () => { alive = false; ac.abort(); };
  }, [id]);

  const doc = useMemo(() => {
    if (!msg?.html) return '';
    // Do NOT strip widths/heights/fonts; only constrain overflow.
    // Keep sender’s original CSS. Only add a tiny safety layer.
    return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<base target="_blank">
<style>
  html,body{margin:0;padding:0}
  /* Prevent horizontal scroll while preserving original layout */
  body{overflow-x:hidden}
  /* Only constrain elements that could overflow the container */
  img, video, canvas, svg, iframe { max-width:100%; height:auto }
  table { max-width:100% } /* do not force width:100% - keep sender widths */
  /* Let long words/urls wrap if needed */
  .dew-wrap { padding:0; margin:0; word-break:normal; overflow-wrap:anywhere }
</style>
</head>
<body>
  <div class="dew-wrap">${msg.html}</div>
</body>
</html>`;
  }, [msg?.html]);

  if (err) {
    return (
      <main style={{ padding: 16, maxWidth: 1200, margin: '0 auto' }}>
        <h3 style={{ marginTop: 0 }}>Error</h3>
        <div style={{ color: '#a00', marginBottom: 12 }}>{err}</div>
        <button onClick={() => location.reload()} style={{ padding: '6px 10px' }}>Retry</button>
      </main>
    );
  }

  if (loading || !msg) {
    return (
      <main style={{ padding: 16, maxWidth: 1200, margin: '0 auto' }}>
        <div style={{ height: 18, width: '60%', background: '#eee', borderRadius: 4, margin: '6px 0' }} />
        <div style={{ height: 12, width: '40%', background: '#f0f0f0', borderRadius: 4, margin: '6px 0 12px' }} />
        <div style={{ border: '1px solid #ddd', borderRadius: 6, background: '#fff', padding: '12px 20px', height: 320 }} />
      </main>
    );
  }

  // Height uses viewport - fills window without horizontal scroll.
  // Adjust the 140px if your page chrome is taller/shorter.
  const iframeH = 'calc(100vh - 140px)';

  return (
    <main style={{ padding: 16, maxWidth: 1200, margin: '0 auto' }}>
      <div style={{ marginBottom: 12, fontWeight: 600 }}>{msg.subject}</div>
      <div style={{ color: '#666', fontSize: 13, marginBottom: 12 }}>
        From: {msg.from} | {new Date(msg.date).toLocaleString()}
      </div>

      <div style={{ border: '1px solid var(--border, #ddd)', borderRadius: 6, background: '#fff' }}>
        <iframe
          title="mail"
          srcDoc={doc}
          style={{ width: '100%', height: iframeH, border: 0, display: 'block' }}
          // Keep formatting intact but allow links/images to load.
          sandbox="allow-same-origin allow-popups allow-popups-to-escape-sandbox allow-forms"
        />
      </div>
    </main>
  );
}
