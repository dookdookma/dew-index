// app/api/news/route.ts

import { NextResponse } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

type Item = { title:string; link:string; ts:number; pub?:string };
type CatMap = Record<string, string[]>;

const FEEDS: CatMap = {
  'DEW Line Theorists': [
    'https://news.google.com/rss/search?q=%22Marshall%20McLuhan%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Vil%C3%A9m%20Flusser%20OR%20Flusser%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Ivan%20Illich%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Paul%20Virilio%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Guy%20Debord%20OR%20Debord%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Jean%20Baudrillard%20OR%20Baudrillard%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Gilles%20Deleuze%20OR%20Deleuze%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Alexander%20R.%20Galloway%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Eugene%20Thacker%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Friedrich%20Kittler%20OR%20Kittler%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Manuel%20Castells%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Susan%20Sontag%20OR%20Sontag%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Jacques%20Lacan%20OR%20Lacan%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Rene%20Girard%20OR%20Girard%22&hl=en-US&gl=US&ceid=US:en',
    'https://news.google.com/rss/search?q=%22Norber%20Wiener%20OR%20Wiener%22&hl=en-US&gl=US&ceid=US:en',
   ],
     Politics: [
    'https://news.google.com/rss/headlines/section/topic/NATION',
    'https://apnews.com/hub/politics?output=rss',
    'https://news.google.com/rss/headlines/section/topic/POLITICS',
    'https://www.politico.com/rss/politics-news.xml',
  ],
  International: [
    'https://news.google.com/rss/headlines/section/topic/WORLD',
    'https://feeds.bbci.co.uk/news/world/rss.xml',
    'https://feeds.reuters.com/Reuters/worldNews',
  ],
  Business: [
    'https://news.google.com/rss/headlines/section/topic/BUSINESS',
    'https://feeds.reuters.com/reuters/businessNews',
    'https://www.cnbc.com/id/10001147/device/rss/rss.html',
  ],
  Tech: [
    'https://www.404media.co/rss',
    'https://news.google.com/rss/headlines/section/topic/TECHNOLOGY',
    'https://techcrunch.com/feed/',
    'https://www.theverge.com/rss/index.xml',
    'https://feeds.arstechnica.com/arstechnica/index',
  ],
  'NYC Metro': [
    'https://gothamist.com/feeds/latest',
    'https://www.ny1.com/nyc/all-boroughs/news.rss',
    'https://www.thecity.nyc/feeds/news/rss.xml',
    'https://rss.nytimes.com/services/xml/rss/nyt/NYRegion.xml',
    'https://www.nbcnewyork.com/news/local/feed/',
    'https://abc7ny.com/feed/',
  ],
};

const TEXT_TIMEOUT_MS = 12000;

async function fetchText(url: string): Promise<string|null> {
  try {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), TEXT_TIMEOUT_MS);
    const r = await fetch(url, { cache:'no-store', signal: ac.signal });
    clearTimeout(t);
    if (!r.ok) return null;
    return await r.text();
  } catch { return null; }
}

function parseRSS(xml: string, fallbackHost: string): Item[] {
  // naive RSS/Atom parser (titles, links, pubDate/updated)
  // split on <item>…</item> or <entry>…</entry>
  const entries: string[] = [];
  const itemRe = /<item[\s\S]*?<\/item>/gi;
  const entryRe = /<entry[\s\S]*?<\/entry>/gi;
  const items = xml.match(itemRe) || xml.match(entryRe) || [];
  for (const raw of items) entries.push(raw);

  const pick = (s: string, re: RegExp) => {
  const m = s.match(re);
  if (!m) return '';
  const x = m[1].trim();
  // strip CDATA without dotAll
  return x.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/, '$1').trim();
};

  const getHost = (u: string) => {
    try { return new URL(u).host.replace(/^www\./,''); } catch { return fallbackHost; }
  };
  const toTs = (d: string) => {
    const t = Date.parse(d);
    return Number.isFinite(t) ? t : Date.now();
  };

  const out: Item[] = [];
  for (const e of entries) {
    const title = pick(e, /<title[^>]*>([\s\S]*?)<\/title>/i) || '(no title)';
    const link  = pick(e, /<link[^>]*>([\s\S]*?)<\/link>/i) ||
                  pick(e, /<link[^>]*href="([^"]+)"/i);
    const date  = pick(e, /<pubDate[^>]*>([\s\S]*?)<\/pubDate>/i) ||
                  pick(e, /<updated[^>]*>([\s\S]*?)<\/updated>/i) ||
                  pick(e, /<dc:date[^>]*>([\s\S]*?)<\/dc:date>/i);
    const pub   = getHost(link) || fallbackHost;
    if (!link) continue;
    out.push({ title, link, ts: toTs(date), pub });
  }
  return out;
}

export async function GET() {
  try {
    const newsByCategory: Record<string, Item[]> = {};
    const cats = Object.keys(FEEDS);

    await Promise.all(cats.map(async (cat) => {
      const urls = FEEDS[cat];
      const lists: Item[][] = [];
      for (const u of urls) {
        const txt = await fetchText(u);
        if (!txt) continue;
        const host = (() => { try { return new URL(u).host.replace(/^www\./,''); } catch { return ''; } })();
        const items = parseRSS(txt, host);
        lists.push(items);
      }
      const merged = lists.flat()
        .filter(it => it.title && it.link)
        .sort((a,b)=> b.ts - a.ts)
        .slice(0, 10);
      if (merged.length) newsByCategory[cat] = merged;
    }));

    return NextResponse.json({ newsByCategory });
  } catch (e: unknown) {
    const error = e instanceof Error ? e.message : 'news_error';
    return NextResponse.json({ newsByCategory:{}, error }, { status: 200 });
  }
}
