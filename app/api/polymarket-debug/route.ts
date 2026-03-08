import { NextRequest, NextResponse } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const DEW_POLY_EXCLUDE_REGEX = /(nba finals|stanley cup|fifa world cup|la liga|premier league|champions league|masters tournament|democratic presidential nomination|republican presidential nomination|win the 2028 us presidential election)/i;

type PM = {
  question: string;
  slug: string;
  link: string;
  yesProb?: number;
  noProb?: number;
  volume24hr?: number;
  volume?: number;
  liquidity?: number;
  endDate?: string;
  category?: string;
};

function toNum(v: unknown): number | undefined {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return undefined;
}

function parseJsonArrayStrings(v: unknown): string[] {
  if (Array.isArray(v)) return v.map((x) => String(x));
  if (typeof v !== 'string') return [];
  try {
    const arr = JSON.parse(v);
    return Array.isArray(arr) ? arr.map((x) => String(x)) : [];
  } catch {
    return [];
  }
}

async function fetchJsonWithTimeout<T>(url: string, timeoutMs: number): Promise<T | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { cache: 'no-store', signal: controller.signal });
    if (!res.ok) return null;
    return await res.json() as T;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchPolymarketIdeas(limit: number, timeoutMs: number, universeMaxSide: number): Promise<{ rawCount: number; ideas: PM[]; universeExcludedCount: number }> {
  const upstreamLimit = Math.max(10, Math.min(500, limit * 3));
  const url = `https://gamma-api.polymarket.com/markets?limit=${upstreamLimit}&active=true&closed=false`;
  const raw = await fetchJsonWithTimeout<unknown>(url, timeoutMs);
  if (!raw) return { rawCount: 0, ideas: [], universeExcludedCount: 0 };
  const arr = Array.isArray(raw) ? raw : [];
  const out: PM[] = [];
  let universeExcludedCount = 0;

  for (const row of arr) {
    const r = row && typeof row === 'object' ? (row as Record<string, unknown>) : {};
    const question = typeof r.question === 'string' ? r.question.trim() : '';
    const slug = typeof r.slug === 'string' ? r.slug.trim() : '';
    if (!question || !slug) continue;
    const outcomesPeek = parseJsonArrayStrings((r as any).outcomes).map((x) => x.toLowerCase());
    const pricesPeek = parseJsonArrayStrings((r as any).outcomePrices).map((x) => Number(x));
    const yesPeek = outcomesPeek.indexOf('yes');
    const noPeek = outcomesPeek.indexOf('no');
    const yv = yesPeek >= 0 && Number.isFinite(pricesPeek[yesPeek]) ? pricesPeek[yesPeek] : undefined;
    const nv = noPeek >= 0 && Number.isFinite(pricesPeek[noPeek]) ? pricesPeek[noPeek] : undefined;
    const maxSideUniverse = Math.max((yv ?? 0), (nv ?? 0));
    const universeExcluded = DEW_POLY_EXCLUDE_REGEX.test(question) || maxSideUniverse > universeMaxSide;
    if (universeExcluded) { universeExcludedCount += 1; continue; }

    const outcomes = parseJsonArrayStrings(r.outcomes).map((x) => x.toLowerCase());
    const prices = parseJsonArrayStrings(r.outcomePrices).map((x) => Number(x));
    const yesIdx = outcomes.indexOf('yes');
    const noIdx = outcomes.indexOf('no');
    const yesProb = yesIdx >= 0 && Number.isFinite(prices[yesIdx]) ? prices[yesIdx] : undefined;
    const noProb = noIdx >= 0 && Number.isFinite(prices[noIdx]) ? prices[noIdx] : undefined;

    out.push({
      question,
      slug,
      link: `https://polymarket.com/event/${slug}`,
      yesProb,
      noProb,
      volume24hr: toNum(r.volume24hr),
      volume: toNum(r.volumeNum ?? r.volume),
      liquidity: toNum(r.liquidityNum ?? r.liquidity),
      endDate: typeof r.endDate === 'string' ? r.endDate : undefined,
      category: typeof r.category === 'string' ? r.category : undefined,
    });
  }

  const ideas = out
    .sort((a, b) => (b.volume24hr ?? b.volume ?? 0) - (a.volume24hr ?? a.volume ?? 0))
    .slice(0, limit);

  return { rawCount: arr.length, ideas, universeExcludedCount };
}

function daysTo(endDate?: string): number | null {
  if (!endDate) return null;
  const t = Date.parse(endDate);
  if (!Number.isFinite(t)) return null;
  return (t - Date.now()) / (1000 * 60 * 60 * 24);
}

export async function GET(req: NextRequest) {
  const url = new URL(req.url);
  const limit = Math.max(10, Math.min(240, Number(url.searchParams.get('limit') ?? process.env.DEW_POLYMARKET_LIMIT ?? '150')));
  const timeoutMs = Math.max(2000, Math.min(30000, Number(url.searchParams.get('timeoutMs') ?? process.env.DEW_FETCH_TIMEOUT_POLYMARKET_MS ?? '9000')));

  const asymMax = Number(url.searchParams.get('asymMax') ?? '0.92');
  const horizonDays = Number(url.searchParams.get('horizonDays') ?? '180');
  const surgeMin = Number(url.searchParams.get('surgeMin') ?? '0.04');
  const surgeMinLong = Number(url.searchParams.get('surgeMinLong') ?? '0.10');
  const longDays = Number(url.searchParams.get('longDays') ?? '90');
  const spreadSumMax = Number(url.searchParams.get('spreadSumMax') ?? '1.25');
  const crowdPrice = Number(url.searchParams.get('crowdPrice') ?? '0.92');
  const crowdSurge = Number(url.searchParams.get('crowdSurge') ?? '0.30');
  const universeMaxSide = Number(url.searchParams.get('universeMaxSide') ?? '0.97');

  const fetched = await fetchPolymarketIdeas(limit, timeoutMs, universeMaxSide);
  const enriched = fetched.ideas.map((m) => {
    const hasYesNo = typeof m.yesProb === 'number' && Number.isFinite(m.yesProb) && typeof m.noProb === 'number' && Number.isFinite(m.noProb);
    const hasV24 = typeof m.volume24hr === 'number' && Number.isFinite(m.volume24hr);
    const hasVTot = typeof m.volume === 'number' && Number.isFinite(m.volume) && (m.volume as number) > 0;
    const d = daysTo(m.endDate);
    const hasEnd = d !== null;

    const v24 = m.volume24hr ?? 0;
    const vTot = m.volume ?? 0;
    const surgePct = vTot > 0 ? v24 / vTot : 0;
    const maxSide = Math.max(m.yesProb ?? 0, m.noProb ?? 0);
    const spreadSum = (m.yesProb ?? 0) + (m.noProb ?? 0);

    const completeness = {
      hasYesNo,
      hasV24,
      hasVTot,
      hasEnd,
      complete: hasYesNo && hasV24 && hasVTot && hasEnd,
    };

    const surgeThreshold = (d !== null && d >= longDays) ? surgeMinLong : surgeMin;
    const filters = {
      asymmetricAlpha: maxSide <= asymMax,
      capitalEfficiency: d === null ? false : d <= horizonDays,
      vpaSurge: surgePct >= surgeThreshold,
      spreadLiquidity: spreadSum <= spreadSumMax,
      girardMimeticTrap: !(maxSide >= crowdPrice && surgePct >= crowdSurge),
    };

    const passAll = completeness.complete && filters.asymmetricAlpha && filters.capitalEfficiency && filters.vpaSurge && filters.spreadLiquidity && filters.girardMimeticTrap;

    return {
      ...m,
      daysToResolution: d,
      surgePct,
      maxSideProb: maxSide,
      completeness,
      filters,
      passAll,
    };
  });

  const summary = {
    requestedLimit: limit,
    timeoutMs,
    upstreamRawCount: fetched.rawCount,
    universeExcludedCount,
    retainedCount: fetched.ideas.length,
    passAllCount: enriched.filter((x) => x.passAll).length,
    filterCounts: {
      rejectedMissingFields: enriched.filter((x) => !x.completeness.complete).length,
      missingYesNo: enriched.filter((x) => !x.completeness.hasYesNo).length,
      missingVolume24h: enriched.filter((x) => !x.completeness.hasV24).length,
      missingVolumeTotal: enriched.filter((x) => !x.completeness.hasVTot).length,
      missingEndDate: enriched.filter((x) => !x.completeness.hasEnd).length,
      asymmetricAlphaFail: enriched.filter((x) => x.completeness.complete && !x.filters.asymmetricAlpha).length,
      capitalEfficiencyFail: enriched.filter((x) => x.completeness.complete && !x.filters.capitalEfficiency).length,
      vpaSurgeFail: enriched.filter((x) => x.completeness.complete && !x.filters.vpaSurge).length,
      spreadLiquidityFail: enriched.filter((x) => x.completeness.complete && !x.filters.spreadLiquidity).length,
      mimeticTrapFail: enriched.filter((x) => x.completeness.complete && !x.filters.girardMimeticTrap).length,
    },
    thresholds: { asymMax, horizonDays, surgeMin, surgeMinLong, longDays, spreadSumMax, crowdPrice, crowdSurge, universeMaxSide },
  };

  return NextResponse.json({
    summary,
    sampleTopByVolume24h: enriched.slice(0, 100),
  });
}
