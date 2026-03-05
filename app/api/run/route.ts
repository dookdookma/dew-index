import { NextRequest, NextResponse } from 'next/server';
import { getAlpacaPositions, fetchDailyBars } from '@/lib/alpaca';
import { windowFrom, computeReturns } from '@/lib/metrics';

function normalizeWeights(pos:{symbol:string, qty:number}[]) {
  const tot = pos.reduce((s,p)=> s+Math.abs(p.qty), 0) || 1;
  const w: Record<string,number> = {};
  for (const p of pos) w[p.symbol] = Math.abs(p.qty)/tot;
  return w;
}

export async function GET(req: NextRequest) {
  const q = req.nextUrl.searchParams;
  const period = (q.get('period')||'1W') as '1D'|'1W'|'1M'|'1Q'|'1Y';
  const dates = windowFrom(period);
  const pos = await getAlpacaPositions();
  const symbols = pos.map(p=>p.symbol);
  const bars = await fetchDailyBars(symbols, dates[0], dates[dates.length-1]);
  const weights = normalizeWeights(pos);
  const res = computeReturns(bars, dates, weights);

  const sort = (key:'r1'|'rW') => [...res.rows].sort((a,b)=> (b[key]??-1)-(a[key]??-1));
  return NextResponse.json({
    window:{ from:dates[0], to:dates[dates.length-1], period },
    index: res.index,
    sleeves: res.sleeves,
    theorists: res.theorists,
    tickers_day: sort('r1'),
    tickers_window: sort('rW'),
  });
}
