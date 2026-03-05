import dayjs from 'dayjs';
import { META, type Sleeve, type Theorist } from '@/data/mappings';

export type WeightMap = Record<string, number>; // symbol→weight 0..1

export function lastNTradingDays(n:number, anchor?:string) {
  let d = anchor ? dayjs(anchor) : dayjs();
  while (d.day()>5) d = d.subtract(1,'day');
  const out:string[]=[];
  while (out.length<n) { if (d.day()>0 && d.day()<6) out.push(d.format('YYYY-MM-DD')); d=d.subtract(1,'day'); }
  return out.reverse();
}
export function windowFrom(period:'1D'|'1W'|'1M'|'1Q'|'1Y', anchor?:string) {
  const map = { '1D':2, '1W':5, '1M':22, '1Q':66, '1Y':252 };
  return lastNTradingDays(map[period], anchor);
}
export function closeMap(rows:{t:string;c:number}[]) {
  return rows.reduce((m,v)=>{ m[v.t.slice(0,10)] = v.c; return m; }, {} as Record<string,number>);
}
export function computeReturns(
  bars: Record<string,{t:string;c:number}[]>,
  dates: string[],
  weights: WeightMap
){
  const first = dates[0], prev = dates[dates.length-2], last = dates[dates.length-1];
  const rows = Object.keys(weights).map(sym=>{
    const series = closeMap(bars[sym]||[]);
    const p0=series[first], pP=series[prev], pL=series[last];
    const r1 = (pL && pP) ? (pL/pP - 1) : null;
    const rW = (pL && p0) ? (pL/p0 - 1) : null;
    const w = weights[sym]||0;
    const meta = META[sym] || { sleeve:'Core' as Sleeve, theorists:[] as Theorist[] };
    return { symbol:sym, sleeve:meta.sleeve, theorists:meta.theorists, w, r1, rW,
      c1_bps: r1==null?null: w*r1*1e4, cW_bps: rW==null?null: w*rW*1e4 };
  });
  const sum = (a:number[])=>a.reduce((x,y)=>x+y,0);
  const idx1 = sum(rows.filter(r=>r.r1!=null).map(r=>r.w*(r.r1 as number)));
  const idxW = sum(rows.filter(r=>r.rW!=null).map(r=>r.w*(r.rW as number)));
  const sleeveAgg = (s:Sleeve, k:'r1'|'rW')=>sum(rows.filter(r=>r.sleeve===s && r[k]!=null).map(r=>r.w*(r[k] as number)));
  const theorAgg  = (t:Theorist,k:'r1'|'rW')=>sum(rows.filter(r=>r.theorists.includes(t)&& r[k]!=null).map(r=>r.w*(r[k] as number)));
  const theorists = (['McLuhan','Flusser','Illich','Virilio','Debord','Baudrillard','Deleuze','Galloway','Thacker','Kittler','Castells','Sontag','Lacan','Girard','Wiener'] as Theorist[])
    .map(t=>({ theorist:t, r1:theorAgg(t,'r1'), rW:theorAgg(t,'rW') }));
  return { rows, index:{ r1:idx1, rW:idxW },
           sleeves:{ Core:{ r1:sleeveAgg('Core','r1'), rW:sleeveAgg('Core','rW') },
                     Satellite:{ r1:sleeveAgg('Satellite','r1'), rW:sleeveAgg('Satellite','rW') } },
           theorists };
}

// Add below existing exports
export function tradingDaysBetweenISO(fromISO: string, toISO: string) {
  let d = dayjs(fromISO);
  const end = dayjs(toISO);
  const out: string[] = [];
  while (d.isBefore(end) || d.isSame(end, 'day')) {
    if (d.day() > 0 && d.day() < 6) out.push(d.format('YYYY-MM-DD'));
    d = d.add(1, 'day');
  }
  return out;
}
