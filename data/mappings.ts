export type Sleeve = 'Core' | 'Satellite';
export type Theorist =
  | 'McLuhan' | 'Flusser' | 'Illich' | 'Virilio' | 'Debord' | 'Baudrillard' | 'Deleuze'
  | 'Galloway' | 'Thacker' | 'Kittler' | 'Castells' | 'Sontag' | 'Lacan' | 'Girard' | 'Wiener';

export type Meta = { sleeve: Sleeve; theorists: Theorist[] };

/** Sleeve + theorist lenses per symbol (30 tickers). */
export const META: Record<string, Meta> = {
  // Core (11)
  SMH:  { sleeve: 'Core', theorists: ['Kittler','Castells','McLuhan'] },
  SRVR: { sleeve: 'Core', theorists: ['Galloway','Castells','Kittler'] },
  CIBR: { sleeve: 'Core', theorists: ['Galloway','Wiener','Kittler'] },
  URNM: { sleeve: 'Core', theorists: ['Illich','Virilio','Thacker'] },
  GRID: { sleeve: 'Core', theorists: ['Illich','Castells','Wiener'] },
  IAU:  { sleeve: 'Core', theorists: ['Debord','Baudrillard','Wiener'] },
  IDNA: { sleeve: 'Core', theorists: ['Deleuze','Sontag','Thacker'] },
  BOTZ: { sleeve: 'Core', theorists: ['Deleuze','Wiener','Kittler'] },
  ILS:  { sleeve: 'Core', theorists: ['Thacker','Wiener','Virilio'] },
  PEJ:  { sleeve: 'Core', theorists: ['Debord','Baudrillard','Lacan'] },
  COPX: { sleeve: 'Core', theorists: ['Illich','Castells','Virilio'] },

  // Satellite (19)
  VRT:  { sleeve: 'Satellite', theorists: ['Castells','Galloway','Wiener'] },
  OKLO: { sleeve: 'Satellite', theorists: ['Illich','Virilio','Wiener'] },
  PWR:  { sleeve: 'Satellite', theorists: ['Illich','Castells','Virilio'] },
  AMKR: { sleeve: 'Satellite', theorists: ['Kittler','McLuhan','Deleuze'] },
  RXRX: { sleeve: 'Satellite', theorists: ['Deleuze','Thacker','Kittler'] },
  NET:  { sleeve: 'Satellite', theorists: ['Galloway','Castells','Wiener'] },
  MU:   { sleeve: 'Satellite', theorists: ['Kittler','Castells','McLuhan'] },
  SMR:  { sleeve: 'Satellite', theorists: ['Illich','Virilio','Wiener'] },
  OKTA: { sleeve: 'Satellite', theorists: ['Galloway','Wiener','Castells'] },
  AMAT: { sleeve: 'Satellite', theorists: ['Kittler','McLuhan','Deleuze'] },
  SDGR: { sleeve: 'Satellite', theorists: ['Deleuze','Sontag','Thacker'] },
  BWXT: { sleeve: 'Satellite', theorists: ['Virilio','Illich','Wiener'] },
  SEAT: { sleeve: 'Satellite', theorists: ['Debord','Baudrillard','Lacan'] },
  MYRG: { sleeve: 'Satellite', theorists: ['Illich','Castells','Virilio'] },
  QDEL: { sleeve: 'Satellite', theorists: ['Sontag','Deleuze','Thacker'] },
  GNRC: { sleeve: 'Satellite', theorists: ['Illich','Wiener','Virilio'] },
  EB:   { sleeve: 'Satellite', theorists: ['Debord','Baudrillard','Lacan'] },
  IBIT: { sleeve: 'Satellite', theorists: ['Debord','Baudrillard','Wiener'] },
  ETHA: { sleeve: 'Satellite', theorists: ['Debord','Baudrillard','Wiener'] },
};

/** Fast sleeve lookup. */
export const SLEEVE_MAP: Record<string, Sleeve> =
  Object.fromEntries(Object.entries(META).map(([sym, m]) => [sym.toUpperCase(), m.sleeve]));
