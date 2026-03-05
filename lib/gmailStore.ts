// lib/gmailStore.ts
import { getRedis } from '@/lib/kv';

export type GmailTokens = {
  access_token?: string;
  refresh_token?: string;
  id_token?: string;
  scope?: string;
  token_type?: string;
  expiry_date?: number;
};

const PREFIX = 'gmail:tokens:';
const keyOf = (email: string) => `${PREFIX}${email}`;

export async function saveTokens(email: string, t: GmailTokens) {
  const r = getRedis();
  if (!r) throw new Error('No KV/Redis configured');
  await r.set(keyOf(email), t); // JSON write
}

export async function loadTokens(email: string): Promise<GmailTokens | null> {
  const r = getRedis();
  if (!r) return null;
  const v = await r.get<GmailTokens>(keyOf(email));
  return v ?? null;
}

export async function clearTokens(email: string) {
  const r = getRedis();
  if (!r) return;
  await r.del(keyOf(email));
}
