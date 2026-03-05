// lib/kv.ts
import { Redis } from '@upstash/redis';

/**
 * Your env names (cannot be changed on Vercel):
 *  - UPSTASH_REDIS_REST_URL_KV_URL
 *  - UPSTASH_REDIS_REST_URL_KV_REST_API_URL
 *  - UPSTASH_REDIS_REST_URL_KV_REST_API_TOKEN
 *  - UPSTASH_REDIS_REST_URL_KV_REST_API_READ_ONLY_TOKEN
 *  - UPSTASH_REDIS_REST_URL_REDIS_URL   (fallback URL)
 */

const URL_CANDIDATES = [
  process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_URL,
  process.env.UPSTASH_REDIS_REST_URL_KV_URL,
  process.env.UPSTASH_REDIS_REST_URL_REDIS_URL, // fallback
].filter(Boolean) as string[];

const TOKEN_CANDIDATES = [
  process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_TOKEN,
  process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_READ_ONLY_TOKEN,
].filter(Boolean) as string[];

let cached: Redis | null = null;

export function getRedis(): Redis {
  if (cached) return cached;
  const url = URL_CANDIDATES[0];
  const token = TOKEN_CANDIDATES[0];

  if (!url || !token) {
    throw new Error('No KV/Redis configured');
  }
  cached = new Redis({ url, token });
  return cached;
}

export async function pingRedis(): Promise<{ ok: boolean; error?: string }> {
  try {
    const r = getRedis();
    // simple write/read roundtrip (PING isn’t always allowed on serverless)
    const key = `kv:__ping__:${Date.now()}`;
    await r.set(key, 'ok', { ex: 30 });
    const v = await r.get<string>(key);
    return { ok: v === 'ok' };
  } catch (e: unknown) {
    return { ok: false, error: e instanceof Error ? e.message : String(e) };
  }
}

export async function kvDiag() {
  const url =
    process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_URL ||
    process.env.UPSTASH_REDIS_REST_URL_KV_URL ||
    process.env.UPSTASH_REDIS_REST_URL_REDIS_URL ||
    null;

  const token =
    process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_TOKEN ||
    process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_READ_ONLY_TOKEN ||
    null;

 let ping: { ok: boolean; error?: string } = { ok: false };
try {
  const pr = await pingRedis();
  ping = { ok: pr.ok, error: pr.error };
} catch (e: unknown) {
  ping = { ok: false, error: e instanceof Error ? e.message : String(e) };
}

  return {
    ok: !!(url && token) && ping.ok,
    ping,
    URL_present: !!url,
    TOKEN_present: !!token,
    urlHead: url ? url.slice(0, 40) : null,
    tokenHead: token ? token.slice(0, 10) : null,
    env: { NODE_ENV: process.env.NODE_ENV || null },
  }
}
