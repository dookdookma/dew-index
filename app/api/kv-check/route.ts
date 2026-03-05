// app/api/kv-check/route.ts
import { NextResponse } from 'next/server';
import { getRedis, pingRedis } from '@/lib/kv';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  const client = getRedis();
  const ping = await pingRedis();

  return NextResponse.json({
    ok: !!client && ping.ok,
    ping,
    URL_present: !!(
      process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_URL ||
      process.env.UPSTASH_REDIS_REST_URL_KV_URL ||
      process.env.UPSTASH_REDIS_REST_URL_REDIS_URL ||
      process.env.UPSTASH_REDIS_REST_URL ||
      process.env.REDIS_URL
    ),
    TOKEN_present: !!(
      process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_TOKEN ||
      process.env.UPSTASH_REDIS_REST_TOKEN ||
      process.env.REDIS_TOKEN ||
      process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_READ_ONLY_TOKEN
    ),
    urlHead: (
      process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_URL ||
      process.env.UPSTASH_REDIS_REST_URL_KV_URL ||
      process.env.UPSTASH_REDIS_REST_URL_REDIS_URL ||
      process.env.UPSTASH_REDIS_REST_URL ||
      process.env.REDIS_URL ||
      ''
    ).slice(0, 28),
    tokenHead: (
      process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_TOKEN ||
      process.env.UPSTASH_REDIS_REST_TOKEN ||
      process.env.REDIS_TOKEN ||
      process.env.UPSTASH_REDIS_REST_URL_KV_REST_API_READ_ONLY_TOKEN ||
      ''
    ).slice(0, 10),
    env: { NODE_ENV: process.env.NODE_ENV, VERCEL_ENV: process.env.VERCEL_ENV },
  });
}
