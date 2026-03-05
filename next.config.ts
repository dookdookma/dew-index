/** @type {import('next').NextConfig} */
const nextConfig = {
  eslint: { ignoreDuringBuilds: true },   // ← skip ESLint on Vercel builds
};
module.exports = nextConfig;
