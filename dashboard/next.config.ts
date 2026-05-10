import type { NextConfig } from "next";

/**
 * Browser code calls relative paths like `/api/dashboard`. Without this rewrite
 * the dev server returns 404 because there's no Next.js route handler at /api/*.
 * The rewrite proxies /api/* to the FastAPI backend (default localhost:8000;
 * override with SKATING_API_URL on the server side).
 *
 * In production behind a single nginx, you'd typically rewrite at the proxy
 * layer instead — this rewrite is mostly a dev-time convenience that also
 * works in standalone Next.js production.
 */
const SKATING_API_URL = process.env.SKATING_API_URL || "http://127.0.0.1:8000";

const nextConfig: NextConfig = {
  async rewrites() {
    return [{ source: "/api/:path*", destination: `${SKATING_API_URL}/api/:path*` }];
  },
};

export default nextConfig;
