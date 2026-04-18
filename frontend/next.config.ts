import type { NextConfig } from "next";

// BACKEND_URL lets Docker / production deployments point at the correct host.
// Defaults to localhost for local development without Docker.
const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8000";

// Baseline security headers applied to every frontend response.
// Mirrors the backend middleware so worldoftaxonomy.com and the API
// present a consistent posture to browsers and scanners.
const securityHeaders = [
  {
    key: "Strict-Transport-Security",
    value: "max-age=63072000; includeSubDomains; preload",
  },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "X-Frame-Options", value: "DENY" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  {
    key: "Permissions-Policy",
    value: "geolocation=(), microphone=(), camera=()",
  },
  // CSP in report-only first. The frontend is Next.js + React + shadcn:
  // hydration needs inline scripts, Tailwind emits inline style tags,
  // and we fetch api.github.com for star counts. Violations are POSTed
  // to /api/v1/csp-report so we can tune before flipping to enforcement.
  {
    key: "Content-Security-Policy-Report-Only",
    value: [
      "default-src 'self'",
      "script-src 'self' 'unsafe-inline' 'unsafe-eval'",
      "style-src 'self' 'unsafe-inline'",
      "img-src 'self' data: blob: https:",
      "font-src 'self' data:",
      "connect-src 'self' https://api.github.com",
      "frame-ancestors 'none'",
      "object-src 'none'",
      "base-uri 'self'",
      "form-action 'self'",
      "upgrade-insecure-requests",
      "report-uri /api/v1/csp-report",
    ].join("; "),
  },
];

const nextConfig: NextConfig = {
  // Drop the default "X-Powered-By: Next.js" response header. Reduces
  // version fingerprinting and keeps generic scanners quiet.
  poweredByHeader: false,
  async rewrites() {
    return [
      {
        source: "/api/v1/:path*",
        destination: `${backendUrl}/api/v1/:path*`,
      },
    ];
  },
  async headers() {
    return [
      {
        source: "/:path*",
        headers: securityHeaders,
      },
    ];
  },
  // Prevent remark and its plugins from being bundled (they are Node-only)
  serverExternalPackages: ['remark', 'remark-gfm', 'remark-html'],
};

export default nextConfig;
