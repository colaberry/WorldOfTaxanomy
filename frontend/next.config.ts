import type { NextConfig } from "next";

// BACKEND_URL lets Docker / production deployments point at the correct host.
// Defaults to localhost for local development without Docker.
const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8000";

const nextConfig: NextConfig = {
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${backendUrl}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
