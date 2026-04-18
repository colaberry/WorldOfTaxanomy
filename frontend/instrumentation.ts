/**
 * Next.js instrumentation hook. Initializes Sentry for server + edge
 * runtimes only when SENTRY_DSN is set. Fails soft so a missing
 * package or unset env never breaks the app.
 */
export async function register() {
  const dsn = process.env.SENTRY_DSN
  if (!dsn) return

  try {
    const Sentry = await import('@sentry/nextjs')

    if (process.env.NEXT_RUNTIME === 'nodejs') {
      Sentry.init({
        dsn,
        environment: process.env.SENTRY_ENVIRONMENT ?? 'production',
        tracesSampleRate: Number(process.env.SENTRY_TRACES_SAMPLE_RATE ?? 0.1),
      })
    }

    if (process.env.NEXT_RUNTIME === 'edge') {
      Sentry.init({
        dsn,
        environment: process.env.SENTRY_ENVIRONMENT ?? 'production',
        tracesSampleRate: Number(process.env.SENTRY_TRACES_SAMPLE_RATE ?? 0.1),
      })
    }
  } catch {
    // Sentry package unavailable or init failed; continue without telemetry.
  }
}
