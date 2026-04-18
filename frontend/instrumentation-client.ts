/**
 * Next.js client-side instrumentation. Initializes Sentry in the browser
 * only when NEXT_PUBLIC_SENTRY_DSN is set. Fails soft on import errors.
 */
const dsn = process.env.NEXT_PUBLIC_SENTRY_DSN

if (dsn) {
  import('@sentry/nextjs')
    .then((Sentry) => {
      Sentry.init({
        dsn,
        environment: process.env.NEXT_PUBLIC_SENTRY_ENVIRONMENT ?? 'production',
        tracesSampleRate: Number(process.env.NEXT_PUBLIC_SENTRY_TRACES_SAMPLE_RATE ?? 0.1),
      })
    })
    .catch(() => {
      // Sentry package unavailable; continue without telemetry.
    })
}
