'use client'

import { useState, useEffect } from 'react'
import Link from 'next/link'
import { AlertTriangle, ArrowRight, CheckCircle2, Loader2, Sparkles } from 'lucide-react'
import { classifyDemo, ApiError, type ClassifyDemoResponse } from '@/lib/api'
import { getSystemColor } from '@/lib/colors'

const EMAIL_KEY = 'wot_classify_lead_email'
const EXAMPLES = [
  'telemedicine platform',
  'bakery that also sells coffee',
  'logistics company for frozen goods',
  'registered nurse in pediatrics',
  'online language-learning marketplace',
]

export function ClassifyTool() {
  const [email, setEmail] = useState('')
  const [emailLocked, setEmailLocked] = useState(false)
  const [text, setText] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<ClassifyDemoResponse | null>(null)

  // Hydrate remembered email from prior session so repeat visitors skip the gate.
  useEffect(() => {
    if (typeof window === 'undefined') return
    const saved = window.localStorage.getItem(EMAIL_KEY)
    if (saved) {
      setEmail(saved)
      setEmailLocked(true)
    }
  }, [])

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)
    const cleanEmail = email.trim().toLowerCase()
    const cleanText = text.trim()
    if (!cleanEmail || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(cleanEmail)) {
      setError('Please enter a valid email address.')
      return
    }
    if (cleanText.length < 2) {
      setError('Please describe your business in at least a few words.')
      return
    }

    setLoading(true)
    try {
      const data = await classifyDemo(cleanEmail, cleanText)
      setResult(data)
      if (typeof window !== 'undefined') {
        window.localStorage.setItem(EMAIL_KEY, cleanEmail)
      }
      setEmailLocked(true)
    } catch (err) {
      if (err instanceof ApiError) {
        setError(`Request failed (${err.status}). Please try again.`)
      } else {
        setError('Something went wrong. Please try again.')
      }
    } finally {
      setLoading(false)
    }
  }

  function useExample(q: string) {
    setText(q)
    setResult(null)
    setError(null)
  }

  return (
    <div className="space-y-6">
      {/* Form card */}
      <form
        onSubmit={handleSubmit}
        className="rounded-xl border border-border bg-card p-5 sm:p-6 space-y-4 shadow-sm"
      >
        <div className="space-y-2">
          <label htmlFor="classify-email" className="block text-sm font-medium">
            Your email
            {emailLocked && (
              <span className="ml-2 text-xs text-muted-foreground font-normal">
                (remembered - <button type="button" className="underline" onClick={() => setEmailLocked(false)}>change</button>)
              </span>
            )}
          </label>
          <input
            id="classify-email"
            type="email"
            autoComplete="email"
            required
            disabled={emailLocked}
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="you@company.com"
            className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm outline-none focus:border-ring focus:ring-2 focus:ring-ring/30 disabled:opacity-60"
          />
          <p className="text-xs text-muted-foreground">
            We use your email only to send occasional updates about WorldOfTaxonomy.
            No spam, unsubscribe anytime.
          </p>
        </div>

        <div className="space-y-2">
          <label htmlFor="classify-text" className="block text-sm font-medium">
            Describe your business, product, or occupation
          </label>
          <textarea
            id="classify-text"
            required
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder="e.g. a telemedicine platform connecting patients to licensed doctors"
            rows={3}
            maxLength={500}
            className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm outline-none focus:border-ring focus:ring-2 focus:ring-ring/30 resize-y"
          />
          <div className="flex flex-wrap gap-2 pt-1">
            <span className="text-xs text-muted-foreground">Try:</span>
            {EXAMPLES.map((ex) => (
              <button
                key={ex}
                type="button"
                onClick={() => useExample(ex)}
                className="text-xs rounded-full border border-border bg-background px-2.5 py-0.5 hover:bg-muted transition-colors"
              >
                {ex}
              </button>
            ))}
          </div>
        </div>

        {error && (
          <div className="flex items-start gap-2 rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive">
            <AlertTriangle className="size-4 mt-0.5 shrink-0" />
            <div>{error}</div>
          </div>
        )}

        <button
          type="submit"
          disabled={loading}
          className="inline-flex items-center justify-center gap-2 rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90 disabled:opacity-60 transition-colors"
        >
          {loading ? (
            <>
              <Loader2 className="size-4 animate-spin" />
              Classifying...
            </>
          ) : (
            <>
              <Sparkles className="size-4" />
              Classify
            </>
          )}
        </button>
      </form>

      {/* Results */}
      {result && <ClassifyResults data={result} />}
    </div>
  )
}

function ClassifyResults({ data }: { data: ClassifyDemoResponse }) {
  const hasMatches = data.matches.length > 0

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <CheckCircle2 className="size-4 text-primary" />
        Results for <span className="font-medium text-foreground">&ldquo;{data.query}&rdquo;</span>
      </div>

      {!hasMatches ? (
        <div className="rounded-xl border border-border bg-card p-6 text-sm">
          <p className="font-medium">No strong matches found.</p>
          <p className="text-muted-foreground mt-2">
            Try a broader description (e.g. &ldquo;hospital&rdquo; instead of &ldquo;22-bed
            rural critical-access hospital&rdquo;), or explore the systems directly on the{' '}
            <Link href="/explore" className="text-primary underline">Explore page</Link>.
          </p>
        </div>
      ) : (
        <div className="grid gap-4">
          {data.matches.map((match) => (
            <SystemMatchCard key={match.system_id} match={match} />
          ))}
        </div>
      )}

      {/* Upgrade CTA */}
      <div className="rounded-xl border border-primary/30 bg-primary/5 p-5 sm:p-6">
        <div className="flex items-start gap-3">
          <Sparkles className="size-5 text-primary mt-0.5" />
          <div className="flex-1 space-y-2">
            <h3 className="font-semibold">Need more systems or programmatic access?</h3>
            <p className="text-sm text-muted-foreground">{data.upgrade_cta}</p>
            <Link
              href="/pricing"
              className="inline-flex items-center gap-1 text-sm font-medium text-primary hover:underline"
            >
              See pricing <ArrowRight className="size-3.5" />
            </Link>
          </div>
        </div>
      </div>

      {/* Disclaimer + report link */}
      <div className="rounded-md border border-border bg-muted/30 p-4 text-xs text-muted-foreground space-y-1.5">
        <p>{data.disclaimer}</p>
        <p>
          <a
            href={data.report_issue_url}
            target="_blank"
            rel="noopener noreferrer"
            className="underline hover:text-foreground"
          >
            Report a data issue on GitHub
          </a>
        </p>
      </div>
    </div>
  )
}

function SystemMatchCard({
  match,
}: {
  match: ClassifyDemoResponse['matches'][number]
}) {
  const color = getSystemColor(match.system_id)

  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div
        className="px-5 py-3 border-b border-border flex items-center justify-between"
        style={{ backgroundColor: `${color}12` }}
      >
        <div className="flex items-center gap-2">
          <span
            className="inline-block size-2.5 rounded-full"
            style={{ backgroundColor: color }}
          />
          <span className="font-semibold text-sm">{match.system_name}</span>
        </div>
        <Link
          href={`/system/${match.system_id}`}
          className="text-xs text-muted-foreground hover:text-foreground hover:underline"
        >
          Browse all codes →
        </Link>
      </div>
      <ul className="divide-y divide-border">
        {match.results.map((r, i) => (
          <li key={`${match.system_id}-${r.code}`} className="px-5 py-3">
            <Link
              href={`/system/${match.system_id}/node/${r.code}`}
              className="flex items-start justify-between gap-4 group"
            >
              <div className="flex-1 min-w-0">
                <div className="flex items-baseline gap-2">
                  <span className="font-mono text-sm font-semibold text-foreground">
                    {r.code}
                  </span>
                  {i === 0 && (
                    <span className="text-[10px] uppercase tracking-wide font-semibold text-primary bg-primary/10 rounded px-1.5 py-0.5">
                      Top match
                    </span>
                  )}
                </div>
                <div className="text-sm text-foreground/90 mt-0.5 group-hover:text-primary transition-colors">
                  {r.title}
                </div>
              </div>
              <ArrowRight className="size-4 text-muted-foreground group-hover:text-primary mt-1 shrink-0" />
            </Link>
          </li>
        ))}
      </ul>
    </div>
  )
}
