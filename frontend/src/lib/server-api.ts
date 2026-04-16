/**
 * Server-side API client for Next.js Server Components.
 *
 * Fetches data from the backend during server-side rendering so that
 * crawlers see fully-rendered HTML. Uses time-based revalidation
 * (ISR) since tag-based caching requires CacheLife profiles in
 * Next.js 15. On-demand invalidation uses revalidatePath via the
 * /api/revalidate webhook endpoint.
 */

const BACKEND_URL = process.env.BACKEND_URL ?? 'http://localhost:8000'

// Default revalidate: 1 hour. Human users always get fresh data
// via client-side React Query refetch with staleTime: 0.
const DEFAULT_REVALIDATE = 3600

export async function serverFetch<T>(
  path: string,
  options?: { revalidate?: number | false },
): Promise<T> {
  const { revalidate = DEFAULT_REVALIDATE } = options ?? {}

  const fetchOptions: RequestInit & { next?: Record<string, unknown> } = {
    headers: { 'Content-Type': 'application/json' },
    next: {},
  }

  if (revalidate !== undefined && revalidate !== false) {
    fetchOptions.next!.revalidate = revalidate
  }

  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 5000)
  try {
    const res = await fetch(`${BACKEND_URL}${path}`, {
      ...fetchOptions,
      signal: controller.signal,
    })
    if (!res.ok) {
      throw new Error(`Server API error ${res.status}: ${path}`)
    }
    return res.json()
  } finally {
    clearTimeout(timeout)
  }
}

// ---- Typed helpers ----

import type {
  ClassificationSystem,
  SystemDetail,
  ClassificationNode,
  Equivalence,
  CrosswalkStat,
} from './types'
import type { CountryProfile } from './api'

export async function serverGetSystems(): Promise<ClassificationSystem[]> {
  return serverFetch('/api/v1/systems')
}

export async function serverGetSystem(id: string): Promise<SystemDetail> {
  return serverFetch(`/api/v1/systems/${id}`)
}

export async function serverGetNode(
  systemId: string,
  code: string,
): Promise<ClassificationNode> {
  return serverFetch(`/api/v1/systems/${systemId}/nodes/${code}`)
}

export async function serverGetChildren(
  systemId: string,
  code: string,
): Promise<ClassificationNode[]> {
  return serverFetch(`/api/v1/systems/${systemId}/nodes/${code}/children`)
}

export async function serverGetAncestors(
  systemId: string,
  code: string,
): Promise<ClassificationNode[]> {
  return serverFetch(`/api/v1/systems/${systemId}/nodes/${code}/ancestors`)
}

export async function serverGetEquivalences(
  systemId: string,
  code: string,
): Promise<Equivalence[]> {
  return serverFetch(`/api/v1/systems/${systemId}/nodes/${code}/equivalences`)
}

export async function serverGetStats(): Promise<CrosswalkStat[]> {
  return serverFetch('/api/v1/equivalences/stats')
}

export async function serverGetCountryProfile(
  code: string,
): Promise<CountryProfile> {
  return serverFetch(`/api/v1/countries/${code.toUpperCase()}`)
}
