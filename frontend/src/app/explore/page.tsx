'use client'

import { Suspense, useState, useEffect, useCallback } from 'react'
import { useSearchParams } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'
import { search, getSystems } from '@/lib/api'
import {
  SYSTEM_CATEGORIES,
  getCategoryForSystem,
  DOMAIN_SECTORS,
  getDomainSector,
  LIFE_SCIENCES_SECTORS,
  getLifeSciencesSector,
} from '@/lib/categories'
import { getSystemColor } from '@/lib/colors'
import Link from 'next/link'
import { Search, X, ChevronDown, Leaf } from 'lucide-react'
import type { ClassificationNodeWithContext } from '@/lib/types'

const EXAMPLE_QUERIES = ['hospital', 'mining', 'software', 'physician', 'logistics', 'agriculture']
const INITIAL_VISIBLE = 5

export default function ExplorePage() {
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center min-h-[50vh]">
        <div className="h-8 w-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
      </div>
    }>
      <ExploreContent />
    </Suspense>
  )
}

function ExploreContent() {
  const searchParams = useSearchParams()
  const initialQuery = searchParams.get('q') || ''

  const [query, setQuery] = useState(initialQuery)
  const [debouncedQuery, setDebouncedQuery] = useState(initialQuery)
  const [selectedSystem, setSelectedSystem] = useState('')
  const [categoryFilter, setCategoryFilter] = useState('all')
  // Track how many results are expanded per category
  const [expanded, setExpanded] = useState<Record<string, number>>({})

  // Sync URL param on mount
  useEffect(() => {
    const q = searchParams.get('q')
    if (q) { setQuery(q); setDebouncedQuery(q) }
  }, [searchParams])

  // Debounce: fire search 300ms after the user stops typing
  useEffect(() => {
    const t = setTimeout(() => setDebouncedQuery(query), 300)
    return () => clearTimeout(t)
  }, [query])

  // Reset category filter and expansions when query changes
  useEffect(() => {
    setCategoryFilter('all')
    setExpanded({})
  }, [debouncedQuery, selectedSystem])

  const { data: systems } = useQuery({
    queryKey: ['systems'],
    queryFn: getSystems,
  })

  const { data: results, isLoading, isFetching } = useQuery({
    queryKey: ['search', debouncedQuery, selectedSystem],
    queryFn: () => search(debouncedQuery, selectedSystem || undefined, 200, true),
    enabled: debouncedQuery.length >= 2,
    staleTime: 2 * 60 * 1000,
  })

  // Build category buckets from results
  const categoryBuckets = (() => {
    if (!results) return []
    const buckets: { catId: string; nodes: ClassificationNodeWithContext[] }[] = []
    const seen = new Map<string, ClassificationNodeWithContext[]>()

    for (const node of results) {
      const cat = getCategoryForSystem(node.system_id)
      if (!seen.has(cat.id)) seen.set(cat.id, [])
      seen.get(cat.id)!.push(node)
    }

    // Preserve SYSTEM_CATEGORIES order
    for (const cat of SYSTEM_CATEGORIES) {
      if (seen.has(cat.id)) {
        buckets.push({ catId: cat.id, nodes: seen.get(cat.id)! })
      }
    }
    return buckets
  })()

  const activeBuckets =
    categoryFilter === 'all'
      ? categoryBuckets
      : categoryBuckets.filter((b) => b.catId === categoryFilter)

  const totalCount = results?.length ?? 0

  const handleExpand = useCallback((catId: string, total: number) => {
    setExpanded((prev) => ({ ...prev, [catId]: total }))
  }, [])

  const handleClear = () => {
    setQuery('')
    setDebouncedQuery('')
    setSelectedSystem('')
    setCategoryFilter('all')
    setExpanded({})
  }

  const isEmpty = debouncedQuery.length >= 2 && !isLoading && results?.length === 0

  return (
    <div className="max-w-4xl mx-auto px-4 sm:px-6 py-8 space-y-6">

      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Explore Classifications</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Search across all {systems?.length ?? '...'} classification systems
          {systems && ` and ${systems.reduce((s, x) => s + x.node_count, 0).toLocaleString()} codes`}
        </p>
      </div>

      {/* Search bar */}
      <div className="flex flex-col sm:flex-row gap-2">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search codes, titles, descriptions..."
            autoFocus
            className="w-full pl-10 pr-9 py-2.5 rounded-lg bg-card border border-border/50 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 focus:border-primary"
          />
          {query && (
            <button
              onClick={handleClear}
              className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
        <select
          value={selectedSystem}
          onChange={(e) => setSelectedSystem(e.target.value)}
          className="px-3 py-2.5 rounded-lg bg-card border border-border/50 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50 sm:w-44"
        >
          <option value="">All Systems</option>
          {systems
            ?.slice()
            .sort((a, b) => a.name.localeCompare(b.name))
            .map((s) => (
              <option key={s.id} value={s.id}>{s.name}</option>
            ))}
        </select>
      </div>

      {/* Category filter pills */}
      {categoryBuckets.length > 1 && (
        <div className="flex flex-wrap gap-1.5">
          <button
            onClick={() => setCategoryFilter('all')}
            className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium transition-colors ${
              categoryFilter === 'all'
                ? 'bg-foreground text-background'
                : 'bg-secondary text-muted-foreground hover:text-foreground'
            }`}
          >
            All
            <span className="font-mono opacity-70">{totalCount}</span>
          </button>
          {categoryBuckets.map(({ catId, nodes }) => {
            const cat = SYSTEM_CATEGORIES.find((c) => c.id === catId)!
            return (
              <button
                key={catId}
                onClick={() => setCategoryFilter(catId === categoryFilter ? 'all' : catId)}
                className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  categoryFilter === catId
                    ? 'text-white'
                    : 'bg-secondary text-muted-foreground hover:text-foreground'
                }`}
                style={categoryFilter === catId ? { backgroundColor: cat.accent } : {}}
              >
                {cat.label}
                <span className="font-mono opacity-70">{nodes.length}</span>
              </button>
            )
          })}
        </div>
      )}

      {/* Spinner while fetching */}
      {(isLoading || isFetching) && debouncedQuery.length >= 2 && (
        <div className="flex justify-center py-8">
          <div className="h-6 w-6 border-2 border-primary border-t-transparent rounded-full animate-spin" />
        </div>
      )}

      {/* Results */}
      {!isLoading && !isFetching && activeBuckets.length > 0 && (
        <div className="space-y-8">
          {activeBuckets.map(({ catId, nodes }) => {
            const cat = SYSTEM_CATEGORIES.find((c) => c.id === catId)!

            // Sector-grouped categories (Domain and Life Sciences)
            if (catId === 'domain' || catId === 'lifesciences') {
              const isDomain = catId === 'domain'
              const sectorDefs = isDomain ? DOMAIN_SECTORS : LIFE_SCIENCES_SECTORS
              const getSector = isDomain
                ? (id: string) => getDomainSector(id)
                : (id: string) => getLifeSciencesSector(id)

              // Build sector groups preserving sector definition order
              const sectorMap = new Map<string, typeof nodes>()
              for (const node of nodes) {
                const sector = getSector(node.system_id)
                const key = sector?.id ?? '_other'
                if (!sectorMap.has(key)) sectorMap.set(key, [])
                sectorMap.get(key)!.push(node)
              }
              // Ordered list: known sectors first, then _other
              const orderedSectors: Array<{ sectorId: string; sectorNodes: typeof nodes }> = []
              for (const sector of sectorDefs) {
                if (sectorMap.has(sector.id)) {
                  orderedSectors.push({ sectorId: sector.id, sectorNodes: sectorMap.get(sector.id)! })
                }
              }
              if (sectorMap.has('_other')) {
                orderedSectors.push({ sectorId: '_other', sectorNodes: sectorMap.get('_other')! })
              }

              return (
                <section key={catId}>
                  {/* Category header */}
                  <div className="flex items-center gap-2 mb-3">
                    <span className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: cat.accent }} />
                    <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                      {cat.label}
                    </span>
                    <span className="text-xs text-muted-foreground font-mono">
                      {nodes.length} match{nodes.length !== 1 ? 'es' : ''}
                    </span>
                    <div className="flex-1 h-px bg-border/30" />
                  </div>

                  {/* Sector sub-groups */}
                  <div className="space-y-4">
                    {orderedSectors.map(({ sectorId, sectorNodes }) => {
                      const sectorDef = sectorDefs.find((s) => s.id === sectorId)
                      const expandKey = `${catId}_${sectorId}`
                      const visible = expanded[expandKey] ?? INITIAL_VISIBLE
                      const shown = sectorNodes.slice(0, visible)
                      const remaining = sectorNodes.length - shown.length
                      return (
                        <div key={sectorId}>
                          {/* Sector sub-header */}
                          <div className="flex items-center gap-1.5 mb-1.5">
                            <span
                              className="w-1.5 h-1.5 rounded-full shrink-0"
                              style={{ backgroundColor: sectorDef?.accent ?? cat.accent }}
                            />
                            <span className="text-[11px] font-semibold text-muted-foreground/80">
                              {sectorDef?.label ?? 'Other'}
                            </span>
                            <span className="text-[11px] text-muted-foreground/50 font-mono">
                              {sectorNodes.length}
                            </span>
                          </div>
                          <div className="space-y-0.5 pl-3">
                            {shown.map((node, i) => (
                              <ResultRow
                                key={`${node.system_id}-${node.code}-${i}`}
                                node={node}
                                systems={systems ?? []}
                                query={debouncedQuery}
                              />
                            ))}
                          </div>
                          {remaining > 0 && (
                            <button
                              onClick={() => handleExpand(expandKey, sectorNodes.length)}
                              className="mt-1.5 flex items-center gap-1.5 pl-3 text-xs text-muted-foreground hover:text-foreground transition-colors"
                            >
                              <ChevronDown className="h-3.5 w-3.5" />
                              Show {remaining} more in {sectorDef?.label ?? 'Other'}
                            </button>
                          )}
                        </div>
                      )
                    })}
                  </div>
                </section>
              )
            }

            // All other categories: flat list
            const visible = expanded[catId] ?? INITIAL_VISIBLE
            const shown = nodes.slice(0, visible)
            const remaining = nodes.length - shown.length

            return (
              <section key={catId}>
                {/* Category header */}
                <div className="flex items-center gap-2 mb-3">
                  <span
                    className="w-2 h-2 rounded-full shrink-0"
                    style={{ backgroundColor: cat.accent }}
                  />
                  <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                    {cat.label}
                  </span>
                  <span className="text-xs text-muted-foreground font-mono">
                    {nodes.length} match{nodes.length !== 1 ? 'es' : ''}
                  </span>
                  <div className="flex-1 h-px bg-border/30" />
                </div>

                {/* Result rows */}
                <div className="space-y-0.5">
                  {shown.map((node, i) => (
                    <ResultRow
                      key={`${node.system_id}-${node.code}-${i}`}
                      node={node}
                      systems={systems ?? []}
                      query={debouncedQuery}
                    />
                  ))}
                </div>

                {/* Show more */}
                {remaining > 0 && (
                  <button
                    onClick={() => handleExpand(catId, nodes.length)}
                    className="mt-2 flex items-center gap-1.5 pl-3 text-xs text-muted-foreground hover:text-foreground transition-colors"
                  >
                    <ChevronDown className="h-3.5 w-3.5" />
                    Show {remaining} more in {cat.label}
                  </button>
                )}
              </section>
            )
          })}
        </div>
      )}

      {/* Empty state */}
      {isEmpty && (
        <div className="text-center py-16 text-muted-foreground space-y-2">
          <Search className="h-8 w-8 mx-auto opacity-30" />
          <p className="text-sm">No results for &ldquo;{debouncedQuery}&rdquo;</p>
          <p className="text-xs">Try a broader term or select a different system</p>
        </div>
      )}

      {/* Prompt state */}
      {!debouncedQuery && (
        <div className="text-center py-16 space-y-4 text-muted-foreground">
          <Search className="h-8 w-8 mx-auto opacity-30" />
          <p className="text-sm">Enter a keyword, code, or phrase above</p>
          <div className="flex flex-wrap gap-2 justify-center">
            {EXAMPLE_QUERIES.map((term) => (
              <button
                key={term}
                onClick={() => { setQuery(term); setDebouncedQuery(term) }}
                className="px-3 py-1.5 rounded-full bg-secondary hover:bg-secondary/70 transition-colors text-xs font-mono"
              >
                {term}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Single result row ────────────────────────────────────────────────────────

function ResultRow({
  node,
  systems,
  query,
}: {
  node: ClassificationNodeWithContext
  systems: { id: string; name: string }[]
  query: string
}) {
  const sysName = systems.find((s) => s.id === node.system_id)?.name ?? node.system_id
  const sysColor = getSystemColor(node.system_id)
  // Direct parent = last ancestor
  const parent = node.ancestors && node.ancestors.length > 0
    ? node.ancestors[node.ancestors.length - 1]
    : null
  // Grandparent = second-to-last ancestor (shown as part of breadcrumb for depth > 2)
  const grandparent = node.ancestors && node.ancestors.length > 1
    ? node.ancestors[node.ancestors.length - 2]
    : null

  return (
    <Link
      href={`/system/${node.system_id}/node/${encodeURIComponent(node.code)}`}
      className="flex items-start gap-3 px-3 py-2.5 rounded-lg hover:bg-card border border-transparent hover:border-border/40 transition-all group"
    >
      {/* Leaf indicator */}
      <span className="shrink-0 w-4 pt-1 flex justify-center">
        {node.is_leaf && <Leaf className="h-3 w-3 text-emerald-500/60" />}
      </span>

      {/* System indicator */}
      <div className="flex items-center gap-1.5 shrink-0 w-28 pt-0.5">
        <span
          className="w-1.5 h-1.5 rounded-full shrink-0"
          style={{ backgroundColor: sysColor }}
        />
        <span
          className="text-[11px] font-medium truncate"
          style={{ color: sysColor }}
        >
          {sysName}
        </span>
      </div>

      {/* Code */}
      <span className="font-mono text-xs text-muted-foreground shrink-0 w-20 pt-0.5 group-hover:text-foreground/70 transition-colors">
        {node.code}
      </span>

      {/* Title + breadcrumb */}
      <div className="flex-1 min-w-0">
        <p className="text-sm text-foreground/85 group-hover:text-foreground transition-colors truncate">
          <Highlight text={node.title} query={query} />
        </p>
        {parent && (
          <p className="text-xs text-muted-foreground truncate mt-0.5">
            {grandparent ? (
              <>{grandparent.title} <span className="opacity-50">›</span> {parent.title}</>
            ) : (
              parent.title
            )}
          </p>
        )}
      </div>
    </Link>
  )
}

// ── Keyword highlight ────────────────────────────────────────────────────────

function Highlight({ text, query }: { text: string; query: string }) {
  if (!query || query.length < 2) return <>{text}</>
  try {
    const re = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi')
    const parts = text.split(re)
    return (
      <>
        {parts.map((part, i) =>
          re.test(part) ? (
            <mark key={i} className="bg-primary/20 text-foreground rounded-sm px-0.5">
              {part}
            </mark>
          ) : (
            part
          )
        )}
      </>
    )
  } catch {
    return <>{text}</>
  }
}
