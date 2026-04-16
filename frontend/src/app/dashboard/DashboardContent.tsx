'use client'

import { Suspense } from 'react'
import { useSearchParams, useRouter } from 'next/navigation'
import { useQuery } from '@tanstack/react-query'
import { getSystems, getStats } from '@/lib/api'
import {
  groupSystemsByCategory, SYSTEM_CATEGORIES, DOMAIN_SECTORS,
  LIFE_SCIENCES_SECTORS, getLifeSciencesSector,
} from '@/lib/categories'
import Link from 'next/link'
import { Globe, GitBranch, Network, ArrowUpRight } from 'lucide-react'
import type { ClassificationSystem, CrosswalkStat } from '@/lib/types'

interface DashboardContentProps {
  initialSystems: ClassificationSystem[] | null
  initialStats: CrosswalkStat[] | null
}

export function DashboardWrapper(props: DashboardContentProps) {
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center min-h-[50vh]">
        <div className="h-8 w-8 border-2 border-primary border-t-transparent rounded-full animate-spin" />
      </div>
    }>
      <DashboardInner {...props} />
    </Suspense>
  )
}

function DashboardInner({ initialSystems, initialStats }: DashboardContentProps) {
  const searchParams = useSearchParams()
  const router = useRouter()
  const activeCat = searchParams.get('cat') ?? ''
  const activeSectorId = searchParams.get('sector') ?? ''

  const { data: systems } = useQuery({
    queryKey: ['systems'],
    queryFn: getSystems,
    initialData: initialSystems ?? undefined,
    staleTime: 0,
  })

  const { data: stats } = useQuery({
    queryKey: ['stats'],
    queryFn: getStats,
    initialData: initialStats ?? undefined,
    staleTime: 0,
  })

  const totalNodes = systems?.reduce((sum, s) => sum + s.node_count, 0) ?? 0
  const totalEdges = stats?.reduce((sum, s) => sum + s.edge_count, 0) ?? 0
  const grouped = systems ? groupSystemsByCategory(systems) : []
  const maxNodes = systems ? Math.max(...systems.map((s) => s.node_count)) : 1

  // Domain sector helpers
  const allDomainSystems = grouped.find((g) => g.category.id === 'domain')?.systems ?? []
  const domainSectorsPresent = DOMAIN_SECTORS.filter((sector) =>
    allDomainSystems.some((s) =>
      s.id === 'domain_adv_materials' ? sector.id === 'materials' : s.id.startsWith(sector.prefix)
    )
  )
  const traditionalSectors = domainSectorsPresent.filter((s) => s.group === 'traditional')
  const emergingSectors = domainSectorsPresent.filter((s) => s.group === 'emerging')

  // Life Sciences sector helpers
  const allLSSystems = grouped.find((g) => g.category.id === 'lifesciences')?.systems ?? []
  const lsSectorsPresent = LIFE_SCIENCES_SECTORS.filter((sector) =>
    allLSSystems.some((s) => getLifeSciencesSector(s.id)?.id === sector.id)
  )

  // Generic active sector (works for both domain and life sciences)
  const activeSectorDef = activeSectorId
    ? (DOMAIN_SECTORS.find((s) => s.id === activeSectorId)
       ?? LIFE_SCIENCES_SECTORS.find((s) => s.id === activeSectorId)
       ?? null)
    : null

  function sectorSystemCount(sectorId: string): number {
    const domainDef = DOMAIN_SECTORS.find((s) => s.id === sectorId)
    if (domainDef) {
      return allDomainSystems.filter((s) =>
        s.id === 'domain_adv_materials' ? domainDef.id === 'materials' : s.id.startsWith(domainDef.prefix)
      ).length
    }
    const lsDef = LIFE_SCIENCES_SECTORS.find((s) => s.id === sectorId)
    if (lsDef) {
      return allLSSystems.filter((s) => getLifeSciencesSector(s.id)?.id === lsDef.id).length
    }
    return 0
  }

  function filterBySector(catSystems: ClassificationSystem[], catId: string) {
    if (!activeSectorId) return catSystems
    if (catId === 'domain') {
      const def = DOMAIN_SECTORS.find((s) => s.id === activeSectorId)
      if (!def) return catSystems
      return catSystems.filter((s) =>
        s.id === 'domain_adv_materials' ? def.id === 'materials' : s.id.startsWith(def.prefix)
      )
    }
    if (catId === 'lifesciences') {
      return catSystems.filter((s) => getLifeSciencesSector(s.id)?.id === activeSectorId)
    }
    return catSystems
  }

  function handleSectorClick(sectorId: string, catId: string) {
    if (sectorId === activeSectorId) {
      router.push(`/dashboard?cat=${catId}`)
    } else {
      router.push(`/dashboard?cat=${catId}&sector=${sectorId}`)
    }
  }

  // Top crosswalk pairs (deduplicated, sorted by edge count)
  const topCrosswalks = (() => {
    if (!stats) return []
    const seen = new Set<string>()
    return stats
      .filter((s) => {
        const key = [s.source_system, s.target_system].sort().join('|')
        if (seen.has(key)) return false
        seen.add(key)
        return true
      })
      .sort((a, b) => b.edge_count - a.edge_count)
      .slice(0, 12)
  })()

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 py-8 space-y-10">

      {/* Page header */}
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Systems Overview</h1>
        <p className="text-sm text-muted-foreground mt-1">
          All {systems?.length ?? '...'} classification systems across {SYSTEM_CATEGORIES.length} categories
        </p>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-4">
        {[
          { icon: Globe,     value: systems?.length ?? 0, label: 'Classification Systems', mono: false, href: '/explore' },
          { icon: GitBranch, value: totalNodes,            label: 'Total Nodes',            mono: true,  href: '/explore' },
          { icon: Network,   value: totalEdges,            label: 'Crosswalk Edges',        mono: true,  href: '/crosswalk-explorer' },
        ].map(({ icon: Icon, value, label, mono, href }) => (
          <Link key={label} href={href} className="p-5 rounded-xl bg-card border border-border/50 space-y-1 hover:border-border hover:shadow-sm transition-all group">
            <div className="flex items-center gap-2 text-muted-foreground">
              <Icon className="h-4 w-4" />
              <span className="text-xs font-medium">{label}</span>
              <ArrowUpRight className="h-3 w-3 ml-auto opacity-0 group-hover:opacity-100 transition-opacity" />
            </div>
            <div className={`text-3xl font-bold ${mono ? 'font-mono tabular-nums' : ''}`}>
              {mono ? value.toLocaleString() : value}
            </div>
          </Link>
        ))}
      </div>

      {/* Category filter tabs */}
      <div className="flex flex-wrap gap-2">
        <Link
          href="/dashboard"
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
            !activeCat
              ? 'bg-primary text-primary-foreground'
              : 'bg-secondary text-muted-foreground hover:text-foreground'
          }`}
        >
          All
        </Link>
        {SYSTEM_CATEGORIES.map((cat) => {
          const g = grouped.find((g) => g.category.id === cat.id)
          if (!g || g.systems.length === 0) return null
          return (
            <Link
              key={cat.id}
              href={`/dashboard?cat=${cat.id}`}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
                activeCat === cat.id
                  ? 'text-white'
                  : 'bg-secondary text-muted-foreground hover:text-foreground'
              }`}
              style={activeCat === cat.id ? { backgroundColor: cat.accent } : {}}
            >
              {cat.label}
              <span className="ml-1.5 opacity-60">{g.systems.length}</span>
            </Link>
          )
        })}
      </div>

      {/* Domain sector sub-filter */}
      {activeCat === 'domain' && domainSectorsPresent.length > 0 && (
        <div className="space-y-2 rounded-xl border border-border/50 bg-card/50 p-4">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Filter by sector</span>
          </div>

          {/* All pill */}
          <div className="flex flex-wrap items-center gap-1.5 mb-2">
            <button
              onClick={() => router.push('/dashboard?cat=domain')}
              className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                !activeSectorId
                  ? 'bg-foreground text-background'
                  : 'bg-secondary text-muted-foreground hover:text-foreground'
              }`}
            >
              All
              <span className="ml-1 font-mono opacity-70">{allDomainSystems.length}</span>
            </button>
          </div>

          {traditionalSectors.length > 0 && (
            <div className="space-y-1.5">
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground/60">Traditional</p>
              <div className="flex flex-wrap gap-1.5">
                {traditionalSectors.slice().sort((a, b) => a.label.localeCompare(b.label)).map((sector) => (
                  <button
                    key={sector.id}
                    onClick={() => handleSectorClick(sector.id, 'domain')}
                    className={`px-2.5 py-1 rounded-full text-xs font-medium transition-colors ${
                      activeSectorId === sector.id
                        ? 'text-white'
                        : 'bg-secondary text-muted-foreground hover:text-foreground'
                    }`}
                    style={activeSectorId === sector.id ? { backgroundColor: sector.accent } : {}}
                  >
                    {sector.label}
                    <span className="ml-1 font-mono opacity-70">{sectorSystemCount(sector.id)}</span>
                  </button>
                ))}
              </div>
            </div>
          )}

          {emergingSectors.length > 0 && (
            <div className="space-y-1.5 mt-2">
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground/60">Emerging Tech</p>
              <div className="flex flex-wrap gap-1.5">
                {emergingSectors.slice().sort((a, b) => a.label.localeCompare(b.label)).map((sector) => (
                  <button
                    key={sector.id}
                    onClick={() => handleSectorClick(sector.id, 'domain')}
                    className={`px-2.5 py-1 rounded-full text-xs font-medium transition-colors ${
                      activeSectorId === sector.id
                        ? 'text-white'
                        : 'bg-secondary text-muted-foreground hover:text-foreground'
                    }`}
                    style={activeSectorId === sector.id ? { backgroundColor: sector.accent } : {}}
                  >
                    {sector.label}
                    <span className="ml-1 font-mono opacity-70">{sectorSystemCount(sector.id)}</span>
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Life Sciences sector sub-filter */}
      {activeCat === 'lifesciences' && lsSectorsPresent.length > 0 && (
        <div className="space-y-2 rounded-xl border border-border/50 bg-card/50 p-4">
          <div className="flex items-center gap-2 mb-3">
            <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Filter by sector</span>
          </div>

          <div className="flex flex-wrap items-center gap-1.5 mb-2">
            <button
              onClick={() => router.push('/dashboard?cat=lifesciences')}
              className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                !activeSectorId
                  ? 'bg-foreground text-background'
                  : 'bg-secondary text-muted-foreground hover:text-foreground'
              }`}
            >
              All
              <span className="ml-1 font-mono opacity-70">{allLSSystems.length}</span>
            </button>
          </div>

          <div className="flex flex-wrap gap-1.5">
            {lsSectorsPresent.map((sector) => (
              <button
                key={sector.id}
                onClick={() => handleSectorClick(sector.id, 'lifesciences')}
                className={`px-2.5 py-1 rounded-full text-xs font-medium transition-colors ${
                  activeSectorId === sector.id
                    ? 'text-white'
                    : 'bg-secondary text-muted-foreground hover:text-foreground'
                }`}
                style={activeSectorId === sector.id ? { backgroundColor: sector.accent } : {}}
              >
                {sector.label}
                <span className="ml-1 font-mono opacity-70">{sectorSystemCount(sector.id)}</span>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Systems grouped by category */}
      <div className="space-y-8">
        {grouped
          .filter((g) => !activeCat || g.category.id === activeCat)
          .map(({ category: cat, systems: catSystems }) => {
            const displaySystems = (cat.id === 'domain' || cat.id === 'lifesciences')
              ? filterBySector(catSystems, cat.id)
              : catSystems
            const sectorLabel = activeSectorDef && (cat.id === 'domain' || cat.id === 'lifesciences')
              ? ` - ${activeSectorDef.label}`
              : ''
            return (
            <div key={cat.id}>
              {/* Category header */}
              <div className="flex items-center gap-3 mb-3">
                <div
                  className="w-3 h-3 rounded-sm shrink-0"
                  style={{ backgroundColor: activeSectorDef && (cat.id === 'domain' || cat.id === 'lifesciences') ? activeSectorDef.accent : cat.accent }}
                />
                <h2 className="text-base font-semibold">{cat.label}{sectorLabel}</h2>
                <span className="text-xs text-muted-foreground">
                  {displaySystems.length} system{displaySystems.length !== 1 ? 's' : ''} &middot;{' '}
                  {displaySystems.reduce((s, x) => s + x.node_count, 0).toLocaleString()} nodes
                </span>
              </div>

              {/* Systems table */}
              <div className="rounded-xl border border-border/50 overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-muted/40 border-b border-border/40">
                      <th className="text-left px-4 py-2.5 text-xs font-medium text-muted-foreground">System</th>
                      <th className="text-left px-4 py-2.5 text-xs font-medium text-muted-foreground hidden sm:table-cell">Region</th>
                      <th className="text-right px-4 py-2.5 text-xs font-medium text-muted-foreground w-20">Nodes</th>
                      <th className="px-4 py-2.5 w-36 hidden md:table-cell" />
                    </tr>
                  </thead>
                  <tbody>
                    {displaySystems
                      .slice()
                      .sort((a, b) => b.node_count - a.node_count)
                      .map((sys) => {
                        const pct = maxNodes > 0 ? (sys.node_count / maxNodes) * 100 : 0
                        const color = sys.tint_color || cat.accent
                        return (
                          <tr
                            key={sys.id}
                            className="border-b border-border/30 last:border-0 hover:bg-muted/20 transition-colors"
                          >
                            <td className="px-4 py-3">
                              <Link
                                href={`/system/${sys.id}`}
                                className="flex items-center gap-2 group"
                              >
                                <span
                                  className="w-2.5 h-2.5 rounded-full shrink-0"
                                  style={{ backgroundColor: color }}
                                />
                                <span className="font-medium group-hover:text-primary transition-colors">
                                  {sys.name}
                                </span>
                                <ArrowUpRight className="h-3 w-3 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity shrink-0" />
                              </Link>
                            </td>
                            <td className="px-4 py-3 text-xs text-muted-foreground hidden sm:table-cell">
                              {sys.region ?? '-'}
                            </td>
                            <td className="px-4 py-3 text-right font-mono text-xs tabular-nums">
                              {sys.node_count.toLocaleString()}
                            </td>
                            {/* Visual bar */}
                            <td className="px-4 py-3 hidden md:table-cell">
                              <div className="h-1.5 rounded-full bg-muted overflow-hidden w-full">
                                <div
                                  className="h-full rounded-full transition-all"
                                  style={{ width: `${Math.max(pct, 1)}%`, backgroundColor: color }}
                                />
                              </div>
                            </td>
                          </tr>
                        )
                      })}
                  </tbody>
                </table>
              </div>
            </div>
          )
          })}
      </div>

      {/* Top crosswalks */}
      {!activeCat && topCrosswalks.length > 0 && (
        <div className="space-y-3">
          <h2 className="text-base font-semibold">Top Crosswalk Connections</h2>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
            {topCrosswalks.map((s, i) => (
              <div
                key={i}
                className="flex items-center justify-between px-4 py-3 rounded-lg bg-card border border-border/50"
              >
                <div className="flex items-center gap-2 text-xs font-mono min-w-0">
                  <span className="truncate text-muted-foreground">{s.source_system}</span>
                  <span className="text-border shrink-0">&#8644;</span>
                  <span className="truncate text-muted-foreground">{s.target_system}</span>
                </div>
                <span className="text-xs font-mono font-semibold tabular-nums shrink-0 ml-2">
                  {s.edge_count.toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
