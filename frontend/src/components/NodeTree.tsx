'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import Link from 'next/link'
import { ChevronRight, Leaf, ExternalLink } from 'lucide-react'
import { getChildren, getEquivalences } from '@/lib/api'
import { getSectorColor, getSystemColor } from '@/lib/colors'
import type { ClassificationNode, ClassificationSystem, Equivalence } from '@/lib/types'

// ── Single tree row ──────────────────────────────────────────────────────────

interface NodeRowProps {
  systemId: string
  node: ClassificationNode
  systems: ClassificationSystem[]
}

function NodeRow({ systemId, node, systems }: NodeRowProps) {
  const [expanded, setExpanded] = useState(false)

  const { data: children, isFetching: loadingChildren } = useQuery({
    queryKey: ['tree-children', systemId, node.code],
    queryFn: () => getChildren(systemId, node.code),
    enabled: expanded && !node.is_leaf,
    staleTime: 5 * 60 * 1000,
  })

  const { data: equivalences } = useQuery({
    queryKey: ['equivalences', systemId, node.code],
    queryFn: () => getEquivalences(systemId, node.code),
    enabled: expanded,
    staleTime: 5 * 60 * 1000,
  })

  const sectorColor = getSectorColor(node.sector_code ?? node.code)

  // One chip per unique target system, first equivalence wins, max 3 systems
  const chips: Equivalence[] = []
  if (equivalences) {
    const seen = new Set<string>()
    for (const e of equivalences) {
      if (!seen.has(e.target_system)) {
        seen.add(e.target_system)
        chips.push(e)
        if (chips.length >= 3) break
      }
    }
  }
  const totalSystems = equivalences
    ? new Set(equivalences.map((e) => e.target_system)).size
    : 0
  const hiddenCount = totalSystems - chips.length

  return (
    <div>
      {/* Row */}
      <div
        role="button"
        tabIndex={0}
        onClick={() => !node.is_leaf && setExpanded((v) => !v)}
        onKeyDown={(e) => {
          if ((e.key === 'Enter' || e.key === ' ') && !node.is_leaf) setExpanded((v) => !v)
        }}
        className={`group flex items-center gap-2 px-2 py-1.5 rounded-lg transition-colors ${
          node.is_leaf ? 'cursor-default' : 'cursor-pointer hover:bg-card/70'
        }`}
      >
        {/* Expand toggle / leaf icon */}
        <span className="w-4 h-4 flex items-center justify-center shrink-0 text-muted-foreground">
          {!node.is_leaf ? (
            <ChevronRight
              className={`h-3.5 w-3.5 transition-transform duration-150 ${
                expanded ? 'rotate-90' : ''
              }`}
            />
          ) : (
            <Leaf className="h-3 w-3 text-emerald-500/50" />
          )}
        </span>

        {/* Code badge */}
        <span
          className="font-mono text-xs px-1.5 py-0.5 rounded shrink-0"
          style={{ color: sectorColor, backgroundColor: `${sectorColor}18` }}
        >
          {node.code}
        </span>

        {/* Title */}
        <span className="text-sm flex-1 min-w-0 truncate text-foreground/75 group-hover:text-foreground transition-colors">
          {node.title}
        </span>

        {/* Equivalence chips - appear after expand + load */}
        {chips.length > 0 && (
          <div className="hidden sm:flex items-center gap-1 shrink-0">
            {chips.map((e) => {
              const sys = systems.find((s) => s.id === e.target_system)
              const sysColor = getSystemColor(e.target_system)
              // "NAICS 2022" -> "NAICS", "HS 2022" -> "HS", etc.
              const prefix = sys?.name.split(' ')[0] ?? e.target_system.split('_')[0].toUpperCase()
              return (
                <Link
                  key={`${e.target_system}-${e.target_code}`}
                  href={`/system/${e.target_system}/node/${encodeURIComponent(e.target_code)}`}
                  onClick={(ev) => ev.stopPropagation()}
                  title={`${sys?.name ?? e.target_system}: ${e.target_code}${e.target_title ? ` - ${e.target_title}` : ''} (${e.match_type})`}
                  className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[11px] font-mono hover:opacity-70 transition-opacity"
                  style={{ color: sysColor, backgroundColor: `${sysColor}15` }}
                >
                  <span className="opacity-60">{prefix}</span>
                  <span className="ml-0.5">{e.target_code}</span>
                </Link>
              )
            })}
            {hiddenCount > 0 && (
              <span className="text-[11px] text-muted-foreground">+{hiddenCount}</span>
            )}
          </div>
        )}

        {/* Detail page link - visible on hover */}
        <Link
          href={`/system/${systemId}/node/${encodeURIComponent(node.code)}`}
          onClick={(e) => e.stopPropagation()}
          title="View full detail"
          className="w-5 h-5 flex items-center justify-center text-muted-foreground hover:text-foreground opacity-0 group-hover:opacity-100 transition-opacity shrink-0"
        >
          <ExternalLink className="h-3.5 w-3.5" />
        </Link>
      </div>

      {/* Children with guide line */}
      {expanded && !node.is_leaf && (
        <div className="ml-3 pl-3 border-l border-border/25">
          {loadingChildren ? (
            <div className="py-2 pl-1 text-xs text-muted-foreground animate-pulse">
              Loading...
            </div>
          ) : children && children.length > 0 ? (
            children.map((child) => (
              <NodeRow
                key={child.code}
                systemId={systemId}
                node={child}
                systems={systems}
              />
            ))
          ) : (
            <div className="py-2 pl-1 text-xs text-muted-foreground">
              No sub-classifications
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ── Tree root ────────────────────────────────────────────────────────────────

export function NodeTree({
  systemId,
  roots,
  systems,
}: {
  systemId: string
  roots: ClassificationNode[]
  systems: ClassificationSystem[]
}) {
  return (
    <div className="rounded-xl border border-border/50 bg-card/20 overflow-hidden">
      {/* Header */}
      <div className="px-4 py-2.5 border-b border-border/40 flex items-center justify-between">
        <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Hierarchy Explorer
        </span>
        <span className="text-xs text-muted-foreground hidden sm:block">
          Click to expand - equivalences appear inline
        </span>
      </div>

      {/* Rows */}
      <div className="p-2">
        {roots.map((root) => (
          <NodeRow
            key={root.code}
            systemId={systemId}
            node={root}
            systems={systems}
          />
        ))}
      </div>
    </div>
  )
}
