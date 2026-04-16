'use client'

import { useState, useMemo, useRef, useCallback } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useRouter } from 'next/navigation'
import { getSystems, getStats, getCrosswalkGraph } from '@/lib/api'
import {
  CrosswalkGraph,
  type CrosswalkGraphHandle,
  type SelectedSystemNode,
} from '@/components/visualizations/CrosswalkGraph'
import { getCategoryForSystem } from '@/lib/categories'
import {
  Network, GitCompareArrows, Loader2, ChevronDown, ArrowLeft,
  Search, X, ExternalLink,
} from 'lucide-react'

type Mode = 'system' | 'code'

export default function CrosswalkExplorerPage() {
  const router = useRouter()
  const graphRef = useRef<CrosswalkGraphHandle>(null)
  const [mode, setMode] = useState<Mode>('system')
  const [sourceSystem, setSourceSystem] = useState('')
  const [targetSystem, setTargetSystem] = useState('')
  const [loadPair, setLoadPair] = useState<{ source: string; target: string } | null>(null)
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedNode, setSelectedNode] = useState<SelectedSystemNode | null>(null)

  const { data: systems } = useQuery({
    queryKey: ['systems'],
    queryFn: getSystems,
  })

  const { data: stats } = useQuery({
    queryKey: ['crosswalk-stats'],
    queryFn: getStats,
  })

  // Systems that have crosswalks
  const crosswalkedSystems = useMemo(() => {
    if (!stats || !systems) return []
    const ids = new Set<string>()
    for (const st of stats) {
      ids.add(st.source_system)
      ids.add(st.target_system)
    }
    return systems
      .filter((s) => ids.has(s.id))
      .sort((a, b) => a.name.localeCompare(b.name))
  }, [systems, stats])

  // Search results filtered from crosswalked systems
  const searchResults = useMemo(() => {
    if (!searchQuery.trim()) return []
    const q = searchQuery.toLowerCase()
    return crosswalkedSystems
      .filter((s) => s.name.toLowerCase().includes(q) || s.id.toLowerCase().includes(q))
      .slice(0, 8)
  }, [searchQuery, crosswalkedSystems])

  // Available targets for chosen source
  const availableTargets = useMemo(() => {
    if (!sourceSystem || !stats) return []
    const targets = new Set<string>()
    for (const st of stats) {
      if (st.source_system === sourceSystem) targets.add(st.target_system)
      if (st.target_system === sourceSystem) targets.add(st.source_system)
    }
    return crosswalkedSystems.filter((s) => targets.has(s.id))
  }, [sourceSystem, stats, crosswalkedSystems])

  const {
    data: graphData,
    isLoading: graphLoading,
    error: graphError,
  } = useQuery({
    queryKey: ['crosswalk-graph', loadPair?.source, loadPair?.target],
    queryFn: () => getCrosswalkGraph(loadPair!.source, loadPair!.target, 1000),
    enabled: !!loadPair,
  })

  function handleEdgeClick(source: string, target: string) {
    setSourceSystem(source)
    setTargetSystem(target)
    setLoadPair({ source, target })
    setMode('code')
    setSelectedNode(null)
  }

  function handleLoadGraph() {
    if (sourceSystem && targetSystem) {
      setLoadPair({ source: sourceSystem, target: targetSystem })
      setMode('code')
    }
  }

  function handleBackToSystem() {
    setMode('system')
    setLoadPair(null)
    setSelectedNode(null)
  }

  function handleNodeClick(system: string, code: string) {
    router.push(`/system/${system}/node/${code}`)
  }

  const handleNodeSelect = useCallback((node: SelectedSystemNode | null) => {
    setSelectedNode(node)
  }, [])

  function handleSearchSelect(systemId: string) {
    setSearchQuery('')
    graphRef.current?.focusNode(systemId)
  }

  const sourceName = systems?.find((s) => s.id === loadPair?.source)?.name ?? loadPair?.source
  const targetName = systems?.find((s) => s.id === loadPair?.target)?.name ?? loadPair?.target

  return (
    <div className="flex flex-col h-[calc(100vh-3.5rem)]">
      {/* Header bar */}
      <div className="border-b border-border/50 bg-card/80 backdrop-blur-sm px-4 sm:px-6 py-3">
        <div className="max-w-7xl mx-auto flex flex-col sm:flex-row sm:items-center gap-3">
          {/* Title + mode toggle */}
          <div className="flex items-center gap-3 shrink-0">
            {mode === 'code' && (
              <button
                onClick={handleBackToSystem}
                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md text-sm text-muted-foreground hover:text-foreground hover:bg-secondary transition-colors"
                title="Back to system graph"
              >
                <ArrowLeft className="h-4 w-4" />
                <span className="hidden sm:inline">Back</span>
              </button>
            )}
            <h1 className="text-lg font-semibold tracking-tight">
              Crosswalk Explorer
            </h1>
            {mode === 'code' && loadPair && (
              <span className="text-xs text-muted-foreground hidden sm:inline">
                {sourceName} / {targetName}
              </span>
            )}
            <div className="flex rounded-lg border border-border/50 overflow-hidden text-xs">
              <button
                className={`px-3 py-1.5 transition-colors ${
                  mode === 'system'
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-card text-muted-foreground hover:text-foreground'
                }`}
                onClick={() => { setMode('system'); setLoadPair(null); setSelectedNode(null) }}
              >
                <Network className="h-3.5 w-3.5 inline mr-1" />
                Systems
              </button>
              <button
                className={`px-3 py-1.5 transition-colors ${
                  mode === 'code'
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-card text-muted-foreground hover:text-foreground'
                }`}
                onClick={() => setMode('code')}
              >
                <GitCompareArrows className="h-3.5 w-3.5 inline mr-1" />
                Code-level
              </button>
            </div>
          </div>

          {/* System mode: search box */}
          {mode === 'system' && (
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Find a system..."
                className="pl-8 pr-8 py-1.5 rounded-md bg-secondary border border-border/50 text-sm w-52 focus:outline-none focus:ring-2 focus:ring-primary/50 placeholder:text-muted-foreground/60"
              />
              {searchQuery && (
                <button
                  onClick={() => setSearchQuery('')}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              )}
              {/* Search dropdown */}
              {searchResults.length > 0 && (
                <div className="absolute top-full left-0 mt-1 w-72 bg-popover border border-border/50 rounded-lg shadow-lg z-30 py-1 max-h-64 overflow-y-auto">
                  {searchResults.map((s) => {
                    const cat = getCategoryForSystem(s.id)
                    return (
                      <button
                        key={s.id}
                        onClick={() => handleSearchSelect(s.id)}
                        className="w-full flex items-center gap-2 px-3 py-2 text-sm hover:bg-secondary/50 transition-colors text-left"
                      >
                        <span
                          className="w-2.5 h-2.5 rounded-sm shrink-0"
                          style={{ backgroundColor: s.tint_color || cat.accent }}
                        />
                        <span className="truncate flex-1">{s.name}</span>
                        <span className="text-[10px] font-mono text-muted-foreground shrink-0">
                          {s.node_count >= 1000
                            ? `${(s.node_count / 1000).toFixed(0)}k`
                            : s.node_count}
                        </span>
                      </button>
                    )
                  })}
                </div>
              )}
            </div>
          )}

          {/* Code-level controls */}
          {mode === 'code' && (
            <div className="flex items-center gap-2 flex-wrap">
              <div className="relative">
                <select
                  value={sourceSystem}
                  onChange={(e) => {
                    setSourceSystem(e.target.value)
                    setTargetSystem('')
                    setLoadPair(null)
                  }}
                  className="appearance-none pl-3 pr-7 py-1.5 rounded-md bg-secondary border border-border/50 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50 cursor-pointer"
                >
                  <option value="">Source system...</option>
                  {crosswalkedSystems.map((s) => (
                    <option key={s.id} value={s.id}>
                      {s.name}
                    </option>
                  ))}
                </select>
                <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
              </div>

              <span className="text-muted-foreground text-xs">to</span>

              <div className="relative">
                <select
                  value={targetSystem}
                  onChange={(e) => {
                    setTargetSystem(e.target.value)
                    setLoadPair(null)
                  }}
                  disabled={!sourceSystem}
                  className="appearance-none pl-3 pr-7 py-1.5 rounded-md bg-secondary border border-border/50 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50 cursor-pointer disabled:opacity-50"
                >
                  <option value="">Target system...</option>
                  {availableTargets.map((s) => (
                    <option key={s.id} value={s.id}>
                      {s.name}
                    </option>
                  ))}
                </select>
                <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
              </div>

              <button
                onClick={handleLoadGraph}
                disabled={!sourceSystem || !targetSystem}
                className="px-3 py-1.5 rounded-md bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Load graph
              </button>
            </div>
          )}

          {/* Stats badge */}
          {mode === 'system' && systems && stats && (
            <div className="sm:ml-auto text-xs text-muted-foreground">
              {new Set([...stats.map((s) => s.source_system), ...stats.map((s) => s.target_system)]).size} systems
              {' - '}
              {stats.reduce((sum, s) => sum + s.edge_count, 0).toLocaleString()} edges
            </div>
          )}

          {mode === 'code' && loadPair && graphData && (
            <div className="sm:ml-auto text-xs text-muted-foreground">
              {graphData.nodes.length} nodes, {graphData.edges.length} edges
              {graphData.truncated && ` (${graphData.total_edges} total)`}
            </div>
          )}
        </div>
      </div>

      {/* Graph area */}
      <div className="flex-1 relative bg-background">
        {mode === 'system' && systems && stats ? (
          <CrosswalkGraph
            ref={graphRef}
            mode="system"
            systems={systems}
            stats={stats}
            onEdgeClick={handleEdgeClick}
            onNodeSelect={handleNodeSelect}
          />
        ) : mode === 'system' ? (
          <div className="flex items-center justify-center h-full">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : null}

        {mode === 'code' && graphLoading && (
          <div className="flex items-center justify-center h-full">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            <span className="ml-2 text-sm text-muted-foreground">Loading crosswalk graph...</span>
          </div>
        )}

        {mode === 'code' && graphError && (
          <div className="flex items-center justify-center h-full">
            <p className="text-sm text-destructive">
              Failed to load graph. Please try another pair.
            </p>
          </div>
        )}

        {mode === 'code' && graphData && graphData.edges.length > 0 && (
          <CrosswalkGraph
            mode="code"
            data={graphData}
            onNodeClick={handleNodeClick}
          />
        )}

        {mode === 'code' && graphData && graphData.edges.length === 0 && !graphLoading && (
          <div className="flex flex-col items-center justify-center h-full gap-3">
            <GitCompareArrows className="h-10 w-10 text-muted-foreground/40" />
            <p className="text-sm text-muted-foreground">
              No crosswalk edges between these systems.
            </p>
          </div>
        )}

        {mode === 'code' && !loadPair && !graphLoading && (
          <div className="flex flex-col items-center justify-center h-full gap-3">
            <GitCompareArrows className="h-10 w-10 text-muted-foreground/40" />
            <p className="text-sm text-muted-foreground">
              Select two systems above and click &ldquo;Load graph&rdquo; to visualize crosswalk edges.
            </p>
          </div>
        )}

        {/* Selected node info panel */}
        {mode === 'system' && selectedNode && (
          <div className="absolute bottom-3 left-3 z-10 bg-card/95 border border-border/50 rounded-lg shadow-lg p-4 w-80 max-h-[50vh] overflow-y-auto">
            <div className="flex items-start justify-between mb-3">
              <div>
                <h3 className="text-sm font-semibold">{selectedNode.name}</h3>
                <p className="text-[11px] text-muted-foreground">
                  {selectedNode.nodeCount.toLocaleString()} nodes - {selectedNode.category}
                </p>
              </div>
              <div className="flex gap-1">
                <button
                  onClick={() => router.push(`/system/${selectedNode.id}`)}
                  className="p-1 rounded text-muted-foreground hover:text-foreground hover:bg-secondary transition-colors"
                  title="View system"
                >
                  <ExternalLink className="h-3.5 w-3.5" />
                </button>
                <button
                  onClick={() => {
                    setSelectedNode(null)
                    graphRef.current?.resetView()
                  }}
                  className="p-1 rounded text-muted-foreground hover:text-foreground hover:bg-secondary transition-colors"
                  title="Close"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            </div>

            <div className="space-y-1.5">
              <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">
                Connected systems ({selectedNode.connectedSystems.length})
              </p>
              {selectedNode.connectedSystems.map((conn) => (
                <button
                  key={conn.id}
                  onClick={() => handleEdgeClick(selectedNode.id, conn.id)}
                  className="w-full flex items-center justify-between gap-2 px-2 py-1.5 rounded-md text-left hover:bg-secondary/50 transition-colors group"
                >
                  <span className="text-xs truncate">{conn.name}</span>
                  <span className="text-[10px] font-mono text-muted-foreground shrink-0 group-hover:text-primary transition-colors">
                    {conn.edgeCount.toLocaleString()} edges
                  </span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Hint overlay */}
        {mode === 'system' && !selectedNode && systems && stats && (
          <div className="absolute bottom-3 left-1/2 -translate-x-1/2 z-10 bg-card/90 border border-border/50 rounded-lg px-4 py-2 text-xs text-muted-foreground pointer-events-none">
            Click a system to see connections - click an edge for code-level mappings
          </div>
        )}
      </div>
    </div>
  )
}
