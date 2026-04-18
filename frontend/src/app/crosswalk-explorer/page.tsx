import { getStaticSystems, getStaticStats, getStaticAllSections } from '@/lib/crosswalk-data'
import CrosswalkExplorerClient from './CrosswalkExplorerClient'

export default function CrosswalkExplorerPage() {
  const allSystems = getStaticSystems()
  const stats = getStaticStats()
  const allSections = getStaticAllSections()

  // Pre-filter: only send crosswalked systems to client (~100 vs 1000, ~10x smaller payload)
  const ids = new Set<string>()
  for (const st of stats) {
    ids.add(st.source_system)
    ids.add(st.target_system)
  }
  const systems = allSystems.filter(s => ids.has(s.id))

  return <CrosswalkExplorerClient systems={systems} stats={stats} allSections={allSections} />
}
