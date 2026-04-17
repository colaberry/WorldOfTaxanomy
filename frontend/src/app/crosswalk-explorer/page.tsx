import { getStaticSystems, getStaticStats } from '@/lib/crosswalk-data'
import CrosswalkExplorerClient from './CrosswalkExplorerClient'

export default function CrosswalkExplorerPage() {
  const systems = getStaticSystems()
  const stats = getStaticStats()

  return <CrosswalkExplorerClient systems={systems} stats={stats} />
}
