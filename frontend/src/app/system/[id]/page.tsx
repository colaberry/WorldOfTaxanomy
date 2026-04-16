import type { Metadata } from 'next'
import { serverGetSystem, serverGetStats, serverGetSystems } from '@/lib/server-api'
import { SystemDetail } from './SystemDetail'

interface Props {
  params: Promise<{ id: string }>
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { id } = await params
  try {
    const system = await serverGetSystem(id)
    return {
      title: `${system.name} - WorldOfTaxonomy`,
      description: `Explore ${system.node_count.toLocaleString()} codes in ${system.full_name ?? system.name}. Browse the hierarchy, search codes, and find cross-system equivalences.`,
      openGraph: {
        title: `${system.name} Classification System`,
        description: `${system.node_count.toLocaleString()} codes across ${system.region ?? 'Global'}. ${system.authority ?? ''}`.trim(),
        url: `https://worldoftaxonomy.com/system/${id}`,
        type: 'website',
      },
      alternates: {
        canonical: `https://worldoftaxonomy.com/system/${id}`,
      },
    }
  } catch {
    return { title: 'System - WorldOfTaxonomy' }
  }
}

export default async function SystemPage({ params }: Props) {
  const { id } = await params

  let system = null
  let stats = null
  let systems = null

  try {
    ;[system, stats, systems] = await Promise.all([
      serverGetSystem(id),
      serverGetStats(),
      serverGetSystems(),
    ])
  } catch {
    // Server fetch failed - client component will fetch from browser
  }

  return (
    <SystemDetail
      id={id}
      initialSystem={system}
      initialStats={stats}
      initialSystems={systems}
    />
  )
}
