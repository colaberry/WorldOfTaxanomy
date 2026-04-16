import type { Metadata } from 'next'
import { DashboardWrapper } from './DashboardContent'
import { serverGetSystems, serverGetStats } from '@/lib/server-api'

export const metadata: Metadata = {
  title: 'Systems Overview - WorldOfTaxonomy',
  description:
    'Browse all 1,000+ classification systems across 16 categories. ' +
    'Industry, health, trade, occupations, regulatory, and more.',
  openGraph: {
    title: 'Systems Overview - WorldOfTaxonomy',
    description: '1,000+ classification systems across 16 categories.',
    url: 'https://worldoftaxonomy.com/dashboard',
    type: 'website',
  },
}

export default async function DashboardPage() {
  let systems = null
  let stats = null

  try {
    ;[systems, stats] = await Promise.all([
      serverGetSystems(),
      serverGetStats(),
    ])
  } catch {
    // Backend unavailable - client component will fetch on its own
  }

  return <DashboardWrapper initialSystems={systems} initialStats={stats} />
}
