import type { MetadataRoute } from 'next'
import { getWikiSlugs } from '@/lib/wiki'
import { getBlogSlugs } from '@/lib/blog'
import { MAJOR_SYSTEMS } from './codes/constants'

const SITE_URL = 'https://worldoftaxonomy.com'
const BACKEND_URL = process.env.BACKEND_URL ?? 'http://localhost:8000'

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const guidePages: MetadataRoute.Sitemap = getWikiSlugs().map((slug) => ({
    url: `${SITE_URL}/guide/${slug}`,
    lastModified: new Date(),
    changeFrequency: 'monthly' as const,
    priority: 0.8,
  }))

  const blogPages: MetadataRoute.Sitemap = getBlogSlugs().map((slug) => ({
    url: `${SITE_URL}/blog/${slug}`,
    lastModified: new Date(),
    changeFrequency: 'weekly' as const,
    priority: 0.7,
  }))

  const staticPages: MetadataRoute.Sitemap = [
    { url: SITE_URL, lastModified: new Date(), changeFrequency: 'weekly', priority: 1.0 },
    { url: `${SITE_URL}/classify`, lastModified: new Date(), changeFrequency: 'weekly', priority: 0.95 },
    { url: `${SITE_URL}/codes`, lastModified: new Date(), changeFrequency: 'weekly', priority: 0.95 },
    { url: `${SITE_URL}/explore`, lastModified: new Date(), changeFrequency: 'weekly', priority: 0.9 },
    { url: `${SITE_URL}/guide`, lastModified: new Date(), changeFrequency: 'weekly', priority: 0.9 },
    { url: `${SITE_URL}/blog`, lastModified: new Date(), changeFrequency: 'weekly', priority: 0.8 },
    { url: `${SITE_URL}/developers`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.8 },
    { url: `${SITE_URL}/api`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.8 },
    { url: `${SITE_URL}/mcp`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.8 },
    ...guidePages,
    ...blogPages,
  ]

  const codesSystemHubs: MetadataRoute.Sitemap = MAJOR_SYSTEMS.map((id) => ({
    url: `${SITE_URL}/codes/${id}`,
    lastModified: new Date(),
    changeFrequency: 'monthly' as const,
    priority: 0.85,
  }))

  try {
    const [systemsRes, rootsRes] = await Promise.all([
      fetch(`${BACKEND_URL}/api/v1/systems`, { next: { revalidate: 3600 } }),
      Promise.all(
        MAJOR_SYSTEMS.map(async (id) => {
          const r = await fetch(`${BACKEND_URL}/api/v1/systems/${id}`, {
            next: { revalidate: 3600 },
          })
          if (!r.ok) return { id, roots: [] as Array<{ code: string }> }
          const detail: { roots: Array<{ code: string }> } = await r.json()
          return { id, roots: detail.roots ?? [] }
        }),
      ),
    ])
    if (!systemsRes.ok) return [...staticPages, ...codesSystemHubs]

    const systems: Array<{ id: string }> = await systemsRes.json()
    const systemUrls: MetadataRoute.Sitemap = systems.map((s) => ({
      url: `${SITE_URL}/system/${s.id}`,
      lastModified: new Date(),
      changeFrequency: 'monthly' as const,
      priority: 0.8,
    }))

    const codeSectorUrls: MetadataRoute.Sitemap = rootsRes.flatMap(({ id, roots }) =>
      roots.map((root) => ({
        url: `${SITE_URL}/codes/${id}/${encodeURIComponent(root.code)}`,
        lastModified: new Date(),
        changeFrequency: 'monthly' as const,
        priority: 0.85,
      })),
    )

    return [...staticPages, ...codesSystemHubs, ...codeSectorUrls, ...systemUrls]
  } catch {
    return [...staticPages, ...codesSystemHubs]
  }
}
