import type { MetadataRoute } from 'next'
import { getWikiSlugs } from '@/lib/wiki'
import { getBlogSlugs } from '@/lib/blog'

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
    { url: `${SITE_URL}/explore`, lastModified: new Date(), changeFrequency: 'weekly', priority: 0.9 },
    { url: `${SITE_URL}/guide`, lastModified: new Date(), changeFrequency: 'weekly', priority: 0.9 },
    { url: `${SITE_URL}/blog`, lastModified: new Date(), changeFrequency: 'weekly', priority: 0.8 },
    { url: `${SITE_URL}/developers`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.8 },
    { url: `${SITE_URL}/api`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.8 },
    { url: `${SITE_URL}/mcp`, lastModified: new Date(), changeFrequency: 'monthly', priority: 0.8 },
    ...guidePages,
    ...blogPages,
  ]

  try {
    const res = await fetch(`${BACKEND_URL}/api/v1/systems`, {
      next: { revalidate: 3600 },
    })
    if (!res.ok) return staticPages

    const systems: Array<{ id: string }> = await res.json()
    const systemUrls: MetadataRoute.Sitemap = systems.map((s) => ({
      url: `${SITE_URL}/system/${s.id}`,
      lastModified: new Date(),
      changeFrequency: 'monthly' as const,
      priority: 0.8,
    }))

    return [...staticPages, ...systemUrls]
  } catch {
    return staticPages
  }
}
