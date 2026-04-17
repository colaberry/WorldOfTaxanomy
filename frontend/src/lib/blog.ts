import fs from 'fs'
import path from 'path'
import { remark } from 'remark'
import remarkGfm from 'remark-gfm'
import remarkHtml from 'remark-html'

const BLOG_DIR = path.join(process.cwd(), 'src', 'content', 'blog')

export interface BlogMeta {
  slug: string
  file: string
  title: string
  description: string
  date: string
  author: string
  tags: string[]
}

let _cachedMeta: BlogMeta[] | null = null

export function getBlogMeta(): BlogMeta[] {
  if (_cachedMeta) return _cachedMeta
  const metaPath = path.join(BLOG_DIR, '_meta.json')
  const raw = fs.readFileSync(metaPath, 'utf-8')
  const data: BlogMeta[] = JSON.parse(raw)
  _cachedMeta = data.sort((a, b) => b.date.localeCompare(a.date))
  return _cachedMeta
}

export function getBlogContent(slug: string): string | null {
  const meta = getBlogMeta()
  const entry = meta.find((e) => e.slug === slug)
  if (!entry) return null
  const filePath = path.join(BLOG_DIR, entry.file)
  if (!fs.existsSync(filePath)) return null
  return fs.readFileSync(filePath, 'utf-8')
}

export async function renderBlogHtml(markdown: string): Promise<string> {
  const result = await remark().use(remarkGfm).use(remarkHtml, { sanitize: false }).process(markdown)
  return result.toString()
}

export function getBlogSlugs(): string[] {
  return getBlogMeta().map((e) => e.slug)
}
