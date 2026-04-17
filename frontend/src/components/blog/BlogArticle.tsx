'use client'

import { useEffect, useRef } from 'react'
import Link from 'next/link'
import { ArrowLeft, ExternalLink, Calendar, User as UserIcon } from 'lucide-react'
import { MermaidBlock } from '@/components/wiki/MermaidBlock'

interface BlogArticleProps {
  title: string
  html: string
  slug: string
  date: string
  author: string
  tags: string[]
}

export function BlogArticle({ title, html, slug, date, author, tags }: BlogArticleProps) {
  const contentRef = useRef<HTMLDivElement>(null)

  const formattedDate = new Date(date + 'T00:00:00').toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  })

  useEffect(() => {
    if (!contentRef.current) return

    const codeBlocks = contentRef.current.querySelectorAll('code.language-mermaid')
    codeBlocks.forEach((block) => {
      const pre = block.parentElement
      if (!pre || pre.tagName !== 'PRE') return

      const mermaidCode = block.textContent || ''
      const container = document.createElement('div')
      container.className = 'mermaid-container my-6'
      container.setAttribute('data-mermaid', mermaidCode)
      pre.replaceWith(container)
    })
  }, [html])

  return (
    <div className="max-w-3xl mx-auto px-4 sm:px-6 py-10">
      <div className="flex items-center gap-3 mb-6">
        <Link
          href="/blog"
          className="flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          All posts
        </Link>
      </div>

      <h1 className="text-2xl font-bold tracking-tight mb-3">{title}</h1>

      <div className="flex items-center gap-4 text-sm text-muted-foreground mb-2">
        <span className="flex items-center gap-1.5">
          <Calendar className="h-3.5 w-3.5" />
          {formattedDate}
        </span>
        <span className="flex items-center gap-1.5">
          <UserIcon className="h-3.5 w-3.5" />
          {author}
        </span>
      </div>

      {tags.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mb-8">
          {tags.map((tag) => (
            <span
              key={tag}
              className="px-2 py-0.5 text-xs rounded-full bg-secondary text-muted-foreground"
            >
              {tag}
            </span>
          ))}
        </div>
      )}

      <article
        ref={contentRef}
        className="prose prose-sm dark:prose-invert max-w-none
          prose-headings:font-semibold prose-headings:tracking-tight
          prose-h2:text-xl prose-h2:mt-10 prose-h2:mb-4
          prose-h3:text-lg prose-h3:mt-8 prose-h3:mb-3
          prose-p:text-muted-foreground prose-p:leading-relaxed
          prose-a:text-primary prose-a:no-underline hover:prose-a:underline
          prose-code:text-sm prose-code:bg-secondary/50 prose-code:px-1.5 prose-code:py-0.5 prose-code:rounded
          prose-pre:bg-secondary/30 prose-pre:border prose-pre:border-border/50
          prose-table:text-sm
          prose-th:text-left prose-th:font-semibold prose-th:border-b prose-th:border-border
          prose-td:border-b prose-td:border-border/30 prose-td:py-2
          prose-li:text-muted-foreground"
        dangerouslySetInnerHTML={{ __html: html }}
      />

      <MermaidBlock />

      <div className="mt-12 pt-6 border-t border-border/50 flex items-center justify-between">
        <Link
          href="/blog"
          className="text-sm text-muted-foreground hover:text-foreground transition-colors"
        >
          Back to all posts
        </Link>
        <a
          href={`https://github.com/colaberry/WorldOfTaxonomy/edit/main/blog/${slug}.md`}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
        >
          Edit on GitHub
          <ExternalLink className="h-3.5 w-3.5" />
        </a>
      </div>
    </div>
  )
}
