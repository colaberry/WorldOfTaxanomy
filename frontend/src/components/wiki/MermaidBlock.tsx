'use client'

import { useEffect } from 'react'
import { useTheme } from 'next-themes'

export function MermaidBlock() {
  const { resolvedTheme } = useTheme()

  useEffect(() => {
    const containers = document.querySelectorAll('.mermaid-container[data-mermaid]')
    if (containers.length === 0) return

    let cancelled = false

    async function renderDiagrams() {
      const mermaid = (await import('mermaid')).default
      mermaid.initialize({
        startOnLoad: false,
        theme: resolvedTheme === 'dark' ? 'dark' : 'default',
        securityLevel: 'loose',
      })

      containers.forEach(async (container, i) => {
        if (cancelled) return
        const code = container.getAttribute('data-mermaid')
        if (!code) return

        try {
          const { svg } = await mermaid.render(`mermaid-${i}-${Date.now()}`, code)
          if (!cancelled) {
            container.innerHTML = svg
            container.classList.add('rendered')
          }
        } catch {
          // If mermaid fails, show the code as a fallback
          container.innerHTML = `<pre class="text-xs text-muted-foreground bg-secondary/30 p-4 rounded overflow-x-auto"><code>${code}</code></pre>`
        }
      })
    }

    renderDiagrams()

    return () => {
      cancelled = true
    }
  }, [resolvedTheme])

  return null
}
