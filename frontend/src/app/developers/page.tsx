import Link from 'next/link'
import { GitFork, Terminal, Braces, ArrowRight, Key, Zap, BookOpen, ChevronRight } from 'lucide-react'

const ENDPOINTS = [
  { method: 'GET',  path: '/api/v1/systems',                               desc: 'List all 82 classification systems' },
  { method: 'GET',  path: '/api/v1/systems/{id}',                          desc: 'System detail with node count and metadata' },
  { method: 'GET',  path: '/api/v1/search?q={term}',                       desc: 'Full-text search across all 532k+ nodes' },
  { method: 'GET',  path: '/api/v1/systems/{id}/nodes/{code}',             desc: 'Fetch a specific classification code' },
  { method: 'GET',  path: '/api/v1/systems/{id}/nodes/{code}/children',    desc: 'Browse hierarchy downward' },
  { method: 'GET',  path: '/api/v1/systems/{id}/nodes/{code}/ancestors',   desc: 'Get full path to root' },
  { method: 'GET',  path: '/api/v1/systems/{id}/nodes/{code}/equivalences','desc': 'Crosswalk mappings to other systems' },
  { method: 'GET',  path: '/api/v1/equivalences/stats',                    desc: 'Crosswalk edge counts per system pair' },
  { method: 'GET',  path: '/api/v1/countries/stats',                       desc: 'Taxonomy coverage stats for all countries' },
  { method: 'GET',  path: '/api/v1/countries/{code}',                      desc: 'Country taxonomy profile (official + recommended systems)' },
  { method: 'POST', path: '/api/v1/auth/register',                         desc: 'Create an account' },
  { method: 'POST', path: '/api/v1/auth/login',                            desc: 'Authenticate and receive a JWT' },
  { method: 'POST', path: '/api/v1/auth/keys',                             desc: 'Create a long-lived API key' },
]

const MCP_TOOLS = [
  { name: 'list_classification_systems',   desc: 'List all 82 available systems with node counts' },
  { name: 'get_industry',                  desc: 'Fetch a code by system + code identifier' },
  { name: 'search_classifications',        desc: 'Full-text search across all nodes' },
  { name: 'browse_children',               desc: 'Navigate hierarchy downward from a parent code' },
  { name: 'get_ancestors',                 desc: 'Get full path from a code back to the root' },
  { name: 'get_equivalences',              desc: 'Get all crosswalk edges for a code' },
  { name: 'translate_code',               desc: 'Convert a code from one system to another' },
  { name: 'translate_across_all_systems', desc: 'Translate a code to every linked system at once' },
  { name: 'get_sector_overview',           desc: 'Summary stats and structure for a sector' },
  { name: 'compare_sector',               desc: 'Compare how a sector is represented across systems' },
  { name: 'find_by_keyword_all_systems',  desc: 'Keyword search in every system simultaneously' },
  { name: 'get_crosswalk_coverage',        desc: 'Show edge counts between system pairs' },
  { name: 'get_system_diff',               desc: 'Diff two classification systems structurally' },
  { name: 'get_siblings',                  desc: 'Get sibling codes at the same hierarchy level' },
  { name: 'get_subtree_summary',           desc: 'Count nodes under a parent code' },
  { name: 'resolve_ambiguous_code',        desc: 'Disambiguate a code that appears in multiple systems' },
  { name: 'get_leaf_count',                desc: 'Count leaf nodes in a subtree' },
  { name: 'get_region_mapping',            desc: 'Map a region or country to its classification systems' },
  { name: 'get_country_taxonomy_profile',  desc: 'Full taxonomy profile for a country by ISO code' },
  { name: 'explore_industry_tree',         desc: 'Interactive step-by-step hierarchy exploration' },
  { name: 'describe_match_types',          desc: 'Explain the crosswalk match quality types (exact, broad, etc.)' },
]

const METHOD_COLORS: Record<string, string> = {
  GET:  'text-emerald-500 bg-emerald-500/10',
  POST: 'text-blue-500 bg-blue-500/10',
}

export default function DevelopersPage() {
  return (
    <div className="max-w-5xl mx-auto px-4 sm:px-6 py-12 space-y-16">

      {/* ── Hero ── */}
      <div className="space-y-3">
        <div className="flex items-center gap-2 text-xs text-muted-foreground font-medium">
          <Zap className="h-3.5 w-3.5 text-primary" />
          Developers
        </div>
        <h1 className="text-3xl sm:text-4xl font-bold tracking-tight">
          Build on the world&apos;s most comprehensive classification graph
        </h1>
        <p className="text-muted-foreground text-base max-w-2xl leading-relaxed">
          82 classification systems, 532k+ nodes, and 58k+ crosswalk edges - available via
          REST API, MCP server (for AI assistants), or directly from the open-source repo.
        </p>
        <div className="flex flex-wrap gap-3 pt-2">
          <Link
            href="https://github.com/colaberry/WorldOfTaxanomy"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-secondary text-secondary-foreground text-sm font-medium hover:bg-secondary/80 transition-colors"
          >
            <GitFork className="h-4 w-4" />
            View on GitHub
          </Link>
          <Link
            href="/explore"
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors"
          >
            <BookOpen className="h-4 w-4" />
            Explore the data
          </Link>
        </div>
      </div>

      {/* ── GitHub ── */}
      <section id="github" className="space-y-5">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-secondary border border-border/50">
            <GitFork className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-semibold">GitHub</h2>
            <p className="text-sm text-muted-foreground">Open source - fork, contribute, or self-host</p>
          </div>
        </div>

        <div className="rounded-xl border border-border/50 bg-card p-6 space-y-4">
          <div className="grid sm:grid-cols-3 gap-4 text-sm">
            {[
              { label: 'Repository', value: 'colaberry/WorldOfTaxanomy' },
              { label: 'License',    value: 'Open Source' },
              { label: 'Stack',      value: 'Python + Next.js + PostgreSQL' },
            ].map(({ label, value }) => (
              <div key={label}>
                <p className="text-xs text-muted-foreground font-medium uppercase tracking-wide mb-1">{label}</p>
                <p className="font-mono text-sm">{value}</p>
              </div>
            ))}
          </div>

          <div className="border-t border-border/50 pt-4 space-y-2">
            <p className="text-sm font-medium">Quick start</p>
            <pre className="rounded-lg bg-secondary/60 px-4 py-3 text-xs font-mono overflow-x-auto text-foreground/90 leading-relaxed">
{`git clone https://github.com/colaberry/WorldOfTaxanomy.git
cd WorldOfTaxanomy

# Install backend dependencies
pip install -r requirements.txt

# Configure database (copy .env.example and fill in DATABASE_URL)
cp .env.example .env

# Run the API
python3 -m uvicorn world_of_taxanomy.api.app:create_app --factory --port 8000

# Run the frontend (separate terminal)
cd frontend && npm install && npm run dev`}
            </pre>
          </div>

          <Link
            href="https://github.com/colaberry/WorldOfTaxanomy"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline"
          >
            Open repository <ArrowRight className="h-3.5 w-3.5" />
          </Link>
        </div>
      </section>

      {/* ── REST API ── */}
      <section id="api" className="space-y-5">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-secondary border border-border/50">
            <Braces className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-semibold">REST API</h2>
            <p className="text-sm text-muted-foreground">HTTP JSON API - no SDK needed</p>
          </div>
        </div>

        <div className="rounded-xl border border-border/50 bg-card divide-y divide-border/50">

          {/* Base URL + auth overview */}
          <div className="p-6 space-y-4">
            <div className="grid sm:grid-cols-3 gap-4 text-sm">
              {[
                { label: 'Base URL',      value: 'https://your-deployment/api/v1' },
                { label: 'Auth',          value: 'Bearer token or API key header' },
                { label: 'Rate limits',   value: '30 req/min anon - 1000 req/min auth' },
              ].map(({ label, value }) => (
                <div key={label}>
                  <p className="text-xs text-muted-foreground font-medium uppercase tracking-wide mb-1">{label}</p>
                  <p className="font-mono text-xs text-foreground/80">{value}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Quick example */}
          <div className="p-6 space-y-2">
            <p className="text-sm font-medium">Example - search across all systems</p>
            <pre className="rounded-lg bg-secondary/60 px-4 py-3 text-xs font-mono overflow-x-auto text-foreground/90 leading-relaxed">
{`# Anonymous (no auth required for read endpoints)
curl "https://your-deployment/api/v1/search?q=physician&limit=5"

# Authenticated (higher rate limit)
curl "https://your-deployment/api/v1/search?q=physician" \\
  -H "Authorization: Bearer <your-token>"

# Translate NAICS -> ISIC
curl "https://your-deployment/api/v1/systems/naics_2022/nodes/6211/equivalences"`}
            </pre>
          </div>

          {/* Auth flow */}
          <div className="p-6 space-y-3">
            <p className="text-sm font-medium flex items-center gap-2">
              <Key className="h-4 w-4 text-muted-foreground" />
              Authentication flow
            </p>
            <ol className="space-y-2 text-sm text-muted-foreground list-none">
              {[
                'POST /api/v1/auth/register  - create an account',
                'POST /api/v1/auth/login     - receive a short-lived JWT (15 min)',
                'POST /api/v1/auth/keys      - create a long-lived API key',
                'Include key in requests:    Authorization: Bearer wot_<your-key>',
              ].map((step, i) => (
                <li key={i} className="flex items-start gap-3">
                  <span className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-secondary text-xs font-medium text-foreground">
                    {i + 1}
                  </span>
                  <code className="font-mono text-xs text-foreground/80 pt-0.5">{step}</code>
                </li>
              ))}
            </ol>
          </div>

          {/* Endpoints table */}
          <div className="p-6 space-y-3">
            <p className="text-sm font-medium">Endpoints</p>
            <div className="space-y-1">
              {ENDPOINTS.map(({ method, path, desc }) => (
                <div key={path} className="flex flex-col sm:flex-row sm:items-center gap-1 sm:gap-3 py-2 border-b border-border/30 last:border-0 text-sm">
                  <span className={`inline-flex items-center rounded px-2 py-0.5 text-[11px] font-mono font-semibold w-fit shrink-0 ${METHOD_COLORS[method] ?? ''}`}>
                    {method}
                  </span>
                  <code className="font-mono text-xs text-foreground/80 flex-1 truncate">{path}</code>
                  <span className="text-xs text-muted-foreground sm:text-right shrink-0 max-w-xs">{desc}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* ── MCP Server ── */}
      <section id="mcp" className="space-y-5">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-secondary border border-border/50">
            <Terminal className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-semibold">MCP Server</h2>
            <p className="text-sm text-muted-foreground">Connect Claude Desktop or any MCP-compatible AI assistant</p>
          </div>
        </div>

        <div className="rounded-xl border border-border/50 bg-card divide-y divide-border/50">

          {/* What it is */}
          <div className="p-6 space-y-3">
            <p className="text-sm leading-relaxed text-muted-foreground">
              The MCP (Model Context Protocol) server lets AI assistants like Claude query the
              taxonomy graph directly - searching codes, translating between systems, navigating
              hierarchies, and exploring country profiles - all from within a conversation.
              No API key or HTTP client needed on the AI side.
            </p>
            <div className="grid sm:grid-cols-3 gap-4 text-sm pt-2">
              {[
                { label: 'Protocol',   value: 'JSON-RPC over stdio' },
                { label: 'Transport',  value: 'stdin / stdout' },
                { label: 'Tools',      value: '21 available' },
              ].map(({ label, value }) => (
                <div key={label}>
                  <p className="text-xs text-muted-foreground font-medium uppercase tracking-wide mb-1">{label}</p>
                  <p className="font-mono text-sm">{value}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Claude Desktop setup */}
          <div className="p-6 space-y-3">
            <p className="text-sm font-medium">Connect to Claude Desktop</p>
            <p className="text-xs text-muted-foreground">
              Add the following to your{' '}
              <code className="font-mono">claude_desktop_config.json</code>
              {' '}(macOS: <code className="font-mono">~/Library/Application Support/Claude/claude_desktop_config.json</code>):
            </p>
            <pre className="rounded-lg bg-secondary/60 px-4 py-3 text-xs font-mono overflow-x-auto text-foreground/90 leading-relaxed">
{`{
  "mcpServers": {
    "world-of-taxanomy": {
      "command": "/usr/bin/python3",
      "args": ["-m", "world_of_taxanomy", "mcp"],
      "env": {
        "PYTHONPATH": "/path/to/WorldOfTaxanomy",
        "DATABASE_URL": "postgresql://user:pass@host/db?sslmode=require"
      }
    }
  }
}`}
            </pre>
            <p className="text-xs text-muted-foreground">
              Replace <code className="font-mono">/path/to/WorldOfTaxanomy</code> with your local clone path
              and supply your Neon (or other PostgreSQL) <code className="font-mono">DATABASE_URL</code>.
              Restart Claude Desktop after saving.
            </p>
          </div>

          {/* Run directly */}
          <div className="p-6 space-y-2">
            <p className="text-sm font-medium">Run the server directly</p>
            <pre className="rounded-lg bg-secondary/60 px-4 py-3 text-xs font-mono overflow-x-auto text-foreground/90 leading-relaxed">
{`# From the repo root (requires DATABASE_URL in environment)
python3 -m world_of_taxanomy mcp`}
            </pre>
          </div>

          {/* Tools list */}
          <div className="p-6 space-y-3">
            <p className="text-sm font-medium">Available tools ({MCP_TOOLS.length})</p>
            <div className="grid sm:grid-cols-2 gap-x-8 gap-y-2">
              {MCP_TOOLS.map(({ name, desc }) => (
                <div key={name} className="flex items-start gap-2 py-1 border-b border-border/20 last:border-0">
                  <ChevronRight className="h-3.5 w-3.5 text-primary shrink-0 mt-0.5" />
                  <div>
                    <code className="text-[11px] font-mono text-foreground/90">{name}</code>
                    <p className="text-[11px] text-muted-foreground mt-0.5">{desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* ── Footer CTA ── */}
      <div className="rounded-xl border border-border/50 bg-card p-6 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
        <div>
          <p className="font-semibold">Questions or contributions?</p>
          <p className="text-sm text-muted-foreground mt-0.5">
            Open an issue or pull request on GitHub - all feedback welcome.
          </p>
        </div>
        <Link
          href="https://github.com/colaberry/WorldOfTaxanomy/issues"
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-2 px-4 py-2 rounded-lg bg-secondary text-secondary-foreground text-sm font-medium hover:bg-secondary/80 transition-colors shrink-0"
        >
          <GitFork className="h-4 w-4" />
          Open an issue
        </Link>
      </div>

    </div>
  )
}
