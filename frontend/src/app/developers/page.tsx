import Link from 'next/link'
import { GitFork, Terminal, Braces, ArrowRight, Key, Zap, BookOpen, ChevronRight, Star, PlusCircle } from 'lucide-react'

async function fetchGithubStars(): Promise<number | null> {
  try {
    const res = await fetch('https://api.github.com/repos/colaberry/WorldOfTaxonomy', {
      headers: { Accept: 'application/vnd.github.v3+json' },
      next: { revalidate: 3600 },
    })
    if (!res.ok) return null
    const data = await res.json()
    return data.stargazers_count ?? null
  } catch {
    return null
  }
}

const ENDPOINTS = [
  { method: 'GET',  path: '/api/v1/systems',                                       desc: 'List all classification systems' },
  { method: 'GET',  path: '/api/v1/systems/{id}',                                  desc: 'System detail with node count and metadata' },
  { method: 'GET',  path: '/api/v1/search?q={term}',                               desc: 'Full-text search across 1.2M+ nodes' },
  { method: 'GET',  path: '/api/v1/systems/{id}/nodes/{code}',                     desc: 'Fetch a specific classification code' },
  { method: 'GET',  path: '/api/v1/systems/{id}/nodes/{code}/children',            desc: 'Browse hierarchy downward' },
  { method: 'GET',  path: '/api/v1/systems/{id}/nodes/{code}/ancestors',           desc: 'Get full path to root' },
  { method: 'GET',  path: '/api/v1/systems/{id}/nodes/{code}/equivalences',        desc: 'Crosswalk mappings to other systems' },
  { method: 'GET',  path: '/api/v1/equivalences/stats',                            desc: 'Crosswalk edge counts per system pair' },
  { method: 'GET',  path: '/api/v1/countries/stats',                               desc: 'Taxonomy coverage stats for all countries' },
  { method: 'GET',  path: '/api/v1/countries/{code}',                              desc: 'Country taxonomy profile (official + recommended systems)' },
  { method: 'POST', path: '/api/v1/classify',                                      desc: 'Classify free-text against all systems (Pro+)' },
  { method: 'GET',  path: '/api/v1/export/systems.jsonl',                          desc: 'Bulk export all systems as JSONL (Pro+)' },
  { method: 'GET',  path: '/api/v1/export/systems/{id}/nodes.jsonl',               desc: 'Bulk export system nodes as JSONL (Pro+)' },
  { method: 'POST', path: '/api/v1/contact',                                       desc: 'Submit enterprise inquiry' },
  { method: 'POST', path: '/api/v1/auth/register',                                 desc: 'Create an account' },
  { method: 'POST', path: '/api/v1/auth/login',                                    desc: 'Authenticate and receive a JWT' },
  { method: 'POST', path: '/api/v1/auth/keys',                                     desc: 'Create a long-lived API key' },
]

const MCP_TOOLS = [
  { name: 'list_classification_systems',   desc: 'List all available systems with node counts' },
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
  { name: 'classify_business',             desc: 'Classify free-text against all taxonomy systems (Pro+)' },
]

const PRICING_TIERS = [
  {
    name: 'Free',
    price: '$0',
    period: '/month',
    features: [
      '5,000 requests/day',
      '100 req/min rate limit',
      'Full search & browse',
      'Classify: 10/day',
      'MCP server: 10/day',
      'Community support',
    ],
    cta: 'Get started',
    ctaHref: '/login',
    highlighted: false,
  },
  {
    name: 'Pro',
    price: '$49',
    period: '/month',
    features: [
      '100,000 requests/day',
      '1,000 req/min rate limit',
      'Full search & browse',
      'Classify: 1,000/day',
      'JSONL bulk export',
      'MCP server: unlimited',
      'Email support',
      '99.9% SLA',
    ],
    cta: 'Start Pro trial',
    ctaHref: '/login',
    highlighted: true,
  },
  {
    name: 'Enterprise',
    price: 'Custom',
    period: '',
    features: [
      'Unlimited requests',
      '10,000 req/min rate limit',
      'Full search & browse',
      'Classify: unlimited',
      'JSONL + custom export',
      'MCP server: unlimited',
      'Dedicated support',
      '99.99% SLA',
    ],
    cta: 'Contact sales',
    ctaHref: '#contact',
    highlighted: false,
  },
]

const METHOD_COLORS: Record<string, string> = {
  GET:  'text-emerald-500 bg-emerald-500/10',
  POST: 'text-blue-500 bg-blue-500/10',
}

export default async function DevelopersPage() {
  const githubStars = await fetchGithubStars()
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
          1,000+ classification systems, 1.2M+ nodes, and 321K+ crosswalk edges - available via
          REST API, MCP server (for AI assistants), or directly from the open-source repo.
        </p>
        <div className="flex flex-wrap gap-3 pt-2">
          <Link
            href="https://github.com/colaberry/WorldOfTaxonomy"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-4 py-2 rounded-lg bg-secondary text-secondary-foreground text-sm font-medium hover:bg-secondary/80 transition-colors"
          >
            <GitFork className="h-4 w-4" />
            View on GitHub
          </Link>
          <Link
            href="https://github.com/colaberry/WorldOfTaxonomy/stargazers"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-4 py-2 rounded-lg border border-border/50 bg-card text-sm font-medium hover:bg-secondary/50 transition-colors"
          >
            <Star className="h-4 w-4 text-yellow-500" />
            Star{githubStars != null ? ` (${githubStars.toLocaleString()})` : ''}
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
          <div className="grid sm:grid-cols-4 gap-4 text-sm">
            {[
              { label: 'Repository', value: 'colaberry/WorldOfTaxonomy' },
              { label: 'License',    value: 'Open Source' },
              { label: 'Stack',      value: 'Python + Next.js + PostgreSQL' },
              { label: 'GitHub Stars', value: githubStars != null ? githubStars.toLocaleString() : '-' },
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
{`git clone https://github.com/colaberry/WorldOfTaxonomy.git
cd WorldOfTaxonomy

# Install backend dependencies
pip install -r requirements.txt

# Configure database (copy .env.example and fill in DATABASE_URL)
cp .env.example .env

# Run the API
python3 -m uvicorn world_of_taxonomy.api.app:create_app --factory --port 8000

# Run the frontend (separate terminal)
cd frontend && npm install && npm run dev`}
            </pre>
          </div>

          <Link
            href="https://github.com/colaberry/WorldOfTaxonomy"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline"
          >
            Open repository <ArrowRight className="h-3.5 w-3.5" />
          </Link>
        </div>
      </section>

      {/* ── REST API ── */}
      {/* ── Guides ── */}
      <section id="guides" className="space-y-5">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-secondary border border-border/50">
            <BookOpen className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-semibold">Guides</h2>
            <p className="text-sm text-muted-foreground">Curated knowledge to use the data effectively</p>
          </div>
        </div>
        <div className="grid sm:grid-cols-2 gap-3">
          {[
            { slug: 'getting-started', title: 'Getting Started', desc: 'API + MCP quickstart, auth, rate limits' },
            { slug: 'crosswalk-map', title: 'Crosswalk Map', desc: 'How 321K+ edges connect classification systems' },
            { slug: 'industry-classification', title: 'Industry Classification', desc: 'Which system to use by country and purpose' },
            { slug: 'medical-coding', title: 'Medical Coding', desc: 'ICD-10 vs ICD-11 vs MeSH vs LOINC compared' },
            { slug: 'trade-codes', title: 'Trade Codes', desc: 'How HS, CPC, UNSPSC, and SITC relate' },
            { slug: 'architecture', title: 'Architecture', desc: 'System design, data flows, and diagrams' },
          ].map((guide) => (
            <Link
              key={guide.slug}
              href={`/guide/${guide.slug}`}
              className="flex items-center justify-between p-3 rounded-lg border border-border/50 bg-card hover:bg-secondary/30 transition-colors group"
            >
              <div>
                <span className="text-sm font-medium group-hover:text-primary transition-colors">{guide.title}</span>
                <p className="text-xs text-muted-foreground mt-0.5">{guide.desc}</p>
              </div>
              <ChevronRight className="h-4 w-4 text-muted-foreground group-hover:text-primary transition-colors shrink-0" />
            </Link>
          ))}
        </div>
        <Link href="/guide" className="inline-flex items-center gap-1.5 text-sm text-primary hover:underline">
          View all guides <ArrowRight className="h-3.5 w-3.5" />
        </Link>
      </section>

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
    "world-of-taxonomy": {
      "command": "/usr/bin/python3",
      "args": ["-m", "world_of_taxonomy", "mcp"],
      "env": {
        "PYTHONPATH": "/path/to/WorldOfTaxonomy",
        "DATABASE_URL": "postgresql://user:pass@host/db?sslmode=require"
      }
    }
  }
}`}
            </pre>
            <p className="text-xs text-muted-foreground">
              Replace <code className="font-mono">/path/to/WorldOfTaxonomy</code> with your local clone path
              and supply your Neon (or other PostgreSQL) <code className="font-mono">DATABASE_URL</code>.
              Restart Claude Desktop after saving.
            </p>
          </div>

          {/* Run directly */}
          <div className="p-6 space-y-2">
            <p className="text-sm font-medium">Run the server directly</p>
            <pre className="rounded-lg bg-secondary/60 px-4 py-3 text-xs font-mono overflow-x-auto text-foreground/90 leading-relaxed">
{`# From the repo root (requires DATABASE_URL in environment)
python3 -m world_of_taxonomy mcp`}
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

      {/* ── Adding a New System ── */}
      <section id="add-system" className="space-y-5">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-secondary border border-border/50">
            <PlusCircle className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-semibold">Adding a New System</h2>
            <p className="text-sm text-muted-foreground">Contribute a classification system in ~10 steps using TDD</p>
          </div>
        </div>

        <div className="rounded-xl border border-border/50 bg-card divide-y divide-border/50">

          {/* Overview */}
          <div className="p-6 space-y-3">
            <p className="text-sm leading-relaxed text-muted-foreground">
              Every system follows the same TDD cycle: write a failing test first, implement the
              ingester to make it green, wire it into the CLI, then run the full suite to confirm
              no regressions. The detailed SOP lives in{' '}
              <code className="font-mono text-xs">docs/adding-a-new-system.md</code>.
            </p>
            <div className="grid sm:grid-cols-3 gap-4 text-sm pt-1">
              {[
                { label: 'New file',     value: 'world_of_taxonomy/ingest/<system>.py' },
                { label: 'Test file',    value: 'tests/test_ingest_<system>.py' },
                { label: 'Wire up',      value: 'world_of_taxonomy/__main__.py' },
              ].map(({ label, value }) => (
                <div key={label}>
                  <p className="text-xs text-muted-foreground font-medium uppercase tracking-wide mb-1">{label}</p>
                  <p className="font-mono text-xs text-foreground/80">{value}</p>
                </div>
              ))}
            </div>
          </div>

          {/* 10-step checklist */}
          <div className="p-6 space-y-3">
            <p className="text-sm font-medium">10-step checklist</p>
            <ol className="space-y-2 text-sm text-muted-foreground list-none">
              {[
                'Write a failing test (test_ingest_<system>.py) - confirm it is red before continuing',
                'Create the ingester (ingest/<system>.py) - parse source data, build SYSTEM + NODES dicts',
                'Set is_leaf correctly - use codes_with_children = {parent for ... if parent} pattern',
                'Implement ingest(conn) - upsert system row, upsert nodes in dependency order',
                'Run the test green - minimum code to pass, nothing more',
                'Add crosswalk edges if a concordance table exists (ingest/crosswalk_<system>.py)',
                'Write a test for equivalences - confirm bidirectional edges are created',
                'Wire into __main__.py ingest command (add system id to the dispatch table)',
                'Run the full test suite - python3 -m pytest tests/ -v - all green before committing',
                'Update CLAUDE.md system table with name, region, and node count',
              ].map((step, i) => (
                <li key={i} className="flex items-start gap-3">
                  <span className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-secondary text-xs font-medium text-foreground">
                    {i + 1}
                  </span>
                  <span className="text-xs text-foreground/80 pt-0.5">{step}</span>
                </li>
              ))}
            </ol>
          </div>

          {/* Three implementation paths */}
          <div className="p-6 space-y-4">
            <p className="text-sm font-medium">Three implementation paths</p>
            <div className="grid sm:grid-cols-3 gap-4">
              {[
                {
                  label: 'Path A - NACE-derived',
                  desc: 'System shares all NACE Rev 2 codes (WZ, ONACE, NOGA, ATECO, NAF, PKD, SBI, etc.). Copy nodes from NACE and create 1:1 equivalence edges. ~15 lines of code.',
                  example: 'nace_derived.py',
                },
                {
                  label: 'Path B - ISIC-derived',
                  desc: 'System is a national adaptation of ISIC Rev 4 (CIIU, VSIC, BSIC, etc.). Copy ISIC nodes and create equivalences. Add country-specific codes if the source deviates.',
                  example: 'isic_derived.py',
                },
                {
                  label: 'Path C - Standalone',
                  desc: 'System has its own source file (CSV, XLSX, JSON, XML, PDF). Parse source, build hierarchy from parent codes, detect leaves via codes_with_children, upsert independently.',
                  example: 'naics.py, loinc.py',
                },
              ].map(({ label, desc, example }) => (
                <div key={label} className="rounded-lg border border-border/50 bg-secondary/30 p-4 space-y-2">
                  <p className="text-xs font-semibold text-foreground">{label}</p>
                  <p className="text-[11px] text-muted-foreground leading-relaxed">{desc}</p>
                  <code className="text-[10px] font-mono text-primary/80">see: {example}</code>
                </div>
              ))}
            </div>
          </div>

          {/* Minimal standalone template */}
          <div className="p-6 space-y-2">
            <p className="text-sm font-medium">Minimal standalone ingester template</p>
            <pre className="rounded-lg bg-secondary/60 px-4 py-3 text-xs font-mono overflow-x-auto text-foreground/90 leading-relaxed">
{`# world_of_taxonomy/ingest/my_system.py
SYSTEM = {
    "id": "my_system_2024",
    "name": "My Classification System 2024",
    "authority": "Issuing Body",
    "region": "Global",
    "version": "2024",
    "description": "...",
}

# (code, title, description, parent_code)
NODES = [
    ("A", "Section A", "Agriculture", None),
    ("A01", "Crop production", "...", "A"),
    ...
]

async def ingest(conn) -> None:
    await conn.execute("""
        INSERT INTO classification_system (...) VALUES (...)
        ON CONFLICT (id) DO UPDATE SET ...
    """, *SYSTEM.values())

    # Compute leaf flags dynamically - never hard-code level == N
    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}

    for code, title, desc, parent in NODES:
        is_leaf = code not in codes_with_children
        await conn.execute("""
            INSERT INTO classification_node (...) VALUES (...)
            ON CONFLICT (system_id, code) DO UPDATE SET ...
        """, SYSTEM["id"], code, title, desc, parent, is_leaf)`}
            </pre>
          </div>

        </div>
      </section>

      {/* ── Pricing ── */}
      <section id="pricing" className="space-y-5">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-secondary border border-border/50">
            <Zap className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-xl font-semibold">Pricing</h2>
            <p className="text-sm text-muted-foreground">Choose the plan that fits your needs</p>
          </div>
        </div>

        <div className="grid sm:grid-cols-3 gap-4">
          {PRICING_TIERS.map((tier) => (
            <div
              key={tier.name}
              className={`rounded-xl border p-6 space-y-4 ${
                tier.highlighted
                  ? 'border-primary bg-primary/5 ring-1 ring-primary/20'
                  : 'border-border/50 bg-card'
              }`}
            >
              <div>
                <h3 className="text-lg font-semibold">{tier.name}</h3>
                <div className="flex items-baseline gap-1 mt-1">
                  <span className="text-3xl font-bold">{tier.price}</span>
                  {tier.period && <span className="text-sm text-muted-foreground">{tier.period}</span>}
                </div>
              </div>
              <ul className="space-y-2">
                {tier.features.map((feature) => (
                  <li key={feature} className="flex items-center gap-2 text-sm text-muted-foreground">
                    <ChevronRight className="h-3.5 w-3.5 text-primary shrink-0" />
                    {feature}
                  </li>
                ))}
              </ul>
              <Link
                href={tier.ctaHref}
                className={`block text-center px-4 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  tier.highlighted
                    ? 'bg-primary text-primary-foreground hover:bg-primary/90'
                    : 'bg-secondary text-secondary-foreground hover:bg-secondary/80'
                }`}
              >
                {tier.cta}
              </Link>
            </div>
          ))}
        </div>
      </section>

      {/* ── Contact Sales ── */}
      <section id="contact" className="space-y-5">
        <div className="rounded-xl border border-border/50 bg-card p-6 space-y-4">
          <h2 className="text-xl font-semibold">Contact Sales</h2>
          <p className="text-sm text-muted-foreground">
            Interested in Enterprise? Tell us about your use case and we&apos;ll get back to you.
          </p>
          <form
            className="grid sm:grid-cols-2 gap-4"
            action="/api/v1/contact"
            method="POST"
            onSubmit={undefined}
          >
            <input
              type="text"
              name="name"
              placeholder="Name"
              required
              className="px-3 py-2 rounded-lg bg-secondary border border-border/50 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50"
            />
            <input
              type="text"
              name="company"
              placeholder="Company"
              className="px-3 py-2 rounded-lg bg-secondary border border-border/50 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50"
            />
            <input
              type="email"
              name="email"
              placeholder="Email"
              required
              className="px-3 py-2 rounded-lg bg-secondary border border-border/50 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 sm:col-span-2"
            />
            <textarea
              name="message"
              placeholder="Tell us about your use case..."
              required
              rows={3}
              className="px-3 py-2 rounded-lg bg-secondary border border-border/50 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 sm:col-span-2 resize-none"
            />
            <button
              type="submit"
              className="px-4 py-2.5 rounded-lg bg-primary text-primary-foreground text-sm font-medium hover:bg-primary/90 transition-colors sm:col-span-2"
            >
              Send inquiry
            </button>
          </form>
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
          href="https://github.com/colaberry/WorldOfTaxonomy/issues"
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
