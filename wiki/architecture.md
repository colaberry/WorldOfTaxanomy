## System Architecture and Data Flows

This guide provides visual documentation of the WorldOfTaxonomy system architecture, data ingestion pipeline, API request handling, MCP session lifecycle, and the four-channel wiki distribution system.

## System Architecture Overview

The platform serves three consumer interfaces - a web application, a REST API, and an MCP server - all backed by a shared PostgreSQL database and wiki knowledge layer.

```mermaid
graph TB
  subgraph Data["Data Layer"]
    PG[(PostgreSQL on Neon)]
    WIKI["wiki/*.md files"]
  end
  subgraph Backend["Python Backend"]
    INGEST["Ingesters - 1,000 systems"]
    API["FastAPI REST API - /api/v1/*"]
    MCP["MCP Server - stdio transport"]
    WIKILOADER["Wiki Loader - wiki.py"]
  end
  subgraph Frontend["Next.js Frontend"]
    NEXT["Next.js 15 App Router"]
    GUIDE["/guide/* pages"]
  end
  subgraph Consumers
    BROWSER["Web Browsers"]
    AIAGENT["AI Agents - Claude, GPT, etc."]
    CRAWLER["AI Crawlers - Perplexity, etc."]
    DEV["Developer Applications"]
  end
  INGEST -->|ingest| PG
  API -->|query| PG
  MCP -->|query| PG
  WIKILOADER -->|read| WIKI
  MCP -->|instructions| WIKILOADER
  NEXT -->|proxy /api/*| API
  NEXT -->|read| WIKI
  GUIDE -->|render| WIKI
  BROWSER --> NEXT
  BROWSER --> GUIDE
  AIAGENT --> MCP
  CRAWLER -->|/llms-full.txt| NEXT
  DEV --> API
```

## Four-Channel Wiki Data Flow

The wiki system follows the "write once, serve four ways" pattern. A single set of curated markdown files feeds all distribution channels.

```mermaid
graph LR
  MD["wiki/*.md - Source of Truth"] --> CH1["Channel 1: Next.js /guide/slug - SEO Web Pages"]
  MD --> CH2["Channel 2: MCP instructions - AI Agent Context"]
  MD --> CH3["Channel 3: llms-full.txt - AI Crawler Discovery"]
  MD --> CH4["Channel 4: GET /api/v1/wiki - Developer API"]
  CH1 --> GOOGLE["Search Engines"]
  CH1 --> HUMANS["Human Readers"]
  CH2 --> AGENTS["AI Agents"]
  CH3 --> CRAWLERS["AI Crawlers"]
  CH4 --> DEVS["Developer Apps"]
```

### Channel Details

| Channel | Format | Cache/Refresh | Audience |
|---------|--------|---------------|----------|
| Web pages at /guide/ | Server-rendered HTML with SEO metadata | Static generation at build time | Human readers, search engines |
| MCP instructions | Plain text injected at session start | Loaded on MCP initialize | AI agents (Claude, GPT, Gemini) |
| llms-full.txt | Concatenated plain text | Regenerated on build | AI crawlers (Perplexity, Google AI) |
| Wiki API | JSON with raw markdown | On-demand from disk | Developer applications, RAG pipelines |

## Classification Data Ingestion Pipeline

Raw data from official sources flows through the ingestion pipeline into three database tables.

```mermaid
graph TD
  subgraph Sources["Official Sources"]
    CSV["CSV files - NAICS, ISIC"]
    XLSX["Excel files - NACE, ANZSIC"]
    HTML["HTML/PDF - SIC, NIC"]
    CURATED["Expert-Curated - Domain taxonomies"]
  end
  subgraph Pipeline["Ingestion Pipeline"]
    PARSE["Parse and Validate"]
    UPSERT["Upsert Nodes into classification_node"]
    XWALK["Build Crosswalks into equivalence"]
    PROV["Set Provenance - 4-tier audit"]
  end
  subgraph DB["Database Tables"]
    SYS["classification_system - 1,000+ systems"]
    NODE["classification_node - 1.2M+ nodes"]
    EQUIV["equivalence - 321K+ edges"]
  end
  CSV --> PARSE
  XLSX --> PARSE
  HTML --> PARSE
  CURATED --> PARSE
  PARSE --> UPSERT
  PARSE --> XWALK
  PARSE --> PROV
  UPSERT --> NODE
  XWALK --> EQUIV
  PROV --> SYS
  SYS --- NODE
  NODE --- EQUIV
```

### Ingestion Steps

1. **Parse**: Read the source file (CSV, Excel, HTML, or hardcoded data). Validate code format, hierarchy, and completeness.
2. **Upsert nodes**: Insert or update rows in `classification_node` with code, title, description, level, parent_code, is_leaf, and seq_order.
3. **Build crosswalks**: Create bidirectional edges in the `equivalence` table with match_type (exact, partial, broader, narrower, related).
4. **Set provenance**: Update `classification_system` with data_provenance tier, source_url, source_date, license, and source_file_hash.

## API Request Flow

Every API request passes through rate limiting and authentication before reaching the query layer.

```mermaid
sequenceDiagram
  participant C as Client
  participant RL as Rate Limiter
  participant AUTH as Auth Layer
  participant R as Router
  participant Q as Query Layer
  participant DB as PostgreSQL

  C->>RL: GET /api/v1/search?q=physician
  RL->>RL: Check rate - 30/min anon, 1000/min auth
  RL->>AUTH: Forward request
  AUTH->>AUTH: Validate JWT or API key
  AUTH->>R: Authenticated request
  R->>Q: search(conn, query, limit)
  Q->>DB: SELECT with ts_vector query
  DB-->>Q: Matching nodes
  Q-->>R: Results with system context
  R-->>C: JSON response
```

### Rate Limit Tiers

| Tier | Requests/Minute | Daily Limit |
|------|-----------------|-------------|
| Anonymous | 30 | Unlimited |
| Free | 1,000 | Unlimited |
| Pro | 5,000 | 100,000 |
| Enterprise | 50,000 | Unlimited |

## MCP Session Lifecycle

When an AI agent connects to the MCP server, it receives structural knowledge about the entire knowledge graph before making any tool calls.

```mermaid
sequenceDiagram
  participant AI as AI Agent
  participant MCP as MCP Server
  participant WIKI as Wiki Loader
  participant DB as PostgreSQL

  AI->>MCP: initialize - JSON-RPC
  MCP->>WIKI: build_wiki_context()
  WIKI-->>MCP: Structural knowledge - ~15K tokens
  MCP-->>AI: serverInfo + instructions + capabilities
  Note over AI: Agent now knows all 1,000 systems and crosswalk topology
  AI->>MCP: tools/call search_classifications
  MCP->>DB: Query nodes
  DB-->>MCP: Results
  MCP-->>AI: Tool response as JSON
  AI->>MCP: resources/read taxonomy://wiki/crosswalk-map
  MCP->>WIKI: load_wiki_page - crosswalk-map
  WIKI-->>MCP: Full markdown content
  MCP-->>AI: Resource content
```

### MCP Capabilities

The server advertises 23 tools and wiki resources:

- **Tools**: list_classification_systems, search_classifications, get_industry, browse_children, get_equivalences, translate_code, classify_business, get_audit_report, and 15 more
- **Resources**: taxonomy://systems, taxonomy://stats, taxonomy://wiki/{slug} for each guide page

## Database Schema

The three core tables and their relationships:

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `classification_system` | System metadata | id, name, region, data_provenance, source_url, source_file_hash |
| `classification_node` | Individual codes | system_id, code, title, level, parent_code, is_leaf |
| `equivalence` | Cross-system mappings | source_system, source_code, target_system, target_code, match_type |

Relationships:
- `classification_node.system_id` references `classification_system.id`
- `equivalence.source_system` and `equivalence.target_system` reference `classification_system.id`
- Parent-child hierarchy within a system is modeled by `classification_node.parent_code`

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Database | PostgreSQL on Neon (with pgbouncer) |
| Backend | Python 3.9+, FastAPI, asyncpg |
| Frontend | Next.js 15, TypeScript, Tailwind CSS v4, shadcn/ui |
| Visualization | D3.js (Galaxy View force simulation) |
| Auth | bcrypt + JWT + API keys |
| Rate Limiting | slowapi |
| MCP | Custom JSON-RPC over stdio |
