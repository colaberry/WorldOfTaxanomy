# WorldOfTaxonomy

<p align="center">
  <strong>279 classification systems. 570,178 codes. 122,769 crosswalk edges.</strong><br>
  The open-source Rosetta Stone for global industry, trade, occupation, and health taxonomies.
</p>

<p align="center">
  <a href="https://github.com/colaberry/WorldOfTaxonomy/actions/workflows/ci.yml">
    <img src="https://github.com/colaberry/WorldOfTaxonomy/actions/workflows/ci.yml/badge.svg" alt="CI" />
  </a>
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT" />
  </a>
  <img src="https://img.shields.io/badge/python-3.9%2B-blue.svg" alt="Python 3.9+" />
  <img src="https://img.shields.io/badge/systems-279-purple.svg" alt="279 systems" />
  <img src="https://img.shields.io/badge/codes-570K%2B-green.svg" alt="570K codes" />
</p>

---

## The Problem

Every country, industry body, and standards organization has its own classification system. When you need to reconcile data across them, you're on your own.

A truck driver in the US is `NAICS 484`, `SOC 53-3032`, `ISCO-08 8332`, `NACE 49.4`, and `ISIC 4923` - five different codes in five different systems that all mean the same thing. Figuring that out manually costs hours. Doing it at scale costs entire teams.

**WorldOfTaxonomy solves this.** One queryable graph connects all 279 systems. One API call translates any code to any other system. One MCP server gives AI agents access to the entire taxonomy universe.

---

## Quick Start

**Docker (recommended - runs in under 2 minutes):**

```bash
git clone https://github.com/colaberry/WorldOfTaxonomy.git
cd WorldOfTaxonomy
docker compose up
```

Open [http://localhost:3000](http://localhost:3000). The API is at [http://localhost:8000](http://localhost:8000).

Then ingest your first systems:

```bash
# Core global systems (~3 minutes)
docker compose exec backend python3 -m world_of_taxonomy ingest naics
docker compose exec backend python3 -m world_of_taxonomy ingest isic
docker compose exec backend python3 -m world_of_taxonomy ingest crosswalk

# Everything (~30-45 minutes)
docker compose exec backend python3 -m world_of_taxonomy ingest all
```

**Python only (bring your own PostgreSQL):**

```bash
pip install -e .
cp .env.example .env   # set DATABASE_URL and JWT_SECRET
python3 -m world_of_taxonomy init
python3 -m world_of_taxonomy ingest naics
python3 -m uvicorn world_of_taxonomy.api.app:create_app --factory --port 8000
```

---

## API in 60 Seconds

```bash
# Translate NAICS 4841 (general freight trucking) to all equivalent systems
curl "http://localhost:8000/api/v1/systems/naics_2022/nodes/4841/translations"

# Search for "hospital" across all 279 systems simultaneously
curl "http://localhost:8000/api/v1/search?q=hospital&grouped=true"

# Get every classification system applicable to Germany
curl "http://localhost:8000/api/v1/countries/DE"

# Find all codes in NACE with no mapping to NAICS
curl "http://localhost:8000/api/v1/diff?a=nace_rev2&b=naics_2022"

# Full text search within a specific system
curl "http://localhost:8000/api/v1/search?q=logistics&system=isco_08"
```

**Python client:**

```python
import httplib2, json

base = "http://localhost:8000/api/v1"

# Get all systems for France
r = httplib2.Http().request(f"{base}/countries/FR")
profile = json.loads(r[1])
# {'official': 'naf_rev2', 'regional': 'nace_rev2', 'recommended': ['isic_rev4', ...]}

# Translate a code
r = httplib2.Http().request(f"{base}/systems/naics_2022/nodes/5415/translations")
translations = json.loads(r[1])
# {'nace_rev2': '62.01', 'isic_rev4': '6201', 'sic_1987': '7371', ...}
```

---

## Use With Claude / AI Agents (MCP)

WorldOfTaxonomy ships with a Model Context Protocol server. Add it to Claude Desktop and your AI gets instant access to all 279 systems as structured tools.

**`~/Library/Application Support/Claude/claude_desktop_config.json`:**

```json
{
  "mcpServers": {
    "world-of-taxonomy": {
      "command": "python3",
      "args": ["-m", "world_of_taxonomy", "mcp"],
      "env": {
        "DATABASE_URL": "your-database-url"
      }
    }
  }
}
```

**21 tools available**, including:

| Tool | What it does |
|------|-------------|
| `translate_code` | Translate any code to a target system |
| `translate_across_all_systems` | One code -> all 279 systems at once |
| `search_classifications` | Full-text search across all codes |
| `get_country_taxonomy_profile` | Official + recommended systems for any country |
| `compare_sector` | Side-by-side root nodes across two systems |
| `get_system_diff` | Codes in system A with no mapping to B |
| `explore_industry_tree` | Browse hierarchy with context |
| `find_by_keyword_all_systems` | Search grouped by system |

**Example prompt to Claude:**
> "I have a dataset with NACE codes. Convert every unique code to NAICS and ISIC equivalents and flag any that have no crosswalk."

---

## What's Covered

**10 categories. 279 systems. Every major region.**

| Category | Systems | Highlights |
|----------|---------|-----------|
| Industry | 68 | NAICS, ISIC, NACE + 58 national adaptations (EU, LATAM, Asia, Africa) |
| Domain Deep-Dives | 149 | Sector vocabularies for 36 industry verticals |
| Health / Clinical | 13 | ICD-11, LOINC (102K codes), ATC, ICD-10-CM/PCS/AM/GM, MeSH |
| Occupational | 10 | SOC, ISCO-08, ESCO (14K skills), O\*NET, ANZSCO, NOC, KldB, ROME |
| Product / Trade | 11 | HS 2022, UNSPSC (77K codes), CPC, SITC, HTS, Schedule B, ECCN |
| Research & Knowledge | 8 | FORD, JEL, LCC, PACS, MSC, ACM CCS, arXiv, ANZSRC |
| Financial / Investment | 7 | GICS, ICB, CFI (ISO 10962), COFOG, COICOP, GHG Protocol, Patent CPC |
| Regulatory & Governance | 12 | GDPR, EU Taxonomy, SFDR, TNFD, GRI, SASB, SDG, SEEA, OECD DAC |
| Geographic | 7 | ISO 3166-1/2, UN M.49, EU NUTS, US FIPS, World Bank income groups |
| Education | 3 | ISCED 2011, ISCED-F 2013, CIP 2020 |

**249 countries** are profiled with their official, regional, and recommended systems.

---

## Use Cases

- **Data engineering**: Reconcile supplier data (NAICS) with EU reporting (NACE) automatically
- **Compliance**: Map your product portfolio to HS codes for any customs jurisdiction
- **HR / Recruitment**: Translate job titles between SOC (US), ESCO (EU), ISCO (global)
- **Financial research**: Bridge GICS sector codes to NAICS for cross-market analysis
- **AI / LLM**: Give your AI agent structured knowledge of every industry and occupation
- **Academic**: Study how different countries classify the same economic activity
- **Healthcare**: Cross-reference ICD-11 diagnoses against ICD-10-CM/PCS variants
- **Trade compliance**: Classify goods across HS, HTS, ECCN, and Schedule B simultaneously

---

## REST API Reference

```
GET /api/v1/systems                                List all systems
GET /api/v1/systems?country={code}                 Systems for a country
GET /api/v1/systems/{id}                           System detail
GET /api/v1/systems/{id}/nodes/{code}              A specific code
GET /api/v1/systems/{id}/nodes/{code}/children     Direct children
GET /api/v1/systems/{id}/nodes/{code}/ancestors    Parent chain to root
GET /api/v1/systems/{id}/nodes/{code}/equivalences Cross-system mappings
GET /api/v1/systems/{id}/nodes/{code}/translations All equivalences in one call
GET /api/v1/systems/{id}/nodes/{code}/siblings     Sibling codes
GET /api/v1/search?q={query}                       Full-text search
GET /api/v1/search?q={query}&grouped=true          Results grouped by system
GET /api/v1/compare?a={sys}&b={sys}                Side-by-side comparison
GET /api/v1/diff?a={sys}&b={sys}                   Codes in A with no match in B
GET /api/v1/countries/stats                        Coverage stats (world map)
GET /api/v1/countries/{code}                       Country taxonomy profile
```

Interactive docs: `http://localhost:8000/docs` (Swagger UI, auto-generated)

**Rate limits** (default): 30 req/min unauthenticated, 1000 req/min with API key.

---

## Architecture

```
WorldOfTaxonomy/
├── world_of_taxonomy/
│   ├── api/              # FastAPI REST API (lifespan pool, rate limiting)
│   │   ├── routers/      # systems, nodes, search, equivalences, countries, auth
│   │   └── schemas.py    # Pydantic response models
│   ├── mcp/              # MCP server (stdio transport, 21 tools)
│   ├── ingest/           # One ingester per system (100+ files)
│   │   ├── naics.py      # Downloads from Census Bureau
│   │   ├── nace_derived.py  # EU national adaptations (copy NACE + equivalences)
│   │   ├── isic_derived.py  # LATAM/Asia/Africa adaptations
│   │   └── crosswalk_*.py   # 20+ crosswalk ingesters
│   ├── query/            # Query layer (browse, search, equivalence)
│   ├── schema.sql        # Core tables
│   └── schema_auth.sql   # Auth tables
├── frontend/             # Next.js 15 + TypeScript + Tailwind + shadcn/ui
│   └── src/app/          # Home (world map), Explore, System, Dashboard, Country
├── tests/                # pytest (test_wot schema isolation, never touches production)
└── data/                 # Downloaded source files (gitignored, re-downloadable)
```

**Database** (PostgreSQL):

```sql
classification_system      -- 279 rows: id, name, region, authority, node_count
classification_node        -- 570K rows: system_id, code, title, level, parent_code
equivalence                -- 122K rows: source_system, source_code, target_system, target_code, match_type
country_system_link        -- 27K rows: country_code, system_id, relevance ('official'|'regional'|'recommended')
```

---

## Contributing

**We are actively seeking contributors to add classification systems.**

Every country has a national industry classification standard that should be in this graph. Most are public domain or openly licensed. Adding one takes about 2 hours following our guide.

**[Read CONTRIBUTING.md](CONTRIBUTING.md)** for the full TDD-enforced guide.

**Quick version:**

1. Pick a system from the [open issues](https://github.com/colaberry/WorldOfTaxonomy/issues?q=is%3Aissue+is%3Aopen+label%3A%22new+system%22) or [request one](https://github.com/colaberry/WorldOfTaxonomy/issues/new?template=new_system_request.md)
2. Write failing tests first (`tests/test_ingest_<system>.py`) - TDD is non-negotiable
3. Implement `world_of_taxonomy/ingest/<system>.py`
4. Wire into `__main__.py`, update `CLAUDE.md`
5. Open a PR - one system per PR

**Systems most wanted:**
- Additional national industry codes (Middle East, Sub-Saharan Africa, Central Asia)
- UNSD/Eurostat statistical classifications
- Commodity classifications (agricultural, mineral, pharmaceutical)
- US state-level occupation codes

---

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full list. Key near-term items:

- [ ] Hosted public API (no self-hosting required)
- [ ] Python client library (`pip install world-of-taxonomy-client`)
- [ ] JavaScript / TypeScript client
- [ ] Bulk export: Parquet + CSV snapshots on Hugging Face Datasets
- [ ] API key dashboard (frontend UI for managing keys)
- [ ] dbt package for warehouse-native crosswalk joins
- [ ] Weekly "new system" digest for contributors

---

## Data Sources and Licensing

Code is MIT licensed. Classification data is sourced from public domain and openly licensed sources (UN, Eurostat, US Census Bureau, WHO, ILO, etc.).

**WorldOfTaxonomy does not redistribute raw data files.** Each ingester downloads data directly from its authoritative source at ingest time. See [DATA_SOURCES.md](DATA_SOURCES.md) for per-system attribution and license information.

---

## Community

- **GitHub Issues**: Bug reports, feature requests, new system requests
- **GitHub Discussions**: Questions, ideas, show-and-tell
- **Contributing**: See [CONTRIBUTING.md](CONTRIBUTING.md)

---

<p align="center">
  Built with precision. Open forever. PRs welcome.
</p>
