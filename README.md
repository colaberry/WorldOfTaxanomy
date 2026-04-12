# WorldOfTaxanomy

**The world's most comprehensive open taxonomy knowledge graph.**

WorldOfTaxanomy federates 36 classification systems - industry, geography, product, trade, occupation, education, health, regulation, and domain deep-dives - into a single queryable knowledge graph. Every code in every system can be translated to its equivalents in every other system through crosswalk edges.

[![CI](https://github.com/colaberry/WorldOfTaxanomy/actions/workflows/ci.yml/badge.svg)](https://github.com/colaberry/WorldOfTaxanomy/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What is this?

Every country, industry body, and international organization has its own classification system. A truck driver in the US is NAICS 484, SOC 53-3032, ISCO-08 8332, ESCO occ/transport-worker - four different codes in four different systems that all mean the same thing.

WorldOfTaxanomy is the Rosetta Stone that connects them all.

**36 systems. ~395,000 codes. ~57,000 crosswalk edges.**

---

## Systems

### Industry Classification

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| NAICS 2022 | North America | 2,125 | North American Industry Classification System |
| ISIC Rev 4 | Global (UN) | 766 | International Standard Industrial Classification |
| NACE Rev 2 | European Union | 996 | EU statistical classification of economic activities |
| SIC 1987 | USA/UK | 1,176 | Standard Industrial Classification |
| ANZSIC 2006 | Australia/NZ | 825 | Australian and NZ Standard Industrial Classification |
| NIC 2008 | India | 2,070 | National Industrial Classification |
| WZ 2008 | Germany | 996 | Klassifikation der Wirtschaftszweige |
| ONACE 2008 | Austria | 996 | Osterreichische Systematik der Wirtschaftstatigkeiten |
| NOGA 2008 | Switzerland | 996 | Nomenclature generale des activites economiques |
| JSIC 2013 | Japan | 20 | Japan Standard Industrial Classification |

### Geography

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| ISO 3166-1 | Global | 271 | Countries with UN M.49 regional hierarchy |
| ISO 3166-2 | Global | 5,246 | Country subdivisions (states, provinces, regions) |
| UN M.49 | Global | 272 | UN geographic regions and sub-regions |

### Product and Trade

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| HS 2022 | Global (WCO) | 6,960 | Harmonized System for international trade |
| CPC v2.1 | Global (UN) | 4,596 | Central Product Classification |
| UNSPSC v24 | Global (GS1 US) | 77,337 | Universal Standard Products and Services Code |

### Occupational

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| SOC 2018 | United States | 1,447 | Standard Occupational Classification |
| ISCO-08 | Global (ILO) | 619 | International Standard Classification of Occupations |
| ESCO Occupations | Europe / Global | ~2,942 | European Skills, Competences, Qualifications and Occupations |
| O*NET-SOC | United States | ~867 | Occupational Information Network |

### Education

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| CIP 2020 | United States | 2,848 | Classification of Instructional Programs |
| ISCED-F 2013 | Global (UNESCO) | 122 | International Standard Classification of Education (Fields) |

### Health and Pharmaceutical

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| ATC WHO 2021 | Global (WHO) | 6,440 | Anatomical Therapeutic Chemical classification |
| ESCO Skills | Europe / Global | ~13,890 | ESCO skills and competences taxonomy |

### Financial and Environmental

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| COFOG | Global (UN) | 188 | Classification of Functions of Government |
| GICS Bridge | Global (MSCI/S&P) | 11 | Global Industry Classification Standard (11 public sectors) |
| GHG Protocol | Global (WRI/WBCSD) | 20 | Greenhouse Gas Protocol scope 1/2/3 categories |

### Regulatory

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| Patent CPC | Global (EPO/USPTO) | ~260,000 | Cooperative Patent Classification |
| CFR Title 49 | United States | 104 | Code of Federal Regulations (Transportation) |
| FMCSA Regulations | United States | 80 | Federal Motor Carrier Safety Administration rules |
| GDPR Articles | European Union | 110 | General Data Protection Regulation articles |
| ISO 31000 | Global (ISO) | 47 | Risk Management Guidelines |

### Domain Deep-Dives (Truck Transportation)

| System | Region | Codes | Description |
|--------|--------|-------|-------------|
| Domain: Truck Freight Types | United States | 44 | Freight mode, equipment, service level, cargo type |
| Domain: Truck Vehicle Classes | United States | 23 | DOT GVWR weight classes 1-8, body types |
| Domain: Truck Cargo Classification | United States | 46 | Commodity groups, DOT hazmat classes 1-9, handling |
| Domain: Truck Carrier Operations | United States | 27 | Carrier type, fleet size, business model, route pattern |

---

## Quickstart

```bash
# Clone and install
git clone https://github.com/colaberry/WorldOfTaxanomy.git
cd WorldOfTaxanomy
pip install -e .

# Set environment variables
cp .env.example .env
# Edit .env and set DATABASE_URL (Neon PostgreSQL) and JWT_SECRET

# Initialize the database schema
python3 -m world_of_taxanomy init

# Ingest a classification system
python3 -m world_of_taxanomy ingest naics
python3 -m world_of_taxanomy ingest isic

# Start the API server
python3 -m uvicorn world_of_taxanomy.api.app:create_app --factory --port 8000

# Start the MCP server (for Claude Desktop)
python3 -m world_of_taxanomy mcp

# Start the frontend (requires Node.js)
cd frontend && npx next dev --port 3000
```

### Try the API

```bash
# Search across all systems
curl "http://localhost:8000/api/v1/search?q=truck+transportation"

# Translate a NAICS code to all equivalent systems
curl "http://localhost:8000/api/v1/systems/naics_2022/nodes/4841/translations"

# Get children of an ISIC division
curl "http://localhost:8000/api/v1/systems/isic_rev4/nodes/H/children"

# System stats
curl "http://localhost:8000/api/v1/systems/stats"
```

---

## Architecture

```
WorldOfTaxanomy/
├── world_of_taxanomy/
│   ├── api/              # FastAPI REST API
│   │   ├── app.py        # App factory with lifespan pool management
│   │   ├── routers/      # systems, nodes, search, equivalences, auth
│   │   └── schemas.py    # Pydantic response models
│   ├── mcp/              # MCP server (stdio transport, 20 tools)
│   │   └── server.py
│   ├── ingest/           # One ingester per classification system (40+ files)
│   │   ├── base.py       # ensure_data_file() download utility
│   │   ├── naics.py      # NAICS 2022
│   │   ├── isic.py       # ISIC Rev 4
│   │   └── ...           # one file per system
│   ├── db.py             # asyncpg pool (Neon PostgreSQL)
│   ├── schema.sql        # classification_system, classification_node, equivalence
│   └── schema_auth.sql   # app_user, api_key, usage_log
├── frontend/             # Next.js 15 + TypeScript + Tailwind CSS + shadcn/ui
│   └── src/app/          # Home, Explore, System detail, Dashboard, Node detail
├── tests/                # 830 tests, pytest, test_wot schema isolation
└── data/                 # Downloaded source files (gitignored, re-downloadable)
```

### Database schema

Three core tables shared by all 36 systems:

```sql
classification_system  -- one row per taxonomy
classification_node    -- all codes across all systems (~395K rows)
equivalence            -- crosswalk edges between codes (~57K rows)
```

Plus the domain deep-dive tables:

```sql
domain_taxonomy        -- registers domain sub-taxonomies
node_taxonomy_link     -- links industry nodes to domain concepts
```

---

## REST API

Base URL: `http://localhost:8000/api/v1`

```
GET /systems                                    List all 36 classification systems
GET /systems/{id}                               System details with root codes
GET /systems/{id}/nodes/{code}                  Get a specific code
GET /systems/{id}/nodes/{code}/children         Child codes
GET /systems/{id}/nodes/{code}/ancestors        Parent chain to root
GET /systems/{id}/nodes/{code}/equivalences     Cross-system mappings
GET /systems/{id}/nodes/{code}/translations     All equivalences in one call
GET /systems/{id}/nodes/{code}/siblings         Sibling codes at same level
GET /systems/{id}/nodes/{code}/subtree          Summary stats for subtree
GET /search?q={query}                           Full-text search across all systems
GET /search?q={query}&grouped=true              Search results grouped by system
GET /compare?a={sys}&b={sys}                    Side-by-side sector comparison
GET /diff?a={sys}&b={sys}                       Codes in A with no mapping to B
GET /systems/stats                              Code counts per system
GET /equivalences/stats                         Crosswalk edge statistics
```

Authentication: `Authorization: Bearer wot_<your_key>` (optional, higher rate limits)

---

## MCP Server (Claude Desktop)

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "world-of-taxanomy": {
      "command": "python3",
      "args": ["-m", "world_of_taxanomy", "mcp"],
      "env": {
        "DATABASE_URL": "your-neon-database-url"
      }
    }
  }
}
```

**20 MCP tools available:** `list_systems`, `get_industry`, `browse_children`, `get_ancestors`, `search_classifications`, `get_equivalences`, `translate_code`, `get_sector_overview`, `translate_across_all_systems`, `compare_sector`, `find_by_keyword_all_systems`, `get_crosswalk_coverage`, `get_system_diff`, `get_siblings`, `get_subtree_summary`, `resolve_ambiguous_code`, `get_leaf_count`, `get_region_mapping`, `describe_match_types`, `explore_industry_tree`

---

## Running Tests

```bash
# Full suite (830 tests)
python3 -m pytest tests/ -v

# Specific system
python3 -m pytest tests/test_ingest_naics.py -v

# With coverage
python3 -m pytest tests/ --cov=world_of_taxanomy
```

Tests use a `test_wot` PostgreSQL schema isolated from production data.

---

## Adding a New Classification System

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full guide.

Short version:
1. Write failing tests first (`tests/test_ingest_<system>.py`) - TDD is mandatory
2. Create `world_of_taxanomy/ingest/<system>.py` with `async def ingest_<system>(conn) -> int`
3. Register in `world_of_taxanomy/__main__.py`
4. Update `CLAUDE.md`, `DATA_SOURCES.md`, `CHANGELOG.md`

---

## Domain Deep-Dive Pattern

Phase 9 introduced a new pattern: **sub-industry taxonomies** that attach below a single classification node to model the internal structure of a specific industry.

```sql
-- Query: all truck domain concepts linked to a NAICS 484 node
SELECT n.code, n.title, tl.taxonomy_id
FROM classification_node n
JOIN node_taxonomy_link tl
  ON tl.node_code = n.code AND tl.system_id = n.system_id
WHERE n.system_id = 'naics_2022' AND n.code LIKE '484%'
ORDER BY n.code;
```

This pattern can be replicated for any industry: healthcare, construction, agriculture, finance, etc.

---

## Data Sources

See [DATA_SOURCES.md](DATA_SOURCES.md) for full attribution and license information.

WorldOfTaxanomy does not redistribute raw data files. Each ingester downloads data directly from the authoritative source at ingest time.

---

## License

MIT License. See [LICENSE](LICENSE).

Classification data is sourced from public domain and open license sources. See [DATA_SOURCES.md](DATA_SOURCES.md) for per-system licensing.
