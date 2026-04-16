## Getting Started with WorldOfTaxonomy

WorldOfTaxonomy is a unified knowledge graph connecting 1,000+ classification systems with 1.2M+ nodes and 321K+ crosswalk edges. It provides three interfaces: a REST API, an MCP server for AI agents, and a web application.

## Quick Start - REST API

Base URL: `https://worldoftaxonomy.com/api/v1`

### List All Classification Systems

```bash
curl https://worldoftaxonomy.com/api/v1/systems
```

Returns an array of all systems with their ID, name, region, node count, and provenance metadata.

### Search Across All Systems

```bash
curl "https://worldoftaxonomy.com/api/v1/search?q=physician"
```

Full-text search across all 1.2M+ nodes. Add `&grouped=true` to group results by system, or `&context=true` to include ancestor paths and children for each match.

### Look Up a Specific Code

```bash
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/6211
```

Returns the node with its title, description, level, parent code, and whether it is a leaf node.

### Browse Children

```bash
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/62/children
```

Returns all direct child codes under a given node.

### Get Cross-System Equivalences

```bash
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/6211/equivalences
```

Returns crosswalk mappings to other systems (e.g., NAICS 6211 maps to ISIC 8620).

### Translate to All Systems at Once

```bash
curl https://worldoftaxonomy.com/api/v1/systems/naics_2022/nodes/6211/translations
```

Returns equivalences across all connected systems in a single call.

## Quick Start - MCP Server

The MCP (Model Context Protocol) server lets AI agents query the knowledge graph directly.

### Setup

```bash
pip install world-of-taxonomy
python -m world_of_taxonomy mcp
```

Transport: stdio. The server exposes 23 tools and wiki-based resources.

### Key MCP Tools

| Tool | Purpose |
|------|---------|
| `list_classification_systems` | List all 1,000+ systems |
| `search_classifications` | Full-text search across all nodes |
| `get_industry` | Look up a specific code |
| `browse_children` | Get child codes |
| `get_equivalences` | Get crosswalk mappings |
| `translate_code` | Translate a code to another system |
| `translate_across_all_systems` | Translate to all connected systems |
| `classify_business` | Classify free text into taxonomy codes |
| `get_audit_report` | Data provenance and quality audit |
| `get_country_taxonomy_profile` | Systems applicable to a country |

### MCP Resources

The server also provides resources that AI agents can read for deeper context:

- `taxonomy://systems` - JSON list of all classification systems
- `taxonomy://stats` - Knowledge graph statistics
- `taxonomy://wiki/{slug}` - Individual guide pages as markdown

## Authentication

### Registration

```bash
curl -X POST https://worldoftaxonomy.com/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com", "password": "your-password"}'
```

### API Keys

After registration, create an API key:

```bash
curl -X POST https://worldoftaxonomy.com/api/v1/auth/keys \
  -H "Authorization: Bearer <your-jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{"name": "My App"}'
```

API keys use the format `wot_` followed by 32 hex characters. Pass them in the Authorization header:

```
Authorization: Bearer wot_your_key_here
```

## Rate Limits

| Tier | Requests/Minute | Daily Limit |
|------|-----------------|-------------|
| Anonymous | 30 | Unlimited |
| Free (authenticated) | 1,000 | Unlimited |
| Pro | 5,000 | 100,000 |
| Enterprise | 50,000 | Unlimited |

## API Endpoints Reference

### Systems

| Endpoint | Description |
|----------|-------------|
| `GET /systems` | List all classification systems |
| `GET /systems/{id}` | System detail with root codes |
| `GET /systems/stats` | Leaf and total node counts per system |
| `GET /systems?group_by=region` | Systems grouped by region |
| `GET /systems?country={code}` | Systems applicable to a country |

### Nodes

| Endpoint | Description |
|----------|-------------|
| `GET /systems/{id}/nodes/{code}` | Look up a specific code |
| `GET /systems/{id}/nodes/{code}/children` | Direct children |
| `GET /systems/{id}/nodes/{code}/ancestors` | Parent chain to root |
| `GET /systems/{id}/nodes/{code}/siblings` | Sibling codes |
| `GET /systems/{id}/nodes/{code}/subtree` | Subtree summary stats |

### Search

| Endpoint | Description |
|----------|-------------|
| `GET /search?q={query}` | Full-text search |
| `GET /search?q={query}&grouped=true` | Results grouped by system |
| `GET /search?q={query}&context=true` | Results with ancestor/child context |

### Crosswalks

| Endpoint | Description |
|----------|-------------|
| `GET /systems/{id}/nodes/{code}/equivalences` | Cross-system mappings |
| `GET /systems/{id}/nodes/{code}/translations` | Translate to all systems |
| `GET /equivalences/stats` | Crosswalk statistics |
| `GET /compare?a={sys}&b={sys}` | Side-by-side sector comparison |
| `GET /diff?a={sys}&b={sys}` | Codes with no mapping |

### Classification

| Endpoint | Description |
|----------|-------------|
| `POST /classify` | Classify free text into taxonomy codes |

### Countries

| Endpoint | Description |
|----------|-------------|
| `GET /countries/stats` | Per-country taxonomy coverage |
| `GET /countries/{code}` | Full taxonomy profile for a country |

## Data Disclaimer

All classification data in WorldOfTaxonomy is provided for informational purposes only. It should not be used as a substitute for official government or standards body publications. Always verify codes against the authoritative source for regulatory, legal, or compliance purposes.
