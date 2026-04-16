"""Ingest API Architecture Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_api_architecture", "API Architecture", "API Architecture Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AA", "API Architecture Types", 1, None),
    ("AA.01", "REST API", 2, 'AA'),
    ("AA.02", "GraphQL API", 2, 'AA'),
    ("AA.03", "gRPC API", 2, 'AA'),
    ("AA.04", "SOAP API", 2, 'AA'),
    ("AA.05", "WebSocket API", 2, 'AA'),
    ("AA.06", "Webhook", 2, 'AA'),
    ("AA.07", "AsyncAPI", 2, 'AA'),
    ("AA.08", "OpenAPI Specification", 2, 'AA'),
    ("AA.09", "API Gateway Pattern", 2, 'AA'),
    ("AA.10", "Backend for Frontend (BFF)", 2, 'AA'),
    ("AA.11", "API Mesh", 2, 'AA'),
    ("AA.12", "Event-Driven API", 2, 'AA'),
    ("AA.13", "Hypermedia API (HATEOAS)", 2, 'AA'),
    ("AA.14", "JSON-RPC", 2, 'AA'),
    ("AA.15", "Server-Sent Events (SSE)", 2, 'AA'),
    ("AA.16", "API Versioning Strategy", 2, 'AA'),
]

async def ingest_domain_api_architecture(conn) -> int:
    sid, short, full, ver, region, authority = _SYSTEM_ROW
    await conn.execute(
        """INSERT INTO classification_system (id, name, full_name, version, region, authority,
                  source_url, source_date, data_provenance, license)
           VALUES ($1,$2,$3,$4,$5,$6,$7,CURRENT_DATE,$8,$9)
           ON CONFLICT (id) DO UPDATE SET name=$2,full_name=$3,version=$4,region=$5,
                  authority=$6,source_url=$7,source_date=CURRENT_DATE,
                  data_provenance=$8,license=$9""",
        sid, short, full, ver, region, authority,
        _SOURCE_URL, _DATA_PROVENANCE, _LICENSE,
    )
    await conn.execute("DELETE FROM classification_node WHERE system_id = $1", sid)
    for code, title, level, parent_code in NODES:
        await conn.execute(
            """INSERT INTO classification_node (system_id, code, title, level, parent_code)
               VALUES ($1,$2,$3,$4,$5)""",
            sid, code, title, level, parent_code,
        )
    count = len(NODES)
    await conn.execute("UPDATE classification_system SET node_count = $1 WHERE id = $2", count, sid)
    return count
