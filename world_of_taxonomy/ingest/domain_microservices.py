"""Ingest Microservices Pattern Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_microservices", "Microservices Pattern", "Microservices Pattern Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MS", "Microservices Patterns", 1, None),
    ("MS.01", "API Gateway Pattern", 2, 'MS'),
    ("MS.02", "Service Discovery", 2, 'MS'),
    ("MS.03", "Circuit Breaker Pattern", 2, 'MS'),
    ("MS.04", "Saga Pattern", 2, 'MS'),
    ("MS.05", "CQRS Pattern", 2, 'MS'),
    ("MS.06", "Event Sourcing", 2, 'MS'),
    ("MS.07", "Strangler Fig Pattern", 2, 'MS'),
    ("MS.08", "Sidecar Pattern", 2, 'MS'),
    ("MS.09", "Ambassador Pattern", 2, 'MS'),
    ("MS.10", "Bulkhead Pattern", 2, 'MS'),
    ("MS.11", "Outbox Pattern", 2, 'MS'),
    ("MS.12", "Database per Service", 2, 'MS'),
    ("MS.13", "Shared Database (Anti-Pattern)", 2, 'MS'),
    ("MS.14", "Choreography vs Orchestration", 2, 'MS'),
    ("MS.15", "Backends for Frontends", 2, 'MS'),
    ("MS.16", "Service Mesh Integration", 2, 'MS'),
]

async def ingest_domain_microservices(conn) -> int:
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
