"""Ingest Data Mesh Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_data_mesh", "Data Mesh", "Data Mesh Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DM", "Data Mesh Types", 1, None),
    ("DM.01", "Domain-Oriented Ownership", 2, 'DM'),
    ("DM.02", "Data as a Product", 2, 'DM'),
    ("DM.03", "Self-Serve Data Platform", 2, 'DM'),
    ("DM.04", "Federated Governance", 2, 'DM'),
    ("DM.05", "Data Product Catalog", 2, 'DM'),
    ("DM.06", "Data Contract", 2, 'DM'),
    ("DM.07", "Data Product API", 2, 'DM'),
    ("DM.08", "Data Product SLA", 2, 'DM'),
    ("DM.09", "Domain Data Team", 2, 'DM'),
    ("DM.10", "Platform Data Team", 2, 'DM'),
    ("DM.11", "Interoperability Standard", 2, 'DM'),
    ("DM.12", "Data Mesh Topology", 2, 'DM'),
    ("DM.13", "Cross-Domain Query", 2, 'DM'),
    ("DM.14", "Polyglot Persistence", 2, 'DM'),
]

async def ingest_domain_data_mesh(conn) -> int:
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
