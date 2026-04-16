"""Ingest Reference Data Management Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_ref_data", "Reference Data", "Reference Data Management Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RD", "Reference Data Types", 1, None),
    ("RD.01", "Code List Management", 2, 'RD'),
    ("RD.02", "Classification Scheme", 2, 'RD'),
    ("RD.03", "Controlled Vocabulary", 2, 'RD'),
    ("RD.04", "Taxonomy Management", 2, 'RD'),
    ("RD.05", "Ontology Management", 2, 'RD'),
    ("RD.06", "Geographic Reference", 2, 'RD'),
    ("RD.07", "Currency Reference", 2, 'RD'),
    ("RD.08", "Industry Standard Code", 2, 'RD'),
    ("RD.09", "Regulatory Code Set", 2, 'RD'),
    ("RD.10", "Cross-Reference Mapping", 2, 'RD'),
    ("RD.11", "Hierarchical Reference", 2, 'RD'),
    ("RD.12", "Temporal Reference", 2, 'RD'),
    ("RD.13", "Reference Data Versioning", 2, 'RD'),
    ("RD.14", "Reference Data Distribution", 2, 'RD'),
]

async def ingest_domain_ref_data(conn) -> int:
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
