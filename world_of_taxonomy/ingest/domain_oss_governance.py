"""Ingest Open Source Governance Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_oss_governance", "Open Source Governance", "Open Source Governance Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("OG", "Open Source Governance Types", 1, None),
    ("OG.01", "Foundation-Governed (Linux Foundation)", 2, 'OG'),
    ("OG.02", "Foundation-Governed (Apache)", 2, 'OG'),
    ("OG.03", "Foundation-Governed (CNCF)", 2, 'OG'),
    ("OG.04", "Corporate-Backed Open Source", 2, 'OG'),
    ("OG.05", "Community-Driven (BDFL)", 2, 'OG'),
    ("OG.06", "Community-Driven (Meritocratic)", 2, 'OG'),
    ("OG.07", "Consortium Model", 2, 'OG'),
    ("OG.08", "Working Group Model", 2, 'OG'),
    ("OG.09", "Technical Steering Committee", 2, 'OG'),
    ("OG.10", "SIG-Based Governance", 2, 'OG'),
    ("OG.11", "RFC Process", 2, 'OG'),
    ("OG.12", "Code of Conduct Framework", 2, 'OG'),
    ("OG.13", "CLA-Based Contribution", 2, 'OG'),
    ("OG.14", "DCO-Based Contribution", 2, 'OG'),
    ("OG.15", "Inner Source", 2, 'OG'),
]

async def ingest_domain_oss_governance(conn) -> int:
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
