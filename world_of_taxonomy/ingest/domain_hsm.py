"""Ingest Hardware Security Module Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_hsm", "Hardware Security Module", "Hardware Security Module Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("HS", "Hardware Security Module Types", 1, None),
    ("HS.01", "General Purpose HSM", 2, 'HS'),
    ("HS.02", "Payment HSM", 2, 'HS'),
    ("HS.03", "Cloud HSM (as a Service)", 2, 'HS'),
    ("HS.04", "Network-Attached HSM", 2, 'HS'),
    ("HS.05", "PCIe HSM", 2, 'HS'),
    ("HS.06", "USB Token HSM", 2, 'HS'),
    ("HS.07", "FIPS 140-2 Level 3", 2, 'HS'),
    ("HS.08", "FIPS 140-3", 2, 'HS'),
    ("HS.09", "Common Criteria (EAL4+)", 2, 'HS'),
    ("HS.10", "Key Ceremony", 2, 'HS'),
    ("HS.11", "Multi-Tenant HSM", 2, 'HS'),
    ("HS.12", "HSM Cluster (HA)", 2, 'HS'),
    ("HS.13", "HSM Audit Logging", 2, 'HS'),
    ("HS.14", "Quantum-Safe HSM", 2, 'HS'),
]

async def ingest_domain_hsm(conn) -> int:
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
