"""Ingest Key Management Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_key_mgmt", "Key Management", "Key Management Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("KM", "Key Management Types", 1, None),
    ("KM.01", "Key Generation", 2, 'KM'),
    ("KM.02", "Key Distribution", 2, 'KM'),
    ("KM.03", "Key Storage (Vault)", 2, 'KM'),
    ("KM.04", "Key Rotation", 2, 'KM'),
    ("KM.05", "Key Revocation", 2, 'KM'),
    ("KM.06", "Key Escrow", 2, 'KM'),
    ("KM.07", "Customer-Managed Keys (CMK)", 2, 'KM'),
    ("KM.08", "Cloud KMS", 2, 'KM'),
    ("KM.09", "Key Hierarchy", 2, 'KM'),
    ("KM.10", "Key Wrapping", 2, 'KM'),
    ("KM.11", "Multi-Region Key", 2, 'KM'),
    ("KM.12", "Key Access Policy", 2, 'KM'),
    ("KM.13", "Key Audit Trail", 2, 'KM'),
    ("KM.14", "BYOK (Bring Your Own Key)", 2, 'KM'),
]

async def ingest_domain_key_mgmt(conn) -> int:
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
