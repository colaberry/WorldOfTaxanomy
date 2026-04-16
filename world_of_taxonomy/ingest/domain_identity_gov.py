"""Ingest Identity Governance Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_identity_gov", "Identity Governance", "Identity Governance Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IG", "Identity Governance Types", 1, None),
    ("IG.01", "Identity Lifecycle Management", 2, 'IG'),
    ("IG.02", "Access Certification", 2, 'IG'),
    ("IG.03", "Role-Based Access Control (RBAC)", 2, 'IG'),
    ("IG.04", "Attribute-Based Access (ABAC)", 2, 'IG'),
    ("IG.05", "Privileged Access Management", 2, 'IG'),
    ("IG.06", "Single Sign-On (SSO)", 2, 'IG'),
    ("IG.07", "Multi-Factor Authentication", 2, 'IG'),
    ("IG.08", "Passwordless Authentication", 2, 'IG'),
    ("IG.09", "Identity Federation", 2, 'IG'),
    ("IG.10", "Directory Services", 2, 'IG'),
    ("IG.11", "Decentralized Identity", 2, 'IG'),
    ("IG.12", "Machine Identity Management", 2, 'IG'),
    ("IG.13", "API Identity and Access", 2, 'IG'),
    ("IG.14", "Identity Analytics", 2, 'IG'),
    ("IG.15", "Entitlement Management", 2, 'IG'),
]

async def ingest_domain_identity_gov(conn) -> int:
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
