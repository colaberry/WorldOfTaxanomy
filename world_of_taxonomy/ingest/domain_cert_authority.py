"""Ingest Certificate Authority Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_cert_authority", "Certificate Authority", "Certificate Authority Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CA", "Certificate Authority Types", 1, None),
    ("CA.01", "Public CA", 2, 'CA'),
    ("CA.02", "Private CA", 2, 'CA'),
    ("CA.03", "Root CA", 2, 'CA'),
    ("CA.04", "Intermediate CA", 2, 'CA'),
    ("CA.05", "Domain Validation (DV)", 2, 'CA'),
    ("CA.06", "Organization Validation (OV)", 2, 'CA'),
    ("CA.07", "Extended Validation (EV)", 2, 'CA'),
    ("CA.08", "Wildcard Certificate", 2, 'CA'),
    ("CA.09", "SAN Certificate", 2, 'CA'),
    ("CA.10", "Code Signing Certificate", 2, 'CA'),
    ("CA.11", "S/MIME Certificate", 2, 'CA'),
    ("CA.12", "Certificate Transparency", 2, 'CA'),
    ("CA.13", "ACME Protocol (Let's Encrypt)", 2, 'CA'),
    ("CA.14", "Certificate Lifecycle", 2, 'CA'),
]

async def ingest_domain_cert_authority(conn) -> int:
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
