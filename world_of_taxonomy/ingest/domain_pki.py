"""Ingest PKI Component Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_pki", "PKI Component", "PKI Component Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PK", "PKI Component Types", 1, None),
    ("PK.01", "Certificate Authority (CA)", 2, 'PK'),
    ("PK.02", "Registration Authority (RA)", 2, 'PK'),
    ("PK.03", "Certificate Repository", 2, 'PK'),
    ("PK.04", "CRL Distribution Point", 2, 'PK'),
    ("PK.05", "OCSP Responder", 2, 'PK'),
    ("PK.06", "Key Pair Generation", 2, 'PK'),
    ("PK.07", "Certificate Enrollment", 2, 'PK'),
    ("PK.08", "Certificate Renewal", 2, 'PK'),
    ("PK.09", "Certificate Revocation", 2, 'PK'),
    ("PK.10", "Cross-Certification", 2, 'PK'),
    ("PK.11", "Bridge CA", 2, 'PK'),
    ("PK.12", "PKI Policy Framework", 2, 'PK'),
    ("PK.13", "HSM Integration", 2, 'PK'),
    ("PK.14", "PKI Automation", 2, 'PK'),
]

async def ingest_domain_pki(conn) -> int:
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
