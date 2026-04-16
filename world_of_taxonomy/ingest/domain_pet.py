"""Ingest Privacy Enhancing Technology Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_pet", "Privacy Enhancing Tech", "Privacy Enhancing Technology Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PT", "Privacy Enhancing Technology Types", 1, None),
    ("PT.01", "Differential Privacy", 2, 'PT'),
    ("PT.02", "Homomorphic Encryption", 2, 'PT'),
    ("PT.03", "Secure Multi-Party Computation", 2, 'PT'),
    ("PT.04", "Federated Learning", 2, 'PT'),
    ("PT.05", "Trusted Execution Environment", 2, 'PT'),
    ("PT.06", "Data Anonymization", 2, 'PT'),
    ("PT.07", "Data Pseudonymization", 2, 'PT'),
    ("PT.08", "K-Anonymity", 2, 'PT'),
    ("PT.09", "Zero-Knowledge Proof", 2, 'PT'),
    ("PT.10", "Private Information Retrieval", 2, 'PT'),
    ("PT.11", "Data Clean Room", 2, 'PT'),
    ("PT.12", "Synthetic Data for Privacy", 2, 'PT'),
    ("PT.13", "Privacy Budget Management", 2, 'PT'),
    ("PT.14", "Confidential Computing", 2, 'PT'),
    ("PT.15", "Data Masking", 2, 'PT'),
]

async def ingest_domain_pet(conn) -> int:
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
