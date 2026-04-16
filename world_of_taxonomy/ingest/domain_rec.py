"""Ingest Renewable Energy Certificate Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_rec", "Renewable Certificate", "Renewable Energy Certificate Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RE", "Renewable Certificate Types", 1, None),
    ("RE.01", "Renewable Energy Certificate (REC)", 2, 'RE'),
    ("RE.02", "Guarantee of Origin (EU)", 2, 'RE'),
    ("RE.03", "I-REC (International)", 2, 'RE'),
    ("RE.04", "TIGR (Tracking Instrument)", 2, 'RE'),
    ("RE.05", "Green-e Certified", 2, 'RE'),
    ("RE.06", "Solar REC (SREC)", 2, 'RE'),
    ("RE.07", "Offshore Wind Certificate", 2, 'RE'),
    ("RE.08", "Bundled REC", 2, 'RE'),
    ("RE.09", "Unbundled REC", 2, 'RE'),
    ("RE.10", "24/7 Carbon-Free Energy", 2, 'RE'),
    ("RE.11", "Power Purchase Agreement (PPA)", 2, 'RE'),
    ("RE.12", "Virtual PPA", 2, 'RE'),
    ("RE.13", "Community Solar Credit", 2, 'RE'),
]

async def ingest_domain_rec(conn) -> int:
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
