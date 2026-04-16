"""Ingest Accreditation Body Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_accreditation", "Accreditation Body", "Accreditation Body Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AB", "Accreditation Body Types", 1, None),
    ("AB.01", "Regional Accreditor (US)", 2, 'AB'),
    ("AB.02", "National Accreditor (US)", 2, 'AB'),
    ("AB.03", "Programmatic Accreditor", 2, 'AB'),
    ("AB.04", "AACSB (Business)", 2, 'AB'),
    ("AB.05", "ABET (Engineering)", 2, 'AB'),
    ("AB.06", "ACPE (Pharmacy)", 2, 'AB'),
    ("AB.07", "LCME (Medical)", 2, 'AB'),
    ("AB.08", "ABA (Law)", 2, 'AB'),
    ("AB.09", "NAEYC (Early Childhood)", 2, 'AB'),
    ("AB.10", "CAEP (Teacher Education)", 2, 'AB'),
    ("AB.11", "EQUIS (European Business)", 2, 'AB'),
    ("AB.12", "AMBA (MBA)", 2, 'AB'),
    ("AB.13", "International Accreditation", 2, 'AB'),
]

async def ingest_domain_accreditation(conn) -> int:
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
