"""Ingest Clean Room Classification Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_cleanroom", "Clean Room Class", "Clean Room Classification Types", "1.0", "Global", "ISO 14644")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CR", "Clean Room Classification Types", 1, None),
    ("CR.01", "ISO Class 1", 2, 'CR'),
    ("CR.02", "ISO Class 2", 2, 'CR'),
    ("CR.03", "ISO Class 3", 2, 'CR'),
    ("CR.04", "ISO Class 4", 2, 'CR'),
    ("CR.05", "ISO Class 5 (Class 100)", 2, 'CR'),
    ("CR.06", "ISO Class 6 (Class 1000)", 2, 'CR'),
    ("CR.07", "ISO Class 7 (Class 10000)", 2, 'CR'),
    ("CR.08", "ISO Class 8 (Class 100000)", 2, 'CR'),
    ("CR.09", "ISO Class 9", 2, 'CR'),
    ("CR.10", "GMP Grade A", 2, 'CR'),
    ("CR.11", "GMP Grade B", 2, 'CR'),
    ("CR.12", "GMP Grade C", 2, 'CR'),
    ("CR.13", "GMP Grade D", 2, 'CR'),
    ("CR.14", "Cleanroom Monitoring", 2, 'CR'),
]

async def ingest_domain_cleanroom(conn) -> int:
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
