"""Ingest Social Determinant of Health Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_sdoh", "Social Determinant", "Social Determinant of Health Types", "1.0", "Global", "WHO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SH", "Social Determinant of Health Types", 1, None),
    ("SH.01", "Economic Stability", 2, 'SH'),
    ("SH.02", "Education Access and Quality", 2, 'SH'),
    ("SH.03", "Healthcare Access and Quality", 2, 'SH'),
    ("SH.04", "Neighborhood and Built Environment", 2, 'SH'),
    ("SH.05", "Social and Community Context", 2, 'SH'),
    ("SH.06", "Food Security", 2, 'SH'),
    ("SH.07", "Housing Stability", 2, 'SH'),
    ("SH.08", "Transportation Access", 2, 'SH'),
    ("SH.09", "Language and Literacy", 2, 'SH'),
    ("SH.10", "Digital Health Literacy", 2, 'SH'),
    ("SH.11", "Social Isolation", 2, 'SH'),
    ("SH.12", "Adverse Childhood Experiences", 2, 'SH'),
    ("SH.13", "Environmental Exposure", 2, 'SH'),
    ("SH.14", "Incarceration History", 2, 'SH'),
]

async def ingest_domain_sdoh(conn) -> int:
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
