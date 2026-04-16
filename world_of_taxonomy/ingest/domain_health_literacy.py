"""Ingest Health Literacy Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_health_literacy", "Health Literacy", "Health Literacy Types", "1.0", "Global", "WHO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("HL", "Health Literacy Types", 1, None),
    ("HL.01", "Functional Health Literacy", 2, 'HL'),
    ("HL.02", "Interactive Health Literacy", 2, 'HL'),
    ("HL.03", "Critical Health Literacy", 2, 'HL'),
    ("HL.04", "Numeracy (Health)", 2, 'HL'),
    ("HL.05", "Digital Health Literacy", 2, 'HL'),
    ("HL.06", "Oral Health Literacy", 2, 'HL'),
    ("HL.07", "Mental Health Literacy", 2, 'HL'),
    ("HL.08", "Medication Literacy", 2, 'HL'),
    ("HL.09", "Navigation Literacy", 2, 'HL'),
    ("HL.10", "Cultural Health Literacy", 2, 'HL'),
    ("HL.11", "Plain Language Standard", 2, 'HL'),
    ("HL.12", "Health Literacy Assessment", 2, 'HL'),
    ("HL.13", "Teach-Back Method", 2, 'HL'),
]

async def ingest_domain_health_literacy(conn) -> int:
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
