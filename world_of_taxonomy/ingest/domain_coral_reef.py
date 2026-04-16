"""Ingest Coral Reef Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_coral_reef", "Coral Reef", "Coral Reef Types", "1.0", "Global", "IUCN")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CL", "Coral Reef Types", 1, None),
    ("CL.01", "Fringing Reef", 2, 'CL'),
    ("CL.02", "Barrier Reef", 2, 'CL'),
    ("CL.03", "Atoll", 2, 'CL'),
    ("CL.04", "Patch Reef", 2, 'CL'),
    ("CL.05", "Platform Reef", 2, 'CL'),
    ("CL.06", "Deep-Sea Coral", 2, 'CL'),
    ("CL.07", "Mesophotic Reef", 2, 'CL'),
    ("CL.08", "Artificial Reef", 2, 'CL'),
    ("CL.09", "Coral Bleaching Assessment", 2, 'CL'),
    ("CL.10", "Reef Restoration Method", 2, 'CL'),
    ("CL.11", "Reef Health Index", 2, 'CL'),
    ("CL.12", "Marine Protected Area (Reef)", 2, 'CL'),
    ("CL.13", "Reef Monitoring Technology", 2, 'CL'),
]

async def ingest_domain_coral_reef(conn) -> int:
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
