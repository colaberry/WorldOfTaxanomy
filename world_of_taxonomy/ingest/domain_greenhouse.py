"""Ingest Greenhouse Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_greenhouse", "Greenhouse Type", "Greenhouse Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GH", "Greenhouse Types", 1, None),
    ("GH.01", "Glass Greenhouse", 2, 'GH'),
    ("GH.02", "Polycarbonate Greenhouse", 2, 'GH'),
    ("GH.03", "Polyethylene Film Greenhouse", 2, 'GH'),
    ("GH.04", "High Tunnel (Hoop House)", 2, 'GH'),
    ("GH.05", "Venlo-Type Greenhouse", 2, 'GH'),
    ("GH.06", "Gothic Arch Greenhouse", 2, 'GH'),
    ("GH.07", "Lean-To Greenhouse", 2, 'GH'),
    ("GH.08", "Shade House", 2, 'GH'),
    ("GH.09", "Climate-Controlled Greenhouse", 2, 'GH'),
    ("GH.10", "Automated Greenhouse", 2, 'GH'),
    ("GH.11", "Solar Greenhouse (Passive)", 2, 'GH'),
    ("GH.12", "Greenhouse Gas Management", 2, 'GH'),
]

async def ingest_domain_greenhouse(conn) -> int:
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
