"""Ingest Invasive Species Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_invasive_sp", "Invasive Species", "Invasive Species Types", "1.0", "Global", "IUCN")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IS", "Invasive Species Types", 1, None),
    ("IS.01", "Invasive Plant Species", 2, 'IS'),
    ("IS.02", "Invasive Animal Species", 2, 'IS'),
    ("IS.03", "Invasive Insect Species", 2, 'IS'),
    ("IS.04", "Invasive Marine Species", 2, 'IS'),
    ("IS.05", "Invasive Freshwater Species", 2, 'IS'),
    ("IS.06", "Invasive Pathogen/Disease", 2, 'IS'),
    ("IS.07", "Ballast Water Pathway", 2, 'IS'),
    ("IS.08", "Horticultural Pathway", 2, 'IS'),
    ("IS.09", "Agricultural Pathway", 2, 'IS'),
    ("IS.10", "Biocontrol Agent", 2, 'IS'),
    ("IS.11", "Eradication Program", 2, 'IS'),
    ("IS.12", "Early Detection System", 2, 'IS'),
    ("IS.13", "Risk Assessment Framework", 2, 'IS'),
]

async def ingest_domain_invasive_sp(conn) -> int:
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
