"""Ingest Six Sigma DMAIC/DMADV Methodology."""
from __future__ import annotations

_SYSTEM_ROW = ("six_sigma", "Six Sigma", "Six Sigma DMAIC/DMADV Methodology", "2024", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SS", "Six Sigma", 1, None),
    ("SS.01", "DMAIC: Define", 2, 'SS'),
    ("SS.02", "DMAIC: Measure", 2, 'SS'),
    ("SS.03", "DMAIC: Analyze", 2, 'SS'),
    ("SS.04", "DMAIC: Improve", 2, 'SS'),
    ("SS.05", "DMAIC: Control", 2, 'SS'),
    ("SS.06", "DMADV: Define", 2, 'SS'),
    ("SS.07", "DMADV: Measure", 2, 'SS'),
    ("SS.08", "DMADV: Analyze", 2, 'SS'),
    ("SS.09", "DMADV: Design", 2, 'SS'),
    ("SS.10", "DMADV: Verify", 2, 'SS'),
    ("SS.11", "Belt Levels: White", 2, 'SS'),
    ("SS.12", "Belt Levels: Yellow", 2, 'SS'),
    ("SS.13", "Belt Levels: Green", 2, 'SS'),
    ("SS.14", "Belt Levels: Black", 2, 'SS'),
    ("SS.15", "Belt Levels: Master Black", 2, 'SS'),
]

async def ingest_six_sigma(conn) -> int:
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
