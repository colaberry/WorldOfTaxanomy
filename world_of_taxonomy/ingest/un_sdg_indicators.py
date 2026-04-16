"""Ingest UN SDG Global Indicator Framework."""
from __future__ import annotations

_SYSTEM_ROW = ("un_sdg_indicators", "SDG Indicators", "UN SDG Global Indicator Framework", "2024", "Global", "United Nations")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("UI", "SDG Indicator Goals", 1, None),
    ("UI.01", "Goal 1: No Poverty", 2, 'UI'),
    ("UI.02", "Goal 2: Zero Hunger", 2, 'UI'),
    ("UI.03", "Goal 3: Good Health", 2, 'UI'),
    ("UI.04", "Goal 4: Quality Education", 2, 'UI'),
    ("UI.05", "Goal 5: Gender Equality", 2, 'UI'),
    ("UI.06", "Goal 6: Clean Water", 2, 'UI'),
    ("UI.07", "Goal 7: Affordable Energy", 2, 'UI'),
    ("UI.08", "Goal 8: Decent Work", 2, 'UI'),
    ("UI.09", "Goal 9: Industry Innovation", 2, 'UI'),
    ("UI.10", "Goal 10: Reduced Inequalities", 2, 'UI'),
    ("UI.11", "Goal 11: Sustainable Cities", 2, 'UI'),
    ("UI.12", "Goal 12: Responsible Consumption", 2, 'UI'),
    ("UI.13", "Goal 13: Climate Action", 2, 'UI'),
    ("UI.14", "Goal 14: Life Below Water", 2, 'UI'),
    ("UI.15", "Goal 15: Life on Land", 2, 'UI'),
    ("UI.16", "Goal 16: Peace and Justice", 2, 'UI'),
    ("UI.17", "Goal 17: Partnerships", 2, 'UI'),
    ("UI.18", "Tier I Indicators", 2, 'UI'),
    ("UI.19", "Tier II Indicators", 2, 'UI'),
]

async def ingest_un_sdg_indicators(conn) -> int:
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
