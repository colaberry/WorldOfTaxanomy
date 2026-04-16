"""Ingest United States Code Title Index."""
from __future__ import annotations

_SYSTEM_ROW = ("usc_titles", "USC Titles", "United States Code Title Index", "2024", "United States", "US Congress")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("US", "USC Titles", 1, None),
    ("US.01", "Title 1: General Provisions", 2, 'US'),
    ("US.05", "Title 5: Government Organization", 2, 'US'),
    ("US.07", "Title 7: Agriculture", 2, 'US'),
    ("US.10", "Title 10: Armed Forces", 2, 'US'),
    ("US.11", "Title 11: Bankruptcy", 2, 'US'),
    ("US.12", "Title 12: Banks and Banking", 2, 'US'),
    ("US.15", "Title 15: Commerce and Trade", 2, 'US'),
    ("US.18", "Title 18: Crimes", 2, 'US'),
    ("US.21", "Title 21: Food and Drugs", 2, 'US'),
    ("US.26", "Title 26: Internal Revenue Code", 2, 'US'),
    ("US.28", "Title 28: Judiciary", 2, 'US'),
    ("US.29", "Title 29: Labor", 2, 'US'),
    ("US.33", "Title 33: Navigation", 2, 'US'),
    ("US.35", "Title 35: Patents", 2, 'US'),
    ("US.38", "Title 38: Veterans Benefits", 2, 'US'),
    ("US.42", "Title 42: Public Health/Welfare", 2, 'US'),
    ("US.44", "Title 44: Public Printing", 2, 'US'),
    ("US.47", "Title 47: Telecommunications", 2, 'US'),
    ("US.49", "Title 49: Transportation", 2, 'US'),
    ("US.50", "Title 50: War and National Defense", 2, 'US'),
    ("US.52", "Title 52: Voting and Elections", 2, 'US'),
    ("US.54", "Title 54: National Park Service", 2, 'US'),
]

async def ingest_usc_titles(conn) -> int:
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
