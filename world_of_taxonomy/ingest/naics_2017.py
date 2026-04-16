"""Ingest North American Industry Classification System 2017."""
from __future__ import annotations

_SYSTEM_ROW = ("naics_2017", "NAICS 2017", "North American Industry Classification System 2017", "2017", "North America", "US Census Bureau")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("N17", "NAICS 2017 Sectors", 1, None),
    ("N17.11", "Agriculture, Forestry, Fishing", 2, 'N17'),
    ("N17.21", "Mining, Quarrying, Oil/Gas", 2, 'N17'),
    ("N17.22", "Utilities", 2, 'N17'),
    ("N17.23", "Construction", 2, 'N17'),
    ("N17.31", "Manufacturing (31-33)", 2, 'N17'),
    ("N17.42", "Wholesale Trade", 2, 'N17'),
    ("N17.44", "Retail Trade (44-45)", 2, 'N17'),
    ("N17.48", "Transportation and Warehousing (48-49)", 2, 'N17'),
    ("N17.51", "Information", 2, 'N17'),
    ("N17.52", "Finance and Insurance", 2, 'N17'),
    ("N17.53", "Real Estate", 2, 'N17'),
    ("N17.54", "Professional Services", 2, 'N17'),
    ("N17.55", "Management of Companies", 2, 'N17'),
    ("N17.56", "Admin and Support", 2, 'N17'),
    ("N17.61", "Educational Services", 2, 'N17'),
    ("N17.62", "Health Care", 2, 'N17'),
    ("N17.71", "Arts and Recreation", 2, 'N17'),
    ("N17.72", "Accommodation and Food", 2, 'N17'),
    ("N17.81", "Other Services", 2, 'N17'),
    ("N17.92", "Public Administration", 2, 'N17'),
]

async def ingest_naics_2017(conn) -> int:
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
