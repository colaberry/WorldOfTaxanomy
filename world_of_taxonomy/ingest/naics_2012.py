"""Ingest North American Industry Classification System 2012."""
from __future__ import annotations

_SYSTEM_ROW = ("naics_2012", "NAICS 2012", "North American Industry Classification System 2012", "2012", "North America", "US Census Bureau")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("N12", "NAICS 2012 Sectors", 1, None),
    ("N12.11", "Agriculture, Forestry, Fishing", 2, 'N12'),
    ("N12.21", "Mining, Quarrying, Oil/Gas", 2, 'N12'),
    ("N12.22", "Utilities", 2, 'N12'),
    ("N12.23", "Construction", 2, 'N12'),
    ("N12.31", "Manufacturing (31-33)", 2, 'N12'),
    ("N12.42", "Wholesale Trade", 2, 'N12'),
    ("N12.44", "Retail Trade (44-45)", 2, 'N12'),
    ("N12.48", "Transportation and Warehousing (48-49)", 2, 'N12'),
    ("N12.51", "Information", 2, 'N12'),
    ("N12.52", "Finance and Insurance", 2, 'N12'),
    ("N12.53", "Real Estate", 2, 'N12'),
    ("N12.54", "Professional Services", 2, 'N12'),
    ("N12.55", "Management of Companies", 2, 'N12'),
    ("N12.56", "Admin and Support", 2, 'N12'),
    ("N12.61", "Educational Services", 2, 'N12'),
    ("N12.62", "Health Care", 2, 'N12'),
    ("N12.71", "Arts and Recreation", 2, 'N12'),
    ("N12.72", "Accommodation and Food", 2, 'N12'),
    ("N12.81", "Other Services", 2, 'N12'),
    ("N12.92", "Public Administration", 2, 'N12'),
]

async def ingest_naics_2012(conn) -> int:
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
