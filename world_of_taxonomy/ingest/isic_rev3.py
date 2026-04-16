"""Ingest ISIC Rev 3.1."""
from __future__ import annotations

_SYSTEM_ROW = ("isic_rev3", "ISIC Rev 3", "ISIC Rev 3.1", "3.1", "Global", "United Nations")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("I3", "ISIC Rev 3 Sections", 1, None),
    ("I3.A", "Agriculture, Hunting, Forestry", 2, 'I3'),
    ("I3.B", "Fishing", 2, 'I3'),
    ("I3.C", "Mining and Quarrying", 2, 'I3'),
    ("I3.D", "Manufacturing", 2, 'I3'),
    ("I3.E", "Electricity, Gas, Water Supply", 2, 'I3'),
    ("I3.F", "Construction", 2, 'I3'),
    ("I3.G", "Wholesale and Retail Trade", 2, 'I3'),
    ("I3.H", "Hotels and Restaurants", 2, 'I3'),
    ("I3.I", "Transport, Storage, Communications", 2, 'I3'),
    ("I3.J", "Financial Intermediation", 2, 'I3'),
    ("I3.K", "Real Estate, Renting, Business", 2, 'I3'),
    ("I3.L", "Public Administration, Defence", 2, 'I3'),
    ("I3.M", "Education", 2, 'I3'),
    ("I3.N", "Health and Social Work", 2, 'I3'),
    ("I3.O", "Other Community, Social, Personal", 2, 'I3'),
    ("I3.P", "Private Households with Employed Persons", 2, 'I3'),
    ("I3.Q", "Extra-Territorial Organizations", 2, 'I3'),
]

async def ingest_isic_rev3(conn) -> int:
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
