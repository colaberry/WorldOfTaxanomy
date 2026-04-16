"""Ingest IAB Tech Lab Content Taxonomy."""
from __future__ import annotations

_SYSTEM_ROW = ("iab_content", "IAB Content Taxonomy", "IAB Tech Lab Content Taxonomy", "3.0", "Global", "IAB Tech Lab")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY 3.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IB", "IAB Content Categories", 1, None),
    ("IB.01", "Arts and Entertainment", 2, 'IB'),
    ("IB.02", "Automotive", 2, 'IB'),
    ("IB.03", "Business and Finance", 2, 'IB'),
    ("IB.04", "Careers", 2, 'IB'),
    ("IB.05", "Education", 2, 'IB'),
    ("IB.06", "Family and Parenting", 2, 'IB'),
    ("IB.07", "Food and Drink", 2, 'IB'),
    ("IB.08", "Health and Fitness", 2, 'IB'),
    ("IB.09", "Home and Garden", 2, 'IB'),
    ("IB.10", "News and Politics", 2, 'IB'),
    ("IB.11", "Personal Finance", 2, 'IB'),
    ("IB.12", "Pets", 2, 'IB'),
    ("IB.13", "Science", 2, 'IB'),
    ("IB.14", "Shopping", 2, 'IB'),
    ("IB.15", "Sports", 2, 'IB'),
    ("IB.16", "Style and Fashion", 2, 'IB'),
    ("IB.17", "Technology and Computing", 2, 'IB'),
    ("IB.18", "Travel", 2, 'IB'),
    ("IB.19", "Real Estate", 2, 'IB'),
    ("IB.20", "Gaming", 2, 'IB'),
]

async def ingest_iab_content(conn) -> int:
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
