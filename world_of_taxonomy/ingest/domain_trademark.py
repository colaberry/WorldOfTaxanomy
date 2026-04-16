"""Ingest Trademark Classification Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_trademark", "Trademark Class", "Trademark Classification Types", "1.0", "Global", "WIPO Nice")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("TM", "Trademark Classification Types", 1, None),
    ("TM.01", "Nice Classification (Goods: 1-34)", 2, 'TM'),
    ("TM.02", "Nice Classification (Services: 35-45)", 2, 'TM'),
    ("TM.03", "Word Mark", 2, 'TM'),
    ("TM.04", "Design Mark (Logo)", 2, 'TM'),
    ("TM.05", "Sound Mark", 2, 'TM'),
    ("TM.06", "Color Mark", 2, 'TM'),
    ("TM.07", "Trade Dress", 2, 'TM'),
    ("TM.08", "Certification Mark", 2, 'TM'),
    ("TM.09", "Collective Mark", 2, 'TM'),
    ("TM.10", "Madrid Protocol Filing", 2, 'TM'),
    ("TM.11", "Trademark Opposition", 2, 'TM'),
    ("TM.12", "Trademark Renewal", 2, 'TM'),
    ("TM.13", "Well-Known Mark (Paris Convention)", 2, 'TM'),
]

async def ingest_domain_trademark(conn) -> int:
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
