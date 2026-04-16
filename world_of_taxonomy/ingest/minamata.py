"""Ingest Minamata Convention on Mercury."""
from __future__ import annotations

_SYSTEM_ROW = ("minamata", "Minamata Convention", "Minamata Convention on Mercury", "2017", "Global", "UNEP")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("HG", "Minamata Convention Articles", 1, None),
    ("HG.03", "Mercury supply sources and trade", 2, 'HG'),
    ("HG.04", "Mercury-added products", 2, 'HG'),
    ("HG.05", "Manufacturing processes using mercury", 2, 'HG'),
    ("HG.07", "Artisanal and small-scale gold mining", 2, 'HG'),
    ("HG.08", "Emissions", 2, 'HG'),
    ("HG.09", "Releases", 2, 'HG'),
    ("HG.10", "Environmentally sound storage", 2, 'HG'),
    ("HG.11", "Mercury wastes", 2, 'HG'),
    ("HG.12", "Contaminated sites", 2, 'HG'),
    ("HG.13", "Financial resources and mechanism", 2, 'HG'),
    ("HG.14", "Capacity building and technical assistance", 2, 'HG'),
    ("HG.16", "Health aspects", 2, 'HG'),
    ("HG.17", "Information exchange", 2, 'HG'),
    ("HG.19", "Research and monitoring", 2, 'HG'),
]

async def ingest_minamata(conn) -> int:
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
