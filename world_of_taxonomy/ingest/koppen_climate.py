"""Ingest Koppen-Geiger Climate Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("koppen_climate", "Koppen Climate", "Koppen-Geiger Climate Classification", "2024", "Global", "Kottek et al.")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("KC", "Koppen Climate Types", 1, None),
    ("KC.01", "Af: Tropical Rainforest", 2, 'KC'),
    ("KC.02", "Am: Tropical Monsoon", 2, 'KC'),
    ("KC.03", "Aw: Tropical Savanna", 2, 'KC'),
    ("KC.04", "BWh: Hot Desert", 2, 'KC'),
    ("KC.05", "BWk: Cold Desert", 2, 'KC'),
    ("KC.06", "BSh: Hot Semi-Arid", 2, 'KC'),
    ("KC.07", "BSk: Cold Semi-Arid", 2, 'KC'),
    ("KC.08", "Cfa: Humid Subtropical", 2, 'KC'),
    ("KC.09", "Cfb: Oceanic", 2, 'KC'),
    ("KC.10", "Csa: Hot-Summer Mediterranean", 2, 'KC'),
    ("KC.11", "Csb: Warm-Summer Mediterranean", 2, 'KC'),
    ("KC.12", "Dfa: Hot-Summer Humid Continental", 2, 'KC'),
    ("KC.13", "Dfb: Warm-Summer Humid Continental", 2, 'KC'),
    ("KC.14", "Dfc: Subarctic", 2, 'KC'),
    ("KC.15", "ET: Tundra", 2, 'KC'),
    ("KC.16", "EF: Ice Cap", 2, 'KC'),
]

async def ingest_koppen_climate(conn) -> int:
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
