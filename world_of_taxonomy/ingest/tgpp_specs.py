"""Ingest 3GPP Technical Specification Series."""
from __future__ import annotations

_SYSTEM_ROW = ("3gpp_specs", "3GPP Specifications", "3GPP Technical Specification Series", "2024", "Global", "3GPP")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("3G", "3GPP Series", 1, None),
    ("3G.21", "Series 21 - Requirements", 2, '3G'),
    ("3G.22", "Series 22 - Service Requirements", 2, '3G'),
    ("3G.23", "Series 23 - Architecture", 2, '3G'),
    ("3G.24", "Series 24 - Signaling Protocols", 2, '3G'),
    ("3G.25", "Series 25 - Radio (UTRA)", 2, '3G'),
    ("3G.26", "Series 26 - Codecs", 2, '3G'),
    ("3G.27", "Series 27 - Data Terminal", 2, '3G'),
    ("3G.28", "Series 28 - Charging", 2, '3G'),
    ("3G.29", "Series 29 - Core Network Protocols", 2, '3G'),
    ("3G.31", "Series 31 - USIM", 2, '3G'),
    ("3G.32", "Series 32 - OAM", 2, '3G'),
    ("3G.33", "Series 33 - Security", 2, '3G'),
    ("3G.34", "Series 34 - UE Testing", 2, '3G'),
    ("3G.35", "Series 35 - Algorithms", 2, '3G'),
    ("3G.36", "Series 36 - LTE Radio", 2, '3G'),
    ("3G.37", "Series 37 - Radio Measurement", 2, '3G'),
    ("3G.38", "Series 38 - 5G NR Radio", 2, '3G'),
]

async def ingest_tgpp_specs(conn) -> int:
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
