"""Ingest RAL Color Standard Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("ral_colors", "RAL Colors", "RAL Color Standard Categories", "2024", "Global", "RAL gGmbH")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RL", "RAL Color Ranges", 1, None),
    ("RL.01", "RAL 1xxx: Yellow Hues", 2, 'RL'),
    ("RL.02", "RAL 2xxx: Orange Hues", 2, 'RL'),
    ("RL.03", "RAL 3xxx: Red Hues", 2, 'RL'),
    ("RL.04", "RAL 4xxx: Violet Hues", 2, 'RL'),
    ("RL.05", "RAL 5xxx: Blue Hues", 2, 'RL'),
    ("RL.06", "RAL 6xxx: Green Hues", 2, 'RL'),
    ("RL.07", "RAL 7xxx: Grey Hues", 2, 'RL'),
    ("RL.08", "RAL 8xxx: Brown Hues", 2, 'RL'),
    ("RL.09", "RAL 9xxx: White/Black Hues", 2, 'RL'),
    ("RL.10", "RAL Design System Plus", 2, 'RL'),
    ("RL.11", "RAL Effect", 2, 'RL'),
    ("RL.12", "RAL Plastics", 2, 'RL'),
]

async def ingest_ral_colors(conn) -> int:
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
