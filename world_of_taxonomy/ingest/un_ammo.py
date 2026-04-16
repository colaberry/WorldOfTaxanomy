"""Ingest UN International Ammunition Technical Guidelines (IATG)."""
from __future__ import annotations

_SYSTEM_ROW = ("un_ammo", "UN Ammunition", "UN International Ammunition Technical Guidelines (IATG)", "2024", "Global", "UNODA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("UA", "IATG Categories", 1, None),
    ("UA.01", "Ammunition Stockpile Management", 2, 'UA'),
    ("UA.02", "Risk Assessment", 2, 'UA'),
    ("UA.03", "Storage and Compatibility", 2, 'UA'),
    ("UA.04", "Infrastructure", 2, 'UA'),
    ("UA.05", "Surveillance and Testing", 2, 'UA'),
    ("UA.06", "Transport", 2, 'UA'),
    ("UA.07", "Destruction Methods", 2, 'UA'),
    ("UA.08", "Explosive Ordnance Disposal", 2, 'UA'),
    ("UA.09", "Demilitarization", 2, 'UA'),
    ("UA.10", "Environmental Guidelines", 2, 'UA'),
    ("UA.11", "Security of Stockpiles", 2, 'UA'),
    ("UA.12", "Record Keeping", 2, 'UA'),
    ("UA.13", "Training and Education", 2, 'UA'),
]

async def ingest_un_ammo(conn) -> int:
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
