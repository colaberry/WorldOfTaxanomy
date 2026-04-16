"""Ingest SEMI Semiconductor Equipment Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("semi_standards", "SEMI Standards", "SEMI Semiconductor Equipment Standards", "2024", "Global", "SEMI")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SE", "SEMI Standards Areas", 1, None),
    ("SE.01", "Process Chemicals (SEMI C)", 2, 'SE'),
    ("SE.02", "Microlithography (SEMI P)", 2, 'SE'),
    ("SE.03", "Silicon Materials (SEMI M)", 2, 'SE'),
    ("SE.04", "Equipment Automation (SEMI E)", 2, 'SE'),
    ("SE.05", "Gases (SEMI C)", 2, 'SE'),
    ("SE.06", "Facilities (SEMI F/S)", 2, 'SE'),
    ("SE.07", "Flat Panel Display (SEMI D)", 2, 'SE'),
    ("SE.08", "MEMS (SEMI MS)", 2, 'SE'),
    ("SE.09", "Photovoltaic (SEMI PV)", 2, 'SE'),
    ("SE.10", "LED (SEMI C)", 2, 'SE'),
    ("SE.11", "3D Packaging (SEMI 3D)", 2, 'SE'),
    ("SE.12", "High-Brightness LED (SEMI HB)", 2, 'SE'),
    ("SE.13", "Compound Semiconductor (SEMI)", 2, 'SE'),
]

async def ingest_semi_standards(conn) -> int:
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
