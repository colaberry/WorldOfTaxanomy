"""Ingest VESA Display Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("vesa_standards", "VESA Standards", "VESA Display Standards", "2024", "Global", "VESA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("VS", "VESA Standards", 1, None),
    ("VS.01", "DisplayPort 2.1", 2, 'VS'),
    ("VS.02", "DisplayPort 2.0", 2, 'VS'),
    ("VS.03", "DisplayPort 1.4", 2, 'VS'),
    ("VS.04", "DisplayPort Alt Mode (USB-C)", 2, 'VS'),
    ("VS.05", "DisplayHDR Certification", 2, 'VS'),
    ("VS.06", "Adaptive-Sync", 2, 'VS'),
    ("VS.07", "DSC (Display Stream Compression)", 2, 'VS'),
    ("VS.08", "EDID/DisplayID", 2, 'VS'),
    ("VS.09", "VESA Mounting Standards (FDMI)", 2, 'VS'),
    ("VS.10", "Embedded DisplayPort (eDP)", 2, 'VS'),
    ("VS.11", "ClearMR Certification", 2, 'VS'),
    ("VS.12", "MediaSync", 2, 'VS'),
]

async def ingest_vesa_standards(conn) -> int:
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
