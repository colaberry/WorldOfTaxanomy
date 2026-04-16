"""Ingest GMDN (Medical Devices)."""
from __future__ import annotations

_SYSTEM_ROW = ("gmdn", "GMDN", "GMDN (Medical Devices)", "2024", "Global", "GMDN Agency")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "GMDN License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GMDN", "GMDN Categories", 1, None),
    ("GMDN.01", "Active implantable devices", 2, 'GMDN'),
    ("GMDN.02", "Anaesthetic/respiratory devices", 2, 'GMDN'),
    ("GMDN.03", "Dental devices", 2, 'GMDN'),
    ("GMDN.04", "Electromechanical medical devices", 2, 'GMDN'),
    ("GMDN.05", "Hospital hardware", 2, 'GMDN'),
    ("GMDN.06", "In vitro diagnostic devices", 2, 'GMDN'),
    ("GMDN.07", "Non-active implantable devices", 2, 'GMDN'),
    ("GMDN.08", "Ophthalmic/optical devices", 2, 'GMDN'),
    ("GMDN.09", "Reusable instruments", 2, 'GMDN'),
    ("GMDN.10", "Single-use devices", 2, 'GMDN'),
    ("GMDN.11", "Assistive products for persons with disability", 2, 'GMDN'),
    ("GMDN.12", "Diagnostic and therapeutic radiation devices", 2, 'GMDN'),
    ("GMDN.13", "Complementary therapy devices", 2, 'GMDN'),
    ("GMDN.14", "Biologically-derived devices", 2, 'GMDN'),
    ("GMDN.15", "Healthcare facility products/technologies", 2, 'GMDN'),
    ("GMDN.16", "Laboratory equipment", 2, 'GMDN'),
]

async def ingest_gmdn(conn) -> int:
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
