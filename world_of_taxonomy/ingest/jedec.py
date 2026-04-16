"""Ingest JEDEC Semiconductor Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("jedec", "JEDEC Standards", "JEDEC Semiconductor Standards", "2024", "Global", "JEDEC")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("JD", "JEDEC Standards Areas", 1, None),
    ("JD.01", "DDR5 SDRAM (JESD79-5)", 2, 'JD'),
    ("JD.02", "DDR4 SDRAM (JESD79-4)", 2, 'JD'),
    ("JD.03", "LPDDR5 (JESD209-5)", 2, 'JD'),
    ("JD.04", "HBM3 (JESD235)", 2, 'JD'),
    ("JD.05", "GDDR6 (JESD250)", 2, 'JD'),
    ("JD.06", "UFS (JESD220)", 2, 'JD'),
    ("JD.07", "eMMC (JESD84)", 2, 'JD'),
    ("JD.08", "Wide I/O (JESD229)", 2, 'JD'),
    ("JD.09", "Component Reliability (JEP150)", 2, 'JD'),
    ("JD.10", "ESD Testing (JS-001/JS-002)", 2, 'JD'),
    ("JD.11", "Moisture Sensitivity (J-STD-020)", 2, 'JD'),
    ("JD.12", "Lead-Free Soldering (J-STD-006)", 2, 'JD'),
    ("JD.13", "Package Outlines (JEP95)", 2, 'JD'),
]

async def ingest_jedec(conn) -> int:
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
