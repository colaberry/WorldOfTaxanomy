"""Ingest IEEE Standards (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("ieee_standards", "IEEE Standards", "IEEE Standards (Skeleton)", "2024", "Global", "IEEE")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "IEEE License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IEEE", "IEEE Standards Areas", 1, None),
    ("IEEE.802", "802 Networking (LAN/MAN)", 2, 'IEEE'),
    ("IEEE.1547", "Distributed Energy Resources", 2, 'IEEE'),
    ("IEEE.2030", "Smart Grid", 2, 'IEEE'),
    ("IEEE.1149", "Test Access Port (JTAG)", 2, 'IEEE'),
    ("IEEE.754", "Floating Point Arithmetic", 2, 'IEEE'),
    ("IEEE.1588", "Precision Time Protocol", 2, 'IEEE'),
    ("IEEE.2413", "IoT Architecture", 2, 'IEEE'),
    ("IEEE.7000", "Ethics in System Design", 2, 'IEEE'),
    ("IEEE.2048", "Virtual Reality and AR", 2, 'IEEE'),
    ("IEEE.1901", "Broadband over Power Line", 2, 'IEEE'),
    ("IEEE.C37", "Power Systems Relay", 2, 'IEEE'),
    ("IEEE.1451", "Smart Transducer Interface", 2, 'IEEE'),
    ("IEEE.2621", "Wireless Diabetes Devices", 2, 'IEEE'),
]

async def ingest_ieee_standards(conn) -> int:
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
