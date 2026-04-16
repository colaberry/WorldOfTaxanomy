"""Ingest Bluetooth SIG Profile Types."""
from __future__ import annotations

_SYSTEM_ROW = ("bluetooth_profiles", "Bluetooth SIG Profiles", "Bluetooth SIG Profile Types", "1.0", "Global", "Bluetooth SIG")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BT", "Bluetooth SIG Profile Types", 1, None),
    ("BT.01", "Advanced Audio Distribution (A2DP)", 2, 'BT'),
    ("BT.02", "Audio/Video Remote Control (AVRCP)", 2, 'BT'),
    ("BT.03", "Basic Imaging (BIP)", 2, 'BT'),
    ("BT.04", "Basic Printing (BPP)", 2, 'BT'),
    ("BT.05", "Device ID (DID)", 2, 'BT'),
    ("BT.06", "Hands-Free (HFP)", 2, 'BT'),
    ("BT.07", "Health Device (HDP)", 2, 'BT'),
    ("BT.08", "Human Interface Device (HID)", 2, 'BT'),
    ("BT.09", "Message Access (MAP)", 2, 'BT'),
    ("BT.10", "Object Push (OPP)", 2, 'BT'),
    ("BT.11", "Personal Area Network (PAN)", 2, 'BT'),
    ("BT.12", "Phone Book Access (PBAP)", 2, 'BT'),
    ("BT.13", "Serial Port (SPP)", 2, 'BT'),
    ("BT.14", "Generic Attribute (GATT)", 2, 'BT'),
    ("BT.15", "Mesh Profile", 2, 'BT'),
    ("BT.16", "LE Audio (BAP/TMAP)", 2, 'BT'),
]

async def ingest_bluetooth_profiles(conn) -> int:
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
