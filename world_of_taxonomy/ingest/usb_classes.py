"""Ingest USB Device Classes."""
from __future__ import annotations

_SYSTEM_ROW = ("usb_classes", "USB-IF Classes", "USB Device Classes", "2024", "Global", "USB-IF")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "USB-IF License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("USB", "USB Device Classes", 1, None),
    ("USB.00", "Device (Use Class Info from Interface)", 2, 'USB'),
    ("USB.01", "Audio", 2, 'USB'),
    ("USB.02", "Communications/CDC Control", 2, 'USB'),
    ("USB.03", "HID (Human Interface)", 2, 'USB'),
    ("USB.05", "Physical", 2, 'USB'),
    ("USB.06", "Image", 2, 'USB'),
    ("USB.07", "Printer", 2, 'USB'),
    ("USB.08", "Mass Storage", 2, 'USB'),
    ("USB.09", "Hub", 2, 'USB'),
    ("USB.0A", "CDC-Data", 2, 'USB'),
    ("USB.0B", "Smart Card", 2, 'USB'),
    ("USB.0D", "Content Security", 2, 'USB'),
    ("USB.0E", "Video", 2, 'USB'),
    ("USB.0F", "Personal Healthcare", 2, 'USB'),
    ("USB.10", "Audio/Video Devices", 2, 'USB'),
    ("USB.11", "Billboard", 2, 'USB'),
    ("USB.12", "USB Type-C Bridge", 2, 'USB'),
    ("USB.DC", "Diagnostic Device", 2, 'USB'),
    ("USB.E0", "Wireless Controller", 2, 'USB'),
    ("USB.EF", "Miscellaneous", 2, 'USB'),
    ("USB.FE", "Application Specific", 2, 'USB'),
    ("USB.FF", "Vendor Specific", 2, 'USB'),
]

async def ingest_usb_classes(conn) -> int:
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
