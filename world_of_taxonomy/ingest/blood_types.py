"""Ingest ABO and Rh Blood Group Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("blood_types", "Blood Types", "ABO and Rh Blood Group Classification", "2024", "Global", "ISBT")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BT", "Blood Type Groups", 1, None),
    ("BT.01", "A+ (A Positive)", 2, 'BT'),
    ("BT.02", "A- (A Negative)", 2, 'BT'),
    ("BT.03", "B+ (B Positive)", 2, 'BT'),
    ("BT.04", "B- (B Negative)", 2, 'BT'),
    ("BT.05", "AB+ (AB Positive)", 2, 'BT'),
    ("BT.06", "AB- (AB Negative)", 2, 'BT'),
    ("BT.07", "O+ (O Positive)", 2, 'BT'),
    ("BT.08", "O- (O Negative, Universal Donor)", 2, 'BT'),
    ("BT.09", "Rh Factor System", 2, 'BT'),
    ("BT.10", "Kell Blood Group", 2, 'BT'),
    ("BT.11", "Duffy Blood Group", 2, 'BT'),
    ("BT.12", "Kidd Blood Group", 2, 'BT'),
    ("BT.13", "MNS Blood Group", 2, 'BT'),
]

async def ingest_blood_types(conn) -> int:
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
