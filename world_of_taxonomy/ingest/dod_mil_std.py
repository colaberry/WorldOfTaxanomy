"""Ingest US DoD MIL-STD Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("dod_mil_std", "DoD MIL-STD", "US DoD MIL-STD Categories", "2024", "United States", "US DoD")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("MS", "MIL-STD Categories", 1, None),
    ("MS.01", "Environmental Testing (MIL-STD-810)", 2, 'MS'),
    ("MS.02", "EMC Testing (MIL-STD-461)", 2, 'MS'),
    ("MS.03", "Reliability (MIL-STD-785)", 2, 'MS'),
    ("MS.04", "Maintainability (MIL-STD-470)", 2, 'MS'),
    ("MS.05", "System Safety (MIL-STD-882)", 2, 'MS'),
    ("MS.06", "Configuration Management (MIL-STD-973)", 2, 'MS'),
    ("MS.07", "Human Factors (MIL-STD-1472)", 2, 'MS'),
    ("MS.08", "Software Development (MIL-STD-498)", 2, 'MS'),
    ("MS.09", "Data Item Descriptions (MIL-STD-963)", 2, 'MS'),
    ("MS.10", "Technical Manuals (MIL-STD-40051)", 2, 'MS'),
    ("MS.11", "Packaging (MIL-STD-2073)", 2, 'MS'),
    ("MS.12", "Marking (MIL-STD-130)", 2, 'MS'),
    ("MS.13", "Symbology (MIL-STD-2525)", 2, 'MS'),
    ("MS.14", "Interoperability (MIL-STD-6016)", 2, 'MS'),
]

async def ingest_dod_mil_std(conn) -> int:
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
