"""Ingest International Standards on Auditing."""
from __future__ import annotations

_SYSTEM_ROW = ("isa_standards", "ISA Standards", "International Standards on Auditing", "2024", "Global", "IAASB")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IA", "ISA Groups", 1, None),
    ("IA.200", "ISA 200-299: General Principles", 2, 'IA'),
    ("IA.300", "ISA 300-399: Planning", 2, 'IA'),
    ("IA.400", "ISA 400-499: Internal Control", 2, 'IA'),
    ("IA.500", "ISA 500-599: Audit Evidence", 2, 'IA'),
    ("IA.600", "ISA 600-699: Using Work of Others", 2, 'IA'),
    ("IA.700", "ISA 700-799: Audit Conclusions/Reporting", 2, 'IA'),
    ("IA.800", "ISA 800-899: Specialized Areas", 2, 'IA'),
    ("IA.ISQM", "ISQM 1-2: Quality Management", 2, 'IA'),
    ("IA.ISRE", "ISRE: Review Engagements", 2, 'IA'),
    ("IA.ISAE", "ISAE: Assurance Engagements", 2, 'IA'),
    ("IA.ISRS", "ISRS: Related Services", 2, 'IA'),
]

async def ingest_isa_standards(conn) -> int:
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
