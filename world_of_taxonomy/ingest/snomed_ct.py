"""Ingest SNOMED CT (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("snomed_ct", "SNOMED CT", "SNOMED CT (Skeleton)", "2024", "Global", "SNOMED International")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "SNOMED CT License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SCT", "SNOMED CT Top-Level Hierarchies", 1, None),
    ("SCT.01", "Clinical Finding", 2, 'SCT'),
    ("SCT.02", "Procedure", 2, 'SCT'),
    ("SCT.03", "Observable Entity", 2, 'SCT'),
    ("SCT.04", "Body Structure", 2, 'SCT'),
    ("SCT.05", "Organism", 2, 'SCT'),
    ("SCT.06", "Substance", 2, 'SCT'),
    ("SCT.07", "Pharmaceutical/Biologic Product", 2, 'SCT'),
    ("SCT.08", "Specimen", 2, 'SCT'),
    ("SCT.09", "Special Concept", 2, 'SCT'),
    ("SCT.10", "Physical Object", 2, 'SCT'),
    ("SCT.11", "Physical Force", 2, 'SCT'),
    ("SCT.12", "Event", 2, 'SCT'),
    ("SCT.13", "Environment/Geographic Location", 2, 'SCT'),
    ("SCT.14", "Social Context", 2, 'SCT'),
    ("SCT.15", "Situation with Explicit Context", 2, 'SCT'),
    ("SCT.16", "Staging and Scales", 2, 'SCT'),
    ("SCT.17", "Qualifier Value", 2, 'SCT'),
    ("SCT.18", "Record Artifact", 2, 'SCT'),
    ("SCT.19", "SNOMED CT Model Component", 2, 'SCT'),
]

async def ingest_snomed_ct(conn) -> int:
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
