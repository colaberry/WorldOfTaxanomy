"""Ingest HEDIS Healthcare Quality Measures."""
from __future__ import annotations

_SYSTEM_ROW = ("hedis", "HEDIS", "HEDIS Healthcare Quality Measures", "2024", "United States", "NCQA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("HE", "HEDIS Domains", 1, None),
    ("HE.01", "Effectiveness of Care", 2, 'HE'),
    ("HE.02", "Access/Availability of Care", 2, 'HE'),
    ("HE.03", "Experience of Care", 2, 'HE'),
    ("HE.04", "Utilization and Relative Resource Use", 2, 'HE'),
    ("HE.05", "Health Plan Descriptive Information", 2, 'HE'),
    ("HE.06", "Prevention and Screening", 2, 'HE'),
    ("HE.07", "Respiratory Conditions", 2, 'HE'),
    ("HE.08", "Cardiovascular Conditions", 2, 'HE'),
    ("HE.09", "Diabetes", 2, 'HE'),
    ("HE.10", "Musculoskeletal Conditions", 2, 'HE'),
    ("HE.11", "Behavioral Health", 2, 'HE'),
    ("HE.12", "Medication Management", 2, 'HE'),
    ("HE.13", "Overuse/Appropriateness", 2, 'HE'),
    ("HE.14", "Patient Safety", 2, 'HE'),
]

async def ingest_hedis(conn) -> int:
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
