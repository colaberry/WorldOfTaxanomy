"""Ingest e-CF v4 (e-Competence Framework)."""
from __future__ import annotations

_SYSTEM_ROW = ("ecf_v4", "e-CF v4", "e-CF v4 (e-Competence Framework)", "4.0", "European Union", "CEN")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ECF", "e-CF Dimensions", 1, None),
    ("ECF.A", "Plan", 2, 'ECF'),
    ("ECF.B", "Build", 2, 'ECF'),
    ("ECF.C", "Run", 2, 'ECF'),
    ("ECF.D", "Enable", 2, 'ECF'),
    ("ECF.E", "Manage", 2, 'ECF'),
    ("ECF.A1", "IS and Business Strategy Alignment", 2, 'ECF'),
    ("ECF.A2", "Service Level Management", 2, 'ECF'),
    ("ECF.A3", "Business Plan Development", 2, 'ECF'),
    ("ECF.A4", "Product/Service Planning", 2, 'ECF'),
    ("ECF.A5", "Architecture Design", 2, 'ECF'),
    ("ECF.A6", "Application Design", 2, 'ECF'),
    ("ECF.A7", "Technology Trend Monitoring", 2, 'ECF'),
    ("ECF.A9", "Innovating", 2, 'ECF'),
    ("ECF.B1", "Application Development", 2, 'ECF'),
    ("ECF.B2", "Component Integration", 2, 'ECF'),
    ("ECF.B3", "Testing", 2, 'ECF'),
    ("ECF.B4", "Solution Deployment", 2, 'ECF'),
    ("ECF.B5", "Documentation Production", 2, 'ECF'),
    ("ECF.C1", "User Support", 2, 'ECF'),
    ("ECF.C2", "Change Support", 2, 'ECF'),
    ("ECF.C3", "Service Delivery", 2, 'ECF'),
    ("ECF.C4", "Problem Management", 2, 'ECF'),
    ("ECF.D1", "Information Security", 2, 'ECF'),
    ("ECF.D2", "ICT Quality Strategy", 2, 'ECF'),
    ("ECF.D3", "Education and Training", 2, 'ECF'),
    ("ECF.E1", "Forecast Development", 2, 'ECF'),
    ("ECF.E2", "Project Management", 2, 'ECF'),
    ("ECF.E3", "Risk Management", 2, 'ECF'),
    ("ECF.E4", "Relationship Management", 2, 'ECF'),
    ("ECF.E5", "Process Improvement", 2, 'ECF'),
    ("ECF.E6", "ICT Quality Management", 2, 'ECF'),
    ("ECF.E7", "Business Change Management", 2, 'ECF'),
    ("ECF.E8", "Information Security Management", 2, 'ECF'),
    ("ECF.E9", "IS Governance", 2, 'ECF'),
]

async def ingest_ecf_v4(conn) -> int:
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
