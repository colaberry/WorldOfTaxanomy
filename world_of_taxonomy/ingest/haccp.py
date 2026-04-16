"""Ingest HACCP Food Safety Principles."""
from __future__ import annotations

_SYSTEM_ROW = ("haccp", "HACCP Principles", "HACCP Food Safety Principles", "2024", "Global", "Codex Alimentarius")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("HA", "HACCP Principles", 1, None),
    ("HA.01", "Principle 1: Hazard Analysis", 2, 'HA'),
    ("HA.02", "Principle 2: Critical Control Points (CCPs)", 2, 'HA'),
    ("HA.03", "Principle 3: Critical Limits", 2, 'HA'),
    ("HA.04", "Principle 4: Monitoring Procedures", 2, 'HA'),
    ("HA.05", "Principle 5: Corrective Actions", 2, 'HA'),
    ("HA.06", "Principle 6: Verification", 2, 'HA'),
    ("HA.07", "Principle 7: Record-Keeping", 2, 'HA'),
    ("HA.08", "Preliminary Step: HACCP Team", 2, 'HA'),
    ("HA.09", "Preliminary Step: Product Description", 2, 'HA'),
    ("HA.10", "Preliminary Step: Intended Use", 2, 'HA'),
    ("HA.11", "Preliminary Step: Flow Diagram", 2, 'HA'),
    ("HA.12", "Preliminary Step: On-Site Confirmation", 2, 'HA'),
]

async def ingest_haccp(conn) -> int:
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
