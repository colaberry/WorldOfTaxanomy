"""Ingest TOGAF Architecture Development Method Phases."""
from __future__ import annotations

_SYSTEM_ROW = ("togaf_adm", "TOGAF ADM", "TOGAF Architecture Development Method Phases", "10", "Global", "The Open Group")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("TG", "TOGAF ADM Phases", 1, None),
    ("TG.01", "Preliminary Phase", 2, 'TG'),
    ("TG.02", "Phase A: Architecture Vision", 2, 'TG'),
    ("TG.03", "Phase B: Business Architecture", 2, 'TG'),
    ("TG.04", "Phase C: Information Systems Architecture", 2, 'TG'),
    ("TG.05", "Phase D: Technology Architecture", 2, 'TG'),
    ("TG.06", "Phase E: Opportunities and Solutions", 2, 'TG'),
    ("TG.07", "Phase F: Migration Planning", 2, 'TG'),
    ("TG.08", "Phase G: Implementation Governance", 2, 'TG'),
    ("TG.09", "Phase H: Architecture Change Management", 2, 'TG'),
    ("TG.10", "Requirements Management", 2, 'TG'),
    ("TG.11", "Architecture Repository", 2, 'TG'),
    ("TG.12", "Architecture Content Framework", 2, 'TG'),
    ("TG.13", "Enterprise Continuum", 2, 'TG'),
]

async def ingest_togaf_adm(conn) -> int:
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
