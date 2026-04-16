"""Ingest PMBOK 7th Edition Performance Domains."""
from __future__ import annotations

_SYSTEM_ROW = ("pmbok7", "PMBOK 7th Ed", "PMBOK 7th Edition Performance Domains", "7", "Global", "PMI")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PM", "PMBOK 7 Domains", 1, None),
    ("PM.01", "Stakeholders", 2, 'PM'),
    ("PM.02", "Team", 2, 'PM'),
    ("PM.03", "Development Approach and Life Cycle", 2, 'PM'),
    ("PM.04", "Planning", 2, 'PM'),
    ("PM.05", "Project Work", 2, 'PM'),
    ("PM.06", "Delivery", 2, 'PM'),
    ("PM.07", "Measurement", 2, 'PM'),
    ("PM.08", "Uncertainty", 2, 'PM'),
    ("PM.09", "Principles: Stewardship", 2, 'PM'),
    ("PM.10", "Principles: Team", 2, 'PM'),
    ("PM.11", "Principles: Stakeholders", 2, 'PM'),
    ("PM.12", "Principles: Value", 2, 'PM'),
    ("PM.13", "Principles: Systems Thinking", 2, 'PM'),
    ("PM.14", "Principles: Leadership", 2, 'PM'),
    ("PM.15", "Principles: Tailoring", 2, 'PM'),
    ("PM.16", "Principles: Quality", 2, 'PM'),
    ("PM.17", "Principles: Complexity", 2, 'PM'),
    ("PM.18", "Principles: Risk", 2, 'PM'),
    ("PM.19", "Principles: Adaptability", 2, 'PM'),
    ("PM.20", "Principles: Change", 2, 'PM'),
]

async def ingest_pmbok7(conn) -> int:
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
