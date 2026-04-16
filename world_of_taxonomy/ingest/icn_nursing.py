"""Ingest ICN International Classification for Nursing Practice."""
from __future__ import annotations

_SYSTEM_ROW = ("icn_nursing", "ICN Nursing", "ICN International Classification for Nursing Practice", "2024", "Global", "ICN")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IN", "ICNP Axes", 1, None),
    ("IN.01", "Focus (Nursing Phenomena)", 2, 'IN'),
    ("IN.02", "Judgement", 2, 'IN'),
    ("IN.03", "Means (Interventions)", 2, 'IN'),
    ("IN.04", "Action", 2, 'IN'),
    ("IN.05", "Time", 2, 'IN'),
    ("IN.06", "Location (Body)", 2, 'IN'),
    ("IN.07", "Client (Beneficiary)", 2, 'IN'),
    ("IN.08", "Nursing Diagnosis Concepts", 2, 'IN'),
    ("IN.09", "Nursing Outcome Concepts", 2, 'IN'),
    ("IN.10", "Nursing Intervention Concepts", 2, 'IN'),
    ("IN.11", "Catalogues (Specialty)", 2, 'IN'),
    ("IN.12", "Mapping to SNOMED CT", 2, 'IN'),
    ("IN.13", "C-HOBIC Integration", 2, 'IN'),
]

async def ingest_icn_nursing(conn) -> int:
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
