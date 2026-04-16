"""Ingest Supply Chain Operations Reference Model."""
from __future__ import annotations

_SYSTEM_ROW = ("scor_model", "SCOR Model", "Supply Chain Operations Reference Model", "13.0", "Global", "ASCM")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SC", "SCOR Processes", 1, None),
    ("SC.01", "Plan", 2, 'SC'),
    ("SC.02", "Source", 2, 'SC'),
    ("SC.03", "Make", 2, 'SC'),
    ("SC.04", "Deliver", 2, 'SC'),
    ("SC.05", "Return", 2, 'SC'),
    ("SC.06", "Enable", 2, 'SC'),
    ("SC.07", "Orchestrate", 2, 'SC'),
    ("SC.08", "Plan Supply Chain", 2, 'SC'),
    ("SC.09", "Source Stocked Product", 2, 'SC'),
    ("SC.10", "Source Make-to-Order", 2, 'SC'),
    ("SC.11", "Source Engineer-to-Order", 2, 'SC'),
    ("SC.12", "Make-to-Stock", 2, 'SC'),
    ("SC.13", "Make-to-Order", 2, 'SC'),
    ("SC.14", "Deliver Stocked Product", 2, 'SC'),
    ("SC.15", "Deliver MTO Product", 2, 'SC'),
    ("SC.16", "Deliver ETO Product", 2, 'SC'),
]

async def ingest_scor_model(conn) -> int:
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
