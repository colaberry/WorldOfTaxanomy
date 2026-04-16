"""Ingest IETF RFC Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("ietf_rfc", "IETF RFC Categories", "IETF RFC Categories", "2024", "Global", "IETF")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RFC", "RFC Categories", 1, None),
    ("RFC.STD", "Internet Standard", 2, 'RFC'),
    ("RFC.BCP", "Best Current Practice", 2, 'RFC'),
    ("RFC.INF", "Informational", 2, 'RFC'),
    ("RFC.EXP", "Experimental", 2, 'RFC'),
    ("RFC.HST", "Historic", 2, 'RFC'),
    ("RFC.PS", "Proposed Standard", 2, 'RFC'),
    ("RFC.DS", "Draft Standard", 2, 'RFC'),
    ("RFC.AR1", "Applications and Real-Time", 2, 'RFC'),
    ("RFC.INT", "Internet", 2, 'RFC'),
    ("RFC.OPS", "Operations and Management", 2, 'RFC'),
    ("RFC.RTG", "Routing", 2, 'RFC'),
    ("RFC.SEC", "Security", 2, 'RFC'),
    ("RFC.TSV", "Transport", 2, 'RFC'),
    ("RFC.WIT", "Web and Internet Transport", 2, 'RFC'),
]

async def ingest_ietf_rfc(conn) -> int:
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
