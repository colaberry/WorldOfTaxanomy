"""Ingest Getty Art and Architecture Thesaurus (Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("getty_aat", "Getty AAT", "Getty Art and Architecture Thesaurus (Skeleton)", "2024", "Global", "J. Paul Getty Trust")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "ODC-By 1.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GA", "AAT Facets", 1, None),
    ("GA.01", "Associated Concepts", 2, 'GA'),
    ("GA.02", "Physical Attributes", 2, 'GA'),
    ("GA.03", "Styles and Periods", 2, 'GA'),
    ("GA.04", "Agents", 2, 'GA'),
    ("GA.05", "Activities", 2, 'GA'),
    ("GA.06", "Materials", 2, 'GA'),
    ("GA.07", "Objects", 2, 'GA'),
    ("GA.08", "Brand Names", 2, 'GA'),
    ("GA.09", "Furnishings and Equipment", 2, 'GA'),
    ("GA.10", "Visual and Verbal Communication", 2, 'GA'),
    ("GA.11", "Built Environment", 2, 'GA'),
    ("GA.12", "Single Built Works", 2, 'GA'),
    ("GA.13", "Open Spaces and Site Elements", 2, 'GA'),
]

async def ingest_getty_aat(conn) -> int:
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
