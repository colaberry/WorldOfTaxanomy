"""Ingest Common Core State Standards."""
from __future__ import annotations

_SYSTEM_ROW = ("ccss", "CCSS", "Common Core State Standards", "2010", "United States", "NGA/CCSSO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CC", "CCSS Domains", 1, None),
    ("CC.01", "ELA: Reading Literature", 2, 'CC'),
    ("CC.02", "ELA: Reading Informational Text", 2, 'CC'),
    ("CC.03", "ELA: Writing", 2, 'CC'),
    ("CC.04", "ELA: Speaking and Listening", 2, 'CC'),
    ("CC.05", "ELA: Language", 2, 'CC'),
    ("CC.06", "Math: Counting and Cardinality (K)", 2, 'CC'),
    ("CC.07", "Math: Operations and Algebraic Thinking", 2, 'CC'),
    ("CC.08", "Math: Number and Operations (Base 10)", 2, 'CC'),
    ("CC.09", "Math: Number and Operations (Fractions)", 2, 'CC'),
    ("CC.10", "Math: Measurement and Data", 2, 'CC'),
    ("CC.11", "Math: Geometry", 2, 'CC'),
    ("CC.12", "Math: Ratios and Proportional Relationships", 2, 'CC'),
    ("CC.13", "Math: Statistics and Probability", 2, 'CC'),
    ("CC.14", "Math: Functions", 2, 'CC'),
    ("CC.15", "Math: Modeling (HS)", 2, 'CC'),
    ("CC.16", "ELA: Reading in History/Social Studies", 2, 'CC'),
    ("CC.17", "ELA: Reading in Science/Technical", 2, 'CC'),
]

async def ingest_ccss(conn) -> int:
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
