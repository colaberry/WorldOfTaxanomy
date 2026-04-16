"""Ingest Bloom's Taxonomy of Educational Objectives (Revised)."""
from __future__ import annotations

_SYSTEM_ROW = ("bloom_taxonomy", "Bloom Taxonomy", "Bloom's Taxonomy of Educational Objectives (Revised)", "2001", "Global", "Anderson/Krathwohl")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BL", "Bloom's Taxonomy", 1, None),
    ("BL.01", "Remember (Knowledge)", 2, 'BL'),
    ("BL.02", "Understand (Comprehension)", 2, 'BL'),
    ("BL.03", "Apply (Application)", 2, 'BL'),
    ("BL.04", "Analyze (Analysis)", 2, 'BL'),
    ("BL.05", "Evaluate (Evaluation)", 2, 'BL'),
    ("BL.06", "Create (Synthesis)", 2, 'BL'),
    ("BL.07", "Cognitive Domain", 2, 'BL'),
    ("BL.08", "Affective Domain", 2, 'BL'),
    ("BL.09", "Psychomotor Domain", 2, 'BL'),
    ("BL.10", "Knowledge Dimension: Factual", 2, 'BL'),
    ("BL.11", "Knowledge Dimension: Conceptual", 2, 'BL'),
    ("BL.12", "Knowledge Dimension: Procedural", 2, 'BL'),
    ("BL.13", "Knowledge Dimension: Metacognitive", 2, 'BL'),
]

async def ingest_bloom_taxonomy(conn) -> int:
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
