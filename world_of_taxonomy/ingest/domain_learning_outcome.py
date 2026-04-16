"""Ingest Learning Outcome Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_learning_outcome", "Learning Outcome", "Learning Outcome Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LO", "Learning Outcome Types", 1, None),
    ("LO.01", "Knowledge Outcome", 2, 'LO'),
    ("LO.02", "Skill Outcome", 2, 'LO'),
    ("LO.03", "Attitude/Values Outcome", 2, 'LO'),
    ("LO.04", "Bloom Taxonomy Level", 2, 'LO'),
    ("LO.05", "Program-Level Outcome (PLO)", 2, 'LO'),
    ("LO.06", "Course-Level Outcome (CLO)", 2, 'LO'),
    ("LO.07", "Institutional Outcome (ILO)", 2, 'LO'),
    ("LO.08", "Graduate Attribute", 2, 'LO'),
    ("LO.09", "Employability Outcome", 2, 'LO'),
    ("LO.10", "Digital Literacy Outcome", 2, 'LO'),
    ("LO.11", "Critical Thinking Outcome", 2, 'LO'),
    ("LO.12", "Assessment Alignment", 2, 'LO'),
    ("LO.13", "Outcome Mapping", 2, 'LO'),
]

async def ingest_domain_learning_outcome(conn) -> int:
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
