"""Ingest Programming Paradigm Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_prog_paradigm", "Programming Paradigm", "Programming Paradigm Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PP", "Programming Paradigms", 1, None),
    ("PP.01", "Object-Oriented Programming", 2, 'PP'),
    ("PP.02", "Functional Programming", 2, 'PP'),
    ("PP.03", "Procedural Programming", 2, 'PP'),
    ("PP.04", "Logic Programming", 2, 'PP'),
    ("PP.05", "Reactive Programming", 2, 'PP'),
    ("PP.06", "Event-Driven Programming", 2, 'PP'),
    ("PP.07", "Declarative Programming", 2, 'PP'),
    ("PP.08", "Concurrent Programming", 2, 'PP'),
    ("PP.09", "Aspect-Oriented Programming", 2, 'PP'),
    ("PP.10", "Metaprogramming", 2, 'PP'),
    ("PP.11", "Generic Programming", 2, 'PP'),
    ("PP.12", "Data-Oriented Design", 2, 'PP'),
    ("PP.13", "Constraint Programming", 2, 'PP'),
    ("PP.14", "Dataflow Programming", 2, 'PP'),
    ("PP.15", "Actor Model", 2, 'PP'),
]

async def ingest_domain_prog_paradigm(conn) -> int:
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
