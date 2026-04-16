"""Ingest Simple Knowledge Organization System Concepts."""
from __future__ import annotations

_SYSTEM_ROW = ("skos", "SKOS", "Simple Knowledge Organization System Concepts", "2009", "Global", "W3C")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "W3C Software License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SK", "SKOS Concepts", 1, None),
    ("SK.01", "Concept (skos:Concept)", 2, 'SK'),
    ("SK.02", "ConceptScheme", 2, 'SK'),
    ("SK.03", "Collection", 2, 'SK'),
    ("SK.04", "OrderedCollection", 2, 'SK'),
    ("SK.05", "prefLabel", 2, 'SK'),
    ("SK.06", "altLabel", 2, 'SK'),
    ("SK.07", "hiddenLabel", 2, 'SK'),
    ("SK.08", "broader", 2, 'SK'),
    ("SK.09", "narrower", 2, 'SK'),
    ("SK.10", "related", 2, 'SK'),
    ("SK.11", "broadMatch", 2, 'SK'),
    ("SK.12", "narrowMatch", 2, 'SK'),
    ("SK.13", "closeMatch", 2, 'SK'),
    ("SK.14", "exactMatch", 2, 'SK'),
    ("SK.15", "notation", 2, 'SK'),
    ("SK.16", "definition/note/scopeNote", 2, 'SK'),
]

async def ingest_skos(conn) -> int:
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
