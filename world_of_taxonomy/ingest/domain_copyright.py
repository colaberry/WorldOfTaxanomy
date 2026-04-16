"""Ingest Copyright Category Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_copyright", "Copyright Category", "Copyright Category Types", "1.0", "Global", "WIPO/Berne")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CR", "Copyright Category Types", 1, None),
    ("CR.01", "Literary Work", 2, 'CR'),
    ("CR.02", "Musical Work", 2, 'CR'),
    ("CR.03", "Dramatic Work", 2, 'CR'),
    ("CR.04", "Artistic Work", 2, 'CR'),
    ("CR.05", "Cinematographic Work", 2, 'CR'),
    ("CR.06", "Sound Recording", 2, 'CR'),
    ("CR.07", "Software Copyright", 2, 'CR'),
    ("CR.08", "Database Copyright", 2, 'CR'),
    ("CR.09", "Architectural Work", 2, 'CR'),
    ("CR.10", "Compilation/Collection", 2, 'CR'),
    ("CR.11", "Derivative Work", 2, 'CR'),
    ("CR.12", "Fair Use/Fair Dealing", 2, 'CR'),
    ("CR.13", "Creative Commons License", 2, 'CR'),
]

async def ingest_domain_copyright(conn) -> int:
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
