"""Ingest Biosimilar Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_biosimilar", "Biosimilar Type", "Biosimilar Types", "1.0", "Global", "FDA/EMA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BS", "Biosimilar Types", 1, None),
    ("BS.01", "Monoclonal Antibody Biosimilar", 2, 'BS'),
    ("BS.02", "Insulin Biosimilar", 2, 'BS'),
    ("BS.03", "Growth Hormone Biosimilar", 2, 'BS'),
    ("BS.04", "Erythropoietin Biosimilar", 2, 'BS'),
    ("BS.05", "G-CSF Biosimilar", 2, 'BS'),
    ("BS.06", "Anti-TNF Biosimilar", 2, 'BS'),
    ("BS.07", "Interchangeable Biosimilar", 2, 'BS'),
    ("BS.08", "Non-Interchangeable Biosimilar", 2, 'BS'),
    ("BS.09", "Biosimilar Extrapolation", 2, 'BS'),
    ("BS.10", "Biosimilar Analytical Study", 2, 'BS'),
    ("BS.11", "Biosimilar Clinical Trial", 2, 'BS'),
    ("BS.12", "Biosimilar Switching Study", 2, 'BS'),
    ("BS.13", "Biosimilar Naming Convention", 2, 'BS'),
]

async def ingest_domain_biosimilar(conn) -> int:
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
