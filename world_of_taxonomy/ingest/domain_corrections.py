"""Ingest Corrections Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_corrections", "Corrections Type", "Corrections Types", "1.0", "United States", "BOP/DOJ")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CO", "Corrections Types", 1, None),
    ("CO.01", "Federal Prison (BOP)", 2, 'CO'),
    ("CO.02", "State Prison", 2, 'CO'),
    ("CO.03", "County Jail", 2, 'CO'),
    ("CO.04", "Private Prison", 2, 'CO'),
    ("CO.05", "Minimum Security", 2, 'CO'),
    ("CO.06", "Medium Security", 2, 'CO'),
    ("CO.07", "Maximum Security", 2, 'CO'),
    ("CO.08", "Supermax Facility", 2, 'CO'),
    ("CO.09", "Juvenile Detention", 2, 'CO'),
    ("CO.10", "Community Corrections", 2, 'CO'),
    ("CO.11", "Probation", 2, 'CO'),
    ("CO.12", "Parole", 2, 'CO'),
    ("CO.13", "Electronic Monitoring", 2, 'CO'),
    ("CO.14", "Reentry Program", 2, 'CO'),
]

async def ingest_domain_corrections(conn) -> int:
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
