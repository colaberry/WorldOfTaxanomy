"""Ingest Petrochemical Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_petrochem", "Petrochemical", "Petrochemical Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PC", "Petrochemical Types", 1, None),
    ("PC.01", "Ethylene", 2, 'PC'),
    ("PC.02", "Propylene", 2, 'PC'),
    ("PC.03", "Butadiene", 2, 'PC'),
    ("PC.04", "Benzene", 2, 'PC'),
    ("PC.05", "Toluene", 2, 'PC'),
    ("PC.06", "Xylene", 2, 'PC'),
    ("PC.07", "Methanol", 2, 'PC'),
    ("PC.08", "Polyethylene (PE)", 2, 'PC'),
    ("PC.09", "Polypropylene (PP)", 2, 'PC'),
    ("PC.10", "PVC (Polyvinyl Chloride)", 2, 'PC'),
    ("PC.11", "PET (Polyethylene Terephthalate)", 2, 'PC'),
    ("PC.12", "Styrene", 2, 'PC'),
    ("PC.13", "Ammonia", 2, 'PC'),
    ("PC.14", "Urea", 2, 'PC'),
]

async def ingest_domain_petrochem(conn) -> int:
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
