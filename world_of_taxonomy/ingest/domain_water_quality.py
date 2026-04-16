"""Ingest Water Quality Index Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_water_quality", "Water Quality Index", "Water Quality Index Types", "1.0", "Global", "EPA/WHO")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("WQ", "Water Quality Index Types", 1, None),
    ("WQ.01", "pH Level", 2, 'WQ'),
    ("WQ.02", "Dissolved Oxygen (DO)", 2, 'WQ'),
    ("WQ.03", "Biochemical Oxygen Demand (BOD)", 2, 'WQ'),
    ("WQ.04", "Chemical Oxygen Demand (COD)", 2, 'WQ'),
    ("WQ.05", "Total Dissolved Solids (TDS)", 2, 'WQ'),
    ("WQ.06", "Turbidity", 2, 'WQ'),
    ("WQ.07", "Fecal Coliform Count", 2, 'WQ'),
    ("WQ.08", "Nitrate Level", 2, 'WQ'),
    ("WQ.09", "Phosphate Level", 2, 'WQ'),
    ("WQ.10", "Heavy Metal Content", 2, 'WQ'),
    ("WQ.11", "Chlorine Residual", 2, 'WQ'),
    ("WQ.12", "Total Suspended Solids (TSS)", 2, 'WQ'),
    ("WQ.13", "PFAS Detection", 2, 'WQ'),
]

async def ingest_domain_water_quality(conn) -> int:
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
