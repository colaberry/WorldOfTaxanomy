"""Ingest Demand Response Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_demand_resp", "Demand Response", "Demand Response Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DA", "Demand Response Types", 1, None),
    ("DA.01", "Price-Based DR (Time-of-Use)", 2, 'DA'),
    ("DA.02", "Critical Peak Pricing", 2, 'DA'),
    ("DA.03", "Real-Time Pricing DR", 2, 'DA'),
    ("DA.04", "Incentive-Based DR", 2, 'DA'),
    ("DA.05", "Direct Load Control", 2, 'DA'),
    ("DA.06", "Interruptible Service", 2, 'DA'),
    ("DA.07", "Emergency DR Program", 2, 'DA'),
    ("DA.08", "Capacity Market DR", 2, 'DA'),
    ("DA.09", "Ancillary Services DR", 2, 'DA'),
    ("DA.10", "Behind-the-Meter DR", 2, 'DA'),
    ("DA.11", "Automated DR (OpenADR)", 2, 'DA'),
    ("DA.12", "Behavioral DR", 2, 'DA'),
    ("DA.13", "Industrial DR", 2, 'DA'),
]

async def ingest_domain_demand_resp(conn) -> int:
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
