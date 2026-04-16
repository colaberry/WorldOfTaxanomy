"""Ingest Virtual Power Plant Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_vpp", "Virtual Power Plant", "Virtual Power Plant Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("VP", "Virtual Power Plant Types", 1, None),
    ("VP.01", "Supply-Side VPP", 2, 'VP'),
    ("VP.02", "Demand-Side VPP", 2, 'VP'),
    ("VP.03", "Mixed VPP", 2, 'VP'),
    ("VP.04", "Technical VPP", 2, 'VP'),
    ("VP.05", "Commercial VPP", 2, 'VP'),
    ("VP.06", "DER Aggregation Platform", 2, 'VP'),
    ("VP.07", "Battery Storage VPP", 2, 'VP'),
    ("VP.08", "EV Fleet VPP", 2, 'VP'),
    ("VP.09", "Solar PV Aggregation", 2, 'VP'),
    ("VP.10", "Wind Farm Aggregation", 2, 'VP'),
    ("VP.11", "Industrial Load VPP", 2, 'VP'),
    ("VP.12", "VPP Optimization Algorithm", 2, 'VP'),
    ("VP.13", "VPP Market Participation", 2, 'VP'),
]

async def ingest_domain_vpp(conn) -> int:
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
