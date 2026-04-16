"""Ingest Gig Worker Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_gig_worker", "Gig Worker Type", "Gig Worker Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GW", "Gig Worker Types", 1, None),
    ("GW.01", "Platform-Based Gig Worker", 2, 'GW'),
    ("GW.02", "Rideshare Driver", 2, 'GW'),
    ("GW.03", "Delivery Courier", 2, 'GW'),
    ("GW.04", "Freelance Knowledge Worker", 2, 'GW'),
    ("GW.05", "Task-Based Worker", 2, 'GW'),
    ("GW.06", "Creative Freelancer", 2, 'GW'),
    ("GW.07", "Independent Contractor", 2, 'GW'),
    ("GW.08", "Temporary Agency Worker", 2, 'GW'),
    ("GW.09", "On-Demand Professional", 2, 'GW'),
    ("GW.10", "Crowdsource Worker", 2, 'GW'),
    ("GW.11", "Digital Nomad", 2, 'GW'),
    ("GW.12", "Gig Worker Classification", 2, 'GW'),
    ("GW.13", "Portable Benefits Model", 2, 'GW'),
]

async def ingest_domain_gig_worker(conn) -> int:
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
