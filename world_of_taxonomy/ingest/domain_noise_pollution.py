"""Ingest Noise Pollution Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_noise_pollution", "Noise Pollution", "Noise Pollution Types", "1.0", "Global", "WHO/EPA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("NP", "Noise Pollution Types", 1, None),
    ("NP.01", "Traffic Noise", 2, 'NP'),
    ("NP.02", "Aircraft Noise", 2, 'NP'),
    ("NP.03", "Railway Noise", 2, 'NP'),
    ("NP.04", "Industrial Noise", 2, 'NP'),
    ("NP.05", "Construction Noise", 2, 'NP'),
    ("NP.06", "Neighborhood Noise", 2, 'NP'),
    ("NP.07", "Wind Turbine Noise", 2, 'NP'),
    ("NP.08", "Underwater Noise", 2, 'NP'),
    ("NP.09", "Noise Mapping", 2, 'NP'),
    ("NP.10", "Noise Barrier/Mitigation", 2, 'NP'),
    ("NP.11", "Occupational Noise Exposure", 2, 'NP'),
    ("NP.12", "Noise Monitoring Station", 2, 'NP'),
]

async def ingest_domain_noise_pollution(conn) -> int:
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
