"""Ingest Ancillary Service Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_ancillary", "Ancillary Service", "Ancillary Service Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AS", "Ancillary Service Types", 1, None),
    ("AS.01", "Frequency Regulation", 2, 'AS'),
    ("AS.02", "Spinning Reserve", 2, 'AS'),
    ("AS.03", "Non-Spinning Reserve", 2, 'AS'),
    ("AS.04", "Voltage Support", 2, 'AS'),
    ("AS.05", "Black Start Capability", 2, 'AS'),
    ("AS.06", "Ramping Service", 2, 'AS'),
    ("AS.07", "Reactive Power Support", 2, 'AS'),
    ("AS.08", "Load Following", 2, 'AS'),
    ("AS.09", "Operating Reserve", 2, 'AS'),
    ("AS.10", "Inertia Service", 2, 'AS'),
    ("AS.11", "Fast Frequency Response", 2, 'AS'),
    ("AS.12", "Primary Frequency Response", 2, 'AS'),
    ("AS.13", "Secondary Frequency Response", 2, 'AS'),
]

async def ingest_domain_ancillary(conn) -> int:
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
