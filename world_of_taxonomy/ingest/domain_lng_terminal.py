"""Ingest LNG Terminal Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_lng_terminal", "LNG Terminal", "LNG Terminal Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("LN", "LNG Terminal Types", 1, None),
    ("LN.01", "Liquefaction Terminal", 2, 'LN'),
    ("LN.02", "Regasification Terminal", 2, 'LN'),
    ("LN.03", "Floating LNG (FLNG)", 2, 'LN'),
    ("LN.04", "FSRU (Floating Storage)", 2, 'LN'),
    ("LN.05", "Small-Scale LNG Terminal", 2, 'LN'),
    ("LN.06", "LNG Bunkering Facility", 2, 'LN'),
    ("LN.07", "LNG Storage Tank (Full Containment)", 2, 'LN'),
    ("LN.08", "LNG Carrier (Ship)", 2, 'LN'),
    ("LN.09", "LNG Truck Loading", 2, 'LN'),
    ("LN.10", "Boil-Off Gas Management", 2, 'LN'),
    ("LN.11", "LNG Spot Market", 2, 'LN'),
    ("LN.12", "LNG Long-Term Contract", 2, 'LN'),
    ("LN.13", "LNG Hub Pricing", 2, 'LN'),
]

async def ingest_domain_lng_terminal(conn) -> int:
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
