"""Ingest Export Control Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_export_ctrl", "Export Control", "Export Control Types", "1.0", "Global", "BIS/Wassenaar")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EC", "Export Control Types", 1, None),
    ("EC.01", "Dual-Use Item Control", 2, 'EC'),
    ("EC.02", "Military Item Control", 2, 'EC'),
    ("EC.03", "Wassenaar Arrangement", 2, 'EC'),
    ("EC.04", "Nuclear Suppliers Group", 2, 'EC'),
    ("EC.05", "Missile Technology Control Regime", 2, 'EC'),
    ("EC.06", "Australia Group (Chemical/Bio)", 2, 'EC'),
    ("EC.07", "Entity List Restriction", 2, 'EC'),
    ("EC.08", "End-Use/End-User Certificate", 2, 'EC'),
    ("EC.09", "Export License Application", 2, 'EC'),
    ("EC.10", "License Exception", 2, 'EC'),
    ("EC.11", "Deemed Export", 2, 'EC'),
    ("EC.12", "Technology Transfer Control", 2, 'EC'),
    ("EC.13", "De Minimis Rule", 2, 'EC'),
]

async def ingest_domain_export_ctrl(conn) -> int:
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
