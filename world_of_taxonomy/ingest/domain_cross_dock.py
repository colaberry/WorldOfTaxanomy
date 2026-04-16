"""Ingest Cross-Dock Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_cross_dock", "Cross-Dock", "Cross-Dock Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("XD", "Cross-Dock Types", 1, None),
    ("XD.01", "Manufacturing Cross-Dock", 2, 'XD'),
    ("XD.02", "Distributor Cross-Dock", 2, 'XD'),
    ("XD.03", "Transportation Cross-Dock", 2, 'XD'),
    ("XD.04", "Retail Cross-Dock", 2, 'XD'),
    ("XD.05", "Opportunistic Cross-Dock", 2, 'XD'),
    ("XD.06", "Pre-Allocated Cross-Dock", 2, 'XD'),
    ("XD.07", "Post-Allocated Cross-Dock", 2, 'XD'),
    ("XD.08", "Continuous Cross-Dock", 2, 'XD'),
    ("XD.09", "Consolidation Cross-Dock", 2, 'XD'),
    ("XD.10", "Deconsolidation Cross-Dock", 2, 'XD'),
    ("XD.11", "LTL Cross-Dock", 2, 'XD'),
    ("XD.12", "E-Commerce Cross-Dock", 2, 'XD'),
]

async def ingest_domain_cross_dock(conn) -> int:
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
