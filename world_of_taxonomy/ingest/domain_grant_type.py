"""Ingest Grant Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_grant_type", "Grant Type", "Grant Types", "1.0", "United States", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("GT", "Grant Types", 1, None),
    ("GT.01", "Federal Research Grant (NIH)", 2, 'GT'),
    ("GT.02", "Federal Research Grant (NSF)", 2, 'GT'),
    ("GT.03", "SBIR/STTR Grant", 2, 'GT'),
    ("GT.04", "Block Grant", 2, 'GT'),
    ("GT.05", "Formula Grant", 2, 'GT'),
    ("GT.06", "Competitive Grant", 2, 'GT'),
    ("GT.07", "Foundation Grant", 2, 'GT'),
    ("GT.08", "Corporate Grant", 2, 'GT'),
    ("GT.09", "State Government Grant", 2, 'GT'),
    ("GT.10", "Community Development Grant", 2, 'GT'),
    ("GT.11", "Fellowship", 2, 'GT'),
    ("GT.12", "Capacity Building Grant", 2, 'GT'),
    ("GT.13", "Pass-Through Grant", 2, 'GT'),
]

async def ingest_domain_grant_type(conn) -> int:
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
