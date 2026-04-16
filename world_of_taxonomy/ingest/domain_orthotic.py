"""Ingest Orthotic Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_orthotic", "Orthotic Type", "Orthotic Types", "1.0", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("OT", "Orthotic Types", 1, None),
    ("OT.01", "Ankle-Foot Orthosis (AFO)", 2, 'OT'),
    ("OT.02", "Knee-Ankle-Foot Orthosis (KAFO)", 2, 'OT'),
    ("OT.03", "Spinal Orthosis", 2, 'OT'),
    ("OT.04", "Cervical Orthosis", 2, 'OT'),
    ("OT.05", "Wrist-Hand Orthosis", 2, 'OT'),
    ("OT.06", "Elbow Orthosis", 2, 'OT'),
    ("OT.07", "Foot Orthosis (Insole)", 2, 'OT'),
    ("OT.08", "Knee Orthosis", 2, 'OT'),
    ("OT.09", "Hip Orthosis", 2, 'OT'),
    ("OT.10", "Thoracolumbosacral Orthosis", 2, 'OT'),
    ("OT.11", "Cranial Remolding Orthosis", 2, 'OT'),
    ("OT.12", "Dynamic Splint", 2, 'OT'),
    ("OT.13", "Static Splint", 2, 'OT'),
    ("OT.14", "Custom vs Prefabricated", 2, 'OT'),
]

async def ingest_domain_orthotic(conn) -> int:
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
