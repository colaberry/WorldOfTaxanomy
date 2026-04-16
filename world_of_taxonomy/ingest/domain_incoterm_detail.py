"""Ingest Incoterm Detailed Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_incoterm_detail", "Incoterm Detail", "Incoterm Detailed Types", "1.0", "Global", "ICC")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IT", "Incoterm Types", 1, None),
    ("IT.01", "EXW (Ex Works)", 2, 'IT'),
    ("IT.02", "FCA (Free Carrier)", 2, 'IT'),
    ("IT.03", "CPT (Carriage Paid To)", 2, 'IT'),
    ("IT.04", "CIP (Carriage Insurance Paid)", 2, 'IT'),
    ("IT.05", "DAP (Delivered at Place)", 2, 'IT'),
    ("IT.06", "DPU (Delivered at Place Unloaded)", 2, 'IT'),
    ("IT.07", "DDP (Delivered Duty Paid)", 2, 'IT'),
    ("IT.08", "FAS (Free Alongside Ship)", 2, 'IT'),
    ("IT.09", "FOB (Free on Board)", 2, 'IT'),
    ("IT.10", "CFR (Cost and Freight)", 2, 'IT'),
    ("IT.11", "CIF (Cost Insurance Freight)", 2, 'IT'),
    ("IT.12", "Risk Transfer Point", 2, 'IT'),
    ("IT.13", "Cost Transfer Point", 2, 'IT'),
]

async def ingest_domain_incoterm_detail(conn) -> int:
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
