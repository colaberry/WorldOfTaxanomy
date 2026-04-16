"""Ingest Bundled Payment Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_bundled_pay", "Bundled Payment", "Bundled Payment Types", "1.0", "United States", "CMS")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BP", "Bundled Payment Types", 1, None),
    ("BP.01", "Retrospective Bundled Payment", 2, 'BP'),
    ("BP.02", "Prospective Bundled Payment", 2, 'BP'),
    ("BP.03", "BPCI Advanced", 2, 'BP'),
    ("BP.04", "CJR (Joint Replacement)", 2, 'BP'),
    ("BP.05", "Acute Care Episode", 2, 'BP'),
    ("BP.06", "Post-Acute Care Episode", 2, 'BP'),
    ("BP.07", "Full Episode of Care", 2, 'BP'),
    ("BP.08", "Surgical Episode", 2, 'BP'),
    ("BP.09", "Maternity Bundle", 2, 'BP'),
    ("BP.10", "Cardiac Bundle", 2, 'BP'),
    ("BP.11", "Oncology Bundle", 2, 'BP'),
    ("BP.12", "Gainsharing Arrangement", 2, 'BP'),
    ("BP.13", "Risk Corridor", 2, 'BP'),
    ("BP.14", "Stop-Loss Protection", 2, 'BP'),
]

async def ingest_domain_bundled_pay(conn) -> int:
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
