"""Ingest Consumer Protection Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_consumer_prot", "Consumer Protection", "Consumer Protection Types", "1.0", "United States", "FTC")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CP", "Consumer Protection Types", 1, None),
    ("CP.01", "Unfair Trade Practice", 2, 'CP'),
    ("CP.02", "Deceptive Advertising", 2, 'CP'),
    ("CP.03", "Product Safety Recall", 2, 'CP'),
    ("CP.04", "Truth in Lending (TILA)", 2, 'CP'),
    ("CP.05", "Fair Debt Collection (FDCPA)", 2, 'CP'),
    ("CP.06", "Warranty Protection (Magnuson-Moss)", 2, 'CP'),
    ("CP.07", "Data Privacy Protection", 2, 'CP'),
    ("CP.08", "Telemarketing Regulation", 2, 'CP'),
    ("CP.09", "Email Marketing (CAN-SPAM)", 2, 'CP'),
    ("CP.10", "Lemon Law", 2, 'CP'),
    ("CP.11", "Consumer Class Action", 2, 'CP'),
    ("CP.12", "Product Liability", 2, 'CP'),
    ("CP.13", "Right to Repair", 2, 'CP'),
]

async def ingest_domain_consumer_prot(conn) -> int:
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
