"""Ingest Payment Card Network Schemes."""
from __future__ import annotations

_SYSTEM_ROW = ("card_schemes", "Card Schemes", "Payment Card Network Schemes", "2024", "Global", "Industry")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("PC", "Card Schemes", 1, None),
    ("PC.01", "Visa", 2, 'PC'),
    ("PC.02", "Mastercard", 2, 'PC'),
    ("PC.03", "American Express", 2, 'PC'),
    ("PC.04", "Discover/Diners Club", 2, 'PC'),
    ("PC.05", "UnionPay", 2, 'PC'),
    ("PC.06", "JCB", 2, 'PC'),
    ("PC.07", "RuPay (India)", 2, 'PC'),
    ("PC.08", "Elo (Brazil)", 2, 'PC'),
    ("PC.09", "Interac (Canada)", 2, 'PC'),
    ("PC.10", "EFTPOS (Australia)", 2, 'PC'),
    ("PC.11", "Mir (Russia)", 2, 'PC'),
    ("PC.12", "Troy (Turkey)", 2, 'PC'),
    ("PC.13", "Verve (Nigeria)", 2, 'PC'),
    ("PC.14", "BC Card (South Korea)", 2, 'PC'),
]

async def ingest_card_schemes(conn) -> int:
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
