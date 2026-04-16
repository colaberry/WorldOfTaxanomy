"""Ingest Corporate Action Event Types (ISO 15022)."""
from __future__ import annotations

_SYSTEM_ROW = ("corporate_action", "Corporate Actions", "Corporate Action Event Types (ISO 15022)", "2024", "Global", "ISO/SWIFT")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CA", "Corporate Action Types", 1, None),
    ("CA.01", "Cash Dividend", 2, 'CA'),
    ("CA.02", "Stock Dividend", 2, 'CA'),
    ("CA.03", "Stock Split", 2, 'CA'),
    ("CA.04", "Reverse Stock Split", 2, 'CA'),
    ("CA.05", "Rights Issue", 2, 'CA'),
    ("CA.06", "Bonus Issue", 2, 'CA'),
    ("CA.07", "Merger/Acquisition", 2, 'CA'),
    ("CA.08", "Spin-Off", 2, 'CA'),
    ("CA.09", "Tender Offer", 2, 'CA'),
    ("CA.10", "Share Buyback", 2, 'CA'),
    ("CA.11", "Exchange Offer", 2, 'CA'),
    ("CA.12", "Warrant Exercise", 2, 'CA'),
    ("CA.13", "Conversion", 2, 'CA'),
    ("CA.14", "Bankruptcy/Liquidation", 2, 'CA'),
    ("CA.15", "Name Change/ISIN Change", 2, 'CA'),
    ("CA.16", "Coupon Payment (Bond)", 2, 'CA'),
    ("CA.17", "Maturity/Redemption", 2, 'CA'),
    ("CA.18", "Proxy Voting", 2, 'CA'),
]

async def ingest_corporate_action(conn) -> int:
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
