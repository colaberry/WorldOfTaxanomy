"""Ingest RICS Valuation Standard (Red Book) Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("rics_valuation", "RICS Valuation", "RICS Valuation Standard (Red Book) Categories", "2024", "Global", "RICS")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("RV", "RICS Categories", 1, None),
    ("RV.01", "Market Value", 2, 'RV'),
    ("RV.02", "Market Rent", 2, 'RV'),
    ("RV.03", "Investment Value", 2, 'RV'),
    ("RV.04", "Fair Value (IFRS 13)", 2, 'RV'),
    ("RV.05", "Equitable Value", 2, 'RV'),
    ("RV.06", "Liquidation Value", 2, 'RV'),
    ("RV.07", "Reinstatement Cost", 2, 'RV'),
    ("RV.08", "Depreciated Replacement Cost", 2, 'RV'),
    ("RV.09", "Existing Use Value", 2, 'RV'),
    ("RV.10", "Marriage Value", 2, 'RV'),
    ("RV.11", "Hope Value", 2, 'RV'),
    ("RV.12", "Compulsory Purchase Compensation", 2, 'RV'),
    ("RV.13", "Discounted Cash Flow (DCF)", 2, 'RV'),
]

async def ingest_rics_valuation(conn) -> int:
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
