"""Ingest FAO Statistical Database Domain Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("fao_stat_domain", "FAOSTAT Domains", "FAO Statistical Database Domain Categories", "2024", "Global", "FAO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "CC BY-NC-SA 3.0 IGO"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FD", "FAOSTAT Domains", 1, None),
    ("FD.01", "Production: Crops and Livestock Products", 2, 'FD'),
    ("FD.02", "Production: Value of Production", 2, 'FD'),
    ("FD.03", "Trade: Crops and Livestock Products", 2, 'FD'),
    ("FD.04", "Trade: Detailed Trade Matrix", 2, 'FD'),
    ("FD.05", "Food Balances: Food Supply", 2, 'FD'),
    ("FD.06", "Food Balances: Food Security", 2, 'FD'),
    ("FD.07", "Prices: Producer Prices", 2, 'FD'),
    ("FD.08", "Prices: Consumer Price Indices", 2, 'FD'),
    ("FD.09", "Land Use", 2, 'FD'),
    ("FD.10", "Emissions: Agriculture", 2, 'FD'),
    ("FD.11", "Emissions: Land Use", 2, 'FD'),
    ("FD.12", "Fertilizers", 2, 'FD'),
    ("FD.13", "Pesticides", 2, 'FD'),
    ("FD.14", "Investment: Government Expenditure", 2, 'FD'),
    ("FD.15", "Investment: Credit to Agriculture", 2, 'FD'),
    ("FD.16", "Population: Annual Population", 2, 'FD'),
]

async def ingest_fao_stat_domain(conn) -> int:
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
