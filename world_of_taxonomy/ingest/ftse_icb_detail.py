"""Ingest FTSE Russell Industry Classification Benchmark (Detailed)."""
from __future__ import annotations

_SYSTEM_ROW = ("ftse_icb_detail", "FTSE Russell ICB", "FTSE Russell Industry Classification Benchmark (Detailed)", "4.0", "Global", "FTSE Russell")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("FI", "ICB Industries", 1, None),
    ("FI.01", "Technology", 2, 'FI'),
    ("FI.02", "Telecommunications", 2, 'FI'),
    ("FI.03", "Health Care", 2, 'FI'),
    ("FI.04", "Financials", 2, 'FI'),
    ("FI.05", "Real Estate", 2, 'FI'),
    ("FI.06", "Consumer Discretionary", 2, 'FI'),
    ("FI.07", "Consumer Staples", 2, 'FI'),
    ("FI.08", "Industrials", 2, 'FI'),
    ("FI.09", "Basic Materials", 2, 'FI'),
    ("FI.10", "Energy", 2, 'FI'),
    ("FI.11", "Utilities", 2, 'FI'),
]

async def ingest_ftse_icb_detail(conn) -> int:
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
