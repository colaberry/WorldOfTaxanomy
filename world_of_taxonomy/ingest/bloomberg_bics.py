"""Ingest Bloomberg Industry Classification System."""
from __future__ import annotations

_SYSTEM_ROW = ("bloomberg_bics", "Bloomberg BICS", "Bloomberg Industry Classification System", "2024", "Global", "Bloomberg LP")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Bloomberg License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BICS", "BICS Sectors", 1, None),
    ("BICS.01", "Communications", 2, 'BICS'),
    ("BICS.02", "Consumer Discretionary", 2, 'BICS'),
    ("BICS.03", "Consumer Staples", 2, 'BICS'),
    ("BICS.04", "Energy", 2, 'BICS'),
    ("BICS.05", "Financials", 2, 'BICS'),
    ("BICS.06", "Health Care", 2, 'BICS'),
    ("BICS.07", "Industrials", 2, 'BICS'),
    ("BICS.08", "Materials", 2, 'BICS'),
    ("BICS.09", "Technology", 2, 'BICS'),
    ("BICS.10", "Utilities", 2, 'BICS'),
    ("BICS.11", "Government", 2, 'BICS'),
    ("BICS.12", "Diversified", 2, 'BICS'),
]

async def ingest_bloomberg_bics(conn) -> int:
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
