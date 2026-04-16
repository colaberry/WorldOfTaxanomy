"""Ingest Court Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_court_type", "Court Type", "Court Types", "1.0", "United States", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CT", "Court Types", 1, None),
    ("CT.01", "US Supreme Court", 2, 'CT'),
    ("CT.02", "US Circuit Court of Appeals", 2, 'CT'),
    ("CT.03", "US District Court", 2, 'CT'),
    ("CT.04", "State Supreme Court", 2, 'CT'),
    ("CT.05", "State Appellate Court", 2, 'CT'),
    ("CT.06", "State Trial Court", 2, 'CT'),
    ("CT.07", "Bankruptcy Court", 2, 'CT'),
    ("CT.08", "Tax Court", 2, 'CT'),
    ("CT.09", "Family Court", 2, 'CT'),
    ("CT.10", "Juvenile Court", 2, 'CT'),
    ("CT.11", "Probate Court", 2, 'CT'),
    ("CT.12", "Small Claims Court", 2, 'CT'),
    ("CT.13", "Military Court (UCMJ)", 2, 'CT'),
    ("CT.14", "Tribal Court", 2, 'CT'),
]

async def ingest_domain_court_type(conn) -> int:
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
