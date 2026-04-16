"""Ingest XBRL Financial Reporting Taxonomy Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("xbrl_taxonomy", "XBRL Taxonomy", "XBRL Financial Reporting Taxonomy Categories", "2024", "Global", "XBRL International")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("XB", "XBRL Categories", 1, None),
    ("XB.01", "US GAAP Taxonomy", 2, 'XB'),
    ("XB.02", "IFRS Taxonomy", 2, 'XB'),
    ("XB.03", "SEC EDGAR Taxonomy", 2, 'XB'),
    ("XB.04", "ESMA ESEF Taxonomy", 2, 'XB'),
    ("XB.05", "Inline XBRL (iXBRL)", 2, 'XB'),
    ("XB.06", "Global Ledger Taxonomy (GL)", 2, 'XB'),
    ("XB.07", "XBRL Dimensions", 2, 'XB'),
    ("XB.08", "XBRL Formula", 2, 'XB'),
    ("XB.09", "Calculation Linkbase", 2, 'XB'),
    ("XB.10", "Presentation Linkbase", 2, 'XB'),
    ("XB.11", "Definition Linkbase", 2, 'XB'),
    ("XB.12", "Label Linkbase", 2, 'XB'),
    ("XB.13", "Reference Linkbase", 2, 'XB'),
]

async def ingest_xbrl_taxonomy(conn) -> int:
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
