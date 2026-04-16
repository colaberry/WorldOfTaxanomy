"""Ingest SWIFT Message Type Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("swift_mt", "SWIFT MT", "SWIFT Message Type Categories", "2024", "Global", "SWIFT")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SW", "SWIFT MT Categories", 1, None),
    ("SW.1xx", "Customer Transfers (MT1xx)", 2, 'SW'),
    ("SW.2xx", "Financial Institution Transfers (MT2xx)", 2, 'SW'),
    ("SW.3xx", "Treasury Markets (MT3xx)", 2, 'SW'),
    ("SW.4xx", "Collections and Cash Letters (MT4xx)", 2, 'SW'),
    ("SW.5xx", "Securities Markets (MT5xx)", 2, 'SW'),
    ("SW.6xx", "Treasury Markets (Precious Metals) (MT6xx)", 2, 'SW'),
    ("SW.7xx", "Documentary Credits (MT7xx)", 2, 'SW'),
    ("SW.8xx", "Travellers Cheques (MT8xx)", 2, 'SW'),
    ("SW.9xx", "Cash Management (MT9xx)", 2, 'SW'),
    ("SW.MX", "MX (ISO 20022) Messages", 2, 'SW'),
    ("SW.GPI", "gpi (Global Payment Innovation)", 2, 'SW'),
    ("SW.API", "API-Based Connectivity", 2, 'SW'),
]

async def ingest_swift_mt(conn) -> int:
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
