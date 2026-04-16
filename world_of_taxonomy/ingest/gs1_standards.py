"""Ingest GS1 Identification and Data Standard Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("gs1_standards", "GS1 Standards", "GS1 Identification and Data Standard Categories", "2024", "Global", "GS1")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Proprietary"

NODES: list[tuple[str, str, int, str | None]] = [
    ("G1", "GS1 Standards", 1, None),
    ("G1.01", "GTIN (Global Trade Item Number)", 2, 'G1'),
    ("G1.02", "GLN (Global Location Number)", 2, 'G1'),
    ("G1.03", "SSCC (Serial Shipping Container Code)", 2, 'G1'),
    ("G1.04", "GRAI (Global Returnable Asset Identifier)", 2, 'G1'),
    ("G1.05", "GIAI (Global Individual Asset Identifier)", 2, 'G1'),
    ("G1.06", "GSRN (Global Service Relation Number)", 2, 'G1'),
    ("G1.07", "GDTI (Global Document Type Identifier)", 2, 'G1'),
    ("G1.08", "GS1 Digital Link", 2, 'G1'),
    ("G1.09", "EPCIS (Electronic Product Code Information Services)", 2, 'G1'),
    ("G1.10", "GS1 EDI (EANCOM)", 2, 'G1'),
    ("G1.11", "GS1 DataMatrix", 2, 'G1'),
    ("G1.12", "GS1 QR Code", 2, 'G1'),
    ("G1.13", "RFID/EPC (Electronic Product Code)", 2, 'G1'),
]

async def ingest_gs1_standards(conn) -> int:
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
