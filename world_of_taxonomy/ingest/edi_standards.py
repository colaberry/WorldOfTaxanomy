"""Ingest Electronic Data Interchange Standard Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("edi_standards", "EDI Standards", "Electronic Data Interchange Standard Categories", "2024", "Global", "UN/CEFACT")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("EI", "EDI Standards", 1, None),
    ("EI.01", "UN/EDIFACT", 2, 'EI'),
    ("EI.02", "ANSI X12", 2, 'EI'),
    ("EI.03", "TRADACOMS", 2, 'EI'),
    ("EI.04", "ebXML", 2, 'EI'),
    ("EI.05", "RosettaNet", 2, 'EI'),
    ("EI.06", "SWIFT FIN", 2, 'EI'),
    ("EI.07", "HL7 (Healthcare)", 2, 'EI'),
    ("EI.08", "MISMO (Mortgage)", 2, 'EI'),
    ("EI.09", "ACORD (Insurance)", 2, 'EI'),
    ("EI.10", "FIX Protocol (Finance)", 2, 'EI'),
    ("EI.11", "OAGIS (Manufacturing)", 2, 'EI'),
    ("EI.12", "GS1 EANCOM", 2, 'EI'),
    ("EI.13", "Peppol BIS (E-Invoicing)", 2, 'EI'),
]

async def ingest_edi_standards(conn) -> int:
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
