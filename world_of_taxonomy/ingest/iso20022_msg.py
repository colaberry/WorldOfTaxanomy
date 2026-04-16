"""Ingest ISO 20022 Business Area Message Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("iso20022_msg", "ISO 20022 Messages", "ISO 20022 Business Area Message Categories", "2024", "Global", "ISO TC68")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("I2", "ISO 20022 Business Areas", 1, None),
    ("I2.acmt", "Account Management (acmt)", 2, 'I2'),
    ("I2.admi", "Administration (admi)", 2, 'I2'),
    ("I2.auth", "Authorities (auth)", 2, 'I2'),
    ("I2.caaa", "Card Transaction (caaa)", 2, 'I2'),
    ("I2.camt", "Cash Management (camt)", 2, 'I2'),
    ("I2.colr", "Collateral Management (colr)", 2, 'I2'),
    ("I2.fxtr", "Foreign Exchange Trade (fxtr)", 2, 'I2'),
    ("I2.pacs", "Payments Clearing and Settlement (pacs)", 2, 'I2'),
    ("I2.pain", "Payments Initiation (pain)", 2, 'I2'),
    ("I2.reda", "Reference Data (reda)", 2, 'I2'),
    ("I2.remt", "Payments Remittance Advice (remt)", 2, 'I2'),
    ("I2.secl", "Securities Clearing (secl)", 2, 'I2'),
    ("I2.semt", "Securities Management (semt)", 2, 'I2'),
    ("I2.sese", "Securities Settlement (sese)", 2, 'I2'),
    ("I2.setr", "Securities Trade (setr)", 2, 'I2'),
    ("I2.tsin", "Trade Services Initiation (tsin)", 2, 'I2'),
]

async def ingest_iso20022_msg(conn) -> int:
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
