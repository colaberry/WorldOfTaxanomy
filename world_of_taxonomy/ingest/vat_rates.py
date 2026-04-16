"""Ingest Global VAT/GST Rate Type Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("vat_rates", "VAT Rate Types", "Global VAT/GST Rate Type Categories", "2024", "Global", "OECD")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("VR", "VAT Rate Types", 1, None),
    ("VR.01", "Standard Rate", 2, 'VR'),
    ("VR.02", "Reduced Rate", 2, 'VR'),
    ("VR.03", "Super-Reduced Rate", 2, 'VR'),
    ("VR.04", "Zero Rate (Taxable)", 2, 'VR'),
    ("VR.05", "Exempt (No Input Deduction)", 2, 'VR'),
    ("VR.06", "Out of Scope", 2, 'VR'),
    ("VR.07", "Parking Rate", 2, 'VR'),
    ("VR.08", "Reverse Charge", 2, 'VR'),
    ("VR.09", "Intra-Community Supply", 2, 'VR'),
    ("VR.10", "Margin Scheme", 2, 'VR'),
    ("VR.11", "Flat-Rate Farmer Scheme", 2, 'VR'),
    ("VR.12", "Import VAT", 2, 'VR'),
    ("VR.13", "Digital Services VAT (OSS)", 2, 'VR'),
]

async def ingest_vat_rates(conn) -> int:
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
