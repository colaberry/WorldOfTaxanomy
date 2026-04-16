"""Ingest Common Data Retention Period Categories."""
from __future__ import annotations

_SYSTEM_ROW = ("data_retention", "Data Retention Periods", "Common Data Retention Period Categories", "2024", "Global", "Industry Consensus")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("DR", "Retention Periods", 1, None),
    ("DR.01", "Real-Time (No Retention)", 2, 'DR'),
    ("DR.02", "24 Hours", 2, 'DR'),
    ("DR.03", "30 Days", 2, 'DR'),
    ("DR.04", "90 Days", 2, 'DR'),
    ("DR.05", "1 Year", 2, 'DR'),
    ("DR.06", "3 Years", 2, 'DR'),
    ("DR.07", "5 Years (HIPAA Minimum)", 2, 'DR'),
    ("DR.08", "6 Years (UK GDPR)", 2, 'DR'),
    ("DR.09", "7 Years (Financial/Tax)", 2, 'DR'),
    ("DR.10", "10 Years (Pharmaceutical)", 2, 'DR'),
    ("DR.11", "15 Years (Medical Records)", 2, 'DR'),
    ("DR.12", "25 Years (Nuclear/Environmental)", 2, 'DR'),
    ("DR.13", "Permanent/Indefinite", 2, 'DR'),
    ("DR.14", "Until Consent Withdrawn", 2, 'DR'),
    ("DR.15", "Until Purpose Fulfilled", 2, 'DR'),
]

async def ingest_data_retention(conn) -> int:
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
