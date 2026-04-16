"""Ingest Anesthesia Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_anesthesia", "Anesthesia Type", "Anesthesia Types", "1.0", "Global", "ASA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AN", "Anesthesia Types", 1, None),
    ("AN.01", "General Anesthesia", 2, 'AN'),
    ("AN.02", "Regional Anesthesia", 2, 'AN'),
    ("AN.03", "Local Anesthesia", 2, 'AN'),
    ("AN.04", "Spinal Anesthesia", 2, 'AN'),
    ("AN.05", "Epidural Anesthesia", 2, 'AN'),
    ("AN.06", "Nerve Block", 2, 'AN'),
    ("AN.07", "Sedation (Conscious)", 2, 'AN'),
    ("AN.08", "Deep Sedation", 2, 'AN'),
    ("AN.09", "Monitored Anesthesia Care", 2, 'AN'),
    ("AN.10", "Topical Anesthesia", 2, 'AN'),
    ("AN.11", "Intravenous Regional (Bier)", 2, 'AN'),
    ("AN.12", "Pediatric Anesthesia", 2, 'AN'),
    ("AN.13", "Obstetric Anesthesia", 2, 'AN'),
    ("AN.14", "Cardiac Anesthesia", 2, 'AN'),
    ("AN.15", "Pain Management", 2, 'AN'),
]

async def ingest_domain_anesthesia(conn) -> int:
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
