"""Ingest ASA Physical Status Classification System."""
from __future__ import annotations

_SYSTEM_ROW = ("asa_physical", "ASA Physical Status", "ASA Physical Status Classification System", "2024", "Global", "ASA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("AS", "ASA Physical Status", 1, None),
    ("AS.01", "ASA I: Normal Healthy Patient", 2, 'AS'),
    ("AS.02", "ASA II: Mild Systemic Disease", 2, 'AS'),
    ("AS.03", "ASA III: Severe Systemic Disease", 2, 'AS'),
    ("AS.04", "ASA IV: Severe Life-Threatening Disease", 2, 'AS'),
    ("AS.05", "ASA V: Moribund Patient", 2, 'AS'),
    ("AS.06", "ASA VI: Brain-Dead Organ Donor", 2, 'AS'),
    ("AS.07", "E Modifier: Emergency Surgery", 2, 'AS'),
    ("AS.08", "Preanesthetic Assessment", 2, 'AS'),
    ("AS.09", "Mallampati Airway Classification", 2, 'AS'),
    ("AS.10", "ASA Monitoring Standards", 2, 'AS'),
]

async def ingest_asa_physical(conn) -> int:
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
