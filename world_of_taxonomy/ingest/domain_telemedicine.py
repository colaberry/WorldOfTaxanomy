"""Ingest Telemedicine Modality Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_telemedicine", "Telemedicine Modality", "Telemedicine Modality Types", "1.0", "Global", "ATA")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("TM", "Telemedicine Modality Types", 1, None),
    ("TM.01", "Synchronous Video Visit", 2, 'TM'),
    ("TM.02", "Asynchronous (Store-and-Forward)", 2, 'TM'),
    ("TM.03", "Remote Patient Monitoring", 2, 'TM'),
    ("TM.04", "Telementoring", 2, 'TM'),
    ("TM.05", "Teleconsultation", 2, 'TM'),
    ("TM.06", "Tele-ICU", 2, 'TM'),
    ("TM.07", "Telepsychiatry", 2, 'TM'),
    ("TM.08", "Teleradiology", 2, 'TM'),
    ("TM.09", "Telepathology", 2, 'TM'),
    ("TM.10", "Teledermatology", 2, 'TM'),
    ("TM.11", "Telepharmacy", 2, 'TM'),
    ("TM.12", "Telerehabilitation", 2, 'TM'),
    ("TM.13", "Mobile Health (mHealth)", 2, 'TM'),
    ("TM.14", "AI-Assisted Telehealth", 2, 'TM'),
]

async def ingest_domain_telemedicine(conn) -> int:
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
