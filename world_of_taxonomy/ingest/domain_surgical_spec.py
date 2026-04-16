"""Ingest Surgical Specialty Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_surgical_spec", "Surgical Specialty", "Surgical Specialty Types", "1.0", "Global", "ACS")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("SS", "Surgical Specialty Types", 1, None),
    ("SS.01", "General Surgery", 2, 'SS'),
    ("SS.02", "Cardiac Surgery", 2, 'SS'),
    ("SS.03", "Thoracic Surgery", 2, 'SS'),
    ("SS.04", "Neurosurgery", 2, 'SS'),
    ("SS.05", "Orthopedic Surgery", 2, 'SS'),
    ("SS.06", "Plastic and Reconstructive", 2, 'SS'),
    ("SS.07", "Vascular Surgery", 2, 'SS'),
    ("SS.08", "Urologic Surgery", 2, 'SS'),
    ("SS.09", "Pediatric Surgery", 2, 'SS'),
    ("SS.10", "Transplant Surgery", 2, 'SS'),
    ("SS.11", "Colorectal Surgery", 2, 'SS'),
    ("SS.12", "Surgical Oncology", 2, 'SS'),
    ("SS.13", "Minimally Invasive Surgery", 2, 'SS'),
    ("SS.14", "Robotic Surgery", 2, 'SS'),
    ("SS.15", "Trauma Surgery", 2, 'SS'),
    ("SS.16", "Bariatric Surgery", 2, 'SS'),
]

async def ingest_domain_surgical_spec(conn) -> int:
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
