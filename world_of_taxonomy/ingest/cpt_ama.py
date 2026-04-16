"""Ingest CPT (AMA Skeleton)."""
from __future__ import annotations

_SYSTEM_ROW = ("cpt_ama", "CPT", "CPT (AMA Skeleton)", "2024", "United States", "AMA")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "AMA License"

NODES: list[tuple[str, str, int, str | None]] = [
    ("CPT", "CPT Category Sections", 1, None),
    ("CPT.E", "Evaluation and Management", 2, 'CPT'),
    ("CPT.A", "Anesthesia", 2, 'CPT'),
    ("CPT.S", "Surgery", 2, 'CPT'),
    ("CPT.R", "Radiology", 2, 'CPT'),
    ("CPT.P", "Pathology and Laboratory", 2, 'CPT'),
    ("CPT.M", "Medicine", 2, 'CPT'),
    ("CPT.II", "Category II (Performance)", 2, 'CPT'),
    ("CPT.III", "Category III (Emerging)", 2, 'CPT'),
    ("CPT.S1", "Surgery - Integumentary", 2, 'CPT'),
    ("CPT.S2", "Surgery - Musculoskeletal", 2, 'CPT'),
    ("CPT.S3", "Surgery - Respiratory", 2, 'CPT'),
    ("CPT.S4", "Surgery - Cardiovascular", 2, 'CPT'),
    ("CPT.S5", "Surgery - Digestive", 2, 'CPT'),
    ("CPT.S6", "Surgery - Urinary", 2, 'CPT'),
    ("CPT.S7", "Surgery - Nervous System", 2, 'CPT'),
    ("CPT.S8", "Surgery - Eye and Ocular", 2, 'CPT'),
    ("CPT.S9", "Surgery - Auditory", 2, 'CPT'),
]

async def ingest_cpt_ama(conn) -> int:
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
