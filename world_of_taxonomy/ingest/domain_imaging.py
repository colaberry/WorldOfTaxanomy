"""Ingest Imaging Modality Types."""
from __future__ import annotations

_SYSTEM_ROW = ("domain_imaging", "Imaging Modality", "Imaging Modality Types", "1.0", "Global", "ACR")
_SOURCE_URL = None
_DATA_PROVENANCE = "expert_curated"
_LICENSE = "CC BY 4.0"

NODES: list[tuple[str, str, int, str | None]] = [
    ("IM", "Imaging Modality Types", 1, None),
    ("IM.01", "X-Ray (Radiography)", 2, 'IM'),
    ("IM.02", "CT Scan (Computed Tomography)", 2, 'IM'),
    ("IM.03", "MRI (Magnetic Resonance)", 2, 'IM'),
    ("IM.04", "Ultrasound (Sonography)", 2, 'IM'),
    ("IM.05", "PET Scan (Positron Emission)", 2, 'IM'),
    ("IM.06", "SPECT Imaging", 2, 'IM'),
    ("IM.07", "Mammography", 2, 'IM'),
    ("IM.08", "Fluoroscopy", 2, 'IM'),
    ("IM.09", "Nuclear Medicine", 2, 'IM'),
    ("IM.10", "Interventional Radiology", 2, 'IM'),
    ("IM.11", "Bone Densitometry (DEXA)", 2, 'IM'),
    ("IM.12", "Angiography", 2, 'IM'),
    ("IM.13", "Echocardiography", 2, 'IM'),
    ("IM.14", "Endoscopy (Imaging)", 2, 'IM'),
    ("IM.15", "Optical Coherence Tomography", 2, 'IM'),
]

async def ingest_domain_imaging(conn) -> int:
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
