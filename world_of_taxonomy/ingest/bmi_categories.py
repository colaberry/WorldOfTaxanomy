"""Ingest WHO Body Mass Index Classification."""
from __future__ import annotations

_SYSTEM_ROW = ("bmi_categories", "BMI Categories", "WHO Body Mass Index Classification", "2024", "Global", "WHO")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("BM", "BMI Categories", 1, None),
    ("BM.01", "Severe Thinness (< 16.0)", 2, 'BM'),
    ("BM.02", "Moderate Thinness (16.0-16.9)", 2, 'BM'),
    ("BM.03", "Mild Thinness (17.0-18.4)", 2, 'BM'),
    ("BM.04", "Normal Range (18.5-24.9)", 2, 'BM'),
    ("BM.05", "Pre-Obese (25.0-29.9)", 2, 'BM'),
    ("BM.06", "Obese Class I (30.0-34.9)", 2, 'BM'),
    ("BM.07", "Obese Class II (35.0-39.9)", 2, 'BM'),
    ("BM.08", "Obese Class III (40.0+)", 2, 'BM'),
    ("BM.09", "Asian BMI Cutoffs", 2, 'BM'),
    ("BM.10", "Pediatric BMI-for-Age", 2, 'BM'),
]

async def ingest_bmi_categories(conn) -> int:
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
