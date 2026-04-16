"""Ingest ICD-10-CA (Canada)."""
from __future__ import annotations

_SYSTEM_ROW = ("icd10_ca", "ICD-10-CA", "ICD-10-CA (Canada)", "2024", "Canada", "CIHI")
_SOURCE_URL = None
_DATA_PROVENANCE = "manual_transcription"
_LICENSE = "Public Domain"

NODES: list[tuple[str, str, int, str | None]] = [
    ("ICD10CA", "ICD-10-CA Chapters", 1, None),
    ("I", "Certain infectious and parasitic diseases", 2, 'ICD10CA'),
    ("II", "Neoplasms", 2, 'ICD10CA'),
    ("III", "Blood diseases and immune disorders", 2, 'ICD10CA'),
    ("IV", "Endocrine, nutritional and metabolic", 2, 'ICD10CA'),
    ("V", "Mental and behavioural disorders", 2, 'ICD10CA'),
    ("VI", "Nervous system diseases", 2, 'ICD10CA'),
    ("VII", "Eye and adnexa diseases", 2, 'ICD10CA'),
    ("VIII", "Ear and mastoid diseases", 2, 'ICD10CA'),
    ("IX", "Circulatory system diseases", 2, 'ICD10CA'),
    ("X", "Respiratory system diseases", 2, 'ICD10CA'),
    ("XI", "Digestive system diseases", 2, 'ICD10CA'),
    ("XII", "Skin and subcutaneous tissue", 2, 'ICD10CA'),
    ("XIII", "Musculoskeletal and connective tissue", 2, 'ICD10CA'),
    ("XIV", "Genitourinary system", 2, 'ICD10CA'),
    ("XV", "Pregnancy, childbirth and puerperium", 2, 'ICD10CA'),
    ("XVI", "Perinatal conditions", 2, 'ICD10CA'),
    ("XVII", "Congenital malformations", 2, 'ICD10CA'),
    ("XVIII", "Symptoms and abnormal findings", 2, 'ICD10CA'),
    ("XIX", "Injury, poisoning and external causes", 2, 'ICD10CA'),
    ("XX", "External causes of morbidity", 2, 'ICD10CA'),
    ("XXI", "Health status and health services", 2, 'ICD10CA'),
    ("XXII", "Special purpose codes", 2, 'ICD10CA'),
]

async def ingest_icd10_ca(conn) -> int:
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
