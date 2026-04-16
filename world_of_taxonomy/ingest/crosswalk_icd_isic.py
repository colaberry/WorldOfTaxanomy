"""ICD-11 MMS -> ISIC Rev 4 semantic crosswalk.

Hand-coded semantic associations between ICD-11 chapter-level conditions
and ISIC Rev 4 industry sections. These edges represent the relationship:
"Healthcare in this industry sector predominantly encounters these conditions."

ICD-11 source codes use the chapter ordinal ("01" through "26") as defined
in the WHO MMS linearization chapter order.

Match type: 'broad' for all edges (many-to-many semantic associations).

~50 edges total (no external file required).
"""
from __future__ import annotations

from typing import Optional

CHUNK = 500

# ICD-11 chapter code -> ISIC Rev 4 section code associations.
# Format: (source_system, source_code, target_system, target_code, match_type)
# ICD-11 chapter codes are ordinal numbers "01"-"26" per WHO MMS chapter order.
# Reference: https://icd.who.int/browse/2024-01/mms/en
ICD_ISIC_LINKS: list[tuple[str, str, str, str, str]] = [
    # Chapter 01: Certain infectious or parasitic diseases
    ("icd_11", "01", "isic_rev4", "Q", "broad"),  # Health services
    ("icd_11", "01", "isic_rev4", "A", "broad"),  # Zoonoses / agriculture workers
    # Chapter 02: Neoplasms
    ("icd_11", "02", "isic_rev4", "Q", "broad"),  # Oncology/health
    # Chapter 03: Diseases of blood or blood-forming organs
    ("icd_11", "03", "isic_rev4", "Q", "broad"),
    # Chapter 04: Diseases of the immune system
    ("icd_11", "04", "isic_rev4", "Q", "broad"),
    # Chapter 05: Endocrine, nutritional or metabolic diseases
    ("icd_11", "05", "isic_rev4", "Q", "broad"),  # Health
    ("icd_11", "05", "isic_rev4", "I", "broad"),  # Food/accommodation - diet-related
    # Chapter 06: Mental, behavioural or neurodevelopmental disorders
    ("icd_11", "06", "isic_rev4", "Q", "broad"),  # Mental health services
    ("icd_11", "06", "isic_rev4", "P", "broad"),  # Education sector
    # Chapter 07: Sleep-wake disorders
    ("icd_11", "07", "isic_rev4", "Q", "broad"),
    # Chapter 08: Diseases of the nervous system
    ("icd_11", "08", "isic_rev4", "Q", "broad"),
    # Chapter 09: Diseases of the visual system
    ("icd_11", "09", "isic_rev4", "Q", "broad"),
    # Chapter 10: Diseases of the ear or mastoid process
    ("icd_11", "10", "isic_rev4", "Q", "broad"),
    # Chapter 11: Diseases of the circulatory system
    ("icd_11", "11", "isic_rev4", "Q", "broad"),
    # Chapter 12: Diseases of the respiratory system
    ("icd_11", "12", "isic_rev4", "Q", "broad"),
    ("icd_11", "12", "isic_rev4", "B", "broad"),  # Mining/occupational lung disease
    ("icd_11", "12", "isic_rev4", "C", "broad"),  # Manufacturing dust/fumes
    # Chapter 13: Diseases of the digestive system
    ("icd_11", "13", "isic_rev4", "Q", "broad"),
    ("icd_11", "13", "isic_rev4", "I", "broad"),  # Food handling/restaurants
    # Chapter 14: Diseases of the skin
    ("icd_11", "14", "isic_rev4", "Q", "broad"),
    ("icd_11", "14", "isic_rev4", "C", "broad"),  # Chemical/manufacturing exposure
    ("icd_11", "14", "isic_rev4", "A", "broad"),  # Agricultural chemicals
    # Chapter 15: Diseases of the musculoskeletal system
    ("icd_11", "15", "isic_rev4", "Q", "broad"),
    ("icd_11", "15", "isic_rev4", "F", "broad"),  # Construction - MSDs
    ("icd_11", "15", "isic_rev4", "C", "broad"),  # Manufacturing - MSDs
    # Chapter 16: Diseases of the genitourinary system
    ("icd_11", "16", "isic_rev4", "Q", "broad"),
    # Chapter 17: Conditions related to sexual health
    ("icd_11", "17", "isic_rev4", "Q", "broad"),
    # Chapter 18: Pregnancy, childbirth or the puerperium
    ("icd_11", "18", "isic_rev4", "Q", "broad"),
    # Chapter 19: Certain conditions originating in the perinatal period
    ("icd_11", "19", "isic_rev4", "Q", "broad"),
    # Chapter 20: Developmental anomalies
    ("icd_11", "20", "isic_rev4", "Q", "broad"),
    # Chapter 21: Symptoms, signs or clinical findings NEC
    ("icd_11", "21", "isic_rev4", "Q", "broad"),
    # Chapter 22: Injury, poisoning or certain other consequences
    ("icd_11", "22", "isic_rev4", "Q", "broad"),  # Emergency/trauma care
    ("icd_11", "22", "isic_rev4", "H", "broad"),  # Transport accidents
    ("icd_11", "22", "isic_rev4", "F", "broad"),  # Construction injuries
    ("icd_11", "22", "isic_rev4", "B", "broad"),  # Mining injuries
    # Chapter 23: External causes of morbidity or mortality
    ("icd_11", "23", "isic_rev4", "Q", "broad"),
    ("icd_11", "23", "isic_rev4", "H", "broad"),  # Transport
    ("icd_11", "23", "isic_rev4", "F", "broad"),  # Construction
    ("icd_11", "23", "isic_rev4", "A", "broad"),  # Agriculture machinery
    ("icd_11", "23", "isic_rev4", "C", "broad"),  # Manufacturing
    # Chapter 24: Factors influencing health status
    ("icd_11", "24", "isic_rev4", "Q", "broad"),
    ("icd_11", "24", "isic_rev4", "O", "broad"),  # Public administration
    # Chapter 25: Codes for special purposes
    ("icd_11", "25", "isic_rev4", "Q", "broad"),
    # Chapter 26: Traditional medicine (supplementary)
    ("icd_11", "26", "isic_rev4", "Q", "broad"),
    # Occupational diseases - cross-chapter industry associations
    ("icd_11", "12", "isic_rev4", "D", "broad"),  # Electricity/utilities - fumes
    ("icd_11", "22", "isic_rev4", "D", "broad"),  # Utilities - electrical injuries
    ("icd_11", "06", "isic_rev4", "R", "broad"),  # Arts/entertainment - mental health
    ("icd_11", "05", "isic_rev4", "C", "broad"),  # Manufacturing - occupational metabolic
]


async def ingest_crosswalk_icd_isic(conn, path: Optional[str] = None) -> int:
    """Insert ICD-11 chapter -> ISIC Rev 4 semantic equivalence edges.

    All links are hand-coded in ICD_ISIC_LINKS.
    Filters target ISIC codes to those present in the isic_rev4 system in DB.
    Returns total edges inserted.
    """
    # Filter to ISIC codes that exist in DB
    isic_codes = {
        row["code"]
        for row in await conn.fetch(
            "SELECT code FROM classification_node WHERE system_id = 'isic_rev4'"
        )
    }

    rows = [link for link in ICD_ISIC_LINKS if link[3] in isic_codes]

    count = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i: i + CHUNK]
        await conn.executemany(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (source_system, source_code, target_system, target_code) DO NOTHING""",
            chunk,
        )
        count += len(chunk)

    return count
