"""ICD-10-AM (Australian Modification) skeleton ingester.

ICD-10-AM is the Australian modification of ICD-10, maintained by the
Australian Institute of Health and Welfare (AIHW) and published in
conjunction with ACHI (Australian Classification of Health Interventions)
and ACS (Australian Coding Standards). Used for hospital morbidity data
collection (NHMD), DRG assignment, and health statistics in Australia and
New Zealand. This skeleton covers all 22 chapters.
"""
from __future__ import annotations

SYSTEM_ID = "icd10_am"

# ICD-10-AM chapters: (code, title)
_CHAPTERS = [
    ("AM01", "Certain infectious and parasitic diseases (A00-B99)"),
    ("AM02", "Neoplasms (C00-D48)"),
    ("AM03", "Diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism (D50-D89)"),
    ("AM04", "Endocrine, nutritional and metabolic diseases (E00-E90)"),
    ("AM05", "Mental and behavioural disorders (F00-F99)"),
    ("AM06", "Diseases of the nervous system (G00-G99)"),
    ("AM07", "Diseases of the eye and adnexa (H00-H59)"),
    ("AM08", "Diseases of the ear and mastoid process (H60-H95)"),
    ("AM09", "Diseases of the circulatory system (I00-I99)"),
    ("AM10", "Diseases of the respiratory system (J00-J99)"),
    ("AM11", "Diseases of the digestive system (K00-K93)"),
    ("AM12", "Diseases of the skin and subcutaneous tissue (L00-L99)"),
    ("AM13", "Diseases of the musculoskeletal system and connective tissue (M00-M99)"),
    ("AM14", "Diseases of the genitourinary system (N00-N99)"),
    ("AM15", "Pregnancy, childbirth and the puerperium (O00-O99)"),
    ("AM16", "Certain conditions originating in the perinatal period (P00-P96)"),
    ("AM17", "Congenital malformations, deformations and chromosomal abnormalities (Q00-Q99)"),
    ("AM18", "Symptoms, signs and abnormal clinical and laboratory findings (R00-R99)"),
    ("AM19", "Injury, poisoning and certain other consequences of external causes (S00-T98)"),
    ("AM20", "External causes of morbidity and mortality (V01-Y98)"),
    ("AM21", "Factors influencing health status and contact with health services (Z00-Z99)"),
    ("AM22", "Codes for special purposes (U00-U99) - Australian extension codes"),
]

# Australian-specific blocks and extensions
_BLOCKS = [
    ("A00-A09", "Intestinal infectious diseases", "AM01"),
    ("A15-A19", "Tuberculosis", "AM01"),
    ("A50-A64", "Infections with predominantly sexual mode of transmission", "AM01"),
    ("B00-B09", "Viral infections characterised by skin and mucous membrane lesions", "AM01"),
    ("B20-B24", "Human immunodeficiency virus disease", "AM01"),
    ("C00-C75", "Malignant neoplasms, stated or presumed to be primary", "AM02"),
    ("C76-C80", "Malignant neoplasms of ill-defined, secondary and unspecified sites", "AM02"),
    ("C81-C96", "Malignant neoplasms, lymphoid, haematopoietic and related tissue", "AM02"),
    ("D00-D09", "In situ neoplasms", "AM02"),
    ("D10-D36", "Benign neoplasms", "AM02"),
    ("F00-F09", "Organic, including symptomatic, mental disorders", "AM05"),
    ("F10-F19", "Mental and behavioural disorders due to psychoactive substance use", "AM05"),
    ("F20-F29", "Schizophrenia, schizotypal and delusional disorders", "AM05"),
    ("F30-F39", "Mood (affective) disorders", "AM05"),
    ("F40-F48", "Neurotic, stress-related and somatoform disorders", "AM05"),
    ("I00-I02", "Acute rheumatic fever", "AM09"),
    ("I10-I15", "Hypertensive diseases", "AM09"),
    ("I20-I25", "Ischaemic heart diseases", "AM09"),
    ("I60-I69", "Cerebrovascular diseases", "AM09"),
    ("J00-J06", "Acute upper respiratory infections", "AM10"),
    ("J09-J18", "Influenza and pneumonia", "AM10"),
    ("J40-J47", "Chronic lower respiratory diseases", "AM10"),
    ("S00-S09", "Injuries to the head", "AM19"),
    ("S10-S19", "Injuries to the neck", "AM19"),
    ("S20-S29", "Injuries to the thorax", "AM19"),
    ("T36-T65", "Poisoning by, adverse effects of and underdosing of drugs, medicaments", "AM19"),
    ("V01-V99", "Transport accidents", "AM20"),
    ("W00-X59", "Other external causes of accidental injury", "AM20"),
    ("Z00-Z13", "Persons encountering health services for examination and investigation", "AM21"),
    ("Z80-Z99", "Persons with potential health hazards related to family and personal history", "AM21"),
]

NODES: list[tuple] = []
for code, title in _CHAPTERS:
    NODES.append((code, title, 1, None))
for code, title, parent in _BLOCKS:
    NODES.append((code, title, 2, parent))


async def ingest_icd10_am(conn) -> int:
    """Ingest ICD-10-AM (Australian Modification) chapter and block skeleton."""
    await conn.execute(
        """
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET node_count = 0
        """,
        SYSTEM_ID,
        "ICD-10-AM",
        "ICD-10 Australian Modification (12th Edition)",
        "Australia / New Zealand",
        "12th Edition (2023)",
        "Australian Institute of Health and Welfare (AIHW)",
        "#7C3AED",
    )

    codes_with_children = {parent for (_, _, _, parent) in NODES if parent is not None}
    count = 0
    for seq, (code, title, level, parent_code) in enumerate(NODES, 1):
        sector = code[:4] if level == 1 else code[:3]
        is_leaf = code not in codes_with_children
        await conn.execute(
            """
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code,
                 sector_code, is_leaf, seq_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (system_id, code) DO UPDATE SET is_leaf = EXCLUDED.is_leaf
            """,
            SYSTEM_ID, code, title, level, parent_code, sector, is_leaf, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = $2",
        count, SYSTEM_ID,
    )
    print(f"  Ingested {count} ICD-10-AM codes")
    return count
