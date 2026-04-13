"""OKVED-2 (Russia) ingester.

Obscherossiysky Klassifikator Vidov Ekonomicheskoy Deyatelnosti, edition 2 (OK 029-2014).
Published by Rosstandart (Federal Agency on Technical Regulation and Metrology).
Based on NACE Rev 2 / ISIC Rev 4 structure with 21 sections (A-U).

Structure:
  Section (letter A-U) = level 0  (21 codes)
  Division (2-digit)   = level 1  -- not included in skeleton
  Group (3-digit)      = level 2  -- not included in skeleton
  Class (4-digit)      = level 3  -- not included in skeleton

Source: https://www.consultant.ru/document/cons_doc_LAW_163467/
"""

from typing import Optional

# -- Section structure (A-U) --------------------------------------------------
# OKVED-2 directly mirrors NACE Rev 2 / ISIC Rev 4 at section level.

OKVED2_SECTIONS = {
    "A": "Agriculture, Forestry, Hunting and Fishing",
    "B": "Mining and Quarrying",
    "C": "Manufacturing",
    "D": "Electricity, Gas, Steam and Air Conditioning Supply",
    "E": "Water Supply; Sewerage, Waste Management and Remediation Activities",
    "F": "Construction",
    "G": "Wholesale and Retail Trade; Repair of Motor Vehicles and Motorcycles",
    "H": "Transportation and Storage",
    "I": "Accommodation and Food Service Activities",
    "J": "Information and Communication",
    "K": "Financial and Insurance Activities",
    "L": "Real Estate Activities",
    "M": "Professional, Scientific and Technical Activities",
    "N": "Administrative and Support Service Activities",
    "O": "Public Administration and Defence; Compulsory Social Security",
    "P": "Education",
    "Q": "Human Health and Social Work Activities",
    "R": "Arts, Entertainment and Recreation",
    "S": "Other Service Activities",
    "T": "Activities of Households as Employers; Undifferentiated Goods and Services",
    "U": "Activities of Extraterritorial Organisations and Bodies",
}

# -- OKVED-2 -> ISIC Rev 4 section-level mapping ------------------------------
# OKVED-2 is a direct adaptation of NACE Rev 2 which mirrors ISIC Rev 4.
# All sections map exactly.

OKVED2_TO_ISIC_MAPPING = {
    "A": [("A", "exact")],
    "B": [("B", "exact")],
    "C": [("C", "exact")],
    "D": [("D", "exact")],
    "E": [("E", "exact")],
    "F": [("F", "exact")],
    "G": [("G", "exact")],
    "H": [("H", "exact")],
    "I": [("I", "exact")],
    "J": [("J", "exact")],
    "K": [("K", "exact")],
    "L": [("L", "exact")],
    "M": [("M", "exact")],
    "N": [("N", "exact")],
    "O": [("O", "exact")],
    "P": [("P", "exact")],
    "Q": [("Q", "exact")],
    "R": [("R", "exact")],
    "S": [("S", "exact")],
    "T": [("T", "exact")],
    "U": [("U", "exact")],
}

# -- Main ingestion ------------------------------------------------------------


async def ingest_okved_2(conn) -> int:
    """Ingest OKVED-2 (Russia) section-level codes.

    Inserts 21 section codes (A-U) and creates ISIC Rev 4 equivalence edges.

    Args:
        conn: asyncpg connection

    Returns:
        Number of codes ingested.
    """
    await conn.execute("""
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ('okved_2', 'OKVED-2',
                'All-Russian Classifier of Economic Activities, Edition 2 (OK 029-2014)',
                'Russia', '2014 (OK 029-2014)',
                'Rosstandart - Federal Agency on Technical Regulation and Metrology',
                '#F97316')
        ON CONFLICT (id) DO UPDATE SET node_count = 0
    """)

    count = 0
    seq = 0
    for code in sorted(OKVED2_SECTIONS.keys()):
        title = OKVED2_SECTIONS[code]
        seq += 1
        await conn.execute("""
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
            VALUES ('okved_2', $1, $2, 0, NULL, $1, TRUE, $3)
            ON CONFLICT (system_id, code) DO NOTHING
        """, code, title, seq)
        count += 1

    edge_count = 0
    for okved_code, isic_targets in OKVED2_TO_ISIC_MAPPING.items():
        for isic_code, match_type in isic_targets:
            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('okved_2', $1, 'isic_rev4', $2, $3)
                ON CONFLICT DO NOTHING
            """, okved_code, isic_code, match_type)
            edge_count += 1

            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('isic_rev4', $1, 'okved_2', $2, $3)
                ON CONFLICT DO NOTHING
            """, isic_code, okved_code, match_type)
            edge_count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'okved_2'",
        count,
    )

    print(f"  Ingested {count} OKVED-2 section codes, {edge_count} equivalence edges")
    return count
