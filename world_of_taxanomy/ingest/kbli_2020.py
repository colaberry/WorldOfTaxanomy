"""KBLI 2020 (Indonesia) ingester.

Klasifikasi Baku Lapangan Usaha Indonesia (KBLI), edition 2020.
Published by BPS (Badan Pusat Statistik - Statistics Indonesia).
Aligned to ISIC Rev 4 with 21 sections (A-U).

Structure:
  Section (letter A-U) = level 0  (21 codes)
  Division (2-digit)   = level 1  -- not included in skeleton
  Group (3-digit)      = level 2  -- not included in skeleton
  Class (4-digit)      = level 3  -- not included in skeleton
  Subclass (5-digit)   = level 4  -- not included in skeleton

Source: https://www.bps.go.id/
"""

from typing import Optional

# -- Section structure (A-U) --------------------------------------------------
# KBLI 2020 is directly aligned to ISIC Rev 4 structure.

KBLI_SECTIONS = {
    "A": "Agriculture, Forestry and Fishing",
    "B": "Mining and Quarrying",
    "C": "Processing Industry (Manufacturing)",
    "D": "Electricity, Gas, Steam/Hot Water and Cold Air Procurement",
    "E": "Water Procurement, Waste and Recycling Management, Waste and Sewage Management",
    "F": "Construction",
    "G": "Wholesale and Retail Trade; Repair of Motor Vehicles and Motorcycles",
    "H": "Transportation and Warehousing",
    "I": "Provision of Accommodation and Provision of Eating and Drinking",
    "J": "Information and Communication",
    "K": "Financial and Insurance Activities",
    "L": "Real Estate Activities",
    "M": "Professional, Scientific and Technical Activities",
    "N": "Rental Activities, Employment, Travel Agency and Business Support Services",
    "O": "Government Administration, Defence and Compulsory Social Security",
    "P": "Education",
    "Q": "Human Health Activities and Social Activities",
    "R": "Arts, Entertainment and Recreation",
    "S": "Other Service Activities",
    "T": "Household Activities as Employers; Undifferentiated Goods and Service Production by Households",
    "U": "Activities of International and Extra-Territorial Organisations",
}

# -- KBLI -> ISIC Rev 4 section-level mapping ---------------------------------
# KBLI 2020 mirrors ISIC Rev 4 structure at section level.

KBLI_TO_ISIC_MAPPING = {
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


async def ingest_kbli_2020(conn) -> int:
    """Ingest KBLI 2020 (Indonesia) section-level codes.

    Args:
        conn: asyncpg connection

    Returns:
        Number of codes ingested.
    """
    await conn.execute("""
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ('kbli_2020', 'KBLI 2020',
                'Klasifikasi Baku Lapangan Usaha Indonesia 2020',
                'Indonesia', '2020',
                'BPS - Badan Pusat Statistik (Statistics Indonesia)',
                '#84CC16')
        ON CONFLICT (id) DO UPDATE SET node_count = 0
    """)

    count = 0
    seq = 0
    for code in sorted(KBLI_SECTIONS.keys()):
        title = KBLI_SECTIONS[code]
        seq += 1
        await conn.execute("""
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
            VALUES ('kbli_2020', $1, $2, 0, NULL, $1, TRUE, $3)
            ON CONFLICT (system_id, code) DO NOTHING
        """, code, title, seq)
        count += 1

    edge_count = 0
    for kbli_code, isic_targets in KBLI_TO_ISIC_MAPPING.items():
        for isic_code, match_type in isic_targets:
            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('kbli_2020', $1, 'isic_rev4', $2, $3)
                ON CONFLICT DO NOTHING
            """, kbli_code, isic_code, match_type)
            edge_count += 1

            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('isic_rev4', $1, 'kbli_2020', $2, $3)
                ON CONFLICT DO NOTHING
            """, isic_code, kbli_code, match_type)
            edge_count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'kbli_2020'",
        count,
    )

    print(f"  Ingested {count} KBLI 2020 section codes, {edge_count} equivalence edges")
    return count
