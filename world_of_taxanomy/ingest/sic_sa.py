"""SIC-SA (South Africa) ingester.

Standard Industrial Classification of all Economic Activities for South Africa,
version 5 (2012), based on ISIC Rev 4.
Published by Statistics South Africa (Stats SA).

Structure:
  Section (letter A-U) = level 0  (21 codes)
  Division (2-digit)   = level 1  -- not included in skeleton
  Group (3-digit)      = level 2  -- not included in skeleton
  Class (4-digit)      = level 3  -- not included in skeleton

Source: https://www.statssa.gov.za/
"""

from typing import Optional

# -- Section structure (A-U) --------------------------------------------------
# SIC-SA is structurally aligned to ISIC Rev 4 at section level.
# South Africa uses the same A-U letter codes with minor title adaptations.

SIC_SA_SECTIONS = {
    "A": "Agriculture, Forestry and Fishing",
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

# -- SIC-SA -> ISIC Rev 4 section-level mapping -------------------------------
# SIC-SA is a direct national adaptation of ISIC Rev 4; sections map exactly.

SIC_SA_TO_ISIC_MAPPING = {
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


async def ingest_sic_sa(conn) -> int:
    """Ingest SIC-SA (South Africa) section-level codes.

    Args:
        conn: asyncpg connection

    Returns:
        Number of codes ingested.
    """
    await conn.execute("""
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ('sic_sa', 'SIC-SA',
                'Standard Industrial Classification of all Economic Activities for South Africa',
                'South Africa', 'Version 5 (2012)',
                'Statistics South Africa (Stats SA)',
                '#F59E0B')
        ON CONFLICT (id) DO UPDATE SET node_count = 0
    """)

    count = 0
    seq = 0
    for code in sorted(SIC_SA_SECTIONS.keys()):
        title = SIC_SA_SECTIONS[code]
        seq += 1
        await conn.execute("""
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
            VALUES ('sic_sa', $1, $2, 0, NULL, $1, TRUE, $3)
            ON CONFLICT (system_id, code) DO NOTHING
        """, code, title, seq)
        count += 1

    edge_count = 0
    for sa_code, isic_targets in SIC_SA_TO_ISIC_MAPPING.items():
        for isic_code, match_type in isic_targets:
            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('sic_sa', $1, 'isic_rev4', $2, $3)
                ON CONFLICT DO NOTHING
            """, sa_code, isic_code, match_type)
            edge_count += 1

            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('isic_rev4', $1, 'sic_sa', $2, $3)
                ON CONFLICT DO NOTHING
            """, isic_code, sa_code, match_type)
            edge_count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'sic_sa'",
        count,
    )

    print(f"  Ingested {count} SIC-SA section codes, {edge_count} equivalence edges")
    return count
