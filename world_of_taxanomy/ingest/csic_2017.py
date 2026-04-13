"""CSIC 2017 (China) ingester.

Chinese Standard Industrial Classification (GB/T 4754-2017).
Published by the National Bureau of Statistics of China (NBS).
Aligned to ISIC Rev 4.

Structure:
  Section (letter A-T) = level 0  (20 codes - China uses A-T, no U)
  Division (2-digit)   = level 1  -- not included in skeleton
  Group (3-digit)      = level 2  -- not included in skeleton
  Class (4-digit)      = level 3  -- not included in skeleton

Source: https://www.stats.gov.cn/
"""

from typing import Optional

# -- Section structure (A-T) --------------------------------------------------
# China's CSIC uses 20 sections (A-T), closely mirroring ISIC Rev 4.
# Note: CSIC omits ISIC section U (Extra-territorial organisations).

CSIC_SECTIONS = {
    "A": "Agriculture, Forestry, Animal Husbandry and Fishery",
    "B": "Mining",
    "C": "Manufacturing",
    "D": "Production and Supply of Electricity, Heat Power, Gas and Water",
    "E": "Construction",
    "F": "Wholesale and Retail Trade",
    "G": "Transportation, Storage and Post",
    "H": "Accommodation and Catering",
    "I": "Information Transmission, Software and Information Technology Services",
    "J": "Financial Intermediation",
    "K": "Real Estate",
    "L": "Leasing and Business Services",
    "M": "Scientific Research and Technical Services",
    "N": "Water Conservancy, Environment and Public Facilities Management",
    "O": "Residential and Other Services",
    "P": "Education",
    "Q": "Health and Social Work",
    "R": "Culture, Sports and Entertainment",
    "S": "Public Administration, Social Security and Social Organisation",
    "T": "International Organisations",
}

# -- CSIC -> ISIC Rev 4 section-level mapping ---------------------------------

CSIC_TO_ISIC_MAPPING = {
    "A": [("A", "broad")],   # Agriculture/Forestry/Fishery -> ISIC A
    "B": [("B", "broad")],   # Mining -> ISIC B
    "C": [("C", "exact")],   # Manufacturing -> ISIC C
    "D": [("D", "broad"), ("E", "broad")],  # Electricity + Water -> ISIC D + E
    "E": [("F", "exact")],   # Construction -> ISIC F
    "F": [("G", "exact")],   # Wholesale and Retail -> ISIC G
    "G": [("H", "broad")],   # Transport/Storage/Post -> ISIC H
    "H": [("I", "exact")],   # Accommodation and Catering -> ISIC I
    "I": [("J", "broad")],   # ICT Services -> ISIC J
    "J": [("K", "broad")],   # Financial Intermediation -> ISIC K
    "K": [("L", "exact")],   # Real Estate -> ISIC L
    "L": [("N", "broad")],   # Leasing and Business -> ISIC N
    "M": [("M", "broad")],   # Scientific Research -> ISIC M
    "N": [("E", "broad")],   # Water/Environment -> ISIC E (partial)
    "O": [("S", "broad")],   # Residential Services -> ISIC S
    "P": [("P", "exact")],   # Education -> ISIC P
    "Q": [("Q", "exact")],   # Health -> ISIC Q
    "R": [("R", "broad")],   # Culture/Sports -> ISIC R
    "S": [("O", "broad")],   # Public Administration -> ISIC O
    "T": [("U", "broad")],   # International Organisations -> ISIC U
}

# -- Main ingestion ------------------------------------------------------------


async def ingest_csic_2017(conn) -> int:
    """Ingest CSIC 2017 (China) section-level codes.

    Args:
        conn: asyncpg connection

    Returns:
        Number of codes ingested.
    """
    await conn.execute("""
        INSERT INTO classification_system
            (id, name, full_name, region, version, authority, tint_color)
        VALUES ('csic_2017', 'CSIC 2017',
                'Chinese Standard Industrial Classification (GB/T 4754-2017)',
                'China', '2017 (GB/T 4754-2017)',
                'National Bureau of Statistics of China',
                '#EF4444')
        ON CONFLICT (id) DO UPDATE SET node_count = 0
    """)

    count = 0
    seq = 0
    for code in sorted(CSIC_SECTIONS.keys()):
        title = CSIC_SECTIONS[code]
        seq += 1
        await conn.execute("""
            INSERT INTO classification_node
                (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
            VALUES ('csic_2017', $1, $2, 0, NULL, $1, TRUE, $3)
            ON CONFLICT (system_id, code) DO NOTHING
        """, code, title, seq)
        count += 1

    edge_count = 0
    for csic_code, isic_targets in CSIC_TO_ISIC_MAPPING.items():
        for isic_code, match_type in isic_targets:
            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('csic_2017', $1, 'isic_rev4', $2, $3)
                ON CONFLICT DO NOTHING
            """, csic_code, isic_code, match_type)
            edge_count += 1

            await conn.execute("""
                INSERT INTO equivalence
                    (source_system, source_code, target_system, target_code, match_type)
                VALUES ('isic_rev4', $1, 'csic_2017', $2, $3)
                ON CONFLICT DO NOTHING
            """, isic_code, csic_code, match_type)
            edge_count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'csic_2017'",
        count,
    )

    print(f"  Ingested {count} CSIC 2017 section codes, {edge_count} equivalence edges")
    return count
