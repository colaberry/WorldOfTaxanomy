"""Singapore Standard Industrial Classification 2020 ingester.

Source: Department of Statistics Singapore (SingStat). Hand-coded from public standard.
Aligned with ISIC Rev 4. Crosswalk edges to isic_rev4 created at section level.
"""
from typing import Optional, List, Tuple

SECTIONS = {
    "A": "Agriculture, Forestry and Fishing",
    "B": "Mining and Quarrying",
    "C": "Manufacturing",
    "D": "Electricity, Gas, Steam and Air Conditioning Supply",
    "E": "Water Supply, Sewerage, Waste Management and Remediation",
    "F": "Construction",
    "G": "Wholesale and Retail Trade",
    "H": "Transportation and Storage",
    "I": "Accommodation and Food Service Activities",
    "J": "Information and Communication",
    "K": "Financial and Insurance Activities",
    "L": "Real Estate Activities",
    "M": "Professional, Scientific and Technical Activities",
    "N": "Administrative and Support Service Activities",
    "O": "Public Administration and Defence",
    "P": "Education",
    "Q": "Human Health and Social Work Activities",
    "R": "Arts, Entertainment and Recreation",
    "S": "Other Service Activities (incl. community and personal services)",
    "T": "Activities of Households as Employers",
    "U": "Activities of Extraterritorial Organisations",
}

SECTION_TO_ISIC = {
    "A": "A",
    "B": "B",
    "C": "C",
    "D": "D",
    "E": "E",
    "F": "F",
    "G": "G",
    "H": "H",
    "I": "I",
    "J": "J",
    "K": "K",
    "L": "L",
    "M": "M",
    "N": "N",
    "O": "O",
    "P": "P",
    "Q": "Q",
    "R": "R",
    "S": "S",
    "T": "T",
    "U": "U",
}

async def ingest_ssic_2020(conn) -> int:
    """Ingest SSIC 2020 (Singapore) into classification_system and classification_node.
    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, region, version, authority, tint_color)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "ssic_2020", "SSIC 2020", "Singapore Standard Industrial Classification 2020",
        "Singapore", "2020", "Department of Statistics Singapore (SingStat)", "#F97316",
    )

    count = 0
    for seq, (code, title) in enumerate(SECTIONS.items(), 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, 0, NULL, $2, TRUE, $4)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "ssic_2020", code, title, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'ssic_2020'",
        count,
    )

    # Equivalence edges to ISIC Rev 4 (section level)
    edge_count = 0
    for sec_code, isic_code in SECTION_TO_ISIC.items():
        await conn.execute(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ($1, $2, 'isic_rev4', $3, 'broad')
               ON CONFLICT DO NOTHING""",
            "ssic_2020", sec_code, isic_code,
        )
        await conn.execute(
            """INSERT INTO equivalence
                   (source_system, source_code, target_system, target_code, match_type)
               VALUES ('isic_rev4', $1, $2, $3, 'broad')
               ON CONFLICT DO NOTHING""",
            isic_code, "ssic_2020", sec_code,
        )
        edge_count += 1

    print(f"  {count} SSIC 2020 nodes, {edge_count * 2} equivalence edges")
    return count
