"""Personal Protective Equipment (PPE) Category Types domain taxonomy ingester.

Classifies personal protective equipment (PPE) by protection category.
Orthogonal to incident type and safety management system.
Aligned with OSHA 29 CFR 1910.132-138 PPE standards, ANSI/ISEA standards,
and NIOSH PPE classification framework.
Used by EHS managers, PPE procurement teams, industrial safety distributors,
and compliance auditors conducting PPE hazard assessments.

Code prefix: dwkfppe_
System ID: domain_workforce_ppe
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
WORKFORCE_PPE_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("dwkfppe_head", "Head and Scalp Protection", 1, None),
    ("dwkfppe_head_hardhat", "Hard hats: Type I/II, Class E/G/C (ANSI Z89.1)", 2, "dwkfppe_head"),
    ("dwkfppe_head_bump", "Bump caps: lightweight for low-impact head bump hazards", 2, "dwkfppe_head"),
    ("dwkfppe_eye", "Eye and Face Protection", 1, None),
    ("dwkfppe_eye_safetyglasses", "Safety glasses: ANSI Z87.1 rated impact-resistant lenses", 2, "dwkfppe_eye"),
    ("dwkfppe_eye_goggles", "Safety goggles: splash, chemical, dust and impact", 2, "dwkfppe_eye"),
    ("dwkfppe_eye_faceshield", "Face shields: splash, grinding, welding (ANSI Z87.1)", 2, "dwkfppe_eye"),
    ("dwkfppe_eye_welding", "Welding helmets and auto-darkening filters (ANSI Z87.1, DIN shade)", 2, "dwkfppe_eye"),
    ("dwkfppe_hearing", "Hearing Protection", 1, None),
    ("dwkfppe_hearing_earplug", "Disposable and reusable earplugs (NRR-rated foam, banded)", 2, "dwkfppe_hearing"),
    ("dwkfppe_hearing_earmuff", "Earmuffs: passive and electronic hearing protection (3M Peltor)", 2, "dwkfppe_hearing"),
    ("dwkfppe_respiratory", "Respiratory Protection", 1, None),
    ("dwkfppe_respiratory_filtering", "Air-purifying respirators (N95, P100, PAPR - NIOSH 42 CFR 84)", 2, "dwkfppe_respiratory"),
    ("dwkfppe_respiratory_supplied", "Supplied-air respirators (SCBA, SAR - OSHA 1910.134)", 2, "dwkfppe_respiratory"),
    ("dwkfppe_hand", "Hand and Arm Protection", 1, None),
    ("dwkfppe_hand_cut", "Cut-resistant gloves (ANSI/ISEA 105 cut level A1-A9)", 2, "dwkfppe_hand"),
    ("dwkfppe_hand_chemical", "Chemical-resistant gloves (nitrile, neoprene, butyl rubber)", 2, "dwkfppe_hand"),
    ("dwkfppe_hand_thermal", "Thermal and heat-resistant gloves for welding, foundry", 2, "dwkfppe_hand"),
    ("dwkfppe_foot", "Foot and Leg Protection", 1, None),
    ("dwkfppe_foot_safetytoe", "Steel-toe / composite-toe footwear (ASTM F2413)", 2, "dwkfppe_foot"),
    ("dwkfppe_foot_slip", "Slip-resistant and EH-rated footwear", 2, "dwkfppe_foot"),
    ("dwkfppe_fall", "Fall Protection and Body Harnesses", 1, None),
    ("dwkfppe_fall_harness", "Full-body harness and personal fall arrest systems (ANSI Z359)", 2, "dwkfppe_fall"),
    ("dwkfppe_fall_lanyard", "Shock-absorbing lanyards and self-retracting lifelines (SRL)", 2, "dwkfppe_fall"),
]

_DOMAIN_ROW = (
    "domain_workforce_ppe",
    "Personal Protective Equipment (PPE) Category Types",
    "OSHA-aligned personal protective equipment (PPE) category and type classification",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['23', '31', '32', '33', '48', '3392']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_workforce_ppe(conn) -> int:
    """Ingest Personal Protective Equipment (PPE) Category Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_workforce_ppe'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_workforce_ppe",
        "Personal Protective Equipment (PPE) Category Types",
        "OSHA-aligned personal protective equipment (PPE) category and type classification",
        "1.0",
        "United States",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in WORKFORCE_PPE_NODES if parent is not None}

    rows = [
        (
            "domain_workforce_ppe",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in WORKFORCE_PPE_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(WORKFORCE_PPE_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_workforce_ppe'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_workforce_ppe'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_workforce_ppe", "primary") for code in naics_codes],
        )

    return count
